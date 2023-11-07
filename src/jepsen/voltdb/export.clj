(ns jepsen.voltdb.export
  "A workload for testing VoltDB's export mechanism. We perform a series of
  write operations. Each write op performs a single transactional procedure
  call which inserts a series of values into both a VoltDB table and an
  exported stream.

    {:f :write, :values [3 4]}

  At the end of the test we read the table (so we know what VoltDB thinks
  happened).

    {:f :db-read, :values [1 2 3 4 ...]}

  ... and the exported data from the stream (so we know what was exported).

    {:f :export-read, :values [1 2 ...]}

  We then compare the two to make sure that records aren't lost, and spurious
  records don't appear in the export."
  (:require [clojure
             [pprint :refer [pprint]]
             [set          :as set]
             [string       :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [checker      :as checker]
             [client       :as client]
             [control      :as c]
             [generator    :as gen]
             [history      :as h]]
            [jepsen.control.util  :as cu]
            [jepsen.voltdb        :as voltdb]
            [jepsen.voltdb [client :as vc]]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(def tmp-local-file "/tmp/tmp-export")

(defn parse-export! [filename]
  (when (not (.exists (io/file filename)))
    (throw (Exception. (str "Local file " filename " does not exist."))))
  (with-open [reader (io/reader filename)]
    (let [data (csv/read-csv reader)]
      (into () (->> data      ; since scv/read-scv produces a lazy collection which dissappears as soon as 
                              ; the file is closed, we need to insert it into an empty before the function is over
                              ; list -> into()
                    (map #(last %))
                    (map #(Long/parseLong %)))))))

(defn download-parse-export!
  "Downloads export file from a remote node"
  [node]
  (locking download-parse-export!
    (c/on node
          (flatten
           (let [local tmp-local-file
                 res ()]
            (io/delete-file local true)
            (map (fn [remote]
                   (if (cu/exists? remote)
                     (do
                       (info "Downloading " remote " to " local "from node " node)
                       (try
                         (c/download remote local)
                         (catch java.io.IOException e
                           (if (= "Pipe closed" (.getMessage e))
                             (info remote "pipe closed")
                             (throw e))))
                       (let [d (parse-export! local)]
                         (info "Getting export data from" remote " to " local " is complete on node " node)
                         (io/delete-file local)
                         (into res d)))
                     (do (info "The file " remote "doesn't exist on node " node)
                         (into res ()))))
                 (jepsen.voltdb/list-export-files))))
            )))

(defn export-data!
  [test]
  (into [] (flatten (map  download-parse-export! (:nodes test))))
  )

(defn parse-stats-results
  "Parse record for the Stats of export"
  [stats]
  
  (let [ff (first stats)
        _ (info "BZ first : " ff)
        r (:rows stats)
        _ (info "BZ rows records : " r)
        tc (map :TUPLE_COUNT r)
        _ (info "BZ tuple count : " tc)
        total (reduce + tc)
        _ (info "BZ total count : " total)
        my_list (map (fn [records] (->> (:rows records)
                                        (map :TUPLE_COUNT)
                                        (reduce +))) stats)
        _ (info "BZ map reduced list : " my_list)]
     {:TUPLE_COUNT (map (fn [records](->> (:rows records)
                       (map :TUPLE_COUNT)
                       (reduce +))) stats)
     :TUPLE_PENDING  (->> (:rows stats)
                          (map :TUPLE_PENDING)
                          (reduce +))}))

(defrecord Client [table-name     ; The name of the table we write to
                   stream-name    ; The name of the stream we write to
                   target-name    ; The name of our export target
                   conn           ; Our VoltDB client connection
                   node           ; The node we're talking to
                   initialized?   ; Have we performed one-time initialization?
                   ]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (vc/connect node test)
           :node node))

  (setup! [_ test]
    (when (deliver initialized? true)
      (info node "Creating tables")
      (c/on node
            (vc/with-race-retry
              ; We test partitioned tables. We'll have an explicit
              ; partition column and send all our writes to one partition.
              ; The `value` column will actually store written values.
              ( if (:export-table test)
                (do
                  (voltdb/sql-cmd! (str "CREATE TABLE " table-name " EXPORT TO TARGET " target-name " on insert (
                                               part   INTEGER NOT NULL,
                                               value  BIGINT NOT NULL
                                               );
                                    PARTITION TABLE " table-name " ON COLUMN part;"))
                  (voltdb/sql-cmd! (str "CREATE PROCEDURE PARTITION ON TABLE " table-name
                                               " COLUMN part FROM CLASS jepsen.procedures.ExportWriteTable;")))
               
                (do
                  (voltdb/sql-cmd! (str "CREATE TABLE " table-name " (
                                               part   INTEGER NOT NULL,
                                               value  BIGINT NOT NULL
                                               );
                                    PARTITION TABLE " table-name " ON COLUMN part;"))
                  (voltdb/sql-cmd! (str "CREATE STREAM " stream-name " PARTITION ON COLUMN part
                                               EXPORT TO TARGET " target-name "(
                                               part INTEGER NOT NULL,
                                               value BIGINT NOT NULL
                                               );")) 
                  (voltdb/sql-cmd! (str "CREATE PROCEDURE PARTITION ON TABLE " table-name 
                                               " COLUMN part FROM CLASS jepsen.procedures.ExportWrite;"))))

            (info node "tables created")))))

  (invoke! [_ test op]
    (try
      (case (:f op)
        ; Write to a random partition
        :write (do (vc/call! conn (if (:export-table test)
                                    "ExportWriteTable"
                                    "ExportWrite")
                             (rand-int 1000)
                             (long-array (:value op)))
                   (assoc op :type :ok))
        ; Read all data from the table '(table-name)
        :db-read (let [v (->>
                          (vc/ad-hoc! conn (str "SELECT value FROM " table-name " ORDER BY value;"))
                               first
                               :rows
                               (map :VALUE))]
                     (assoc op :type :ok :value v))
        ; Read all exported data from cvs file
        :export-read (let [stats (vc/call! conn "@Statistics" "export" )
                           _ (info "BZ stats : " stats)
                           _ (info "BZ parse stats : " (parse-stats-results stats))
                           v (export-data! test)]
                        (assoc op :type :ok :value v))
        )
        (catch Exception e
              (assoc op :type type, :error op)
              (throw e))))

  (teardown! [_ test])

  (close! [_ test]
    (vc/close! conn)))

(defn rand-int-chunks
  "A lazy sequence of sequential integers grouped into randomly sized small
  vectors like [1 2] [3 4 5 6] [7] ..."
  ([opts] (rand-int-chunks opts 0))
  ([opts start]
   (lazy-seq
     (let [chunk-size (inc (rand-int (:transactionsize opts 16)))
           end        (+ start chunk-size)
           chunk      (vec (range start end))]
       (cons chunk (rand-int-chunks opts end))))))

(defn checker
  "Basic safety checker. Just checks for set inclusion, not order or
  duplicates."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; What elements were acknowledged to the client?
            client-ok (->> history
                           h/oks
                           (h/filter-f :write)
                           (mapcat :value)
                           (into (sorted-set)))
            _ (info "client-ok count: " (count client-ok))
            ; Which elements did we tell the client had failed?
            client-failed (->> history
                               h/fails
                               (h/filter-f :write)
                               (mapcat :value)
                               (into (sorted-set)))
            _ (info "client-failed count: " (count client-failed))
            ; Which elements showed up in the DB reads?
            db-read (->> history
                         h/oks
                         (h/filter-f :db-read)
                         (mapcat :value)
                         (into (sorted-set)))
            _ (info "db-read values count " (count db-read))
            ; Which elements showed up in the export?
            export-read (->> history
                             h/oks
                             (h/filter-f :export-read)
                             (mapcat :value)
                             (into (sorted-set)))
            _ (info "export-read values count: " (count export-read))
            ; Did we lose any writes confirmed to the client?
            lost-transactions (set/difference client-ok db-read)
            _ (info "lost-transaction count: " (count lost-transactions))
            ; Did we loose transaction in export-read
            lost-export (set/difference db-read export-read)
            _ (info "lost-export count: " (count lost-export))
            ; Writes present in export but missing from DB
            phantom-export (set/difference export-read db-read)
            _ (info "phantom-export count: " (count phantom-export))
            ; Writes present in the export but the client thought they failed 
            exported-but-client-failed (set/intersection export-read client-failed) ] 

        {:valid? (and (empty? lost-transactions)
                      (empty? phantom-export) 
                      (empty? lost-export))
         :client-ok-count                  (count client-ok)
         :client-failed-count              (count client-failed)
         :db-read-count                    (count db-read)
         :export-read-count                (count export-read)
         :lost-transaction-count           (count lost-transactions)
         :lost-export-count                (count lost-export)
         :phantom-export-count             (count phantom-export)
         :exported-but-client-failed-count (count exported-but-client-failed)
         ;:lost-transactions                lost-transactions
         ;:lost_export                      lost-export
         :phantom-export                   phantom-export
         ;:exported-but-client-failed       exported-but-client-failed
         }))))

(defn workload
  "Takes CLI options and constructs a workload map."
  [opts]
  {:client (map->Client {:table-name  "export_table"
                         :stream-name "export_stream"
                         :target-name "export_target"
                         :initialized? (promise)})
   :generator       (->> (rand-int-chunks opts)
                         (map (fn [chunk]
                                {:f :write, :value chunk})))
   :final-generator (gen/phases
                      [(gen/until-ok {:f :db-read})
                       (gen/until-ok {:f :export-read})])
   :checker (checker)})
