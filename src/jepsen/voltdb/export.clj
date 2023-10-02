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

(defn parse-export! [filename]
  (when (not (.exists (io/file filename)))
    (throw (Exception. (str "Local file " filename " does not exist."))))
  (with-open [reader (io/reader filename)]
    (let [data (csv/read-csv reader)
          column-data  (map #(last %) data)]
      (into () column-data)))) 

(defn download-parse-export!
  "Downloads export file from a remote "
  [node]
  (locking download-parse-export!
    (c/on node
          (let [local "/tmp/bz_junk"
                _ (io/delete-file local true)
                remote-files (jepsen.voltdb/list-export-files)
                ;remote-files (into () (cu/ls-full (str jepsen.voltdb/base-dir "/voltdbroot/" jepsen.voltdb/export-csv-dir)))
                remote (first remote-files)]
            (if (cu/exists? remote)
             (do
               (info "Downloading " remote " to " local "from node " node)
               (try
                 (c/download remote local)
                 (catch java.io.IOException e
                   (if (= "Pipe closed" (.getMessage e))
                     (info remote "pipe closed")
                     (throw e)))
                 )
               (let [d (parse-export! local)]
                 (info "BZ HERE 7 download of " remote " to " local " is complete on node " node " data: " d)
                 (io/delete-file local)
                 (into () d))
               )
             (do (info "The file " remote "doesn't exist on node " node)
                 (into ())))
          ))))

(defn export-data!
  [test]
  (into [] (flatten (map  download-parse-export! (:nodes test))))
  )

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
              ; I'm not exactly sure what to do here--we want to test
              ; partitioned tables, I think, so we'll have an explicit
              ; partition column and send all our writes to one partition.
              ;
              ; The `value` column will actually store written values.
              (voltdb/sql-cmd! (str
                "CREATE TABLE " table-name " (
                part   INTEGER NOT NULL,
                value  BIGINT NOT NULL
                );
                PARTITION TABLE " table-name " ON COLUMN part;

                CREATE STREAM " stream-name " PARTITION ON COLUMN part
                EXPORT TO TARGET export_target (
                  part INTEGER NOT NULL,
                  value BIGINT NOT NULL
                );"))
              (voltdb/sql-cmd!
                (str "CREATE PROCEDURE PARTITION ON TABLE " table-name 
                     " COLUMN part FROM CLASS jepsen.procedures.ExportWrite;"))
            (info node "tables created")))))

  (invoke! [_ test op]
    (try
      (case (:f op)
        ; Write to a random partition
        :write (do (vc/call! conn "ExportWrite"
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
        :export-read (let [v (export-data! test)]
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
  ([] (rand-int-chunks 0))
  ([start]
   (lazy-seq
     (let [chunk-size (inc (rand-int 16))
           end        (+ start chunk-size)
           chunk      (vec (range start end))]
       (cons chunk (rand-int-chunks end))))))

(defn checker
  "Basic safety checker. Just checks for set inclusion, not order or
  duplicates."
  []
  ; TODO: This is just a sketch; I haven't gotten to feed this actual results
  ; yet
  (reify checker/Checker
    (check [this test history opts]
      (let [; What elements were acknowledged to the client?
            client-ok (->> history
                           h/oks
                           (h/filter-f :write)
                           (mapcat :value)
                           (into (sorted-set)))
            ; Which elements did we tell the client had failed?
            client-failed (->> history
                               h/fails
                               (h/filter-f :write)
                               (mapcat :value)
                               (into (sorted-set)))
            ; Which elements showed up in the DB reads?
            db-read (->> history
                         h/oks
                         (h/filter-f :db-read)
                         (mapcat :value)
                         (into (sorted-set)))
            _ (info (str "BZ HERE db-read values: " db-read))
            ; Which elements showed up in the export? 
            export-read (->> history
                             h/oks
                             (h/filter-f :export-read)
                             (mapcat :value)
                             (into (sorted-set)))
            ; Did we lose any writes confirmed to the client?
            lost-transactions          (set/difference client-ok db-read)
            ; Did we loo
            ;lost-export (set/difference db-read export-read)
            lost-export (set/difference client-ok export-read)
            ; Writes present in export but missing from DB
            phantom-export (set/difference export-read db-read)
            ; Writes present in the export but the client thought they failed 
            exported-but-client-failed (set/intersection export-read client-failed) ] 
                                                                                    
        {:valid? (and ;(empty? lost-transactions)
                      ;(empty? phantom-export)
                      (empty? lost-export))
         :client-ok-count                  (count client-ok)
         :client-failed-count              (count client-failed)
         :db-read-count                    (count db-read)
         :export-read-count                (count export-read)
         :lost-transaction-count           (count lost-transactions)
         :lost-export-count                (count lost-export)
         :phantom-export-count             (count phantom-export)
         :exported-but-client-failed-count (count exported-but-client-failed) 
         :lost-transactions                lost-transactions
         :exported-but-client-failed       exported-but-client-failed
         :db-unseen                        lost-export
         :phantom-export                   phantom-export }))))

(defn workload
  "Takes CLI options and constructs a workload map."
  [opts]
  {:client (map->Client {:table-name  "export_table"
                         :stream-name "export_stream"
                         :target-name "export_target"
                         :initialized? (promise)})
   :generator       (->> (rand-int-chunks)
                         (map (fn [chunk]
                                {:f :write, :value chunk})))
   :final-generator (gen/once
                      [(gen/until-ok {:f :db-read})
                       (gen/until-ok {:f :export-read})])
   :checker (checker)})
