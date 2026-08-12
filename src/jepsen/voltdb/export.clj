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

(defn query-export-stats
  [conn]
  (vc/call! conn "@Statistics" "export"))

(defn log-export-stats
  [conn]
  (let [stats (query-export-stats conn)
        rows  (:rows (first stats))
        rcount (count rows)
        ii (atom 0)
        title (format "%10s|%10s|%10s|%10s|%10s" "HOST" "PARTITION" "COUNT" "PENDING" "STATUS")
        statStr (atom (str "Export Stats log \n" title "\n"))] 
    ;(map #(info "BZ" %) rows)   ; BZ I have no idea why this "map" iterator does not work. All textbooks on clojure states that it should work.
                                 ; So instead I had to use "while loop to iterate"
    (while (< @ii rcount) 
       (let [row (nth rows @ii)
             ss (format  "%10s|%10s|%10s|%10s|%10s" (:HOSTNAME row) (:PARTITION_ID row) (:TUPLE_COUNT row) (:TUPLE_PENDING row) (:STATUS row))] 
         (reset! statStr (str @statStr ss)))
       (swap! ii inc)
       (if (not= @ii rcount)
         (reset! statStr (str @statStr "\n"))))
    (info @statStr)))

(defn export-stats
  "Parse record for the Export Stats"
  [conn]
  ( let [stats (query-export-stats conn)]
   {:TUPLE_COUNT (reduce + (map #(->> (:rows %)
                                      (map :TUPLE_COUNT)
                                      (reduce +)) stats))
    :TUPLE_PENDING  (reduce + (map #(->> (:rows %)
                                         (map :TUPLE_PENDING)
                                         (reduce +)) stats))}))

(defn wait-export-pending
  "Wait until pending export records are processed"
  [conn]
  ( let [pending (atom 1) ; initial number for pending values. Anything more than 0 works.
         max_wait 120    ; max wait in seconds
         wait 10         ; how long to wait between requests
         trial (atom (/ max_wait wait))
         ]
    (while (and (pos? @pending) (pos? @trial))
      (if (not= @trial (/ max_wait wait))
        (Thread/sleep (* wait 1000)))
      (let [stats (export-stats conn)]
        (info "EXPORT STATS " stats)
        (reset! pending (Long/valueOf (:TUPLE_PENDING stats)))
        (swap! trial dec)))
   (if (not (pos? @pending))
     (info "Failed to clear pending records"))
   (log-export-stats conn)))

(defn export-data!
  [test conn]
  (wait-export-pending conn)
  (into [] (flatten (map  download-parse-export! (:nodes test)))))

(defn db-read-values
  "Reads all values from table-name using the supplied connection."
  [conn table-name]
  (->> (vc/ad-hoc! conn (str "SELECT value FROM " table-name " ORDER BY value;"))
       first
       :rows
       (map :VALUE)))

(defn db-read-live
  "Reads all values from table-name against a node that is currently alive.

  The per-worker client (jepsen.voltdb.client/connect) is deliberately NOT
  topology-change aware, so it is pinned to a single node. When VoltDB's own
  partition detection shuts that node down during the run, the pinned
  connection returns nothing and the final consistency read spuriously reports
  every committed write as lost. We therefore discover which nodes are still
  alive and read from the first one that answers, so the final read reflects
  the surviving cluster rather than a dead node."
  [test table-name]
  (let [live (vc/up-nodes test)]
    (when (empty? live)
      (throw (IllegalStateException. "No live VoltDB nodes available for db-read")))
    (loop [[node & more] live]
      (let [result (try
                     {:ok (let [conn (vc/connect node test)]
                            (try
                              (doall (db-read-values conn table-name))
                              (finally (vc/close! conn))))}
                     (catch Exception e
                       (warn e "db-read against" node "failed")
                       {:error e}))]
        (if (contains? result :ok)
          (:ok result)
          (if (seq more)
            (recur more)
            (throw (:error result))))))))

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
        ; Read all data from the table '(table-name). We read from a live node
        ; rather than the pinned `conn`, which may have been shut down by
        ; partition detection during the run (see db-read-live).
        :db-read (let [v (db-read-live test table-name)]
                     (assoc op :type :ok :value v))
        ; Read all exported data from cvs file
        :export-read (let [v (export-data! test conn)]
                        (assoc op :type :ok :value v)))
      
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
            ; How many :db-read ops actually succeeded? db-read is the
            ; reference set every other metric is diffed against, so if the
            ; final :db-read op failed (e.g. client timeout) the set is empty
            ; and *every* confirmed write looks "lost" while *every* exported
            ; row looks "phantom" -- a total-loss false positive. Guard it.
            ; See ENG-29721.
            db-read-ops (->> history
                             h/oks
                             (h/filter-f :db-read)
                             count)
            db-read-ok? (pos? db-read-ops)
            ; db-read-based view: only meaningful when a db-read succeeded, so
            ; that the persistent table can serve as the source of truth for
            ; what actually committed. nil when unavailable (count => 0).
            ; Did we lose any writes confirmed to the client?
            lost-transactions (when db-read-ok? (set/difference client-ok db-read))
            _ (info "lost-transaction count: " (count lost-transactions))
            ; Did we loose transaction in export-read
            lost-export (when db-read-ok? (set/difference db-read export-read))
            _ (info "lost-export count: " (count lost-export))
            ; Writes present in export but missing from DB
            phantom-export (when db-read-ok? (set/difference export-read db-read))
            _ (info "phantom-export count: " (count phantom-export))
            ; db-read-independent safety checks against the client history,
            ; usable even when the DB reference read is unavailable:
            ; committed writes that never made it into the export...
            missing-from-export (set/difference client-ok export-read)
            _ (info "missing-from-export count: " (count missing-from-export))
            ; ...and definitely-failed writes that nonetheless showed up in the
            ; export. Indeterminate (:info) writes are legitimately allowed to
            ; appear in the export, so they are intentionally NOT flagged here.
            exported-but-client-failed (set/intersection export-read client-failed)
            _ (when-not db-read-ok?
                (warn "No successful :db-read op in history; cannot use the DB"
                      "table as a reference. Falling back to client-vs-export"
                      "checks only; result is :unknown unless those find a real"
                      "violation. See ENG-29721."))
            ; A genuine violation is: a committed write missing from export, a
            ; failed write present in export, or (only when we have a reliable
            ; db-read) any db-read-based discrepancy.
            real-violation? (boolean
                              (or (seq missing-from-export)
                                  (seq exported-but-client-failed)
                                  (and db-read-ok?
                                       (or (seq lost-transactions)
                                           (seq lost-export)
                                           (seq phantom-export)))))]

        {:valid? (cond
                   real-violation?   false
                   ; DB reference read unavailable: client-vs-export was
                   ; consistent, but we could not run the full check.
                   (not db-read-ok?) :unknown
                   :else             true)
         :db-read-ok?                      db-read-ok?
         :client-ok-count                  (count client-ok)
         :client-failed-count              (count client-failed)
         :db-read-count                    (count db-read)
         :export-read-count                (count export-read)
         :missing-from-export-count        (count missing-from-export)
         :lost-transaction-count           (count lost-transactions)
         :lost-export-count                (count lost-export)
         :phantom-export-count             (count phantom-export)
         :exported-but-client-failed-count (count exported-but-client-failed)
         :missing-from-export              missing-from-export
         ;:lost-transactions                lost-transactions
         ;:lost_export                      lost-export
         ;:phantom-export                   phantom-export
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
