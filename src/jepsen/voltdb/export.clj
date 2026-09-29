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

(defn with-live-conn
  "Calls (f conn) with a connection to a node that is currently alive, and
  returns its result. `what` names the operation, for logging.

  The per-worker client (jepsen.voltdb.client/connect) is deliberately NOT
  topology-change aware, so it is pinned to a single node. When VoltDB's own
  partition detection shuts that node down during the run, the pinned
  connection is dead and the final reads fail, which the checker would
  otherwise see as every committed write being lost. We therefore discover
  which nodes are still alive and use the first one that answers, so the final
  reads reflect the surviving cluster rather than a dead node. See ENG-29692,
  ENG-30047."
  [test what f]
  (let [live (vc/up-nodes test)]
    (when (empty? live)
      (throw (IllegalStateException.
               (str "No live VoltDB nodes available for " what))))
    (loop [[node & more] live]
      (let [result (try
                     {:ok (let [conn (vc/connect node test)]
                            (try
                              (f conn)
                              (finally (vc/close! conn))))}
                     (catch Exception e
                       (warn e what "against" node "failed")
                       {:error e}))]
        (if (contains? result :ok)
          (:ok result)
          (if (seq more)
            (recur more)
            (throw (:error result))))))))

(defn export-data!
  "Waits for pending export to drain (asking a live node, not the pinned one),
  then downloads the export files from every node over SSH. The files are on
  disk, so a node whose VoltDB is down still contributes its files."
  [test]
  (with-live-conn test "export-read" wait-export-pending)
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
  See with-live-conn."
  [test table-name]
  (with-live-conn test "db-read"
    (fn [conn] (doall (db-read-values conn table-name)))))

(defn conn!
  "Returns this client's connection to its pinned node, (re)connecting if we
  don't have one yet. Returns nil, rather than throwing, if the node can't be
  reached."
  [{:keys [conn node]} test]
  (or @conn
      (try
        (reset! conn (vc/connect node test))
        (catch Exception e
          (warn "Could not connect to" node ":" (.getMessage e))
          nil))))

(defrecord Client [table-name     ; The name of the table we write to
                   stream-name    ; The name of the stream we write to
                   target-name    ; The name of our export target
                   conn           ; Atom of our VoltDB client connection, or nil
                   node           ; The node we're talking to
                   initialized?   ; Have we performed one-time initialization?
                   ]
  client/Client
  ; open! must not throw when the pinned node is down. If it does, Jepsen
  ; fails every op on this worker with [:no-client ...] before invoke! runs,
  ; including the final :db-read and :export-read, which don't need the
  ; pinned node at all. So we connect lazily in conn! instead. See ENG-30047.
  (open! [this test node]
    (let [this (assoc this :conn (atom nil) :node node)]
      (conn! this test)
      this))

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

  (invoke! [this test op]
    (case (:f op)
      ; Write to a random partition. With no connection the write was never
      ; sent, so it definitely failed.
      :write (if-let [c (conn! this test)]
               (do (vc/call! c (if (:export-table test)
                                 "ExportWriteTable"
                                 "ExportWrite")
                             (rand-int 1000)
                             (long-array (:value op)))
                   (assoc op :type :ok))
               (assoc op :type :fail, :error [:no-conn node]))
      ; Read all data from the table '(table-name). We read from a live node
      ; rather than the pinned `conn`, which may have been shut down by
      ; partition detection during the run (see with-live-conn).
      :db-read (let [v (db-read-live test table-name)]
                 (assoc op :type :ok :value v))
      ; Read all exported data from cvs file
      :export-read (let [v (export-data! test)]
                     (assoc op :type :ok :value v))))

  (teardown! [_ test])

  (close! [_ test]
    (when-let [c @conn]
      (vc/close! c))))

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
            db-read-ok? (->> history
                             h/oks
                             (h/filter-f :db-read)
                             seq
                             boolean)
            ; The same holds for :export-read, the reference set for everything
            ; export-side: a failed export read leaves an empty set, and every
            ; committed write looks "missing from export". See ENG-30047.
            ; This deliberately keys on the op succeeding, not on the set being
            ; non-empty: a successful read that finds nothing is real loss.
            export-read-ok? (->> history
                                 h/oks
                                 (h/filter-f :export-read)
                                 seq
                                 boolean)
            ; Each metric is only computed when every read it depends on
            ; succeeded; nil otherwise (count => 0).
            ; Did we lose any writes confirmed to the client?
            lost-transactions (when db-read-ok?
                                (set/difference client-ok db-read))
            _ (info "lost-transaction count: " (count lost-transactions))
            ; Did we loose transaction in export-read
            lost-export (when (and db-read-ok? export-read-ok?)
                          (set/difference db-read export-read))
            _ (info "lost-export count: " (count lost-export))
            ; Writes present in export but missing from DB
            phantom-export (when (and db-read-ok? export-read-ok?)
                             (set/difference export-read db-read))
            _ (info "phantom-export count: " (count phantom-export))
            ; db-read-independent safety checks against the client history,
            ; usable even when the DB reference read is unavailable:
            ; committed writes that never made it into the export...
            missing-from-export (when export-read-ok?
                                  (set/difference client-ok export-read))
            _ (info "missing-from-export count: " (count missing-from-export))
            ; ...and definitely-failed writes that nonetheless showed up in the
            ; export. Indeterminate (:info) writes are legitimately allowed to
            ; appear in the export, so they are intentionally NOT flagged here.
            exported-but-client-failed (when export-read-ok?
                                         (set/intersection export-read
                                                           client-failed))
            ; Why we couldn't run the full check, if we couldn't.
            unknown-reasons (cond-> []
                              (not db-read-ok?)
                              (conj :no-successful-db-read)
                              (not export-read-ok?)
                              (conj :no-successful-export-read))
            _ (when (seq unknown-reasons)
                (warn "Final reads failed:" unknown-reasons "- skipping the"
                      "checks that depend on them; result is :unknown unless"
                      "the remaining checks find a real violation."
                      "See ENG-29721, ENG-30047."))
            ; A genuine violation is any discrepancy we were able to compute.
            real-violation? (boolean
                              (or (seq missing-from-export)
                                  (seq exported-but-client-failed)
                                  (seq lost-transactions)
                                  (seq lost-export)
                                  (seq phantom-export)))]

        {:valid? (cond
                   real-violation?        false
                   ; A reference read is unavailable: what we could check was
                   ; consistent, but we could not run the full check.
                   (seq unknown-reasons)  :unknown
                   :else                  true)
         :unknown-reasons                  unknown-reasons
         :db-read-ok?                      db-read-ok?
         :export-read-ok?                  export-read-ok?
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

(defn final-read
  "A generator for the final read op f, retried until it succeeds.

  A map on its own is a one-shot generator, so (gen/until-ok {:f f}) makes a
  single attempt; it needs gen/repeat to retry. A connection refused right
  after the nemesis stops is transient, so we retry up to 10 times, 10s apart.
  Pinned to one thread so attempts run one at a time. See ENG-30047."
  [f]
  (->> {:f f}
       gen/repeat
       (gen/delay 10)
       (gen/limit 10)
       gen/until-ok
       (gen/on-threads #{0})))

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
   :final-generator (gen/phases (final-read :db-read)
                                (final-read :export-read))
   :checker (checker)})
