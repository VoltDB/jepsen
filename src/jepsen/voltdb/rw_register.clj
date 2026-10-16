(ns jepsen.voltdb.rw-register
  "Elle read/write register workload (jepsen.tests.cycle.wr). Transactions
  are vectors of [:r k nil] and [:w k v] micro-ops over integer keys, run as a
  single multi-partition stored procedure. Elle checks the resulting history
  for cycles under the requested consistency models (strict serializable by
  default) and names the anomaly it finds. The same client also serves the
  long-fork workload, whose transactions have the same shape."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [control :as c]]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.tests.long-fork :as long-fork]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb.client :as vc]))

(def table "rw_registers")

(defn read-only?
  "Is every micro-op in this transaction a read?"
  [txn]
  (every? #(= :r (first %)) txn))

(defn apply-txn!
  "Runs txn through RWRegisterTxn and fills in read values. A read of a key
  that was never written returns no rows, which we report as nil."
  [conn txn]
  (let [fs  (into-array String (map (comp name first) txn))
        ks  (long-array (map second txn))
        vs  (long-array (map #(or (nth % 2) -1) txn))
        res (vc/call! conn "RWRegisterTxn" fs ks vs)]
    (mapv (fn [[f k _ :as mop] table]
            (case f
              :w mop
              :r [f k (-> table :rows first :VALUE)]))
          txn
          res)))

(defrecord Client [conn node initialized?]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :conn (vc/connect node (select-keys test [:procedure-call-timeout
                                                     :connection-response-timeout]))))

  (setup! [this test]
    (when (deliver initialized? true)
      (info node "creating" table)
      (c/on node
            (vc/with-race-retry
              (voltdb/sql-cmd! (str "CREATE TABLE " table " (
                                   id     BIGINT NOT NULL,
                                   value  BIGINT NOT NULL,
                                   PRIMARY KEY (id)
                                   );
                                   PARTITION TABLE " table " ON COLUMN id;"))
              ; No PARTITION clause: keys span partitions, so this is MP.
              (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                               jepsen.procedures.RWRegisterTxn;")))
      (info node table "created")))

  (invoke! [this test op]
    (let [txn (:value op)]
      (vc/with-errors op (read-only? txn)
        (assoc op :type :ok, :value (apply-txn! conn txn)))))

  (teardown! [this test])

  (close! [this test]
    (vc/close! conn)))

(defn client
  []
  (map->Client {:initialized? (promise)}))

(defn workload
  "Elle rw-register workload. Options are passed through to
  elle.rw-register/gen and check: :key-count, :min-txn-length,
  :max-txn-length, :max-writes-per-key, :consistency-models."
  [opts]
  (assoc (wr/test (select-keys opts [:key-count
                                     :min-txn-length
                                     :max-txn-length
                                     :max-writes-per-key
                                     :consistency-models]))
         :client (client)))

(defn long-fork-workload
  "Long-fork workload (jepsen.tests.long-fork) over the same table and
  procedure: single writes of 1 to fresh keys, and reads of groups of keys,
  checked for the long-fork anomaly."
  [opts]
  (assoc (long-fork/workload (:long-fork-group-size opts 2))
         :client (client)))
