(ns jepsen.voltdb.list-append
  "Elle list-append workload (jepsen.tests.cycle.append), the standard
  transactional test in every published Jepsen analysis since 2020.
  Transactions are vectors of [:r k nil] and [:append k v] micro-ops over
  integer keys; each key names a list. Elle infers the version order of each
  list from the appends it sees and checks the history for cycles under the
  requested consistency models, naming the anomaly (G0, G1a/b/c, G-single,
  G2-item, realtime violations). Runs as one multi-partition procedure, so it
  exercises the MP coordinator path that single and multi barely touch."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [control :as c]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb.client :as vc]))

(def table "lists")

(defn read-only?
  [txn]
  (every? #(= :r (first %)) txn))

(defn parse-list
  "Lists are stored as comma-separated longs. A key that was never appended to
  has no row, which we report as the empty list."
  [s]
  (if (or (nil? s) (str/blank? s))
    []
    (mapv #(Long/parseLong %) (str/split s #","))))

(defn apply-txn!
  [conn txn]
  (let [fs  (into-array String (map (comp name first) txn))
        ks  (long-array (map second txn))
        vs  (long-array (map #(or (nth % 2) -1) txn))
        res (vc/call! conn "ListAppendTxn" fs ks vs)]
    (mapv (fn [[f k _ :as mop] table]
            (case f
              :append mop
              :r      [f k (parse-list (-> table :rows first :VALUE))]))
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
                                   value  VARCHAR(65536) NOT NULL,
                                   PRIMARY KEY (id)
                                   );
                                   PARTITION TABLE " table " ON COLUMN id;"))
              ; No PARTITION clause: keys span partitions, so this is MP.
              (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                               jepsen.procedures.ListAppendTxn;")))
      (info node table "created")))

  (invoke! [this test op]
    (let [txn (:value op)]
      (vc/with-errors op (read-only? txn)
        (assoc op :type :ok, :value (apply-txn! conn txn)))))

  (teardown! [this test])

  (close! [this test]
    (vc/close! conn)))

(defn workload
  "Options are passed through to elle.list-append/gen and check: :key-count,
  :min-txn-length, :max-txn-length, :max-writes-per-key,
  :consistency-models."
  [opts]
  (assoc (append/test (select-keys opts [:key-count
                                         :min-txn-length
                                         :max-txn-length
                                         :max-writes-per-key
                                         :consistency-models]))
         :client (map->Client {:initialized? (promise)})))
