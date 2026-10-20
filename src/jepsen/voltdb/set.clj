(ns jepsen.voltdb.set
  "Set workload: every client inserts unique integers into a partitioned
  table; reads return the whole table. jepsen.checker/set-full verifies that
  every acknowledged insert is eventually present and nothing unacknowledged
  appears, and with :linearizable? true flags reads that miss an element
  acknowledged before the read began. Generic lost-write detection with a far
  larger history than dirty-read."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb.client :as vc]))

(def table "sets")

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
                                   id  BIGINT NOT NULL,
                                   PRIMARY KEY (id)
                                   );
                                   PARTITION TABLE " table " ON COLUMN id;"))))
      (info node table "created")))

  (invoke! [this test op]
    (case (:f op)
      ; The auto-generated insert procedure is single-partition on id.
      :add  (vc/with-errors op false
              (vc/call! conn "SETS.insert" (:value op))
              (assoc op :type :ok))
      ; A multi-partition read of the whole table.
      :read (vc/with-errors op true
              (->> (vc/ad-hoc! conn (str "SELECT id FROM " table ";"))
                   first
                   :rows
                   (map :ID)
                   (into (sorted-set))
                   (assoc op :type :ok, :value)))))

  (teardown! [this test])

  (close! [this test]
    (vc/close! conn)))

(defn workload
  "Adds on most threads, a staggered read on two reserved threads, and a final
  read on every thread once the cluster has recovered."
  [opts]
  {:client          (map->Client {:initialized? (promise)})
   :checker         (checker/set-full {:linearizable? true})
   :generator       (gen/reserve 2 (->> (repeat {:f :read})
                                        (gen/stagger 5))
                                  (->> (range)
                                       (map (fn [x] {:f :add, :value x}))))
   :final-generator (gen/each-thread {:f :read})})
