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

(def async-slack-ms
  "Added to --commandlog-frequency when judging a lost write: covers the
  command log fsync itself and the time between VoltDB dying and the
  nemesis recording :killed-at."
  1000)

(defn async-checker
  "For --commandlog-async, where VoltDB acknowledges an insert before fsyncing
  it: a power loss may lose acknowledged inserts, but only ones acknowledged
  within the fsync window (--commandlog-frequency ms, plus async-slack-ms)
  before VoltDB died, as recorded in power-loss ops' :killed-at. Fails the
  test for a lost insert acknowledged earlier than that, or with no later
  power loss, and for an element in the final read that no insert attempted
  or that a failed insert carried."
  [window-ms]
  (reify checker/Checker
    (check [this test history opts]
      (let [window   (* 1000000 (+ window-ms async-slack-ms))
            acked    (->> history
                          (filter #(and (= :ok (:type %)) (= :add (:f %))))
                          (map (juxt :value :time))
                          (into {}))
            failed   (->> history
                          (filter #(and (= :fail (:type %)) (= :add (:f %))))
                          (map :value)
                          set)
            tried    (->> history
                          (filter #(and (= :invoke (:type %)) (= :add (:f %))))
                          (map :value)
                          set)
            final    (->> history
                          (filter #(and (= :ok (:type %)) (= :read (:f %))))
                          last
                          :value)
            kills    (->> history
                          (keep #(when (= :nemesis (:process %))
                                   (:killed-at (:value %))))
                          sort)
            lost     (remove (set final) (keys acked))
            age      (fn [v] (let [t (acked v)]
                               (when-let [k (first (filter #(<= t %) kills))]
                                 (- k t))))
            too-old  (->> lost
                          (remove #(when-let [a (age %)] (<= a window)))
                          sort)
            ages     (keep age lost)
            unexpected (->> final
                            (filter #(or (failed %) (not (tried %))))
                            sort)]
        (if (nil? final)
          {:valid? :unknown, :error "No successful read"}
          {:valid?              (and (empty? too-old) (empty? unexpected))
           :window-ms           (+ window-ms async-slack-ms)
           :acknowledged-count  (count acked)
           :lost-count          (count lost)
           :max-lost-age-ms     (when (seq ages)
                                  (long (/ (reduce max ages) 1000000)))
           :lost-outside-window (vec (take 20 too-old))
           :lost-outside-window-count (count too-old)
           :unexpected          (vec (take 20 unexpected))})))))

(defn workload
  "Adds on most threads, a staggered read on two reserved threads, and a final
  read on every thread once the cluster has recovered. With
  --commandlog-async, checked by async-checker instead of set-full."
  [opts]
  {:client          (map->Client {:initialized? (promise)})
   :checker         (if (:commandlog-async opts)
                      (async-checker (:commandlog-frequency opts))
                      (checker/set-full {:linearizable? true}))
   :generator       (gen/reserve 2 (->> (repeat {:f :read})
                                        (gen/stagger 5))
                                  (->> (range)
                                       (map (fn [x] {:f :add, :value x}))))
   :final-generator (gen/each-thread {:f :read})})
