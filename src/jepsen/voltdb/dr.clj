(ns jepsen.voltdb.dr
  "Database replication test using jepsen.role: the test's nodes are split
  into two clusters, :a and :b, each a complete VoltDB cluster with its own
  deployment, and replication runs between them. Linearizability does not
  hold across clusters, so the check is convergence-based.

  Modes (--dr-mode):

    xdcr     Active-active. Clients on each cluster insert into the same
             DR'd table, with disjoint values (even on :a, odd on :b), so
             there are no conflicts to resolve.
    one-way  :a is the producer (role master) and :b the consumer (role
             replica). Only :a takes writes.

  The workload is a set: {:f :add :value n} inserts; the final
  {:f :final-read} op reads the table on both clusters, retrying until they
  agree or --dr-drain-timeout elapses, and returns both sets. The checker
  requires every acknowledged add to be present on both clusters, and both
  final reads to be identical. Faults from --nemesis land on any node of
  either cluster."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [history :as h]
                    [role :as role]]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb.client :as vc]))

(def table "dr_sets")
(def dr-port 5555)

(defn roles
  "Splits nodes into the two clusters: the first --dr-split nodes are :a, the
  rest :b."
  [nodes split]
  (let [[a b] (split-at split nodes)]
    (when (or (empty? a) (empty? b))
      (throw (IllegalArgumentException.
               (str "--dr-split " split " leaves a cluster empty with "
                    (count nodes) " nodes"))))
    {:a (vec a), :b (vec b)}))

(defn dr-config
  "The :dr deployment settings for a role."
  [mode roles r]
  (let [other   (case r :a :b, :b :a)
        source  (->> (get roles other)
                     (map #(str % ":" dr-port))
                     (clojure.string/join ","))]
    (case mode
      :xdcr    {:id (case r :a 1, :b 2), :role "xdcr", :source source}
      :one-way (case r
                 :a {:id 1, :role "master"}
                 :b {:id 2, :role "replica", :source source}))))

(defrecord DrDB [inner dr]
  db/DB
  (setup!    [_ test node] (db/setup!    inner (assoc test :dr dr) node))
  (teardown! [_ test node] (db/teardown! inner (assoc test :dr dr) node))
  db/Kill
  (kill!  [_ test node] (db/kill!  inner (assoc test :dr dr) node))
  (start! [_ test node] (db/start! inner (assoc test :dr dr) node))
  db/Pause
  (pause!  [_ test node] (db/pause!  inner (assoc test :dr dr) node))
  (resume! [_ test node] (db/resume! inner (assoc test :dr dr) node))
  db/LogFiles
  (log-files [_ test node] (db/log-files inner (assoc test :dr dr) node)))

(defn db
  "A jepsen.role composite DB: each role gets the normal VoltDB DB with its
  own :dr settings. In one-way mode the replica starts after the master."
  [inner mode roles]
  (role/db {:a (DrDB. inner (dr-config mode roles :a))
            :b (if (= mode :one-way)
                 {:db   (DrDB. inner (dr-config mode roles :b))
                  :deps [:a]}
                 (DrDB. inner (dr-config mode roles :b)))}))

(defn create-schema!
  "Creates the DR'd table on the first node of each cluster."
  [test]
  (doseq [r [:a :b]]
    (let [node (first (role/nodes test r))]
      (info node "creating" table "for cluster" r)
      (c/on node
            (vc/with-race-retry
              (voltdb/sql-cmd! (str "CREATE TABLE " table " (
                                   id  BIGINT NOT NULL,
                                   PRIMARY KEY (id)
                                   );
                                   PARTITION TABLE " table " ON COLUMN id;
                                   DR TABLE " table ";")))))))

(defn read-set
  [conn]
  (->> (vc/ad-hoc! conn (str "SELECT id FROM " table ";"))
       first
       :rows
       (map :ID)
       (into (sorted-set))))

(defn read-cluster
  "Reads the table from the first live node of a cluster."
  [test r]
  (let [live (filter (set (vc/up-nodes test)) (role/nodes test r))]
    (when (empty? live)
      (throw (IllegalStateException. (str "No live node in cluster " r))))
    (with-open [conn (vc/connect (first live) test)]
      (read-set conn))))

(defn final-read
  "Reads both clusters until they agree, or the drain timeout passes."
  [test]
  (let [deadline (+ (System/nanoTime)
                    (* 1e9 (:dr-drain-timeout test 120)))]
    (loop []
      (let [a (read-cluster test :a)
            b (read-cluster test :b)]
        (if (or (= a b) (< deadline (System/nanoTime)))
          {:a a, :b b, :converged? (= a b)}
          (do (info "Waiting for DR to drain:" (count a) "vs" (count b) "rows")
              (Thread/sleep 5000)
              (recur)))))))

(defrecord Client [conn node cluster initialized?]
  client/Client
  (open! [this test node]
    ; In one-way mode every worker writes to :a; in xdcr each worker writes
    ; to the cluster its node belongs to.
    (let [cluster (if (= :one-way (:dr-mode test))
                    :a
                    (role/role test node))
          node    (if (= :one-way (:dr-mode test))
                    (let [as (role/nodes test :a)]
                      (nth as (mod (.indexOf ^java.util.List (:nodes test) node)
                                   (count as))))
                    node)]
      (assoc this
             :node node
             :cluster cluster
             :conn (vc/connect node (select-keys test [:procedure-call-timeout
                                                       :connection-response-timeout])))))

  (setup! [this test]
    (when (deliver initialized? true)
      (create-schema! test)))

  (invoke! [this test op]
    (case (:f op)
      :add        (let [v (:value op)]
                    ; xdcr: :a owns even values, :b odd, so clusters never
                    ; write the same key.
                    (if (and (= :xdcr (:dr-mode test))
                             (not= (even? v) (= :a cluster)))
                      (assoc op :type :fail, :error :wrong-cluster)
                      (vc/with-errors op false
                        (vc/call! conn "DR_SETS.insert" v)
                        (assoc op :type :ok, :cluster cluster))))
      :final-read (vc/with-errors op true
                    (assoc op :type :ok, :value (final-read test)))))

  (teardown! [this test])

  (close! [this test]
    (vc/close! conn)))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [adds   (->> history h/oks (h/filter-f :add) (map :value) (into (sorted-set)))
            finals (->> history h/oks (h/filter-f :final-read) (map :value))
            final  (last finals)
            a      (:a final)
            b      (:b final)
            lost-a (when a (set/difference adds a))
            lost-b (when b (set/difference adds b))]
        {:valid? (cond (nil? final)                 :unknown
                       (not (:converged? final))    false
                       (or (seq lost-a) (seq lost-b)) false
                       :else true)
         :add-count   (count adds)
         :a-count     (count a)
         :b-count     (count b)
         :converged?  (:converged? final)
         :lost-on-a   (take 100 lost-a)
         :lost-on-b   (take 100 lost-b)
         :lost-on-a-count (count lost-a)
         :lost-on-b-count (count lost-b)}))))

(defn workload
  "Keys merged into the test map: :roles, :dr-mode, :dr-drain-timeout."
  [opts]
  (let [mode  (:dr-mode opts :xdcr)
        roles (roles (:nodes opts) (:dr-split opts (quot (count (:nodes opts)) 2)))]
    {:roles           roles
     :dr-mode         mode
     :dr-drain-timeout (:dr-drain-timeout opts 120)
     :client          (map->Client {:initialized? (promise)})
     :checker         (checker)
     :generator       (->> (range)
                           (map (fn [x] {:f :add, :value x})))
     ; One final read on one thread, retried until it succeeds.
     :final-generator (->> {:f :final-read}
                           gen/repeat
                           (gen/delay 10)
                           (gen/limit 10)
                           gen/until-ok
                           (gen/on-threads #{0}))}))
