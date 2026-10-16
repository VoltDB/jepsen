(ns jepsen.voltdb.nemesis
  "Various packages of faults we can inject against VoltDB clusters."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
             [db :as db]
             [nemesis :as n]
             [generator :as gen]
             [net :as net]
             [util :as util]]
            [jepsen.control [util :as cu]]
            [jepsen.nemesis [combined :as nc]
             [time :as nt]]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [client :as vc]]
            [clojure.set :as set]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.voltdb.client ClientResponse)))


(defn rando-nemesis
  "Tries to confuse VoltDB by spewing random writes into an unrelated table.
  This happens in an independent thread; the nemesis returns from :invoke
  immediately, even though it's still writing.

  Expects invocations of the form {:f :rando, :value some-node}."
  [opts]
  (let [initialized? (promise)
        writes       (atom 0)]
    (reify n/Nemesis
      (setup! [this test]
        (let [node (first (:nodes test))
              conn (vc/connect node {})]
          (when (deliver initialized? true)
            (try
              (c/on node
                    (vc/with-race-retry
                      ; Create table
                      (voltdb/sql-cmd! "CREATE TABLE mentions (
                                       well     INTEGER NOT NULL,
                                       actually INTEGER NOT NULL
                                       );"))
                    (info node "mentions table created"))
              (finally
                (vc/close! conn)))))
        this)

      (invoke! [this test op]
        (assert (= :rando (:f op)))
        (let [conn (vc/connect (:value op)
                               {:procedure-call-timeout 100
                                :reconnect? false})]
          (future
            (util/with-thread-name "rando"
              (try
                (let [; Run for 10 seconds
                      deadline (+ (System/nanoTime)
                                  (* 10 1e9))]
                  (loop [i 0]
                    (when (< (System/nanoTime) deadline)
                      ; (call! conn "MENTIONS.insert" i (rand-int 1000)))
                      ; If we go TOO fast we'll start forcing other ops to
                      ; time out. If we go too slow we won't get a long
                      ; enough log.
                      (Thread/sleep 1)
                      (vc/async-call!
                        conn "MENTIONS.insert" i (rand-int 1000)
                        (fn [^ClientResponse res]
                          (when (or (= ClientResponse/SUCCESS
                                       (.getStatus res))
                                    ; not sure why this happens but it's ok?
                                    (= ClientResponse/UNINITIALIZED_APP_STATUS_CODE
                                       (.getStatus res)))
                            (->> res
                                 .getResults
                                 (map vc/volt-table->map)
                                 first
                                 :rows
                                 first
                                 :modified_tuples
                                 (swap! writes +)))))
                      (recur (inc i)))))
                (catch Exception e
                  (info "Rando nemesis crashed with" (.getMessage e)))
                (finally
                  (info "Rando nemesis finished writing")
                  (vc/close! conn))))))
        (assoc op :value {:running :in-background, :cumulative-writes @writes}))

      (teardown! [this test])

      n/Reflection
      (fs [_] #{:rando}))))

(defn rando-generator
  "A generator for rando operations."
  [opts]
  (->> (fn [test ctx]
         {:type :info, :f :rando, :value (rand-nth (:nodes test))})
       (gen/stagger (:interval opts))))

(defn rando-package
  "A combined nemesis package for injecting random writes into some table."
  [opts]
  (when ((:faults opts) :rando)
    {:nemesis (rando-nemesis opts)
     :generator (rando-generator opts)
     :perf #{{:name "rando"
              :fs [:rando]
              :color "#E9A0E6"}}}))

(defn await-up-nemesis
  "A nemesis with no generator of its own; the runner's recovery phase sends it
  {:f :await-up} after starting every node, and it blocks until each node
  answers client connections again, or the timeout (:await-up-timeout opts,
  default 600 s) elapses. Returns the nodes still down, if any, as the op
  value, so a failed recovery shows up in the history rather than as a final
  read against a dead node."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= :await-up (:f op)))
      (let [timeout-ms (* 1000 (:await-up-timeout opts 600))
            deadline   (+ (System/nanoTime) (* timeout-ms 1000000))
            nodes      (set (:nodes test))]
        (loop []
          (let [down (set/difference nodes (set (vc/up-nodes test)))]
            (cond (empty? down)
                  (assoc op :value :all-up)

                  (< deadline (System/nanoTime))
                  (do (warn "Nodes still down after recovery:" down)
                      (assoc op :value {:still-down down}))

                  :else
                  (do (info "Waiting for" down "to come up")
                      (Thread/sleep 5000)
                      (recur)))))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:await-up})))

(defn await-up-package
  "Package wrapping await-up-nemesis. Always included: the runner's recovery
  phase depends on it regardless of which faults are enabled."
  [opts]
  {:nemesis (await-up-nemesis opts)
   :generator nil
   :perf #{}})

(defn cluster-restart-nemesis
  "Kills VoltDB on every node at once, waits a little, then starts every node
  again, which recovers the cluster from the command log, and blocks until
  all nodes answer client connections. The random :kill nemesis with an :all
  target can produce the same sequence, but only by chance and with the
  restart governed by the kill/start flip-flop; this one makes whole-cluster
  recovery a first-class fault that happens on every nemesis interval.

  Responds to {:f :cluster-restart}. The op value records how long the
  cluster took to come back, or the nodes still down at the timeout."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= :cluster-restart (:f op)))
      (let [db       (:db test)
            start-ns (System/nanoTime)]
        (info "Killing VoltDB on every node")
        (c/on-nodes test (partial db/kill! db))
        (Thread/sleep (* 1000 (:cluster-restart-down-time opts 5)))
        (info "Starting VoltDB on every node (recovery from the command log)")
        (c/on-nodes test (partial db/start! db))
        (let [timeout-ms (* 1000 (:await-up-timeout opts 600))
              deadline   (+ start-ns (* timeout-ms 1000000))
              nodes      (set (:nodes test))]
          (loop []
            (let [down (set/difference nodes (set (vc/up-nodes test)))]
              (cond (empty? down)
                    (assoc op :value {:recovered-in-ms
                                      (long (/ (- (System/nanoTime) start-ns)
                                               1000000))})

                    (< deadline (System/nanoTime))
                    (do (warn "Nodes still down after cluster restart:" down)
                        (assoc op :value {:still-down down}))

                    :else
                    (do (Thread/sleep 5000)
                        (recur))))))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:cluster-restart})))

(defn cluster-restart-package
  "Package for the :cluster-restart fault."
  [opts]
  (when ((:faults opts) :cluster-restart)
    {:nemesis   (cluster-restart-nemesis opts)
     :generator (->> (repeat {:type :info, :f :cluster-restart})
                     (gen/stagger (:interval opts)))
     :perf      #{{:name  "cluster-restart"
                   :fs    #{:cluster-restart}
                   :color "#E97A4F"}}}))

(defn nemesis-package
  "Takes nemesis options (see jepsen.nemesis.combined for details and examples)
  and constructs a package of nemesis and generators."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        ; TODO: custom nemeses--port these over from jepsen.voltdb.
        (concat [(rando-package opts)
                 (cluster-restart-package opts)
                 (await-up-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
