(ns jepsen.voltdb.membership
  "A jepsen.nemesis.membership state machine for VoltDB cluster membership.
  Nodes leave through `voltadmin stop`, VoltDB's graceful, k-safe removal of a
  host, and come back through `voltdb start`, a live rejoin. Each node's view
  of the cluster comes from @SystemInformation OVERVIEW; an operation is
  resolved once the merged view agrees it took effect. Never more than
  k-factor nodes are out at once, which keeps the cluster k-safe.

  Elastic add and remove of nodes beyond the initial count are not modelled
  yet: they need spare nodes the matrix does not have."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.nemesis.membership :as membership]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb.client :as vc]))

(defn overview-hostnames
  "Asks node for @SystemInformation OVERVIEW and returns the set of HOSTNAME
  values the cluster reports, or nil if the node can't be reached."
  [node]
  (try
    (with-open [conn (vc/connect node {:procedure-call-timeout 2000
                                       :reconnect? false})]
      (->> (vc/call! conn "@SystemInformation" "OVERVIEW")
           first
           :rows
           (filter #(= "HOSTNAME" (:KEY %)))
           (map :VALUE)
           set))
    (catch Exception e
      nil)))

(defn member?
  "Does a set of reported hostnames contain this test node? Volt reports the
  host's own hostname, which may be the node name or a longer form of it."
  [hostnames node]
  (boolean (some (fn [h] (or (= h node)
                             (str/starts-with? h (str node "."))
                             (str/starts-with? node (str h "."))))
                 hostnames)))

(defn stop-node!
  "Gracefully stops target through voltadmin on some other, live node."
  [test target]
  (let [via (or (first (remove #{target} (vc/up-nodes test)))
                (throw (IllegalStateException.
                         (str "No live node other than " target
                              " to run voltadmin stop from"))))]
    (info "Stopping" target "via voltadmin on" via)
    (c/on via
          (c/cd voltdb/base-dir
                (c/exec (c/env {"JAVA_HOME" (voltdb/db-java-home)})
                        "bin/voltadmin" :stop :--timeout 120 target)))))

(defrecord Membership
  [stopped   ; Set of nodes we have stopped and not yet rejoined
   kfactor   ; Cluster k-factor: how many nodes may be out at once
   ; Merged in by the membership nemesis:
   node-views view pending]
  membership/State
  (setup! [this test] this)

  (node-view [this test node]
    (overview-hostnames node))

  (merge-views [this test]
    ; The view most of the reachable nodes agree on; ties go to the larger
    ; view, which is the one that has seen the most recent rejoin.
    (->> (vals node-views)
         (remove nil?)
         frequencies
         (sort-by (fn [[v n]] [n (count v)]))
         last
         first))

  (fs [this] #{:stop-node :rejoin})

  (op [this test]
    (let [nodes      (:nodes test)
          in-flight  (set (map (comp :value first) pending))
          candidates (->> nodes
                          (remove stopped)
                          (remove in-flight)
                          (filter (partial member? view)))
          can-stop?  (and (seq candidates)
                          (< (+ (count stopped) (count in-flight)) kfactor))
          can-rejoin? (seq (remove in-flight stopped))]
      (cond (and can-stop? (or (not can-rejoin?) (< (rand) 0.5)))
            {:type :info, :f :stop-node, :value (rand-nth candidates)}

            can-rejoin?
            {:type :info, :f :rejoin, :value (rand-nth (vec (remove in-flight stopped)))}

            :else :pending)))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :stop-node (do (stop-node! test value)
                     [(assoc op :value value)
                      (update this :stopped conj value)])
      :rejoin    (do (c/on value (voltdb/rejoin! test value))
                     [(assoc op :value value)
                      (update this :stopped disj value)])))

  (resolve [this test] this)

  (resolve-op [this test [op op']]
    (let [node (:value op)]
      (case (:f op)
        :stop-node (when-not (member? view node) this)
        :rejoin    (when (member? view node) this))))

  (teardown! [this test] this))

(defn rejoin-all-generator
  "Final generator: rejoins whatever is still stopped, one node at a time,
  reading the state atom so each rejoin sees the previous one's effect."
  [state node-count]
  (->> (fn [test ctx]
         (when-let [node (first (:stopped @state))]
           {:type :info, :f :rejoin, :value node}))
       (gen/limit node-count)))

(defn package
  "Membership package for nemesis-package: {:faults #{:membership}
  :kfactor k :interval s}."
  [opts]
  (when ((:faults opts) :membership)
    (let [pkg (membership/package
                {:faults     (:faults opts)
                 :interval   (:interval opts)
                 :membership {:state (map->Membership {:stopped #{}
                                                       :kfactor (:kfactor opts 1)})
                              :log-view? true
                              :log-resolve-op? true}})]
      (assoc pkg
             :final-generator (rejoin-all-generator (:state pkg)
                                                    (count (:nodes opts)))
             :perf #{{:name  "membership"
                      :start #{:stop-node}
                      :stop  #{:rejoin}
                      :color "#9AE48B"}}))))
