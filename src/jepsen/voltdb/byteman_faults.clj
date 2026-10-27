(ns jepsen.voltdb.byteman-faults
  "Faults injected with Byteman rules (ENG-30396): each loads one rule file
  from resources/byteman, sets up the operation the rule targets, and
  removes the rule again, recording enough to show the rule fired and the
  cluster came through.

  Every run's op value has the same core keys:

    :fault      the fault keyword
    :fired      {node count} of the rule's 'BYTEMAN <label> fired' lines
                written during this run
    :down-after nodes that were up before the run and are down after it
    :problems   what went wrong, empty when the run passed; see check-run

  plus fault-specific details. byteman-checker fails the test on any run
  with problems, and on any enabled fault that never ran."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]]
            [jepsen.control.util :as cu]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [byteman :as byteman]
                           [client :as vc]]))

(def faults
  "The Byteman faults, by name. :rule is the rule file, :label the label
  its traceln lines carry."
  {:snapshot-io-error     {:rule  "snapshot-write-ioexception.btm"
                           :label "snapshot-write-ioexception"}
   :client-response-delay {:rule  "client-response-delay.btm"
                           :label "client-response-delay"}
   :promotion-delay       {:rule  "promotion-delay.btm"
                           :label "promotion-delay"}
   :rejoin-kill           {:rule  "rejoin-kill.btm"
                           :label "rejoin-kill"}
   :replay-delay          {:rule  "replay-delay.btm"
                           :label "replay-delay"}
   :replay-error          {:rule  "replay-error.btm"
                           :label "replay-error"}})

(def rule-set
  "The faults --nemesis byteman-rules expands to: the plan's first rule set.
  :replay-error is left out because it makes a node log a FATAL on purpose,
  which the Jenkins job counts as a failure."
  [:snapshot-io-error :client-response-delay :promotion-delay :rejoin-kill
   :replay-delay])

;; Helpers

(defn- java-home [] (voltdb/db-java-home))

(defn- on-nodes-or-error
  "Runs (f node) on each node in parallel, returning {node result}, where a
  node that throws maps to {:error message} instead of failing the op."
  [test nodes f]
  (if (empty? nodes)
    {}
    (c/on-nodes test nodes
                (fn [test node]
                  (try (f node)
                       (catch Exception e
                         {:error (.getMessage e)}))))))

(defn- fired-counts
  "{node count} of the fault's fired lines on each node."
  [test nodes fault]
  (on-nodes-or-error test nodes
                     (fn [_] (byteman/fired-count voltdb/base-dir
                                                  (:label (faults fault))))))

(defn- fired-since
  "Per-node difference between two fired-counts results; nodes whose count
  could not be read map to their error."
  [before after]
  (into {}
        (for [[node a] after]
          [node (let [b (get before node 0)]
                  (if (and (number? a) (number? b)) (- a b) a))])))

(defn- total-fired [fired]
  (->> (vals fired) (filter number?) (reduce + 0)))

(defn- load-rule! [test nodes fault]
  (on-nodes-or-error test nodes
                     (fn [_] (byteman/load-rules! voltdb/base-dir (java-home)
                                                  (:rule (faults fault)))
                       :loaded)))

(defn- unload-rule! [test nodes fault]
  (on-nodes-or-error test nodes
                     (fn [_] (byteman/unload-rules! voltdb/base-dir (java-home)
                                                    (:rule (faults fault)))
                       :unloaded)))

(defn- running? [test node]
  (c/on-nodes test [node]
              (fn [_ _] (c/su (cu/daemon-running? voltdb/pidfile)))))

(defn- await-nodes-up
  "Waits until every node answers client connections, up to the
  :await-up-timeout. Returns the nodes still down."
  [test nodes opts]
  (let [deadline (+ (System/currentTimeMillis)
                    (* 1000 (:await-up-timeout opts 600)))]
    (loop []
      (let [down (set/difference (set nodes) (set (vc/up-nodes test)))]
        (if (or (empty? down) (< deadline (System/currentTimeMillis)))
          (vec down)
          (do (Thread/sleep 2000)
              (recur)))))))

(defn- await-until
  "Polls (pred) every second until it is truthy or timeout-ms elapses.
  Returns the last value."
  [timeout-ms pred]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [v (pred)]
        (if (or v (< deadline (System/currentTimeMillis)))
          v
          (do (Thread/sleep 1000)
              (recur)))))))

(defn- start-nodes! [test nodes]
  (c/on-nodes test nodes (partial db/start! (:db test))))

(defn- kill-nodes! [test nodes]
  (c/on-nodes test nodes (partial db/kill! (:db test))))

(defn- set-boot-rule! [test node fault]
  (c/on-nodes test [node]
              (fn [_ _] (byteman/set-boot-rules! voltdb/base-dir
                                                 (:rule (faults fault))))))

(defn- clear-boot-rule!
  "Unloads the startup rule from the node's JVM if it is running, then
  empties the startup file."
  [test node fault]
  (c/on-nodes test [node]
              (fn [_ _]
                (when (c/su (cu/daemon-running? voltdb/pidfile))
                  (try (byteman/unload-rules! voltdb/base-dir (java-home))
                       (catch Exception e
                         (warn "Unloading startup rules on" node "failed:"
                               (.getMessage e)))))
                (byteman/clear-boot-rules! voltdb/base-dir))))

;; Faults. Each takes the test, the op options and the nodes up before the
;; run, and returns the fault-specific part of the op value, including any
;; fault-specific :problems.

(defmulti run-fault
  "Injects one fault. Returns a map merged into the op value."
  (fn [fault test opts up] fault))

;; snapshot-io-error: milestone 2.

(def snapshot-dir
  "Where :snapshot-io-error writes its snapshots on each node: outside
  voltdbroot, so the half-written files never meet a restore scan or the
  file-corruption nemesis. Teardown removes it with the rest of base-dir."
  (str (byteman/dir voltdb/base-dir) "/snapshots"))

(def snapshot-timeout-ms
  "How long :snapshot-io-error waits for its snapshot to finish on every
  host."
  60000)

(defn- snapshot-status
  "The @Statistics SNAPSHOTSTATUS rows for the given nonce, one per host and
  table."
  [conn nonce]
  (->> (vc/call! conn "@Statistics" "SNAPSHOTSTATUS" (int 0))
       first
       :rows
       (filter #(= nonce (:NONCE %)))))

(defn- await-snapshot-done
  "Polls SNAPSHOTSTATUS until the nonce has rows and none is IN_PROGRESS, or
  the timeout elapses. Returns the last rows seen."
  [conn nonce]
  (let [deadline (+ (System/currentTimeMillis) snapshot-timeout-ms)]
    (loop []
      (let [rows (snapshot-status conn nonce)]
        (if (or (< deadline (System/currentTimeMillis))
                (and (seq rows)
                     (not-any? #(= "IN_PROGRESS" (:RESULT %)) rows)))
          rows
          (do (Thread/sleep 1000)
              (recur)))))))

(defmethod run-fault :snapshot-io-error [fault test opts up]
  ; The rule fires only for nonces starting BYTEMAN_.
  (let [nonce  (str "BYTEMAN_" (System/currentTimeMillis))
        node   (rand-nth up)
        loaded (on-nodes-or-error test up
                                  (fn [_]
                                    (c/exec :mkdir :-p snapshot-dir)
                                    (byteman/load-rules!
                                      voltdb/base-dir (java-home)
                                      (:rule (faults fault)))
                                    :loaded))
        result (try
                 (let [conn (vc/connect node {:procedure-call-timeout
                                              snapshot-timeout-ms
                                              :connection-response-timeout
                                              snapshot-timeout-ms})]
                   (try
                     (let [res  (.callProcedure
                                  conn "@SnapshotSave"
                                  (into-array Object [snapshot-dir nonce (int 1)]))
                           save (->> (.getResults res)
                                     (map vc/volt-table->map)
                                     first
                                     :rows)]
                       ; :save-results is recorded but not judged: a blocking
                       ; save reports SUCCESS for snapshots whose writes
                       ; failed (ENG-30420).
                       {:save-status    (.getStatus res)
                        :save-results   (frequencies (map :RESULT save))
                        :status-results (frequencies
                                          (map :RESULT
                                               (await-snapshot-done conn nonce)))})
                     (finally (vc/close! conn))))
                 (catch Exception e
                   {:error (.getMessage e)})
                 (finally
                   (unload-rule! test up fault)))]
    (cond-> (assoc result :nonce nonce :node node :loaded loaded)
      (not (pos? (get (:status-results result) "FAILURE" 0)))
      (update :problems (fnil conj []) :not-reported-failed))))

;; client-response-delay

(def client-response-window-ms
  "How long :client-response-delay leaves its rule loaded."
  10000)

(defmethod run-fault :client-response-delay [fault test opts up]
  (let [loaded (load-rule! test up fault)]
    (try
      (Thread/sleep client-response-window-ms)
      {:loaded loaded :window-ms client-response-window-ms}
      (finally
        (unload-rule! test up fault)))))

;; promotion-delay

(defmethod run-fault :promotion-delay [fault test opts up]
  ; Needs a survivor to promote on; with one node up there is nothing to do.
  (if (< (count up) 2)
    {:skipped :too-few-nodes-up}
    (let [victim    (rand-nth up)
          survivors (vec (remove #{victim} up))
          before    (fired-counts test survivors fault)
          loaded    (load-rule! test survivors fault)]
      (try
        (info "promotion-delay: killing" victim)
        (kill-nodes! test [victim])
        ; Wait for the promotions to reach repair on a survivor, then for
        ; the 3 s delays to run out.
        (let [promoted? (await-until
                          60000
                          #(pos? (total-fired
                                   (fired-since before
                                                (fired-counts test survivors
                                                              fault)))))]
          (Thread/sleep 5000)
          (info "promotion-delay: rejoining" victim)
          (start-nodes! test [victim])
          (let [still-down (await-nodes-up test [victim] opts)]
            (cond-> {:victim victim :loaded loaded}
              (not promoted?)  (update :problems (fnil conj []) :no-promotion)
              (seq still-down) (update :problems (fnil conj [])
                                       :victim-did-not-rejoin))))
        (finally
          (unload-rule! test survivors fault))))))

;; rejoin-kill

(def rejoin-kill-timeout-ms
  "How long :rejoin-kill waits for the rejoining node to reach the stream
  snapshot and die."
  180000)

(defmethod run-fault :rejoin-kill [fault test opts up]
  (if (< (count up) 2)
    {:skipped :too-few-nodes-up}
    (let [victim (rand-nth up)]
      (info "rejoin-kill: killing" victim "and rejoining it with the rule")
      (kill-nodes! test [victim])
      (set-boot-rule! test victim fault)
      (let [before (fired-counts test [victim] fault)]
        (start-nodes! test [victim])
        (let [died?  (await-until rejoin-kill-timeout-ms
                                  #(and (pos? (total-fired
                                                (fired-since
                                                  before
                                                  (fired-counts test [victim]
                                                                fault))))
                                        (not (get (running? test victim)
                                                  victim))))
              _      (clear-boot-rule! test victim fault)
              _      (info "rejoin-kill: rejoining" victim "without the rule")
              _      (start-nodes! test [victim])
              still-down (await-nodes-up test [victim] opts)]
          (cond-> {:victim victim}
            (not died?)      (update :problems (fnil conj [])
                                     :rejoining-node-not-killed)
            (seq still-down) (update :problems (fnil conj [])
                                     :retry-rejoin-failed)))))))

;; replay-delay and replay-error: whole-cluster recovery from the command log
;; with the rule on one node.

(defn- recover-with-boot-rule!
  "Kills every node, starts them all again with the fault's rule in the
  victim's startup script, and waits for the cluster. Returns
  {:victim :still-down}."
  [fault test opts]
  (let [nodes  (:nodes test)
        victim (rand-nth nodes)]
    (info (name fault) ": restarting the cluster with the rule on" victim)
    (kill-nodes! test nodes)
    (set-boot-rule! test victim fault)
    (start-nodes! test nodes)
    {:victim victim :still-down (await-nodes-up test nodes opts)}))

(defmethod run-fault :replay-delay [fault test opts up]
  (let [{:keys [victim still-down]} (recover-with-boot-rule! fault test opts)]
    (clear-boot-rule! test victim fault)
    (cond-> {:victim victim}
      (seq still-down) (update :problems (fnil conj [])
                               :recovery-incomplete))))

(defmethod run-fault :replay-error [fault test opts up]
  ; The victim should crash during replay. Whatever the others do, the
  ; cluster then gets a clean restart without the rule, which must recover.
  (let [nodes  (:nodes test)
        {:keys [victim still-down]} (recover-with-boot-rule! fault test opts)
        crashed? (not (get (running? test victim) victim))]
    (clear-boot-rule! test victim fault)
    (info "replay-error: restarting the cluster without the rule")
    (kill-nodes! test nodes)
    (start-nodes! test nodes)
    (let [down (await-nodes-up test nodes opts)]
      (cond-> {:victim victim :crashed crashed?
               :down-during-faulted-recovery still-down}
        (not crashed?) (update :problems (fnil conj []) :victim-did-not-crash)
        (seq down)     (update :problems (fnil conj [])
                               :clean-recovery-failed)))))

;; The nemesis

(defn nemesis
  "Runs the Byteman faults. Responds to {:f <fault>} for every key in
  faults. A run with no node up is recorded as skipped."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [fault (:f op)
            up    (vec (vc/up-nodes test))]
        (assert (faults fault))
        (if (empty? up)
          (assoc op :value {:fault fault :skipped :no-nodes-up})
          (let [nodes  (:nodes test)
                before (fired-counts test nodes fault)
                result (try (run-fault fault test opts up)
                            (catch Exception e
                              (warn e "Byteman fault" fault "failed")
                              {:problems [:error] :error (.getMessage e)}))]
            (if (:skipped result)
              (assoc op :value (assoc result :fault fault))
              (let [fired (fired-since before (fired-counts test nodes fault))
                    down  (vec (set/difference (set up)
                                               (set (vc/up-nodes test))))
                    value (cond-> (merge result
                                         {:fault      fault
                                          :fired      fired
                                          :down-after down})
                            (zero? (total-fired fired))
                            (update :problems (fnil conj []) :never-fired)
                            (seq down)
                            (update :problems (fnil conj [])
                                    :nodes-down-after)
                            true
                            (update :problems (fnil vec [])))]
                (info "Byteman" fault (pr-str value))
                (assoc op :value value)))))))

    (teardown! [this test]
      ; Leave no rule in a startup script for the next test on these nodes
      (c/on-nodes test (fn [_ _]
                         (try (byteman/clear-boot-rules! voltdb/base-dir)
                              (catch Exception _)))))

    n/Reflection
    (fs [_] (set (keys faults)))))

(defn checker
  "Fails the test if any Byteman fault run has :problems, or if a fault in
  enabled ran no unskipped run (a fault that never ran proves nothing)."
  [enabled]
  (reify checker/Checker
    (check [this test history opts]
      (let [runs     (->> history
                          (filter #(and (= :nemesis (:process %))
                                        (faults (:f %))
                                        (map? (:value %))
                                        (:fault (:value %))))
                          (map :value))
            done     (->> runs (remove :skipped) (map :fault) frequencies)
            missing  (vec (remove done enabled))
            problems (vec (filter #(seq (:problems %)) runs))]
        {:valid?   (and (empty? missing) (empty? problems))
         :runs     done
         :skipped  (count (filter :skipped runs))
         :never-ran missing
         :problems problems}))))

(defn package
  "Nemesis package for whichever Byteman faults are in (:faults opts)."
  [opts]
  (let [enabled (filter (:faults opts) (keys faults))]
    (when (seq enabled)
      {:nemesis   (nemesis opts)
       ; Round robin from a shuffled start, so every enabled fault runs
       ; before any runs twice. Runs are long (several restart nodes or the
       ; whole cluster) and the nemesis runs one op at a time, so a test
       ; needs a time limit of roughly two minutes per fault.
       :generator (->> (cycle (shuffle enabled))
                       (map (fn [f] {:type :info, :f f}))
                       (gen/stagger (:interval opts)))
       :perf      #{{:name  "byteman"
                     :fs    (set enabled)
                     :color "#C9A0E6"}}})))
