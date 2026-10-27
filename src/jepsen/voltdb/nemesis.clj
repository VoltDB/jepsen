(ns jepsen.voltdb.nemesis
  "Various packages of faults we can inject against VoltDB clusters."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
             [control :as c]
             [db :as db]
             [nemesis :as n]
             [generator :as gen]
             [net :as net]
             [util :as util]]
            [jepsen.control [util :as cu]]
            [jepsen.nemesis [combined :as nc]
             [time :as nt]]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [byteman :as byteman]
                           [client :as vc]
                           [membership :as membership]]
            [clojure.set :as set]
            [clojure.string :as str]
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

(defn recovery-checker
  "Fails the test when a recovery step left nodes down: any :await-up or
  :cluster-restart op whose value carries :still-down. Data checks can pass
  on the surviving nodes, so without this a green result would not establish
  that the cluster recovered. Kept separate from the workload checker so the
  two outcomes stay distinguishable."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [stuck (->> history
                       (filter #(and (= :nemesis (:process %))
                                     (#{:await-up :cluster-restart} (:f %))
                                     (map? (:value %))
                                     (:still-down (:value %))))
                       (map (fn [op] {:f (:f op)
                                      :time (:time op)
                                      :still-down (:still-down (:value op))})))]
        {:valid? (empty? stuck)
         :recoveries-with-nodes-down (count stuck)
         :failed-recoveries (vec stuck)}))))

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

(def snapshot-io-error-rule
  "Rule file (resources/byteman) for the :snapshot-io-error fault."
  "snapshot-write-ioexception.btm")

(def snapshot-io-error-dir
  "Where the :snapshot-io-error nemesis writes its snapshots on each node:
  outside voltdbroot, so the half-written files never meet a restore scan or
  the file-corruption nemesis. Teardown removes it with the rest of base-dir."
  (str (byteman/dir voltdb/base-dir) "/snapshots"))

(def snapshot-io-error-timeout-ms
  "How long the :snapshot-io-error nemesis waits for its snapshot to finish
  (succeed or fail) on every host."
  60000)

(defn- on-nodes-or-error
  "Runs (f node) on each node in parallel, returning {node result}, where a
  node that throws maps to {:error message} instead of failing the op."
  [test nodes f]
  (c/on-nodes test nodes
              (fn [test node]
                (try (f node)
                     (catch Exception e
                       {:error (.getMessage e)})))))

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
  (let [deadline (+ (System/currentTimeMillis) snapshot-io-error-timeout-ms)]
    (loop []
      (let [rows (snapshot-status conn nonce)]
        (if (or (< deadline (System/currentTimeMillis))
                (and (seq rows)
                     (not-any? #(= "IN_PROGRESS" (:RESULT %)) rows)))
          rows
          (do (Thread/sleep 1000)
              (recur)))))))

(defn- fired-count
  "How many times the snapshot-write-ioexception rule logged that it fired
  for the nonce, from the node's stdout.log (Byteman's traceln writes to the
  server's stdout)."
  [nonce]
  (-> (c/exec :grep :-c (str "BYTEMAN snapshot-write-ioexception fired nonce="
                             nonce " ")
              (str voltdb/base-dir "/log/stdout.log")
              (c/lit "|| true"))
      str/trim
      Long/parseLong))

(defn snapshot-io-error-nemesis
  "Byteman milestone 2 (ENG-30396): makes a snapshot fail with an IOException
  mid-write. Loads resources/byteman/snapshot-write-ioexception.btm into
  every node that is up, takes a blocking @SnapshotSave whose nonce starts
  with BYTEMAN_ (the rule only fires for those), waits for SNAPSHOTSTATUS to
  settle, unloads the rule, then counts the rule's 'fired' lines in each
  node's stdout.log. Needs --byteman, which the runner turns on for this
  fault.

  Responds to {:f :snapshot-io-error}. The op value records which nodes were
  up before and after, where the rule loaded, SNAPSHOTSTATUS results, the
  save's own result rows, and the fired counts; snapshot-io-error-checker
  judges them."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= :snapshot-io-error (:f op)))
      (let [up        (vec (vc/up-nodes test))
            java-home (voltdb/db-java-home)]
        (if (empty? up)
          (assoc op :value {:skipped :no-nodes-up})
          (let [nonce  (str "BYTEMAN_" (System/currentTimeMillis))
                node   (rand-nth up)
                loaded (on-nodes-or-error
                         test up
                         (fn [_]
                           (c/exec :mkdir :-p snapshot-io-error-dir)
                           (byteman/load-rules! voltdb/base-dir java-home
                                                snapshot-io-error-rule)
                           :loaded))
                result (try
                         (let [conn (vc/connect
                                      node {:procedure-call-timeout
                                            snapshot-io-error-timeout-ms
                                            :connection-response-timeout
                                            snapshot-io-error-timeout-ms})]
                           (try
                             (let [res  (.callProcedure
                                          conn "@SnapshotSave"
                                          (into-array Object
                                            [snapshot-io-error-dir
                                             nonce
                                             (int 1)]))
                                   save (->> (.getResults res)
                                             (map vc/volt-table->map)
                                             first
                                             :rows)]
                               {:save-status  (.getStatus res)
                                :save-results (frequencies (map :RESULT save))
                                :status-results
                                (frequencies
                                  (map :RESULT (await-snapshot-done conn nonce)))})
                             (finally (vc/close! conn))))
                         (catch Exception e
                           {:error (.getMessage e)})
                         (finally
                           (on-nodes-or-error
                             test up
                             (fn [_] (byteman/unload-rules!
                                       voltdb/base-dir java-home
                                       snapshot-io-error-rule)))))
                fired  (on-nodes-or-error test up (fn [_] (fired-count nonce)))
                down   (vec (set/difference (set up) (set (vc/up-nodes test))))]
            (info "snapshot-io-error" nonce "on" node ":" result
                  "fired:" fired "down after:" down)
            (assoc op :value (merge {:nonce      nonce
                                     :node       node
                                     :loaded     loaded
                                     :fired      fired
                                     :down-after down}
                                    result))))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:snapshot-io-error})))

(defn snapshot-io-error-checker
  "Judges the :snapshot-io-error ops. A run passes when the rule fired on at
  least one node, the snapshot was reported failed (some SNAPSHOTSTATUS row
  is FAILURE), and every node that was up before the fault is still up.
  Per the plan, a rule that never fired is a failure, as is a test where no
  run happened. The runner adds this checker only when the fault is enabled."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [runs (->> history
                      (filter #(and (= :nemesis (:process %))
                                    (= :snapshot-io-error (:f %))
                                    (map? (:value %))
                                    (:nonce (:value %))))
                      (map :value))
            problems
            (for [{:keys [nonce fired status-results down-after error]} runs
                  :let [fired-total (->> (vals fired) (filter number?)
                                         (reduce + 0))
                        p (cond-> []
                            error
                            (conj :error)
                            (zero? fired-total)
                            (conj :never-fired)
                            (not (pos? (get status-results "FAILURE" 0)))
                            (conj :not-reported-failed)
                            (seq down-after)
                            (conj :nodes-down-after))]
                  :when (seq p)]
              {:nonce nonce :problems p :down-after down-after
               :error error :status-results status-results})]
        {:valid?   (boolean (and (seq runs) (empty? problems)))
         :runs     (count runs)
         :problems (vec problems)}))))

(defn snapshot-io-error-package
  "Package for the :snapshot-io-error fault (Byteman)."
  [opts]
  (when ((:faults opts) :snapshot-io-error)
    {:nemesis   (snapshot-io-error-nemesis opts)
     :generator (->> (repeat {:type :info, :f :snapshot-io-error})
                     (gen/stagger (:interval opts)))
     :perf      #{{:name  "snapshot-io-error"
                   :fs    #{:snapshot-io-error}
                   :color "#C9A0E6"}}}))

(defn preinstalled-bitflip
  "jepsen.nemesis/bitflip, minus its setup!, which install-archive!s the
  helper into /opt/jepsen/bitflip on every node in parallel and races with
  itself when /opt is shared between the nodes (the lab's LXD containers).
  jepsen.voltdb/os installs the helper once instead."
  []
  (let [inner (n/bitflip)]
    (reify
      n/Nemesis
      (setup! [this test] this)
      (invoke! [this test op] (n/invoke! inner test op))
      (teardown! [this test] (n/teardown! inner test))
      n/Reflection
      (fs [this] (n/fs inner)))))

(defn nemesis-packages
  "jepsen.nemesis.combined/nemesis-packages, with the file-corruption
  package's nemesis built on the pre-installed bitflip helper."
  [opts]
  (let [db (:db opts)]
    (for [pkg (nc/nemesis-packages opts)]
      (if (= #{:bitflip :truncate} (set (mapcat :fs (:perf pkg))))
        (assoc pkg :nemesis (nc/file-corruption-nemesis
                              db (preinstalled-bitflip) (n/truncate-file)))
        pkg))))

(defn nemesis-package
  "Takes nemesis options (see jepsen.nemesis.combined for details and examples)
  and constructs a package of nemesis and generators."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nemesis-packages opts)
        ; TODO: custom nemeses--port these over from jepsen.voltdb.
        (concat [(rando-package opts)
                 (cluster-restart-package opts)
                 (snapshot-io-error-package opts)
                 (membership/package opts)
                 (await-up-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
