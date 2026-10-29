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
            [jepsen.voltdb [byteman-faults :as byteman-faults]
                           [client :as vc]
                           [membership :as membership]]
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

(defn recovery-checker
  "Fails the test when a recovery step left nodes down: any :await-up,
  :cluster-restart or power-loss op whose value carries :still-down. Data checks can pass
  on the surviving nodes, so without this a green result would not establish
  that the cluster recovered. Kept separate from the workload checker so the
  two outcomes stay distinguishable."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [stuck (->> history
                       (filter #(and (= :nemesis (:process %))
                                     (#{:await-up :cluster-restart :power-loss :torn-power-loss
                                        :snapshot-power-loss :node-power-loss} (:f %))
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

(defn start-all-and-await!
  "Starts VoltDB on every node, which recovers the cluster from the command
  log, and blocks until all nodes answer client connections. Returns
  {:recovered-in-ms n}, counted from start-ns, or {:still-down nodes} at the
  timeout (:await-up-timeout opts, default 600 s)."
  [test opts start-ns]
  (info "Starting VoltDB on every node (recovery from the command log)")
  (c/on-nodes test (partial db/start! (:db test)))
  (let [timeout-ms (* 1000 (:await-up-timeout opts 600))
        deadline   (+ start-ns (* timeout-ms 1000000))
        nodes      (set (:nodes test))]
    (loop []
      (let [down (set/difference nodes (set (vc/up-nodes test)))]
        (cond (empty? down)
              {:recovered-in-ms (long (/ (- (System/nanoTime) start-ns)
                                         1000000))}

              (< deadline (System/nanoTime))
              (do (warn "Nodes still down after cluster restart:" down)
                  {:still-down down})

              :else
              (do (Thread/sleep 5000)
                  (recur)))))))

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
      (let [start-ns (System/nanoTime)]
        (info "Killing VoltDB on every node")
        (c/on-nodes test (partial db/kill! (:db test)))
        (Thread/sleep (* 1000 (:cluster-restart-down-time opts 5)))
        (assoc op :value (start-all-and-await! test opts start-ns))))

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

(defn power-loss-nemesis
  "Power loss on every node at once, for --lazyfs: kills VoltDB everywhere,
  drops every write LazyFS holds that was not fsynced, then restarts and
  recovers the cluster as cluster-restart does. With a synchronous command
  log, every acknowledged write was fsynced first, so none may be lost.

  Responds to {:f :power-loss}. The op value is cluster-restart's, plus
  :killed-at, the test's relative time in nanoseconds once VoltDB was dead on
  every node, which the async command log checker measures losses against."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= :power-loss (:f op)))
      (let [start-ns (System/nanoTime)]
        (c/on-nodes test voltdb/power-off!)
        (let [killed-at (util/relative-time-nanos)]
          (Thread/sleep (* 1000 (:cluster-restart-down-time opts 5)))
          (assoc op :value (assoc (start-all-and-await! test opts start-ns)
                                  :killed-at killed-at)))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:power-loss})))

(defn power-loss-package
  "Package for the :power-loss fault."
  [opts]
  (when ((:faults opts) :power-loss)
    {:nemesis   (power-loss-nemesis opts)
     :generator (->> (repeat {:type :info, :f :power-loss})
                     (gen/stagger (:interval opts)))
     :perf      #{{:name  "power-loss"
                   :fs    #{:power-loss}
                   :color "#C0392B"}}}))

(defn torn-power-loss-nemesis
  "Power loss on every node at once in the middle of a command log write, for
  --lazyfs: each node's next write to its active command log segment is torn
  in two, one half reaches the disk, and every other unsynced write is lost
  (see voltdb/torn-power-off!). Then recovers the cluster as power-loss does.
  The torn write was never fsynced, so its transaction was never
  acknowledged: recovery must stop at the last whole record and lose nothing
  acknowledged.

  Responds to {:f :torn-power-loss}. The op value is cluster-restart's, plus
  :torn, each node's torn-power-off! result, and :killed-at as power-loss."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= :torn-power-loss (:f op)))
      (let [start-ns  (System/nanoTime)
            torn      (c/on-nodes test voltdb/torn-power-off!)
            killed-at (util/relative-time-nanos)]
        (Thread/sleep (* 1000 (:cluster-restart-down-time opts 5)))
        (assoc op :value (assoc (start-all-and-await! test opts start-ns)
                                :torn torn
                                :killed-at killed-at))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:torn-power-loss})))

(defn restored-snapshot
  "The nonce of the snapshot this node's running VoltDB restored from, per
  volt.log, or nil when it restored none. The log spans restarts, so only
  lines after the last process start count."
  []
  (let [line (c/exec :awk "/PID of this process/ {r = \"\"} /Restoring from path:/ {r = $0} END {print r}"
                     (str voltdb/base-dir "/log/volt.log"))]
    (second (re-find #"with nonce: (\S+)" line))))

(defn snapshot-power-loss-nemesis
  "Power loss on every node at once while a snapshot is being written, for
  --lazyfs: starts a non-blocking @SnapshotSave into voltdbroot/snapshots,
  where recovery looks for snapshots, waits a random time up to
  --snapshot-power-loss-delay ms, then cuts power and recovers as power-loss
  does. Recovery prefers the newest snapshot, so it must find the
  interrupted one incomplete and use an older one; loading it partially
  would lose acknowledged writes, which the workload checkers catch.

  Responds to {:f :snapshot-power-loss}. The op value is cluster-restart's,
  plus the snapshot's :nonce, :delay-ms, the save's :save-status, and
  :restored, the snapshot each node restored from (the nonce when the
  snapshot was complete in time), and :killed-at as power-loss."
  [opts]
  (let [n (atom 0)]
    (reify n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (assert (= :snapshot-power-loss (:f op)))
        (let [nonce    (str "jepsen_power_loss_" (swap! n inc))
              delay-ms (rand-int (inc (:snapshot-power-loss-delay opts 500)))
              node     (rand-nth (:nodes test))
              status   (try
                         (let [conn (vc/connect node {})]
                           (try
                             (.getStatus (.callProcedure
                                           conn "@SnapshotSave"
                                           (into-array Object
                                                       [(str voltdb/base-dir "/voltdbroot/snapshots")
                                                        nonce (int 0)])))
                             (finally (vc/close! conn))))
                         (catch Exception e
                           (str "save failed: " (.getMessage e))))
              _        (Thread/sleep delay-ms)
              start-ns (System/nanoTime)]
          (c/on-nodes test voltdb/power-off!)
          (let [killed-at (util/relative-time-nanos)
                _         (Thread/sleep (* 1000 (:cluster-restart-down-time opts 5)))
                v         (start-all-and-await! test opts start-ns)]
            (assoc op :value (assoc v
                                    :killed-at   killed-at
                                    :nonce       nonce
                                    :delay-ms    delay-ms
                                    :save-status status
                                    :restored    (c/on-nodes
                                                   test (fn [_ _] (restored-snapshot))))))))

      (teardown! [this test])

      n/Reflection
      (fs [_] #{:snapshot-power-loss}))))

(defn rejoins-completed
  "How many rejoins this node's volt.log reports as completed."
  []
  (parse-long
    (c/exec :bash :-c (str "grep -c 'Node rejoin completed' " voltdb/base-dir
                           "/log/volt.log || true"))))

(defn node-power-loss-nemesis
  "Power loss on one node, chosen at random, for --lazyfs: kills VoltDB there
  and drops its unsynced writes, while the rest of the cluster keeps serving
  (needs --kfactor 1 or more), then starts it again, which rejoins it to the
  live cluster, and waits until volt.log reports the rejoin complete.

  Responds to {:f :node-power-loss}. The op value is the :node, and
  :rejoined-in-ms, or :still-down when the rejoin did not complete within
  --await-up-timeout."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= :node-power-loss (:f op)))
      (let [node    (rand-nth (:nodes test))
            timeout (* 1000 (:await-up-timeout opts 600))
            rejoin! (fn [test node]
                      (let [before (rejoins-completed)]
                        (voltdb/power-off! test node)
                        (Thread/sleep (* 1000 (:cluster-restart-down-time opts 5)))
                        (let [start-ns (System/nanoTime)]
                          (db/start! (:db test) test node)
                          (try (util/await-fn
                                 (fn rejoined? []
                                   (when-not (< before (rejoins-completed))
                                     (throw+ {:type ::not-rejoined})))
                                 {:retry-interval 1000
                                  :log-interval   30000
                                  :log-message    (str "Waiting for " node " to rejoin")
                                  :timeout        timeout})
                               {:node           node
                                :rejoined-in-ms (long (/ (- (System/nanoTime) start-ns)
                                                         1000000))}
                               (catch Exception _
                                 (warn node "did not rejoin")
                                 {:node node, :still-down #{node}})))))]
        (assoc op :value (get (c/on-nodes test [node] rejoin!) node))))

    (teardown! [this test])

    n/Reflection
    (fs [_] #{:node-power-loss})))

(defn lazyfs-package
  "Package for a LazyFS power-loss fault f, built by (nemesis-fn opts)."
  [opts f nemesis-fn color]
  (when ((:faults opts) f)
    {:nemesis   (nemesis-fn opts)
     :generator (->> (repeat {:type :info, :f f})
                     (gen/stagger (:interval opts)))
     :perf      #{{:name (name f), :fs #{f}, :color color}}}))

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
                 (power-loss-package opts)
                 (lazyfs-package opts :torn-power-loss
                                 torn-power-loss-nemesis "#8E44AD")
                 (lazyfs-package opts :snapshot-power-loss
                                 snapshot-power-loss-nemesis "#2471A3")
                 (lazyfs-package opts :node-power-loss
                                 node-power-loss-nemesis "#B9770E")
                 (byteman-faults/package opts)
                 (membership/package opts)
                 (await-up-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
