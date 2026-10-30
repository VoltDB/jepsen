(ns jepsen.voltdb.runner
  "Runs VoltDB tests from the command line."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [core :as jepsen]
                    [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [dirty-read :as dirty-read]
                           [export     :as export]
                           [multi      :as multi]
                           [nemesis    :as nemesis]
                           [byteman-faults :as byteman-faults]
                           [rw-register :as rw-register]
                           [dr :as dr]
                           [bank :as bank]
                           [list-append :as list-append]
                           [set :as vset]
                           [single     :as single]
                           [redundant-register :as redundant-register]]))

(def workloads
  "A map of workload names names to functions that take CLI options and return
  workload maps"
  {:dirty-read         dirty-read/workload
   :export             export/workload
   :long-fork          rw-register/long-fork-workload
   :multi              multi/workload
   :redundant-register redundant-register/workload
   :rw-register        rw-register/workload
   :set                vset/workload
   :list-append        list-append/workload
   :bank               bank/workload
   :dr                 dr/workload
   :single             single/workload})

(def consistency-models
  "Consistency models Elle can check for; see elle.consistency-model."
  #{:strict-serializable :serializable :snapshot-isolation
    :repeatable-read :read-committed :read-uncommitted
    :strong-session-serializable :strong-session-snapshot-isolation})

(def nemeses
  "All nemesis faults we know about."
  #{:partition :packet :clock :pause :kill :rando :cluster-restart
    :file-corruption :membership :snapshot-io-error :client-response-delay
    :promotion-delay :rejoin-kill :replay-delay :replay-error :power-loss
    :torn-power-loss :snapshot-power-loss :node-power-loss})

(def lazyfs-nemeses
  "Faults that need --lazyfs, which they turn on."
  #{:power-loss :torn-power-loss :snapshot-power-loss :node-power-loss})

(def corruptible-dirs
  "Directories under the voltdbroot whose files the file-corruption nemesis
  may damage, by name, for --file-corruption-dirs."
  {:command-log          "command_log"
   :command-log-snapshot "command_log_snapshot"
   :snapshots            "snapshots"})

(defn corruptions
  "Builds the :corruptions list for jepsen.nemesis.combined's file corruption
  package from the CLI options: for each chosen directory, a bitflip of
  --bitflip-probability of the bits of a random file in it, and a truncation
  dropping a geometrically distributed number of bytes (mean
  --truncate-mean-bytes) from the end of a random file."
  [opts]
  (for [dir   (:file-corruption-dirs opts)
        :let  [path (str voltdb/base-dir "/voltdbroot/" (corruptible-dirs dir))]
        c     [{:type :bitflip
                :file path
                :probability (:bitflip-probability opts)}
               {:type :truncate
                :file path
                :drop {:distribution :geometric
                       :p (/ 1.0 (:truncate-mean-bytes opts))}}]]
    c))

(def packet-behaviors
  "Named tc-netem behaviours for --packet-behaviors, each a map as
  jepsen.net/all-packet-behaviors expects. Unspecified fields take jepsen's
  defaults (50 ms +/- 10 ms normal-distributed delay, 20% loss etc.). The
  cluster's 2 s heartbeat timeout is what these are aimed at: a clean
  partition is binary, these are the slow, lossy, reordering networks that
  make membership flap."
  {:delay     {:delay {}}
   :slow      {:delay {:time :500ms :jitter :250ms}}
   :loss      {:loss {}}
   :corrupt   {:corrupt {:percent :5%}}
   :duplicate {:duplicate {}}
   :reorder   {:reorder {}}
   :rate      {:rate {:rate :1mbit}}
   :mixed     {:delay {} :loss {:percent :5%} :duplicate {:percent :5%}}})

(defn parse-behaviors
  "Takes a comma-separated list of packet-behaviors names and returns a vector
  of behaviour maps."
  [spec]
  (->> (str/split spec #",")
       (remove str/blank?)
       (mapv (fn [b]
               (or (get packet-behaviors (keyword b))
                   (throw (IllegalArgumentException.
                            (str "Unknown packet behaviour " b ", want one of "
                                 (str/join ", " (map name (keys packet-behaviors)))))))))))

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all  [:partition :clock]
   ; The plan's first Byteman rule set (ENG-30396)
   :byteman-rules byteman-faults/rule-set})

(def partition-specs
  "Partition specs jepsen.nemesis.combined's partition package (and
  jepsen.nemesis.combined/grudge) understand. :primaries needs a db/Primary
  implementation, which we don't have. Note :all is not a partition spec."
  #{:one :minority-third :majority :majorities-ring})

(def node-specs
  "Node specs jepsen.nemesis.combined/db-nodes understands, for kill, pause,
  packet and file-corruption targets. Note :majorities-ring is a partition
  spec only."
  #{:one :minority-third :minority :majority :all})

(defn parse-targets
  "Takes a comma-separated string of node specs and returns a vector of
  keywords, e.g. \"one,majority\" -> [:one :majority]."
  [spec]
  (->> (str/split spec #",")
       (remove str/blank?)
       (mapv keyword)))

(defn valid-targets?
  "A non-empty vector whose every element is in specs."
  [specs targets]
  (and (seq targets) (every? specs targets)))

(def all-nemeses
  "Combinations of nemeses we run through for test-all"
  [[]
   [:rando :kill]
   [:rando :partition]
   [:rando :pause]
   [:rando :clock]
   [:rando :kill :partition]
   [:packet]
   [:packet :kill]
   [:cluster-restart]
   [:file-corruption :cluster-restart]
   [:membership]
   [:membership :partition]
   ; all-tests passes these straight through, so no special names here
   byteman-faults/rule-set])

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def opt-spec
  "Command line options for tools.cli"
  [[nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "4n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--force-download" "Re-download tarballs, even if cached locally"]

   [nil "--byteman" "Attach the Byteman agent (listener on localhost:9091) to every VoltDB server JVM, so rules from resources/byteman can be loaded mid-test. Off by default; with no rules loaded it should not change behaviour."]

   [nil "--lazyfs" "Put voltdbroot (command log, snapshots, config) on LazyFS, which loses writes that were not fsynced when its cache is dropped. Builds LazyFS on each node; needs /dev/fuse and root. The power-loss faults (power-loss, torn-power-loss, snapshot-power-loss, node-power-loss) turn this on."]

   [nil "--lose-unsynced-config" "With --lazyfs, a power loss also drops unsynced writes outside the command log and snapshots (config, catalog, .initialized, export cursors). Off by default: VoltDB never fsyncs those (ENG-30442), so turning this on mostly re-finds that bug."]

   [nil "--commandlog-async" "Asynchronous command log: VoltDB acknowledges a transaction before fsyncing it, and fsyncs every --commandlog-frequency ms. Power loss may then lose writes acknowledged within that window; the set workload checks exactly that, and only the set workload supports this."]

   [nil "--commandlog-frequency MS" "With --commandlog-async, how often VoltDB fsyncs the command log, in milliseconds."
    :default 200
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--snapshot-power-loss-delay MS" "The snapshot-power-loss fault cuts power a random 0 to MS milliseconds after starting a snapshot."
    :default 500
    :parse-fn parse-long
    :validate [#(not (neg? %)) "Must not be negative"]]

   [nil "--node-count NUMBER" "Run on only the first NUMBER of the given nodes, e.g. a 3-node cluster from a 5-node pool."
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   ["-l" "--license FILE" "Path to the VoltDB license file on the control node"
    :default "license.xml"]

   [nil "--nemesis FAULTS" "A comma-separated list of faults to inject."
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (fn [nem]
                                 (or (nemeses nem)
                                     (special-nemeses nem))))
               (cli/one-of (concat nemeses (keys special-nemeses)))]]
    
    ["-k" "--kfactor NUMBER" "K-factor for database installation"
    :parse-fn #(Long/parseLong %)
    :default 1
    :validate [pos? "Must be positive"]]

    [nil "--sitesperhost NUMBER" "Sites per host value for the database installation"
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

    [nil "--transactionsize NUMBER" "the range for the number of records per transaction"
    :parse-fn #(Long/parseLong %) 
    :default 16
    :validate [pos? "Must be positive"]]

   [nil "--partition-targets SPECS" "Comma-separated partition specs for network partitions: one, minority-third, majority, majorities-ring. The original default was majority,majorities-ring; it was later narrowed to one, which never forces partition detection or a mastership change at high k-factors."
    :default [:majority :majorities-ring]
    :default-desc "majority,majorities-ring"
    :parse-fn parse-targets
    :validate [(partial valid-targets? partition-specs)
               (str "Must be a non-empty list. " (cli/one-of partition-specs))]]

   [nil "--kill-targets SPECS" "Comma-separated node specs for process kills: one, minority-third, minority, majority, all. all kills the whole cluster, which then recovers from the command log on restart."
    :default [:one :majority :all]
    :default-desc "one,majority,all"
    :parse-fn parse-targets
    :validate [(partial valid-targets? node-specs)
               (str "Must be a non-empty list. " (cli/one-of node-specs))]]

   [nil "--pause-targets SPECS" "Comma-separated node specs for SIGSTOP pauses: one, minority-third, minority, majority, all."
    :default [:one :majority :all]
    :default-desc "one,majority,all"
    :parse-fn parse-targets
    :validate [(partial valid-targets? node-specs)
               (str "Must be a non-empty list. " (cli/one-of node-specs))]]

   [nil "--cluster-restart-down-time SECONDS" "How long the cluster-restart nemesis leaves every node dead before starting them all again."
    :default 5
    :parse-fn #(Long/parseLong %)
    :validate [#(not (neg? %)) "Must not be negative"]]

   [nil "--await-up-timeout SECONDS" "How long recovery waits for every node to answer client connections again before giving up and recording the nodes still down."
    :default 600
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--packet-targets SPECS" "Comma-separated node specs for packet faults: one, minority-third, minority, majority, all."
    :default [:one :minority-third :majority]
    :default-desc "one,minority-third,majority"
    :parse-fn parse-targets
    :validate [(partial valid-targets? node-specs)
               (str "Must be a non-empty list. " (cli/one-of node-specs))]]

   [nil "--packet-behaviors NAMES" "Comma-separated tc-netem behaviours the packet nemesis picks from: delay, slow, loss, corrupt, duplicate, reorder, rate, mixed."
    :default (mapv packet-behaviors [:delay :slow :loss :corrupt :duplicate :reorder :mixed])
    :default-desc "delay,slow,loss,corrupt,duplicate,reorder,mixed"
    :parse-fn parse-behaviors]

   [nil "--file-corruption-targets SPECS" "Comma-separated node specs for file corruption: one, minority-third, minority, majority, all. More than a minority risks damaging every replica of a partition, which no database can survive."
    :default [:one]
    :default-desc "one"
    :parse-fn parse-targets
    :validate [(partial valid-targets? node-specs)
               (str "Must be a non-empty list. " (cli/one-of node-specs))]]

   [nil "--file-corruption-dirs NAMES" "Comma-separated voltdbroot directories the file-corruption nemesis damages: command-log, command-log-snapshot, snapshots."
    :default [:command-log :command-log-snapshot :snapshots]
    :default-desc "command-log,command-log-snapshot,snapshots"
    :parse-fn parse-targets
    :validate [(partial every? corruptible-dirs) (cli/one-of corruptible-dirs)]]

   [nil "--bitflip-probability P" "Fraction of the bits of the chosen file a bitflip corruption flips."
    :default 1e-5
    :parse-fn #(Double/parseDouble %)
    :validate [#(< 0 % 1) "Must be between 0 and 1"]]

   [nil "--truncate-mean-bytes N" "Mean number of bytes a truncate corruption drops from the end of the chosen file (geometric distribution)."
    :default 4096
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--nemesis-interval SECONDS" "How long between nemesis operations, on average, for each class of fault?"
    ; In my testing, Volt often takes 20 seconds or so just to start up--we
    ; don't want to go too fast here.
    :default  30
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   ; Elle workload options (rw-register, list-append), passed through to
   ; elle.*/gen and check.
   [nil "--key-count NUMBER" "Number of distinct keys active at any point in the Elle workloads."
    :default 10
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--min-txn-length NUMBER" "Minimum number of micro-ops per Elle transaction."
    :default 1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--max-txn-length NUMBER" "Maximum number of micro-ops per Elle transaction."
    :default 4
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--max-writes-per-key NUMBER" "How many writes an Elle key receives before it is retired."
    :default 32
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--consistency-models MODELS" "Comma-separated consistency models Elle should check the history against."
    :default [:strict-serializable]
    :default-desc "strict-serializable"
    :parse-fn parse-targets
    :validate [(partial every? consistency-models) (cli/one-of consistency-models)]]

   [nil "--negative-balances" "Let the bank workload's transfers drive balances negative (the checker then only verifies the total)."]

   [nil "--dr-mode MODE" "For the dr workload: xdcr (active-active, both clusters take writes) or one-way (master to replica)."
    :default :xdcr
    :parse-fn keyword
    :validate [#{:xdcr :one-way} "Must be xdcr or one-way"]]

   [nil "--dr-split NUMBER" "For the dr workload: how many of the nodes form cluster a; the rest form cluster b. Defaults to half."
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--dr-drain-timeout SECONDS" "For the dr workload: how long the final read waits for both clusters to agree."
    :default 120
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--long-fork-group-size NUMBER" "How many keys each long-fork read checks at once."
    :default 2
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-p" "--procedure-call-timeout MILLISECONDS"
    "How long should we wait before timing out procedure calls?"
    :default 1000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-r" "--rate HZ" "Approximate number of requests per second, total"
    :default 100
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--recovery-delay SECONDS"
    "How long should we wait before killing nodes and recovering?"
    :default 0
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--strong-reads" "Use stored procedure including a write for all reads"]

   [nil "--export-table" "Use clause table export into target for export testing"]

   [nil "--skip-os" "Don't perform OS setup"]

   ["-u" "--tarball URL" "URL for the VoltDB tarball to install. May be either HTTP, HTTPS, or a local file on this control node. For instance, --tarball https://foo.com/voltdb-ent.tar.gz, or file://voltdb-ent.tar.gz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar)")
               "Must be a file://, http://, or https:// URL including .tar"]]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn voltdb-test
  "Takes parsed CLI options from -main and constructs a Jepsen test map."
  [opts]
  (let [; Byteman faults need the agent on the servers
        byteman-on    (filter (set (:nemesis opts)) (keys byteman-faults/faults))
        opts          (cond-> opts
                        (seq byteman-on) (assoc :byteman true)
                        ; Power loss drops LazyFS's unsynced writes
                        (some (set (:nemesis opts)) lazyfs-nemeses) (assoc :lazyfs true))
        workload-name (:workload opts)
        _             (when (and (:commandlog-async opts) (not= :set workload-name))
                        (throw (IllegalArgumentException.
                                 "--commandlog-async is only checked by the set workload")))
        ; Right now workloads construct entire test maps. We'll refactor this
        ; in the next commit.
        workload ((workloads workload-name) opts)
        db       (let [inner (voltdb/db (:tarball opts) (:force-download opts))]
                   (if (= :dr workload-name)
                     (dr/db inner (:dr-mode workload) (:roles workload))
                     inner))
        nemesis (nemesis/nemesis-package
                  {:db        db
                   :nodes     (:nodes opts)
                   :faults    (:nemesis opts)
                   ; Targets come from the CLI (see --partition-targets etc.);
                   ; the defaults restore the original majority-level faults.
                   :partition {:targets (:partition-targets opts)}
                   :pause     {:targets (:pause-targets opts)}
                   :kill      {:targets (:kill-targets opts)}
                   :packet    {:targets   (:packet-targets opts)
                               :behaviors (:packet-behaviors opts)}
                   ; Damage to files is only observed when they are read
                   ; back: at a rejoin or a command-log recovery. Pair this
                   ; fault with kill or cluster-restart, e.g.
                   ; --nemesis file-corruption,cluster-restart.
                   :file-corruption {:targets     (:file-corruption-targets opts)
                                     :corruptions (vec (corruptions opts))}

                   :kfactor   (:kfactor opts)
                   :cluster-restart-down-time (:cluster-restart-down-time opts)
                   :await-up-timeout (:await-up-timeout opts)
                   :snapshot-power-loss-delay (:snapshot-power-loss-delay opts)
                   :interval  (:nemesis-interval opts)})
        gen (->> (:generator workload)
                 (gen/stagger (/ (:rate opts)))
                 (gen/nemesis
                   [(gen/sleep 5)
                    (:generator nemesis)])
                 (gen/time-limit (:time-limit opts)))
        ; Is there a final generator for this workload?
        gen (if-let [final (:final-generator workload)]
              (gen/phases gen
                          ; Recovery: heal the network, resume paused and
                          ; restart killed processes.
                          (gen/log "Recovering cluster")
                          (gen/nemesis (:final-generator nemesis))
                          ; Majority partitions make the minority shut itself
                          ; down through partition detection, and that is not a
                          ; fault the nemesis packages know to undo. db/start!
                          ; skips nodes whose daemon is still running, so an
                          ; unconditional start on every node is safe: it
                          ; rejoins the ones that died, or recovers the whole
                          ; cluster from the command log if all of them did.
                          (gen/nemesis {:type :info, :f :start, :value :all})
                          (gen/log "Waiting for recovery")
                          (gen/nemesis {:type :info, :f :await-up})
                          ; Final generators
                          (gen/clients final))
              ; No final generator
              gen)
        ; Elle workloads wrap the whole generator so their final reads can
        ; see the highest key used.
        gen (if-let [wrap (:wrap-generator workload)]
              (wrap gen)
              gen)]
    (merge tests/noop-test
           opts
           ; Some workloads (e.g. bank) parameterize their generator and
           ; checker through keys on the test map.
           (dissoc workload :client :checker :generator :final-generator
                   :wrap-generator)
           {:name (str (name workload-name)
                       " " (str/join "," (map name (:nemesis opts))))
            :os        (if (:skip-os opts)
                         os/noop
                         (voltdb/os debian/os))
            :generator gen
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :db        db
            :checker   (checker/compose
                         (cond->
                           {:perf       (checker/perf {:nemeses (:perf nemesis)})
                            :clock      (checker/clock-plot)
                            :stats      (checker/stats)
                            :exceptions (checker/unhandled-exceptions)
                            :recovery   (nemesis/recovery-checker)
                            :workload   (:checker workload)}
                           (seq byteman-on)
                           (assoc :byteman (byteman-faults/checker
                                             byteman-on))))})))

(defn all-tests
  "Turns CLI options into a sequence of tests to perform."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] (keys workloads))]
    (for [n nemeses, w workloads, i (range (:test-count opts))]
      (voltdb-test (assoc opts :workload w, :nemesis n)))))

(defn take-nodes
  "--node-count: keeps only the first N of the given nodes."
  [parsed]
  (if-let [n (get-in parsed [:options :node-count])]
    (update-in parsed [:options :nodes] #(vec (take n %)))
    parsed))

(defn opt-fn
  "jepsen.cli/test-opt-fn, with take-nodes between parsing the nodes and
  parsing the concurrency, so --concurrency 4n counts only the nodes the test
  runs on."
  [parsed]
  (-> parsed
      cli/rename-ssh-options
      (cli/rename-options {:leave-db-running :leave-db-running?})
      (cli/rename-options {:logging-json :logging-json?})
      cli/parse-nodes
      take-nodes
      cli/parse-concurrency))

(defn -main
  "Main entry point for the CLI. Takes CLI options and runs tests, launches a
  web server, analyzes results, etc."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  voltdb-test
                                         :opt-spec opt-spec
                                         :opt-fn*  opt-fn})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec opt-spec
                                      :opt-fn*  opt-fn})
                   (cli/serve-cmd))
            args))
