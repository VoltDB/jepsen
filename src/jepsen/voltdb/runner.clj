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
                           [rw-register :as rw-register]
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
   :single             single/workload})

(def consistency-models
  "Consistency models Elle can check for; see elle.consistency-model."
  #{:strict-serializable :serializable :snapshot-isolation
    :repeatable-read :read-committed :read-uncommitted
    :strong-session-serializable :strong-session-snapshot-isolation})

(def nemeses
  "All nemesis faults we know about."
  ; TODO: add bitflip/truncate
  #{:partition :packet :clock :pause :kill :rando :cluster-restart})

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
   :all  [:partition :clock]})

(def node-specs
  "Node specs jepsen.nemesis.combined understands for partition, kill and pause
  targets. :primaries needs a db/Primary implementation, which we don't have."
  #{:one :minority-third :majority :majorities-ring :all})

(defn parse-targets
  "Takes a comma-separated string of node specs and returns a vector of
  keywords, e.g. \"one,majority\" -> [:one :majority]."
  [spec]
  (->> (str/split spec #",")
       (remove str/blank?)
       (mapv keyword)))

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
   [:cluster-restart]])

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

   [nil "--partition-targets SPECS" "Comma-separated node specs for network partitions: one, minority-third, majority, majorities-ring, all. The original default was majority,majorities-ring; it was later narrowed to one, which never forces partition detection or a mastership change at high k-factors."
    :default [:majority :majorities-ring]
    :default-desc "majority,majorities-ring"
    :parse-fn parse-targets
    :validate [(partial every? node-specs) (cli/one-of node-specs)]]

   [nil "--kill-targets SPECS" "Comma-separated node specs for process kills: one, minority-third, majority, majorities-ring, all. all kills the whole cluster, which then recovers from the command log on restart."
    :default [:one :majority :all]
    :default-desc "one,majority,all"
    :parse-fn parse-targets
    :validate [(partial every? node-specs) (cli/one-of node-specs)]]

   [nil "--pause-targets SPECS" "Comma-separated node specs for SIGSTOP pauses: one, minority-third, majority, majorities-ring, all."
    :default [:one :majority :all]
    :default-desc "one,majority,all"
    :parse-fn parse-targets
    :validate [(partial every? node-specs) (cli/one-of node-specs)]]

   [nil "--cluster-restart-down-time SECONDS" "How long the cluster-restart nemesis leaves every node dead before starting them all again."
    :default 5
    :parse-fn #(Long/parseLong %)
    :validate [#(not (neg? %)) "Must not be negative"]]

   [nil "--await-up-timeout SECONDS" "How long recovery waits for every node to answer client connections again before giving up and recording the nodes still down."
    :default 600
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--packet-targets SPECS" "Comma-separated node specs for packet faults: one, minority-third, majority, majorities-ring, all."
    :default [:one :minority-third :majority]
    :default-desc "one,minority-third,majority"
    :parse-fn parse-targets
    :validate [(partial every? node-specs) (cli/one-of node-specs)]]

   [nil "--packet-behaviors NAMES" "Comma-separated tc-netem behaviours the packet nemesis picks from: delay, slow, loss, corrupt, duplicate, reorder, rate, mixed."
    :default (mapv packet-behaviors [:delay :slow :loss :corrupt :duplicate :reorder :mixed])
    :default-desc "delay,slow,loss,corrupt,duplicate,reorder,mixed"
    :parse-fn parse-behaviors]

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
  (let [workload-name (:workload opts)
        ; Right now workloads construct entire test maps. We'll refactor this
        ; in the next commit.
        workload ((workloads workload-name) opts)
        db       (voltdb/db (:tarball opts) (:force-download opts))
        nemesis (nemesis/nemesis-package
                  {:db        db
                   :nodes     (:nodes test)
                   :faults    (:nemesis opts)
                   ; Targets come from the CLI (see --partition-targets etc.);
                   ; the defaults restore the original majority-level faults.
                   :partition {:targets (:partition-targets opts)}
                   :pause     {:targets (:pause-targets opts)}
                   :kill      {:targets (:kill-targets opts)}
                   :packet    {:targets   (:packet-targets opts)
                               :behaviors (:packet-behaviors opts)}

                   :cluster-restart-down-time (:cluster-restart-down-time opts)
                   :await-up-timeout (:await-up-timeout opts)
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
                         {:perf       (checker/perf {:nemeses (:perf nemesis)})
                          :clock      (checker/clock-plot)
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :workload   (:checker workload)})})))

(defn all-tests
  "Turns CLI options into a sequence of tests to perform."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] (keys workloads))]
    (for [n nemeses, w workloads, i (range (:test-count opts))]
      (voltdb-test (assoc opts :workload w, :nemesis n)))))

(defn -main
  "Main entry point for the CLI. Takes CLI options and runs tests, launches a
  web server, analyzes results, etc."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn voltdb-test
                                         :opt-spec opt-spec})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
