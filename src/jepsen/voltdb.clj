(ns jepsen.voltdb
  "OS and database setup functions, plus some older, currently unused nemeses
  that should be ported over to voltdb.nemesis. see also: https://jepsen-io.github.io/jepsen/"
  (:require [jepsen [core         :as jepsen]
             [db           :as db]
             [control      :as c :refer [|]]
             [checker      :as checker]
             [client       :as client]
             [generator    :as gen]
             [independent  :as independent]
             [nemesis      :as nemesis]
             [net          :as net]
             [os           :as os]
             [tests        :as tests]
             [util         :as util :refer [await-fn meh timeout]]]
            [jepsen.os.debian     :as debian]
            [jepsen.nemesis.time  :as nt]
            [jepsen.control.util  :as cu]
            [jepsen.control.net   :as cn]
            [jepsen.voltdb.client :as vc]
            [jepsen.voltdb.byteman :as byteman]
            [knossos.model        :as model]
            [clojure.data.xml     :as xml]
            [clojure.string       :as str]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.voltdb VoltTable
                       VoltType
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse
                              ProcedureCallback)))

(def base-dir "/tmp/jepsen-voltdb")
(def client-port 21212)
(def export-csv-dir "export")
(def export-csv-files "export")
(def pidfile (str base-dir "/pidfile"))

(defn db-java-home
  "JAVA_HOME to use for the VoltDB processes on the DB nodes (voltdb init,
  voltdb start, sqlcmd). Jepsen 0.3.10+ needs JDK 21 on the control node, but
  the server kits are qualified on JDK 17, so the two are decoupled: set
  VOLT_JAVA_HOME for the DB side; it defaults to the control node's JAVA_HOME."
  []
  (or (System/getenv "VOLT_JAVA_HOME")
      (System/getenv "JAVA_HOME")))

(defn list-export-files
  "List export files for the export tests"
  []
  (let [export-dir (str base-dir "/voltdbroot/" export-csv-dir)]
    (if (cu/exists? export-dir)
      (into [] (filter #(re-find #".*export.*csv\z" %)     ;here substring "export" is same as export-csv-files
                      (into () (cu/ls-full export-dir))))
      (into []))))

(def helper-install-lock
  "Serializes the installation of jepsen's helper binaries across nodes."
  (Object.))

(def bitflip-url
  "Where jepsen.nemesis/bitflip fetches its helper from (same release)."
  "https://github.com/aybabtme/bitflip/releases/download/v0.2.0/bitflip_0.2.0_Linux_x86_64.tar.gz")

(defn os
  "Given OS, plus python & jdk"
  [os]
  (reify os/OS
    (setup! [_ test node]
      (os/setup! os test node)
      ; The clock nemesis compiles two C helpers into /opt/jepsen on every
      ; node, in parallel, at nemesis setup. On the lab's LXD hosts /opt is
      ; bind-mounted from the host and shared by all containers, so the
      ; concurrent gcc + mv a.out race and one node fails with "cannot stat
      ; a.out" (ENG-29876). jepsen skips the compile when the binary already
      ; exists, so build them here one node at a time: on a shared /opt the
      ; first node does the work and the rest find it, on a private /opt
      ; each node builds its own.
      (locking helper-install-lock
        (nt/install!)
        ; Same story for the bitflip helper the file-corruption nemesis
        ; installs with install-archive!, which rm -rf's and re-creates
        ; /opt/jepsen/bitflip on every node in parallel: on a shared /opt
        ; one node's mv lands in a directory another node just removed
        ; ("cannot create regular file .../README.md"). Install it once
        ; here, and jepsen.voltdb.nemesis uses a bitflip nemesis that
        ; skips the install.
        (when-not (cu/exists? (str nemesis/bitflip-dir "/bitflip"))
          (c/su (cu/install-archive! bitflip-url nemesis/bitflip-dir))))
      ;(comment these should already be installed and in the path
      ;(debian/install ["python3" "openjdk-17-jdk-headless"])
      ;(c/exec :update-alternatives :--install "/usr/bin/python" "python"
      ;        "/usr/bin/python3" 1))
      )

    (teardown! [_ test node]
      (os/teardown! os test node))))

(defn install!
  "Install the given tarball URL"
  [node url force?]
   (if-let [[m path _ filename] (re-find #"^file://((.*/)?([^/]+))$" url)]
     (do ; We're installing a local tarball from the control node; upload it.
       ; A root-owned /tmp/jepsen left behind by an earlier run makes the
       ; upload fail with Permission denied, so make the directory ours.
       (c/su (c/exec :mkdir :-p "/tmp/jepsen")
             (c/exec :chmod "1777" "/tmp/jepsen"))
       (let [remote-path (str "/tmp/jepsen/" filename)]
         (c/upload path remote-path)
         (info (str "unpacking file://" remote-path "to " url " " base-dir))
         (cu/install-archive! (str "file://" remote-path)
                              base-dir {:force? force?})))
      ; Probably an HTTP URI; just let install-archive handle it
     (cu/install-archive! url base-dir {:force? force?}))
   (c/exec :mkdir (str base-dir "/log"))
   (info "VoltDB unpacked"))

(defn deployment-xml
  "Generate a deployment.xml string for the given test."
  [test]
  (xml/emit-str
   (xml/sexp-as-element
    [:deployment {}
      [:cluster ( let [nodeCount (count (:nodes test)) kfac (:kfactor test) sph (:sitesperhost test)
                clusterSpec {:hostcount nodeCount
                :kfactor ( if-not ( < kfac nodeCount )
                                 (throw (Exception. (str "kfactor must be smaller than nodes count = " nodeCount)))
                            kfac)} ]
                ( if (= sph nil) clusterSpec
                                 (merge clusterSpec {:sitesperhost sph}) ) )]
     [:paths {}
      [:voltdbroot {:path base-dir}]]
     ; We need to choose a heartbeat high enough so that we can spam
     ; isolated nodes with requests *before* they kill themselves
     ; but low enough that a new majority is elected and performs
     ; some operations.
     [:heartbeat {:timeout 2}] ; seconds
     ; TODO: consider changing commandlog enabled to false to speed up startup
     [:commandlog {:enabled true, :synchronous true, :logsize 128}
      [:frequency {:time 2}]] ; milliseconds
     ; Not exactly sure what these do! Adapted from ghostbuster -- KRK 2023
     [:systemsettings
      [:flushinterval {:minimum 10}
       [:export {:interval 10}]]]
     ; Database replication, when the test gives this cluster a DR role:
     ; {:id 1, :role "xdcr" | "master" | "replica", :source "h1,h2"}.
     ; jepsen.voltdb.dr sets this per role through a wrapping DB.
     (when-let [dr (:dr test)]
       [:dr {:id (:id dr), :role (:role dr)}
        (when-let [src (:source dr)]
          [:connection {:source src}])])
     ; Export configuration, for export tests
     [:export
      [:configuration {:enabled true, :target "export_target", :type "file"}
       [:property {:name "type"} "csv"]
       [:property {:name "nonce"} export-csv-files]
       [:property {:name "outdir"} export-csv-dir]
       ]]])))

(defn init-db!
  "run voltdb init"
  [node]
  (info "Initializing voltdb")
          (c/cd base-dir
              ; We think there's a bug that breaks sqlcmd if it runs early in
              ; the creation of a fresh DB--it'll log "Cannot invoke
              ; java.util.Map.values() because arglists is null". To work
              ; around that, we're creating a table so the schema is nonempty.
                (let [init-schema "CREATE TABLE work_around_volt_bug (
                                 id int not null
                                 );"
                      init-schema-file "init-schema"]
                  (cu/write-file! init-schema init-schema-file)
                  (c/exec (c/env {"JAVA_HOME" (db-java-home)
                                  })
                          "bin/voltdb"
                          :init
                          :-s init-schema-file
                          :--config (str base-dir "/deployment.xml")
                          ;| :tee (str base-dir "/log/stdout.log")
                          )))
  (info node "initialized"))

(defn configure!
  "Prepares config files and creates fresh DB."
  [test node]
          (c/cd base-dir
                (c/upload (:license test) (str base-dir "/license.xml"))
                (cu/write-file! (deployment-xml test) "deployment.xml")
                (init-db! node)
                ; this is so jepsen can find the log in a standard place
                (c/exec :mkdir :-p "log")
                (c/exec :ln :-f :-s (str base-dir "/voltdbroot/log/volt.log") (str base-dir "/log/volt.log"))))

(defn await-log
  "Blocks until voltdb.log contains the given string."
  [line]
  (let [file (str base-dir "/log/volt.log")]
            (c/cd base-dir
                  ; There used to be a sleep here of *four minutes*. Why? --KRK
                  (c/exec :tail :-n 20 file
                          | :grep :-m 1 :-f line
                          ; What is this xargs FOR? What was I thinking seven
                          ; years ago? --KRK, 2023
                          | :xargs (c/lit (str "echo \"\" >> " file
                                               " \\;"))))))

(defn await-start
  "Blocks until the node is up, responding to client connections, and
  @SystemInformation OVERVIEW returns."
  [node]
  (info "Waiting for" node "to start")
  (cu/await-tcp-port client-port {:log-interval 30000
                                  :timeout 300000})
  (with-open [conn (vc/connect node {:procedure-call-timeout 100
                                     :reconnect? false})]
    ; Per Ruth, just being able to ask for SystemInformation should indicate
    ; the cluster is ready to use. We'll make sure we get at least one table
    ; back, just in case.
    (await-fn (fn check-system-info []
                (let [overview (vc/call! conn "@SystemInformation" "OVERVIEW")]
                  (when (empty? overview)
                    (throw+ {:type ::empty-overview}))))
              {:log-message "Waiting for @SystemInformation"
               :log-interval 10000
               :retry-interval 1000
               :timeout 240000}))
  (info node "started"))

(defn await-rejoin
  "Blocks until the logfile reports 'Node rejoin completed'"
  [node]
  (info "Waiting for" node "to rejoin")
  (await-log "Node rejoin completed")
  (info node "rejoined"))

(defn start-daemon!
  "Starts the VoltDB daemon."
  [test]
          (c/cd base-dir
                (info "Starting voltdb")
                (cu/start-daemon! {:env (cond-> {:JAVA_HOME (db-java-home)}
                                          ; --byteman: attach the agent to the
                                          ; server JVM (bin/voltdb appends
                                          ; VOLTDB_OPTS to its JVM arguments)
                                          (:byteman test)
                                          (assoc :VOLTDB_OPTS
                                                 (byteman/agent-opts base-dir)))
                                   :logfile (str base-dir "/log/stdout.log")
                                   :pidfile pidfile
                                   :chdir   base-dir}
                                  "bin/voltdb"
                                  :start
                                  :--count (count (:nodes test))
                                  :--host (->> (:nodes test)
                                               (map cn/ip)
                                               (str/join ",")))))

(defn recover!
  "Restarts all nodes in the test."
  [test]
  (c/on-nodes test (partial db/start! (:db test))))

(defn rejoin!
  "Rejoins a voltdb node. Serialized to work around a bug in voltdb where
  multiple rejoins can take down cluster nodes."
  [test node]
  ; This bug has been fixed, so we probably don't need to lock here - KRK 2023
  (locking rejoin!
    (info "rejoining" node)
    (db/start! (:db test) test node)
    (await-rejoin node)))

(defn stop-recover!
  "Stops all nodes, then recovers all nodes. Useful when Volt's lost majority
  and nodes kill themselves."
  ([test]
   (c/on-nodes test (partial db/kill! (:db test)))
   (recover! test)))

(defn sql-cmd!
  "Takes an SQL query and runs it on the local node via sqlcmd"
  [query]
  (c/cd base-dir
      (c/exec (c/env {"JAVA_HOME" (db-java-home)
                      })
                      "bin/sqlcmd" (str "--query=" query))))

(defn download-first-jar!
  "Downloads the first jar in dir whose name matches re to procedures/, if
  there is one and we don't have it yet. jepsen 0.3.14's cu/ls takes a plain
  directory, so the glob is applied here."
  [dir re]
  (when-let [f (->> (cu/ls dir)
                    (filter #(re-find re %))
                    sort
                    first)]
    (let [src  (str dir "/" f)
          dest (io/file (str "procedures/" f))]
      (when-not (.exists dest)
        (info "Downloading" f "to" (.getCanonicalPath dest))
        (c/download src (.getCanonicalPath dest))))))

(defn snarf-procedure-deps!
  "Downloads the client jar, and the jar holding VoltProcedure, from the
  current node to procedures/, so we can compile stored procedures. Before
  15.0 VoltProcedure lived in voltdb/voltdb-*.jar; from 15.0 it is in
  lib/volt-procedure-*.jar. Whichever is present is taken."
  []
  (download-first-jar! (str base-dir "/voltdb") #"^voltdbclient-.*\.jar$")
  (download-first-jar! (str base-dir "/voltdb") #"^voltdb-.*\.jar$")
  (download-first-jar! (str base-dir "/lib")    #"^volt-procedure-.*\.jar$"))

(defn build-stored-procedures!
  "Compiles and packages stored procedures in procedures/"
  []
  (sh "mkdir" "obj" :dir "procedures/")
  ; javac runs on the control node (JDK 21 since jepsen 0.3.10). The
  ; procedures are still emitted as Java 8 class files so the same jar loads
  ; into any server kit the matrix runs, including the LTS 13.3.x line.
  ; JDK 21 accepts -source/-target 8 with a deprecation warning.
  (let [r (sh "bash" "-c" (str (System/getenv "JAVA_HOME") "/bin/javac -verbose -source 8 -target 8 -classpath " (str base-dir) "/lib/*.jar:" (str base-dir) "/voltdb/*.jar:./:./* -d ./obj *.java")
              :dir "procedures/")]
    (when-not (zero? (:exit r))
      (throw (RuntimeException. (str "STDOUT:\n" (:out r)
                                     "\n\nSTDERR:\n" (:err r))))))
  (let [r (sh "jar" "cvf" "jepsen-procedures.jar" "-C" "obj" "."
              :dir "procedures/")]
    (when-not (zero? (:exit r))
      (throw (RuntimeException. (str "STDOUT:\n" (:out r)
                                     "\n\nSTDERR:\n" (:err r)))))))

(def procedures-lock
  "Serializes building and uploading the stored procedures jar. A composed
  test (the dr workload's jepsen.role/db) has one primary per cluster, and
  each primary builds the jar in the same procedures/ directory on the control
  node. Concurrent `jar cvf` runs replace each other's output, so one fails
  with NoSuchFileException: jepsen-procedures.jar, or a node is sent a jar the
  other build is still rewriting (ENG-30359)."
  (Object.))

(defn upload-stored-procedures!
  "Uploads stored procedures jar."
  [node]
  (c/upload (.getCanonicalPath (io/file "procedures/jepsen-procedures.jar"))
            (str base-dir "/jepsen-procedures.jar"))
  (info node "stored procedures uploaded"))

(defn load-stored-procedures!
  "Load stored procedures into voltdb."
  [node]
  (sql-cmd! "load classes jepsen-procedures.jar")
  (info node "stored procedures loaded"))

(defn db
  "VoltDB around the given package tarball URL"
  [url force-download?]
  (reify db/DB
    (setup! [this test node]
      ; Download and unpack
      (install! node url force-download?)
      ; The Byteman jars and rules go in after the kit, which replaces base-dir
      (when (:byteman test)
        (byteman/install! base-dir))

      ; Prepare stored procedures in parallel
      (let [procedures (future (when (= node (jepsen/primary test))
                                 (locking procedures-lock
                                   (snarf-procedure-deps!)
                                   (build-stored-procedures!)
                                   (upload-stored-procedures! node))))]
        ; Boot
        (configure! test node)
        (db/start! this test node)
        (await-start node)
        ; Fail setup here, not mid-test, if the agent did not attach: listing
        ; the loaded rules needs the agent's listener to answer.
        (when (:byteman test)
          (info node "Byteman agent attached; loaded rules:"
                (byteman/list-rules base-dir (db-java-home))))

        ; Wait for convergence
        (jepsen/synchronize test 240)

        ; Finish procedures
        @procedures
        (when (= node (jepsen/primary test))
          (load-stored-procedures! node))))

    ; BZ TODO need proper shut-down before killing processes....
    (teardown! [this test node]
      (db/kill! this test node)
      (c/su
       (c/exec :rm :-rf (c/lit (str base-dir "/*"))))
      (vc/kill-reconnect-threads!))

    db/LogFiles
    (log-files [db test node]
      (let [export-files (list-export-files)]
        (info "Exported files " export-files)
        (concat
          [(str base-dir "/log/stdout.log")
           (str base-dir "/log/volt.log")
           (str base-dir "/deployment.xml")]
          export-files)))

    ; This is a debug version of "kill" to print debug info
    ; kill runs with pid taken from pidfile as in "cu/stop-deamon".
    ; Here we repeat this code to print PID
    db/Kill
    (kill! [this test node]
      (do
        (info "Killing VoltDB on node " node)
        (c/su
         (do
           ( if (cu/exists? pidfile)
             (let [pid (Long/parseLong (c/exec :cat pidfile))]
               (if (not (cu/daemon-running? pidfile))
                 (info "Proces with id " pid "does NOT exist. Shutdown is not needed")))
             (info "The pid file " pidfile "does NOT exist. Shutdown is not needed."))
           ;this cu/stop-daemon! function won't be successfull if pidfile does not contain the right pid for the volt process
           (cu/stop-daemon! pidfile)))))

    (start! [this test node]
      ;(start-daemon! test))
      (c/su
              ; Before running "start" check if a voltdb process already exists.
              ; If it exists, The linux "exec" command overwrites pid file while faling to restart volt.
              ; Afterwards, the pid file becomes wrong.
              ; Note that nemesis "kill" starts DB on all nodes, even those where voltdb has not been killed.
              ; On those nodes we must not restard voltdb.
       (if (and (cu/exists? pidfile) (cu/daemon-running? pidfile))
         (let [pid (Long/parseLong (c/exec :cat pidfile))]
           (info "The rocess with PID " pid "exists. Skipping starting VoltDB"))
         (do (info "Starting VoltDB on node " node)
             (start-daemon! test)))))

    ; SIGSTOP / SIGCONT the VoltDB server. jepsen 0.3.12 to 0.3.14 ship a
    ; control.util/grepkill! that sends no signal at all, so this uses pkill
    ; directly. 'placementgroup' only appears on the voltdb command line,
    ; which is also what the Jenkins script keys on. The bracketed pattern
    ; keeps pkill -f from matching the shell that runs it: with a plain
    ; pattern it SIGSTOPped its own parent bash, which then never returned
    ; and hung the test.
    db/Pause
    (pause! [this test node]
      (c/su (c/exec :bash :-c "pkill -STOP -f '[p]lacementgroup' || true")))

    (resume! [this test node]
      (c/su (c/exec :bash :-c "pkill -CONT -f '[p]lacementgroup' || true")))))
