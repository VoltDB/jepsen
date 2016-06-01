(ns jepsen.voltdb
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [tests        :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [knossos.model        :as model]
            [clojure.data.xml     :as xml]
            [clojure.string       :as str]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.tools.logging :refer [info warn]])
  (:import (org.voltdb VoltTable
                       VoltType
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse)))

(def username "voltdb")
(def base-dir "/opt/voltdb")

(defn install!
  "Install the given tarball URL"
  [node url]
  (c/su
    (debian/install-jdk8!)
    (info "JDK8 installed")
    (cu/install-tarball! node url base-dir)
    (cu/ensure-user! username)
    (c/exec :chown :-R (str username ":" username) base-dir)
    (info "VoltDB unpacked")))

(defn deployment-xml
  "Generate a deployment.xml string for the given test."
  [test]
  (xml/emit-str
    (xml/sexp-as-element
      [:deployment {}
       [:cluster {:hostcount (count (:nodes test))
                  :kfactor (:k-factor test (dec (count (:nodes test))))}]
       [:paths {}
        [:voltdbroot {:path base-dir}]]
       [:heartbeat {:timeout 5}] ; seconds
       [:commandlog {:enabled true, :synchronous true}
        [:frequency {:time 2}]]]))) ; milliseconds

(defn configure!
  "Prepares config files and creates fresh DB."
  [test node]
  (c/sudo username
        (c/cd base-dir
              (c/exec :echo (deployment-xml test) :> "deployment.xml"))))

(defn close!
  "Calls c.close"
  [^Client c]
  (.close c))

(defn up?
  "Is the given node ready to accept connections? Returns node, or nil."
  [node]
  (let [config (ClientConfig. "" "")]
    (.setProcedureCallTimeout config 100)
    (.setConnectionResponseTimeout config 100)

    (let [c (ClientFactory/createClient config)]
      (try
        (.createConnection c (name node))
        (.getInstanceId c)
        node
      (catch java.net.ConnectException e)
      (finally (close! c))))))

(defn up-nodes
  "What DB nodes are actually alive?"
  [test]
  (remove nil? (pmap up? (:nodes test))))

(defn await-initialization
  "Blocks until the logfile reports 'Server completed initialization'."
  [node]
  (info "Waiting for" node "to initialize")
  (c/cd base-dir
        ; hack hack hack
        (Thread/sleep 5000)
        (c/exec :tail :-n 1 :-f "log/volt.log"
                | :grep :-m 1 "completed initialization"
                | :xargs (c/lit "echo \"\" >> log/volt.log \\;")))
  (info node "initialized"))

(defn await-rejoin
  "Blocks until the logfile reports 'Node rejoin completed'"
  [node]
  (info "Waiting for" node "to rejoin")
  (c/cd base-dir
        ; hack hack hack
        (Thread/sleep 5000)
        (c/exec :tail :-n 1 :-f "log/volt.log"
                | :grep :-m 1 "Node rejoin completed"
                | :xargs (c/lit "echo \"\" >> log/volt.log \\;")))
  (info node "rejoined"))

(defn start-daemon!
  "Starts the daemon with the given command."
  [test cmd host]
  (c/sudo username
    (cu/start-daemon! {:logfile (str base-dir "/stdout.log")
                       :pidfile (str base-dir "/pidfile")
                       :chdir   base-dir}
                      (str base-dir "/bin/voltdb")
                      cmd
                      :--deployment (str base-dir "/deployment.xml")
                      :--host host)))

(defn start!
  "Starts voltdb, creating a fresh DB"
  [test node]
  (start-daemon! test :create (jepsen/primary test))
  (await-initialization node))

(defn recover!
  "Recovers an entire cluster, or with a node, a single node."
  ([test]
   (jepsen.core/on-nodes test recover!))
  ([test node]
   (info "recovering" node)
   (start-daemon! test :recover (jepsen/primary test))
   (await-initialization node)))

(defn rejoin!
  "Rejoins a voltdb node. Serialized to work around a bug in voltdb where
  multiple rejoins can take down cluster nodes."
  [test node]
  (locking rejoin!
    (info "rejoining" node)
    (start-daemon! test :rejoin (rand-nth (up-nodes test)))
    (await-rejoin node)))

(defn stop!
  "Stops voltdb"
  [test node]
  (c/su
    (cu/stop-daemon! (str base-dir "/pidfile"))))

(defn stop-recover!
  "Stops all nodes, then recovers all nodes. Useful when Volt's lost majority
  and nodes kill themselves."
  ([test]
   (jepsen.core/on-nodes test stop!)
   (jepsen.core/on-nodes test recover!)))

(defn sql-cmd!
  "Takes an SQL query and runs it on the local node via sqlcmd"
  [query]
  (c/cd base-dir
        (c/sudo username
                (c/exec "bin/sqlcmd" (str "--query=" query)))))

(defn snarf-procedure-deps!
  "Downloads voltdb.jar from the current node to procedures/, so we can compile
  stored procedures."
  []
  (let [dir  (str base-dir "/voltdb/")
        f    (first (c/cd dir (cu/ls (c/lit "voltdb-*.jar"))))
        src  (str dir f)
        dest (io/file (str "procedures/" f))]
    (when-not (.exists dest)
      (info "Downloading" f "to" (.getCanonicalPath dest))
      (c/download src (.getCanonicalPath dest)))))

(defn build-stored-procedures!
  "Compiles and packages stored procedures in procedures/"
  []
  (sh "mkdir" "obj" :dir "procedures/")
  (let [r (sh "bash" "-c" "javac -classpath \"./:./*\" -d ./obj *.java"
              :dir "procedures/")]
    (when-not (zero? (:exit r))
      (throw (RuntimeException. (str "STDOUT:\n" (:out r)
                                     "\n\nSTDERR:\n" (:err r))))))
  (let [r (sh "jar" "cvf" "jepsen-procedures.jar" "-C" "obj" "."
              :dir "procedures/")]
    (when-not (zero? (:exit r))
      (throw (RuntimeException. (str "STDOUT:\n" (:out r)
                                     "\n\nSTDERR:\n" (:err r)))))))

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

(defn kill-reconnect-threads!
  "VoltDB client leaks reconnect threads; this kills them all."
  []
  (doseq [t (keys (Thread/getAllStackTraces))]
    (when (= "Retry Connection" (.getName t))
      ; The reconnect loop swallows Exception so we can't even use interrupt
      ; here. Luckily I don't think it has too many locks we have to worry
      ; about.
      (.stop t))))

(defn db
  "VoltDB around the given package tarball URL"
  [url]
  (reify db/DB
    (setup! [_ test node]
      ; Download and unpack
      (install! node url)

      ; Prepare stored procedures in parallel
      (let [procedures (future (when (= node (jepsen/primary test))
                                 (snarf-procedure-deps!)
                                 (build-stored-procedures!)
                                 (upload-stored-procedures! node)))]
        ; Boot
        (configure! test node)
        (start! test node)

        ; Wait for convergence
        (jepsen/synchronize test)

        ; Finish procedures
        @procedures
        (when (= node (jepsen/primary test))
          (load-stored-procedures! node))))

    (teardown! [_ test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf (c/lit (str base-dir "/*"))))
      (kill-reconnect-threads!))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/log/volt.log")])))


(defn connect
  "Opens a connection to the given node and returns a voltdb client. Options:

      :procedure-call-timeout
      :connection-response-timeout"
  ([node]
   (connect node {}))
  ([node opts]
   (let [opts (merge {:procedure-call-timeout 1000
                      :connection-response-timeout 1000}
                     opts)]
     (-> (doto (ClientConfig. "" "")
           (.setReconnectOnConnectionLoss true)
           (.setProcedureCallTimeout (:procedure-call-timeout opts))
           (.setConnectionResponseTimeout (:connection-response-timeout opts)))
         (ClientFactory/createClient)
         (doto
           (.createConnection (name node)))))))

(defn volt-table->map
  "Converts a VoltDB table to a data structure like

  {:status status-code
   :schema [{:column_name VoltType, ...}]
   :rows [{:k1 v1, :k2 v2}, ...]}"
  [^VoltTable t]
  (let [column-count (.getColumnCount t)
        column-names (loop [i     0
                            cols  (transient [])]
                       (if (= i column-count)
                         (persistent! cols)
                         (recur (inc i)
                                (conj! cols (keyword (.getColumnName t i))))))
        basis        (apply create-struct column-names)
        column-types (loop [i 0
                            types (transient [])]
                       (if (= i column-count)
                         (persistent! types)
                         (recur (inc i)
                                (conj! types (.getColumnType t i)))))
        row          (doto (.cloneRow t)
                       (.resetRowPosition))]
  {:status (.getStatusCode t)
   :schema (apply struct basis column-types)
   :rows (loop [rows (transient [])]
           (if (.advanceRow row)
             (let [cols (object-array column-count)]
               (loop [j 0]
                 (when (< j column-count)
                   (aset cols j (.get row j ^VoltType (nth column-types j)))
                   (recur (inc j))))
               (recur (conj! rows (clojure.lang.PersistentStructMap/construct
                                    basis
                                    (seq cols)))))
             ; Done
             (persistent! rows)))}))

(defn call!
  "Call a stored procedure and returns a seq of VoltTable results."
  [^Client client procedure & args]
  (let [res (.callProcedure client procedure (into-array Object args))]
    ; Docs claim callProcedure will throw, but tutorial checks anyway so ???
    (assert (= (.getStatus res) ClientResponse/SUCCESS))
    (map volt-table->map (.getResults res))))

(defn ad-hoc!
  "Run an ad-hoc SQL stored procedure."
  [client & args]
  (apply call! client "@AdHoc" args))

;; Nemeses

(defn isolated-killer-nemesis
  "A nemesis which partitions away single nodes, then kills them. On :stop,
  rejoins the dead nodes."
  []
  (nemesis/node-start-stopper
    #(take (rand-int (/ 2 (count %))) (shuffle %))
    (fn [test node]
      ; Isolate this node
      (nemesis/partition! test
                          (nemesis/complete-grudge
                            [[node] (remove #{node} (:nodes test))]))
      (info "Partitioned away" node)
      (Thread/sleep 5000)
      (stop! test node)
      :killed)
    (fn [test node]
      (net/heal! (:net test) test)
      (info "Network healed")
      (rejoin! test node)
      :rejoined)))

(defn recover-nemesis
  "A nemesis which responds to :recover ops by killing and recovering all nodes
  in the test."
  []
  (reify client/Client
    (setup! [this test _] this)

    (invoke! [this test op]
      (assoc op :type :info, :value
             (case (:f op)
               :recover (do (stop-recover! test)
                            [:recovered (:nodes test)]))))

    (teardown! [this test])))

(defn with-recover-nemesis
  "Merges a recovery nemesis into another nemesis, handling :recover ops by
  killing and recovering the cluster, and all others passed through to the
  given nemesis."
  [nem]
  (nemesis/compose {#{:recover}                 (recover-nemesis)
                    #(when (not= :recover %) %) nem}))

;; Generators

(defn final-recovery
  "A generator which emits a :stop, followed by a :recover, for the nemesis,
  then sleeps to allow clients to reconnect."
  []
  (gen/phases
    (gen/nemesis (gen/once {:type :info :f :stop}))
    (gen/log "Recovering cluster")
    (gen/nemesis
      (gen/once {:type :info, :f :recover}))
    (gen/log "Waiting for reconnects")
    (gen/sleep 10)))
