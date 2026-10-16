(ns jepsen.voltdb.client
  "A wrapper around the VoltDB client library. Includes support functions for
  opening and closing clients, converting datatypes, handling errors, etc."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.voltdb VoltTable
                       VoltType
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse
                              ProcedureCallback)))

(defn close!
  "Closes a client."
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

(defn kill-reconnect-threads!
  "VoltDB client leaks reconnect threads; this kills them all."
  []
  (doseq [t (keys (Thread/getAllStackTraces))]
    (when (= "Retry Connection" (.getName t))
      ; The reconnect loop swallows Exception so we can't even use interrupt
      ; here. Luckily I don't think it has too many locks we have to worry
      ; about.
      (.stop t))))

(defn connect
  "Opens a connection to the given node and returns a voltdb client. Options:

      :reconnect?
      :procedure-call-timeout
      :connection-response-timeout"
  ([node]
   (connect node {}))
  ([node opts]
   (let [opts (merge {:procedure-call-timeout 100
                      :connection-response-timeout 1000}
                     opts)
         config (doto (ClientConfig. "" "")
                  ; We don't want to try and connect to all nodes
                  (.setTopologyChangeAware false)
                  (.setProcedureCallTimeout (:procedure-call-timeout opts))
                  (.setConnectionResponseTimeout (:connection-response-timeout opts)))
         client (ClientFactory/createClient config)]
     (try
       (.createConnection client (name node))
       client
       (catch Throwable t
         (.close client)
         (throw t))))))

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

(defn async-call!
  "Call a stored procedure asynchronously. Returns a promise of a seq of
  VoltTable results. If a final fn is given, passes ClientResponse to that fn."
  [^Client client procedure & args]
  (let [p (promise)]
    (.callProcedure client
                    (reify ProcedureCallback
                      (clientCallback [this res]
                        (when (fn? (last args))
                          ((last args) res))
                        (deliver p (map volt-table->map (.getResults res)))))
                    procedure
                    (into-array Object (if (fn? (last args))
                                         (butlast args)
                                         args)))))

(defn proc-call-error
  "Maps a ProcCallException to the :error keyword we record in the history:
  :timeout, :conn-lost, :mastership-change or :user-abort, or the raw message
  for anything else."
  [^org.voltdb.client.ProcCallException e]
  (let [m (or (.getMessage e) "")]
    (condp re-find m
      #"^No response received in the allotted time"           :timeout
      #"^Connection to database host .+ was lost before a response" :conn-lost
      #"^Transaction dropped due to change in mastership"      :mastership-change
      #"(?i)user abort|VoltAbortException"                     :user-abort
      m)))

(defmacro with-errors
  "Runs body, which should return a completed op, and converts VoltDB client
  exceptions into :fail or :info completions of op. A failed call whose
  outcome is known (no connection was available, so nothing was sent, or the
  server aborted the transaction) is :fail; a timeout or lost connection on a
  call that may have written is :info unless read-only? is true, in which case
  it is :fail too, since a read that didn't complete changed nothing."
  [op read-only? & body]
  `(try ~@body
        (catch org.voltdb.client.NoConnectionsException e#
          (assoc ~op :type :fail, :error :no-conns))
        (catch org.voltdb.client.ProcCallException e#
          (let [err# (proc-call-error e#)]
            (assoc ~op
                   :type (if (or ~read-only? (= :user-abort err#)) :fail :info)
                   :error err#)))))

(defn ad-hoc!
  "Run an ad-hoc SQL stored procedure."
  [client & args]
  (apply call! client "@AdHoc" args))

(defmacro with-race-retry
  "If you try to perform DDL concurrently using @AdHoc, Volt tends to complain:
  Invalid catalog update(@AdHoc) request: Can't do catalog update(@AdHoc) while
  another one is in progress. Please retry catalog update(@AdHoc) later. This
  macro performs exponential backoff and retry of its body, catching that
  specific error."
  [& body]
  `(loop [tries# 10
          delay# 10]
     (let [r# (try+ ~@body
                    ; This branch catches errors thrown by the sql-cmd! shell
                    ; wrapper. Later we should add one for the actual client.
                    (catch [:type :jepsen.control/nonzero-exit, :exit 255] e#
                      (info "with-race-retry caught")
                      (if (and (pos? tries#)
                               (re-find #"Can't do catalog update.+ while another one is in progress" (:err e#)))
                        ::retry
                        (throw+ e#))))]
       (if (= r# ::retry)
         ; Delay rises by a factor of 1-2 each time
         (let [delay'# (* delay# (+ 1 (rand)))]
           (info "Sleeping for" delay'# "ms")
           (Thread/sleep delay'#)
           (recur (dec tries#) delay'#))
         ; Done
         r#))))
