(ns jepsen.voltdb.single
  "Implements a table of single registers identified by id. Verifies
  linearizability over independent registers."
  (:require [jepsen [core         :as jepsen]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [os           :as os]
                    [tests        :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.voltdb        :as voltdb]
            [jepsen.voltdb [client :as vc]]
            [knossos.model        :as model]
            [clojure.string       :as str]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]))

(defn client
  "A single-register client. Options:

      :strong-reads                 Whether to perform normal or strong selects
      :procedure-call-timeout       How long in ms to wait for proc calls
      :connection-response-timeout  How long in ms to wait for connections"
  ([opts] (client nil nil (promise) opts))
  ([conn node initialized? opts]
   (reify client/Client
     (open! [_ test node]
       (let [conn (vc/connect
                    node (select-keys opts
                                      [:procedure-call-timeout
                                       :connection-response-timeout]))]
         (client conn node initialized? opts)))

     (setup! [_ test]
       (when (deliver initialized? true)
         (info node "creating table")
         (vc/with-race-retry
           (c/on node
                 ; Create table
                 (voltdb/sql-cmd! "CREATE TABLE registers (
                                  id          INTEGER UNIQUE NOT NULL,
                                  value       INTEGER NOT NULL,
                                  PRIMARY KEY (id)
                                  );
                                  PARTITION TABLE registers ON COLUMN id;")
                 (voltdb/sql-cmd! "CREATE PROCEDURE registers_cas
                                  PARTITION ON TABLE registers COLUMN id
                                  AS
                                  UPDATE registers SET value = ?
                                  WHERE id = ? AND value = ?;")
                 (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                                  jepsen.procedures.SRegisterStrongRead;")
                 (voltdb/sql-cmd! "PARTITION PROCEDURE SRegisterStrongRead
                                  ON TABLE registers COLUMN id;")))
         (info node "table created")))

     (invoke! [this test op]
       ;(info "Process " (:process op) "using node" node)
       (try
         (let [id     (key (:value op))
               value  (val (:value op))]
           (case (:f op)
             :read   (let [proc (if (:strong-reads opts)
                                  "SRegisterStrongRead"
                                  "REGISTERS.select")
                           v (-> conn
                                 (vc/call! proc id)
                                 first
                                 :rows
                                 first
                                 :VALUE)]
                       (assoc op
                              :type :ok
                              :value (independent/tuple id v)))
             :write (do (vc/call! conn "REGISTERS.upsert" id value)
                        (assoc op :type :ok))
             :cas   (let [[v v'] value
                          res (-> conn
                                  (vc/call! "registers_cas" v' id v)
                                  first
                                  :rows
                                  first
                                  :modified_tuples)]
                      (assert (#{0 1} res))
                      (assoc op :type (if (zero? res) :fail :ok)))))
         ;(catch org.voltdb.client.NoConnectionsException e
         ;  (Thread/sleep 1000)
         ;  (assoc op :type :fail, :error :no-conns))
         (catch org.voltdb.client.ProcCallException e
           (let [type (if (= :read (:f op)) :fail :info)]
             (condp re-find (.getMessage e)
               #"^No response received in the allotted time"
               (assoc op :type type, :error :timeout)

               #"^Connection to database host .+ was lost before a response"
               (assoc op :type type, :error :conn-lost)

               #"^Transaction dropped due to change in mastership"
               (assoc op :type type, :error :mastership-change)

               (throw e))))))

     (teardown! [_ test])

     (close! [_ test]
       (vc/close! conn)))))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Takes CLI options and constructs a workload map. Special options

      :strong-reads                 Whether to perform normal or strong selects
      :no-reads                     Don't bother with reads at all
      :procedure-call-timeout       How long in ms to wait for proc calls
      :connection-response-timeout  How long in ms to wait for connections"
  [opts]
  (let [n (count (:nodes opts))]
    {:client  (client (select-keys opts [:strong-reads
                                         :procedure-call-timeout
                                         :connection-response-timeout]))
     :checker (checker/compose
                {:linear   (independent/checker
                             (checker/linearizable
                               {:model (model/cas-register nil)}))
                 :timeline (independent/checker (timeline/html))})
     :generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn per-key [id]
                    ; First n processes do writes/cas, the others do reads
                    ; (or cas, if no-reads is true).
                    (->> (gen/mix [w cas])
                         (gen/reserve n (if (:no-reads opts)
                                          (gen/stagger 2 cas)
                                          r))
                         (gen/limit 150))))}))
