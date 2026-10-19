(ns jepsen.voltdb.bank
  "Bank workload (jepsen.tests.bank): transfers between accounts in one
  multi-partition transaction, and reads of every balance. The checker
  requires every read to show the same total and, unless negative balances
  are allowed, no negative balance: a classic, cheap catch for lost or torn
  multi-partition writes and for reads that observe a transaction halfway."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [control :as c]]
            [jepsen.tests.bank :as bank]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb.client :as vc]))

(def table "accounts")

(defrecord Client [conn node initialized?]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :conn (vc/connect node (select-keys test [:procedure-call-timeout
                                                     :connection-response-timeout]))))

  (setup! [this test]
    (when (deliver initialized? true)
      (info node "creating" table)
      (c/on node
            (vc/with-race-retry
              (voltdb/sql-cmd! (str "CREATE TABLE " table " (
                                   id       BIGINT NOT NULL,
                                   balance  BIGINT NOT NULL,
                                   PRIMARY KEY (id)
                                   );
                                   PARTITION TABLE " table " ON COLUMN id;"))
              ; No PARTITION clause: from and to may be anywhere, so MP.
              (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                               jepsen.procedures.BankTransfer;")))
      ; The whole balance starts in the first account.
      (let [[first-account & others] (:accounts test)]
        (vc/call! conn "ACCOUNTS.insert" first-account (:total-amount test))
        (doseq [a others]
          (vc/call! conn "ACCOUNTS.insert" a 0)))
      (info node table "created and populated")))

  (invoke! [this test op]
    (case (:f op)
      :transfer (let [{:keys [from to amount]} (:value op)]
                  (vc/with-errors op false
                    (vc/call! conn "BankTransfer" from to amount
                              (if (:negative-balances? test) 1 0))
                    (assoc op :type :ok)))
      :read     (vc/with-errors op true
                  (->> (vc/ad-hoc! conn (str "SELECT id, balance FROM " table ";"))
                       first
                       :rows
                       (map (juxt :ID :BALANCE))
                       (into {})
                       (assoc op :type :ok, :value)))))

  (teardown! [this test])

  (close! [this test]
    (vc/close! conn)))

(defn workload
  "jepsen.tests.bank/test supplies :accounts, :total-amount, :max-transfer,
  the generator and the checker; the runner merges those keys into the test
  map, where the generator, checker and this client read them."
  [opts]
  (assoc (bank/test {:negative-balances? (boolean (:negative-balances opts))})
         :client (map->Client {:initialized? (promise)})))
