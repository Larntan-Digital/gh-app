(ns ghas-acs.db
	(:require
		[cheshire.core :refer [generate-string parse-string]]
		[clojure.tools.logging :as log]
		[next.jdbc.date-time]
		[next.jdbc.prepare]
		[next.jdbc.result-set]
		[conman.core :as conman]
		[ghas-acs.config :refer [env]]
		[mount.core :refer [defstate]])
	(:import (org.postgresql.util PGobject)))


(defstate ^:dynamic *db*
					:start (if-let [jdbc-url (env :database-url)]
									 (conman/connect! (conj {:jdbc-url jdbc-url :driver-class-name "org.postgresql.Driver"}
																					(env :database-pool)))

									 (do
										 (log/warn "database connection URL was not found, please set :database-url in your config, e.g: dev-config.edn")
										 *db*))
					:stop (conman/disconnect! *db*))

(conman/bind-connection *db* "sql/queries.sql")


(def service-charge-rates (atom nil))
(def max-allowed-balances (atom nil))
(def loan-periods (atom nil))

(defn serviceq-post-charged? [charging-method]
	(condp = charging-method
		:pre   false
		:post  true
		:postg true
		(throw (Exception. (format "!unknownParamValue(charging-method, value=%s)" charging-method)))))

(defn serviceq-after-grace-period? [charging-method]
	(condp = charging-method
		:pre   false
		:post  false
		:postg true))

(defn load-denom-config []
	(reset! service-charge-rates (get-denoms))
	(reset! max-allowed-balances (when-not (empty?  (get-loan-max-bals))
																 (doall (reduce (fn [ret [loan-type amount max-bal]]
																									(assoc ret [(keyword loan-type) amount] max-bal))
																								{}  (get-loan-max-bals)))))

	(reset! loan-periods (get-loan-periods)))


(defn getService [sq [loan_type amount]]
	(loop [v (first sq)
		   vx (next sq)]
		(if (nil? v)
			(throw (ex-info "No matching service fee"
					   {:error-class :no-matching-service-fee :amount amount
						:loan_type loan_type}))
			(let [loan_typ (:loan_type v)
				  loan_amt (:loanable_amount v)
				  serviceq (:nullif v)
				  serviceq-amt (:nullif_2 v)]
				(if (and (= (keyword loan_typ) loan_type)
						(= loan_amt amount))
					(cond (and serviceq serviceq-amt) nil
						serviceq-amt serviceq-amt
						serviceq  (* amount (/ serviceq 100)))
					(recur (first vx) (next vx)))))))


(defn getLoanPeriod [ld [loan_type amount]]
	(loop [v (first ld)
				 vx (next ld)]
		(if (nil? v)
			(throw (ex-info "No matching loan period"
											{:error-class :no-matching-loan-period :amount amount :loan_type loan_type}))
			(let [loan_typ (:loan_type v)
						loan_amt (:loanable_amount v)
						date_part (:date_part v)]
				(if (and (= (keyword loan_typ) loan_type)
								 (= loan_amt amount))
					v
					(recur (first vx) (next vx)))))))




(defn get-net-amount
	[loan-type requested-amount charging-method]
	(condp = charging-method
		:pre (if-let [serviceq (getService @service-charge-rates [loan-type requested-amount])]
						(- requested-amount serviceq)
						(throw (Exception. (format "Unknown loan amount %s"
																			 {:error-class :unknown-loan-amount
																				:loan-type loan-type :amount requested-amount}))))
		:post requested-amount))


(defn get-gross-amount
	[loan-type requested-amount charging-method]
	(condp = charging-method
		:pre   requested-amount
		:post  (if-let [serviceq (getService @service-charge-rates [loan-type requested-amount])]
							(+ requested-amount serviceq)
							(throw (ex-info "Unknown gross amount."
															{:error-class :unknown-loan-amount
															 :loan-type loan-type :amount requested-amount})))))



(defn get-fee-amount
	([loan-type requested-amount]
	 (if-let [serviceq (getService @service-charge-rates [(keyword loan-type) requested-amount])]
		 serviceq
		 (throw (ex-info "Unknown fee amount."
										 {:error-class :unknown-loan-amount
											:loan-type (keyword loan-type)  :amount requested-amount})))))


(defn get-loan-period [loan-type requested-amount]
	(if-let [ld 	(getLoanPeriod @loan-periods [(keyword loan-type) requested-amount])]
		ld
		(throw (ex-info "Unknown loan period."
										{:error-class :unknown-loan-period
										 :loan-type (keyword loan-type)  :amount requested-amount}))))

(defn get-subscriber-info [values]
	(reduce conj {} (proc-get-subscriber-info values)))

(defn check-unreconciled-loan [values]
	(reduce conj {} (check-unreconciled-sub values)))

(defn update-tbl-loan-req [values]
	(updateLoanReq values))



(defn insert-tbl-loan-req [values]
	(insertTblLoanReq values))

(defn new-credit-request [values]
	(insertVtopCreditReq values))

(defn updateVtopReqStatus [values]
	(procUpdateVtopReqStatus values))



(defn startLendingRecon []
	(proc_payment_gateway_reconcile))


;;sessions
(defn newSession [values]
	(new-session! values))

(defn getSessionData [value]
	(reduce conj {} (%get-session-data value)))

(defn updateSession [values]
	(log/infof "updateSession values %s"values)
	(%update-session! values))

(defn clearExpiredSessions []
	(clear-expired-sessions!))

(defn closeSession [values]
	(close-session! values))


(defn updateDataStatus [values]
	(proc_update_data_status values))

(defn selectLoan [values]
	(select-loan values))

(defn pgobj->clj [^org.postgresql.util.PGobject pgobj]
	(let [type (.getType pgobj)
				value (.getValue pgobj)]
		(case type
			"json" (parse-string value true)
			"jsonb" (parse-string value true)
			"citext" (str value)
			value)))

(extend-protocol next.jdbc.result-set/ReadableColumn
	java.sql.Timestamp
	(read-column-by-label [^java.sql.Timestamp v _]
		(.toLocalDateTime v))
	(read-column-by-index [^java.sql.Timestamp v _2 _3]
		(.toLocalDateTime v))
	java.sql.Date
	(read-column-by-label [^java.sql.Date v _]
		(.toLocalDate v))
	(read-column-by-index [^java.sql.Date v _2 _3]
		(.toLocalDate v))
	java.sql.Time
	(read-column-by-label [^java.sql.Time v _]
		(.toLocalTime v))
	(read-column-by-index [^java.sql.Time v _2 _3]
		(.toLocalTime v))
	java.sql.Array
	(read-column-by-label [^java.sql.Array v _]
		(vec (.getArray v)))
	(read-column-by-index [^java.sql.Array v _2 _3]
		(vec (.getArray v)))
	org.postgresql.util.PGobject
	(read-column-by-label [^org.postgresql.util.PGobject pgobj _]
		(pgobj->clj pgobj))
	(read-column-by-index [^org.postgresql.util.PGobject pgobj _2 _3]
		(pgobj->clj pgobj)))

(defn clj->jsonb-pgobj [value]
	(doto (PGobject.)
		(.setType "jsonb")
		(.setValue (generate-string value))))

(extend-protocol next.jdbc.prepare/SettableParameter
	clojure.lang.IPersistentMap
	(set-parameter [^clojure.lang.IPersistentMap v ^java.sql.PreparedStatement stmt ^long idx]
		(.setObject stmt idx (clj->jsonb-pgobj v)))
	clojure.lang.IPersistentVector
	(set-parameter [^clojure.lang.IPersistentVector v ^java.sql.PreparedStatement stmt ^long idx]
		(let [conn      (.getConnection stmt)
					meta      (.getParameterMetaData stmt)
					type-name (.getParameterTypeName meta idx)]
			(if-let [elem-type (when (= (first type-name) \_)
													 (apply str (rest type-name)))]
				(.setObject stmt idx (.createArrayOf conn elem-type (to-array v)))
				(.setObject stmt idx (clj->jsonb-pgobj v))))))
