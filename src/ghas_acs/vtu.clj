(ns ghas-acs.vtu
			[:require [ghas-acs.utils :as utils]
								[clojure.tools.logging :as log]
								[clojure.data.json :as json]
								[ghas-acs.config :refer [env]]
								[ghas-acs.db :as db]
								[clj-http.client :as http :exclude get]
								[ghas-acs.rmqutils :as rmqutils]
								[clj-time.format :as f]
								[clj-time.local :as l]
								[clojure.string :as str]
								[clojure.zip :as zip]
								[clojure.xml :as xml]
								[clojure.data.zip.xml :as zip-xml :only [xml1 text]]
								[ghas-acs.counters :as counters]
								[clojure.core.async :as async]]
			(:use [ghas-acs.counters])
			(:import (org.joda.time.format DateTimeFormat)
							 (java.io ByteArrayInputStream File)
							 (java.net SocketTimeoutException ConnectException ProtocolException)
							 (java.util.concurrent TimeoutException)))


	(def status-value
			 "status 0 means successful and loan is set in db,
			 1 means failed and sub is removed from unreconciled state
			 2 means sub is in unreconciled state"
			 {:ok						0
				:succeeded			0
				:failed 				1
				:to-reconcile 	2})



	;; -------
	;;  PG Utilities.
	(let [known-error? #{:pg-error :low-balance :malformed-resp
											 :pg-protocol-error :dup-txn-succeeded :bad-amount
											 :txn-disallowed :dup-txn-failed :charging-system-error
											 :txn-limit-exceeded :bad-sub-status :bad-msisdn
											 :all-accounts-disabled               ;;used when all accounts are disabled
											 ;; ---
											 :connection-refused :timeout-on-connect :timeout-on-read
											 :socket-error :io-error :verification-failed}

				verified-failure? #{:connection-refused
														;:timeout-on-connect ;;this might not be a verified failure
														;; ---
														:pg-error
														:bucket-empty
														:low-balance
														:verification-failed
														;;:malformed-resp ;;due to paymentgateway sending us wrong vend responses we cannot gurantee it's a failure
														:pg-protocol-error
														:dup-txn-succeeded
														:bad-amount
														:txn-disallowed
														:dup-txn-failed
														;:charging-system-error -- not sure
														:txn-limit-exceeded
														:bad-sub-status
														:bad-msisdn
														:all-accounts-disabled               ;;used when all accounts are disabled
														}]
			 (defn need-reconciliation? [error-class]
				 (if (verified-failure? error-class) false true))
			 (defn check-failure [error-class]
				 (if (verified-failure? error-class) true false)))


	(defn- get-status-kwd [status]
				 (condp = status
					 "06" :pg-protocol-error
					 "11" :dup-txn-succeeded
					 "12" :bucket-empty
					 "55" :verification-failed
					 "57" :txn-disallowed
					 "94" :dup-txn-failed
					 "96" :charging-system-error
					 "98" :txn-limit-exceeded
					 "A8" :bad-sub-status
					 "A9" :bad-msisdn
					 status))



	(def pg-cred-count (atom 0))

	(defn pg-get-needed-cred []
				(let [pg (str/split (get-in env [:pg :pg-credentials]) #",")
							v (get pg @pg-cred-count)]
					;;(println v)
					(when (> (count pg) 1)
						(if (= @pg-cred-count (- (count pg) 1))
							(reset! pg-cred-count 0)
							(swap! pg-cred-count inc))) v))


	(defn- get-data-bundle-id [amount]
				 (let [datab  (str/split (get-in env [:pg :pg-data-bundles]) #",") ]
					 (log/debugf "pg data bundles %s|%s" datab amount)
					 (loop [n (first datab)
									nx (next datab)]
						 (if (nil? n)
							 (throw (ex-info "No matching bundle id for amount."
															 {:error-class :no-matching-bundle-id :amount amount}))
							 (let [split-d (str/split n #"=")
										 z (utils/toInt (get split-d 0))
										 amt (utils/toInt amount)]
								 (if (= amt z)
									 (get split-d 1)
									 (recur (first nx) (next nx))))))))

	;


	(defn success-credit [request-id subscriber loan-type amount response-data %amount status]
				(swap! *countof-pg-succeeded* inc)
				(log/info (format "CallPGresp(%s,%s,%s,%s) -> [pg amount: %s] %s %s "
													request-id subscriber loan-type amount %amount status response-data))
				(let [{txnid :txnid balance :balance resp-status :resp-status resp-code :response-code} response-data]
					{:status :ok :error-status :no-error :external-response-code resp-status :external-txn-id txnid
					 :post-event-balance balance}))

	(defn failed-credit [request-id  subscriber amount message http-status response-data]
				(let [_ (swap! *countof-pg-failed* inc)
							{txnid :txnid balance :balance resp-status :resp-status resp-code :response-code error-class :error-class} response-data
							check-recon (need-reconciliation? (get-status-kwd resp-status))
							status (if check-recon :to-reconcile :failed)]
					(log/error (format "cannotCallPG(%s,%s,%s) -> [%s] [status: %s] %s "
														 request-id subscriber amount message http-status response-data))
					#_(finally
							(let []
								(set-credit-request-status request-id
																					 (if (rpc/need-reconciliation? error-class)
																						 :to-reconcile :failed)
																					 error-class nil status txnid balance)))
					;status error-status external-response-code external-txn-id post-event-balance
					{:status status :error-class error-class :error-status resp-status :error-msg (get-status-kwd resp-status) :external-response-code resp-status :external-txn-id txnid
					 :post-event-balance balance}))

	(defn- parse-pg-response [content]
				 (let [response (xml/parse (ByteArrayInputStream. (.getBytes content "UTF-8")))
							 response (zip/xml-zip response)]
					 {:subscriber    (zip-xml/xml1-> response :env:Body :VendResponse :DestAccount zip-xml/text)
						:amount        (zip-xml/xml1-> response :env:Body :VendResponse :Amount zip-xml/text)
						:status        (zip-xml/xml1-> response :env:Body :VendResponse :StatusId zip-xml/text)
						:response-code (zip-xml/xml1-> response :env:Body :VendResponse :ResponseCode zip-xml/text)
						:txnid         (zip-xml/xml1-> response :env:Body :VendResponse :TxRefId zip-xml/text)
						:balance       nil}))
	;



	(defn pg-credit-sub [request-id subscriber loan-type amount]
		;; An amount mismatch doesn't result in a failure it implies a
		;; promotion is happening. The normal events occur for a successull
		;; transaction however an additional entry is made to
		;; tbl_airtime_bonuses taking note.
				(let [pg-cred 			 					(pg-get-needed-cred) ; Get the next enabled account.
							pgdetails								(str/split pg-cred #"-")
							pgId										(get pgdetails 0)
							pgPass									(get pgdetails 1)
							request-xml             (format (slurp (File. (get-in env [:pg :pg-credit-request-xml]))) (utils/validate-sub subscriber) (utils/cedis amount)
																							(condp = loan-type
																								:airtime (get-in env [:pg :msg-lending-from-paymentgateway])
																								:data    (get-data-bundle-id (str amount)))
																							(str (condp = loan-type
																										 :airtime "BMC-"
																										 :data    "BMD-")
																									 request-id)
																							pgId pgPass)
							fullUrl  (get-in env [:pg :pg-account-url])
							request (str/trim request-xml)
							options  {:user-agent	"ERL/http"
												:timeout    (get-in env [:pg :pg-timeout-credit])
												:timeout-connect (get-in env [:pg :pg-timeout-credit])
												:timeout-read (get-in env [:pg :pg-timeout-credit])
												:body       request
												:headers    {"Content-Type" "application/xml"}}
							_ (log/infof "PG Request %s|%s|%s" fullUrl options request)
							loginfo-req             {:sub subscriber :amt amount :txid request-id}
							{:keys [status body error] :as response} (utils/with-func-timed (format "CallPG (args=%s)" loginfo-req)
																																							(fn [error]
																																								{:http-status :failed
																																								 :body loginfo-req
																																								 :error error})
																																							(http/post fullUrl options))]

					(if error
						(let [inst (condp instance? error
												 TimeoutException :timeout-on-connect
												 ConnectException :connection-refused
												 SocketTimeoutException	:timeout-on-read
												 ProtocolException :http-protocol-error
												 Exception :other-exception)]
							(failed-credit request-id subscriber amount (.getMessage error) status {:status inst :resp-status inst :error-class (.getMessage error)}))


						(let [result (parse-pg-response body)]
							(log/infof "Contents of results [%s]" (into {} result))
							;; Validate RPC result.
							(let [{%subscriber :subscriber
										 %amount     :amount
										 ;; ---
										 %balance    :balance
										 %txnid      :txnid
										 %status     :status
										 %ret        :response-code} result
										response-data         				{:txnid %txnid :balance %balance :resp-status %status :response-code %ret}]
								(cond
									;request-id subscriber amount message http-status response-data]
									;							(let [{txnid :txnid balance :balance resp-status :status}
									(not= (utils/submsisdn %subscriber %subscriber)  subscriber)    (failed-credit request-id subscriber amount (format "msisdnMismatch(expected=%s,actual=%s)" subscriber %subscriber)
																																																 status response-data)
									(not (and %ret %txnid %status))  (failed-credit request-id subscriber amount "missingParams()" status
																																	response-data)
									(not= %ret "0")                    (failed-credit request-id subscriber amount  (format "callfailedBadRet(%s)" %ret)
																																		status response-data)
									;(and checking? (= %status "11")) (fn-on-success request-id subscriber loan-type amount response-data)
									(= %status "13")                 (failed-credit request-id subscriber amount  (format "bad-amount(%s)" %status)
																																	status response-data)
									(not= %status "00")              (failed-credit request-id subscriber amount (format "callfailedBadStatus(%s|%s)" (get-status-kwd %status) %status)
																																	status response-data)
									;request-id subscriber amount message http-status response-data
									;; ---
									(= %status "00")                 (success-credit request-id subscriber loan-type amount response-data %amount status))))
						)))


	(defn process-lend [args]
				(let [{:keys [loan-type advance-name request-id subscriber
											amount-requested to-repay principal serviceq repay-time channel]} args
							;; Messaging ---
							msg-succ-type (condp = loan-type
															:airtime :airtime-lend-ok
															:data :data-lend-ok)
							msg-failed-type (condp = loan-type
																:airtime :airtime-lend-failed
																:data :data-lend-failed)

							msg-unreconciled-type (condp = loan-type
																			:airtime :airtime-lend-unrecon
																			:data :data-lend-unrecon)

							;; ---
							;messages (let [ret (into {} (map (fn [k msg-fn]
							;																	[k (msg-fn msg-args)])
							;																[:ok :queued :failed]
							;																msg-fns))]
							;					(log/debug (str "messages = " ret))
							;					ret)
							time-processed (f/unparse
															 (f/formatters :mysql) (l/local-now))
							{:keys [status error-class error-status error-msg external-response-code external-txn-id post-event-balance] :as ret}
							(do
								(db/new-credit-request {:request-id request-id :subscriber subscriber
																				:loan-type  (name loan-type) :amount principal})
								(pg-credit-sub request-id subscriber loan-type principal)
								;;testing result
								;{:status :ok :error-class nil :error-status :no-error, :error-msg nil :external-response-code "00" :external-txn-id "2020073001025250501008993" :post-event-balance nil}
								)

							_ (log/debugf "Status loan %s" [request-id error-class (status status-value) error-status error-msg time-processed external-response-code external-txn-id post-event-balance])]

					(db/updateVtopReqStatus {:request-id request-id :status (status status-value) :error (str error-status)
																	 :time_processed time-processed :external-response-code (str external-response-code)
																	 :external-txn-id external-txn-id :post-event-balance post-event-balance})

					;queue PE notification
					(if-not (check-failure error-status)
						(rmqutils/send-pe-notif {:subscriber subscriber}))
					(if (= status :ok) (do
															 (reset! *amountof-successful-loans* (+ principal @*amountof-successful-loans*))
															 (async/go (rmqutils/send-sms msg-succ-type {:request-id request-id :subscriber subscriber :loan-type loan-type
																																					 :to-repay   to-repay :advance-name advance-name :serviceq serviceq
																																					 :repay-time repay-time :principal principal :gross-amount (+ principal serviceq)}))
															 true)
														 (if (env :send-failed-sms)
															 (let [{:keys [error-class error-cause-class need-trace? throwable]} ret]
																 (async/go (rmqutils/send-sms (if (= status :failed)
																																msg-failed-type
																																msg-unreconciled-type) {:request-id request-id :subscriber subscriber :loan-type loan-type
																																												:to-repay to-repay :advance-name advance-name :serviceq serviceq
																																												:repay-time repay-time :principal principal :gross-amount (+ principal serviceq)}))
																 (log/error (format "unableToprocessLend(%s) -> %s" args {:class error-class :cause-class error-cause-class})
																						(when need-trace? throwable))
																 false)
															 false))))
