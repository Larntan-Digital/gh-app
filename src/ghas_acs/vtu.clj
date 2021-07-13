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
			  [clojure.core.async :as async]
			  [clj-time.core :as t]]
	(:use [ghas-acs.counters])
	(:import (org.joda.time.format DateTimeFormat)
			 (java.io ByteArrayInputStream File)
			 (java.net SocketTimeoutException ConnectException ProtocolException NoRouteToHostException)
			 (java.util.concurrent TimeoutException)
			 (org.joda.time ReadableInstant)))




(def pg-cred-count (atom 0))

(defn pg-get-needed-cred []
	(let [pg (str/split (get-in env [:pg :pg-credentials]) #",")
		  v  (get pg @pg-cred-count)]
		;;(println v)
		(when (> (count pg) 1)
			(if (= @pg-cred-count (- (count pg) 1))
				(reset! pg-cred-count 0)
				(swap! pg-cred-count inc))) v))




;<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body><ns2:requestTopupResponse xmlns:ns2=\"http://external.interfaces.ers.seamless.com/\"><return><ersReference>2021031009440260201000006</ersReference><resultCode>0</resultCode><resultDescription>SUCCESS : You have topped up .5000 GHS to 233235657738. Your balance is now 9.5000 GHS. Ref: 2021031009440260201000006</resultDescription><requestedTopupAmount><currency>GHS</currency><value>0.5</value></requestedTopupAmount>
; <senderPrincipal><principalId><id>BMC1TST</id><type>RESELLERID</type></principalId><principalName>BMC 1</principalName>
; 	<accounts><account><accountSpecifier><accountId>BMC1TST</accountId><accountTypeId>RESELLER</accountTypeId></accountSpecifier>
; 	<balance><currency>GHS</currency>
; <value>9.5000</value></balance></account></accounts><status>Active</status>
; </senderPrincipal><topupAccountSpecifier><accountId>233235657738</accountId><accountTypeId>AIRTIME</accountTypeId></topupAccountSpecifier><topupAmount><currency>GHS</currency>
; <value>0.5000</value></topupAmount><topupPrincipal><principalId><id>233235657738</id><type>SUBSCRIBERID</type></principalId><principalName></principalName><accounts><account><accountSpecifier><accountId>233235657738</accountId><accountTypeId>AIRTIME</accountTypeId></accountSpecifier></account><account><accountSpecifier><accountId>233235657738</accountId><accountTypeId>DATA_BUNDLE</accountTypeId></accountSpecifier></account></accounts></topupPrincipal></return></ns2:requestTopupResponse></soap:Body></soap:Envelope>
(defn- parse-pg-response [content]
	(let [response (xml/parse (ByteArrayInputStream. (.getBytes content "UTF-8")))
		  response (zip/xml-zip response)]
		{:subscriber        (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :topupAccountSpecifier :accountId zip-xml/text)
		 :amount            (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :requestedTopupAmount :value zip-xml/text)
		 :resultCode        (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :resultCode zip-xml/text)
		 :status            (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :senderPrincipal :status zip-xml/text)
		 :resultDescription (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :resultDescription zip-xml/text)
		 :txnid             (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :ersReference zip-xml/text)
		 :balance           (zip-xml/xml1-> response :soap:Body :ns2:requestTopupResponse :return :senderPrincipal
								:accounts :account :balance :value zip-xml/text)}))


(defn parse-credit [request-id subscriber loan-type response-data amount message]
	(let [{txnid             :txnid balance :balance resultCode :resultCode
		   resultDescription :resultDescription} response-data
		  stat (if (= resultCode "0")
				   (do (swap! *countof-pg-succeeded* inc)
					   (log/info (format "CallPGresp(%s,%s,%s,%s) -> [%s] %s %s "
									 request-id subscriber loan-type amount amount resultCode response-data))
					   :ok)
				   (do (swap! *countof-pg-failed* inc)
					   (log/errorf "cannotCallPG(%s,%s,%s) -> [%s] [status: %s] %s" request-id subscriber amount message
						   resultCode response-data)
					   :failed))]
		{:status                 stat :subscriber subscriber :loan-type loan-type :amount amount :error-status resultDescription
		 :external-response-code resultCode :external-txn-id txnid :request-id request-id
		 :post-event-balance     balance}))

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
		"104" :insufficient-sender-credit
		"A8" :bad-sub-status
		"A9" :bad-msisdn
		status))


(defn call-endpoint [endpoint fullUrl payload options]
	(let [{:keys [type opt]} options
		  {:keys [status body error] :as response} (utils/with-func-timed (format "callEndpoint(%s) -> %s [%s]" endpoint payload options)
													   (fn [error]
														   {:http-status :failed
															:body        opt
															:error       error})
													   (if (= type :get)
														   (http/get fullUrl payload)
														   (http/post fullUrl payload)))]
		{:error error :status status :body body}))


(defn pg-credit-sub [request-id subscriber loan-type amount]
	(let [request-xml (format (slurp (File. (get-in env [:pg :pg-credit-request-xml])))
						  request-id
						  (utils/validate-sub subscriber)
						  (utils/validate-sub subscriber)
						  (utils/cedis amount))
		  fullUrl     (get-in env [:pg :pg-account-url])
		  request     (str/trim request-xml)
		  payload     {:user-agent      "ERL/http"
					   :connection-timeout (get-in env [:pg :pg-timeout-credit])
					   :socket-timeout     (get-in env [:pg :pg-timeout-credit])
					   :body            request
					   :headers         {"Content-Type" "application/xml"}}
		  _           (log/infof "PG Request %s -> [%s]" fullUrl payload)
		  loginfo-req {:sub subscriber :amt amount :txid request-id}
		  {:keys [status body error] :as response} (call-endpoint "PG" fullUrl payload {:type :post :opt loginfo-req})

		  #_(utils/with-func-timed (format "CallPG (args=%s)" loginfo-req)
													   (fn [error]
														   {:http-status :failed
															:body        loginfo-req
															:error       error})
													   (http/post fullUrl payload))]

		(if error
			(let [inst (condp instance? error
						   TimeoutException :timeout-on-connect
						   ConnectException :connection-refused
						   SocketTimeoutException :timeout-on-read
						   ProtocolException :http-protocol-error
						   NoRouteToHostException :no-route-to-host
						   Exception :other-exception)]
				(parse-credit request-id subscriber loan-type {} amount inst))

			(let [msisdn subscriber
				  {:keys [subscriber amount resultCode status resultDescription txnid balance] :as result} (parse-pg-response body)]
				(log/infof "Contents of results [%s]" result)
				(cond
					(not= (utils/submsisdn subscriber :other) msisdn) (parse-credit request-id subscriber loan-type result amount (format "msisdnMismatch(expected=%s,actual=%s)" msisdn subscriber))
					(not (and txnid subscriber)) (parse-credit request-id subscriber loan-type result amount "missingParams()")
					(not= resultCode "0") (parse-credit request-id subscriber loan-type result amount (format "callfailedBadResponse(%s)" resultCode))
					(not= status "0") (parse-credit request-id subscriber loan-type result amount (format "callfailedBadStatus(%s|%s)" (get-status-kwd resultCode) resultCode))
					(= resultCode "0") (parse-credit request-id subscriber loan-type result amount "success"))))))

(def status-value
	{:ok     0
	 :failed 1})

(def ^:dynamic bundleid nil)


(defn process-data-loan [subscriber amount-requested loan-id loaninfo]
	(let [url (get-in env [:pg :profit-guru])
		  options    {:type :post
					  :user-agent      "ERL/http"
					  :connection-timeout (get-in env [:pg :pg-timeout-credit])
					  :socket-timeout     (get-in env [:pg :pg-timeout-credit])}
		  ;http://10.161.139.5:18081/PrismWS/HSI/subscriptions/subscribe/233230557412?plan=???&charge=true
		  plan (binding [bundleid amount-requested]
				   (let [datavalues (get-in env [:pg :bundle-values])
						 _ (log/debugf "Bundle values %s |%s" datavalues bundleid)
						 result (eval datavalues)]
					   (if (number? result)
						   (eval datavalues)
						   (throw (ex-info "No matching bundle id for amount."
									  {:error-class :no-matching-bundle-id :amount amount-requested :bundleid bundleid})))))
		  path  (format url (utils/validate-sub subscriber) plan)
		  ;(str url "/subscribe/" (utils/validate-sub subscriber) "?plan="plan"&charge=true")
		  ;http://10.161.139.5:18081/PrismWS/HSI/subscriptions/subscribe/%s?plan=%s&charge=true
		  ;http://10.161.77.88/REWARD?msisdn=%s&campaign_name=BorrowMeData&reward_id=%s
		  ;http://10.161.77.88/REWARD?msisdn=233xyz&campaign_name=BorrowMeData&reward_id=a
		  _ (log/infof "Calling ProfitGuru request -> %s|%s" path loaninfo)
		  {:keys [error body] :as response} (call-endpoint "profitGuru" path options {:type :post :opt nil})
		  _ (log/infof "profitGuru response [sub=%s|body=%s|error=%s]" subscriber body (when error (.getMessage error)))
		  parsebody (xml/parse (ByteArrayInputStream. (.getBytes body "UTF-8")))
		  prism_result (get-in ((zip/xml-zip parsebody) 0) [:attrs :prism_result])
		  state (if (= prism_result "0")
					(do
						(when-not (nil? loaninfo)
							(db/updateDataStatus {:loan_id loan-id}))
						:ok)
					:failed)]
	(if (nil? loaninfo)
		state
		(let [{:keys [cedis_loaned cedis_serviceq expected_repay_time]} loaninfo
			  _ (log/infof "Processing sms for airtime-to-data conversion(%s|%s)" subscriber {:cedis_loaned cedis_loaned
																							  :cedis_serviceq :cedis_serviceq
																							  :expected_repay_time expected_repay_time})
			  principal (- cedis_loaned cedis_serviceq)
			  repay-time (f/unparse (f/formatter "EEE, dd MMM yyyy")
							 (-> (t/default-time-zone)
								 (f/formatter "YYYY-MM-dd'T'HH:mm:ss.SSSSSS" "YYYY-MM-dd")
								 (f/parse (.toString ^ReadableInstant expected_repay_time))))
			  {:keys [advance_name]} (db/get-loan-period :data cedis_loaned)]
			(if (= state :ok)
				(do
					(reset! *amountof-successful-loans* (+ principal @*amountof-successful-loans*))
					(async/go
						(rmqutils/send-sms :data-lend-ok {:request-id loan-id :subscriber subscriber :loan-type :data
														  :to-repay   cedis_loaned :advance-name advance_name :serviceq cedis_serviceq
														  :repay-time repay-time :principal principal :gross-amount cedis_loaned})))
				(do
					(reset! *amountof-failed-data-loans* (+ principal @*amountof-failed-data-loans*))
					(async/go
						(rmqutils/send-sms :data-lend-not-processed {:request-id loan-id :subscriber subscriber :loan-type :data
																	 :to-repay   cedis_loaned :advance-name advance_name :serviceq cedis_serviceq
																	 :repay-time repay-time :principal principal :gross-amount cedis_loaned}))))))))


(defn process-lend [args]
	(let [{:keys [loan-type advance-name request-id subscriber
				  amount-requested to-repay principal serviceq repay-time channel]} args
		  msg-succ-type   (condp = loan-type
							  :airtime :airtime-lend-ok
							  :data :data-lend-ok)
		  msg-failed-type (condp = loan-type
							  :airtime :airtime-lend-failed
							  :data :data-lend-failed)

		  time-processed  (f/unparse
							  (f/formatters :mysql) (l/local-now))
		  {:keys [status error-status external-response-code external-txn-id post-event-balance] :as ret}
		  (do
			  (db/new-credit-request {:request-id request-id :subscriber subscriber
									  :loan-type  (name loan-type) :amount principal})
			  ;(pg-credit-sub request-id subscriber loan-type principal)
			  ;;testing result
			   {:status :ok :subscriber subscriber :loan-type loan-type
			  :amount principal :error-status "sucess"
			   :external-response-code "0" :external-txn-id "2020073001025250501008993"
			   :request-id request-id
			   :post-event-balance 0}
			  )]
		(db/updateVtopReqStatus {:request-id request-id :status (status-value status) :error error-status
								 :time_processed  time-processed :external-response-code external-response-code
								 :external-txn-id external-txn-id :post-event-balance (if (not (nil? post-event-balance))
																						  (* (utils/toInt post-event-balance)
																							  (env :denom-factor))
																						  0)})
		(if (= status :ok) (let [state (if (= loan-type :data)
										   ;;if its a data request call profit guru
										   (process-data-loan subscriber amount-requested request-id nil)
										   :ok)]
							   (if (= state :ok)
								   (do
									   (reset! *amountof-successful-loans* (+ principal @*amountof-successful-loans*))
									   (async/go
										   (rmqutils/send-sms msg-succ-type {:request-id request-id :subscriber subscriber :loan-type loan-type
																	 :to-repay   to-repay :advance-name advance-name :serviceq serviceq
																	 :repay-time repay-time :principal principal :gross-amount (+ principal serviceq)}))
									   true)
								   (do
									   (reset! *amountof-failed-data-loans* (+ principal @*amountof-failed-data-loans*))
									   (async/go
										   (rmqutils/send-sms :data-lend-not-processed {:request-id request-id :subscriber subscriber :loan-type loan-type
																						:to-repay   to-repay :advance-name advance-name :serviceq serviceq
																						:repay-time repay-time :principal principal :gross-amount (+ principal serviceq)}))
									   false)))
			false)))

