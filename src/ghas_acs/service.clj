(ns ghas-acs.service
	(:use [ghas-acs.counters]
				[ghas-acs.sessions]
				[ghas-acs.config])
	(:require [clojure.tools.logging :as log]
						[ghas-acs.utils :as utils]
						[ghas-acs.ussd-menu :as menu]
						[ghas-acs.config :refer [env]]
						[ghas-acs.vtu :as vtu]
						[clojure.string :as str]
						[clojure.data.json :as json]
						[clj-time.format :as f]
						[ghas-acs.db :as db]
						[ghas-acs.rmqutils :as rmqutils]
						[clj-time.core :as t])
	(:import (java.net URLDecoder)))


(declare proceed-handle-ussd-request do-handler get-start-state proceedLend
				 lend new-loan-request)

(defmacro with-request-params [parameters & body]
	(let [session-data-var (gensym "session-data")
				bindings (mapcat (fn [param]
													 (let [kwd (keyword (str param))]
														 `(~param (let [ret# (~session-data-var ~kwd)]
																				(when (nil? ret#)
																					(throw (Exception. (format ~(str "UnableTotransition() -> unexpectedNullParam(" kwd ")")))))
																				ret#))))
												 parameters)]
		`(let [~session-data-var (deref ~'session-data)
					 ~@bindings]
			 ~@body)))

(defn make-response
	([message action]
	 {:status  200
		:headers {"Content-Type" "text/plain"}
		:body    (json/write-str {:message message
								  :action  (condp = action
											   :terminate "S"
											   :continue "C"
											   "S")})}))

(def ^:dynamic *language* :en)

(defn handle-ussd-request [request]
	;localhost:8080/as/ussd/erl?sub=8156545907&sid=12324354646565656565&state=0&msg=*321#
	(log/infof "USSD Request recieved %s" (:params request))
	(utils/increment-counter *countof-ussd-requests* (request :remote-addr))
	(let [params (:params request)
				{subscriber     :sub
				 session-id		:sid
				 option			:msg
				 state			:state} params]
		(if (and subscriber session-id option state)
			;; Alright, we have all the stuff that we expect. Proceed.
				 (utils/with-func-timed (str "UssdRequest | " subscriber) ""
					 (try
						 (let [option (when option (str/replace (URLDecoder/decode option "UTF-8") #"[\n\t ]" ""))
							   is-new? (condp = state "0" true "1" false :unknown)]
							 (if (= is-new? :unknown)
								 (do
									 (utils/increment-counter *countof-aborted-ussd-sessions* is-new?)
									 (log/errorf "badSubState(%s,sub=%s,sid=%s,msg=%s)"
										 state subscriber session-id option)

									 (make-response "Client error occured" :terminate))
								 (do
									 ;(log/info "proceed-handle-ussd-request" session-id subscriber is-new? option)
									 (proceed-handle-ussd-request session-id (utils/submsisdn subscriber) is-new? option)
									 ;(make-response "successful" :terminate)
									 )))
						 (catch Exception e
							 (log/errorf "!UssdRequest(params=%s|%s)" params (.getMessage e) e)
							 (make-response "Server error" :terminate))))

			;; USSD gateway has sent an unintelligible request.
			(do (log/error (format "notProperlyFormedRequest(msisdn=%s,sid=%s,opt=%s)" subscriber session-id option))
					(make-response "Server error" :terminate)))))



(defn proceed-handle-ussd-request [session-id subscriber is-new? input]
	(try
		(binding [*language* "en"]
			(let [session (do-handler session-id subscriber is-new? input)
						_ (log/debugf "proceed-handle-ussd-request[%s|%s]" (menu/final-state-p (session :state)) session)
						result  (menu/render-state session)
						_ (log/debugf "resultRender %s|%s [%s]" session-id subscriber result)]
				(make-response result (if (menu/final-state-p (session :state)) :terminate :continue))))
		(catch Exception e
			(do
				(swap! *countof-error-ussd-sessions* inc)
				(log/error (format "CannotHandleMenu(%s) => %s"
							   [session-id subscriber is-new? input] (.getMessage e)) e)
				(make-response (if (= "duplicatedSessionID" (.getMessage e))
								   (get-in env [:as :msg :as-duplicate-error-msg])
								   (get-in env [:as :msg :as-error-msg]) )  :terminate)))))

(defn getSubscriberInfo [subscriber]
	(into {} (map (fn [[k v]]
									[k (if (nil? v) 0 v)]))
				(clojure.set/rename-keys (db/get-subscriber-info {:sub subscriber})
																 {:max_qualified :max-qualified
																	:max_loan_counter :max-loan-counter
																	:queue_count :queue-count
																	:loan_count :loan-count
																	:queue_total :queue-total
																	:max_loanable :max-loanable
																	:loan_balance :loan-balance
																	:oldest_loan_time :oldest-loan-time
																	:loans_total :loans-total
																	:oldest_expected_repay	:oldest-expected-repay})))



(defn do-handler [session-id subscriber is-new? input]
	(let [parsedInput (utils/parse-ussd-request input)
				[_ service-code _] parsedInput
				pre-session-data(if is-new?
									(let [request-id (let [timestamp (System/currentTimeMillis)
														   rand (format "%04d" (rand-int 9999))
														   request-id (str timestamp rand)]
														 (biginteger request-id))
										  subscriber-info (getSubscriberInfo subscriber)
										  _ (log/infof "subscriber-info[%s]=>%s" subscriber subscriber-info)]
										(conj {:session-id   session-id :subscriber subscriber
											   :service-code service-code :ussd-string input :ma-balance 0 :request-id request-id}
											(dissoc subscriber-info :oldest-loan-time)))
									;subscriber not a new user
									(let [{:keys [subscriber_no session_data]} (get-session-data session-id)
										  _ (when (nil? session_data)
												(throw (Exception. (format "sessionDataNotFound [%s|%s]" subscriber session_data))))
										  _ (log/debugf "sessionState(session-id,%s,input=%s,msisdn=%s) = %s" session-id input subscriber_no (read-string session_data))]
										(read-string session_data)))
				session-data (atom (if is-new?
									   (let [[start-state session-data] (get-start-state pre-session-data)
											 stateSession (assoc session-data :state start-state :menu-3p (when (= start-state :menu-others) true))
											 _  (db/updateSession {:session-id session-id :subscriber subscriber :state start-state :session-data (str stateSession)})]
										   stateSession)
									   (menu/state-automaton (atom pre-session-data) input)))
				{:keys [service-class state service-code profile-id loan-count loan-balance]} @session-data
				verify-loan-type (fn [loan-type]
									 (cond (= loan-type :lend-airtime) :airtime
										 (= loan-type :lend-data) :data))
				verify-loan-failure (fn [loan-type]
										(cond (= loan-type :lend-airtime) :lend-airtime-failed
											(= loan-type :lend-data) :lend-data-failed))]

		(log/debugf "Processing session state [menu-state=%s|msisdn=%s|state=%s|%s]" (menu/final-state-p state) subscriber state @session-data)
		(when (> (:max-qualified @session-data) 0) (utils/increment-counter *countof-qualifiers*  (:max-qualified @session-data)))
		(condp = state
			(some #{state} [:borrow-now-again :borrow-data-again]) (let [request-id (let [timestamp (System/currentTimeMillis)
																						  rand (format "%04d" (rand-int 9999))
																						  request-id (str timestamp rand)]
																						(biginteger request-id))
																		 subinfo (getSubscriberInfo subscriber)
																		 pre-session (conj {:session-id   session-id :subscriber subscriber
																							:service-code service-code :ussd-string input :ma-balance 0 :request-id request-id}
																						 (reduce conj {} (dissoc subinfo :oldest-loan-time)))
																		 new-pre-session (conj @session-data pre-session)
																		 _ (reset! session-data new-pre-session)]
																	   (log/infof "new state [%s,%s]" state @session-data)
																	   (db/updateSession {:session-id session-id :subscriber subscriber :state state :session-data (str pre-session)}))
			(some #{state} [:lend-airtime :lend-data]) (let [{:keys [request-id loan-type max-loanable amount-requested amount-net serviceq menu-3p advance-name bundle-size]} @session-data
															 _ (log/debugf "processing for loan => %s" @session-data) msisdn-3p (when menu-3p (utils/submsisdn input))
															 _ (reset! session-data (assoc @session-data :msisdn-3p msisdn-3p :type state :outstanding (int (/ (- max-loanable (+ amount-net serviceq)) (env :denom-factor)))))]
														   (with-request-params [subscriber session-id request-id amount-requested serviceq max-loanable]
															   (when-not (proceedLend {:loan-type        (verify-loan-type state)
																					   :subscriber       subscriber :service-class service-class :session-id session-id :direct? is-new?
																					   :advance-name     advance-name :request-id request-id
																					   :amount-requested amount-requested :serviceq serviceq :max-loanable max-loanable
																					   :bundle-size		bundle-size
																					   :channel          :ussd :short-code service-code :misc-args {:profile profile-id} :loan_count loan-count})
																   (reset! session-data (session-data-update @session-data :state (verify-loan-failure state))))))
			:trigger-rec (with-request-params [subscriber] (rmqutils/trigger-recovery {:sub     subscriber
																					   :amount 	loan-balance               ;(/ loan-balance (env :denom-factor))
																					   :time 	(f/unparse
																									(f/formatter "yyyyMMddHHmmss") (t/now))
																					   :attempts 0
																					   :time-queued (System/currentTimeMillis)
																					   :type "manual",
																					   :channel "manual"}))
			:convert-to-data (with-request-params [subscriber] (let [{:keys [subscriber_fk loan_id cedis_loaned cedis_serviceq expected_repay_time] :as getLoan}  (db/selectLoan {:subscriber subscriber})
																	 _ (log/infof "convertToData [%s]"getLoan)]
																   (if (empty? getLoan)
																	   {:status "failed" :message "not found"}
																	   (let [resp (vtu/process-data-loan subscriber_fk cedis_loaned loan_id {:cedis_loaned cedis_loaned :cedis_serviceq cedis_serviceq :expected_repay_time expected_repay_time})]
																		   resp))))
			true)
		;; Handle persistence.
		(if (menu/final-state-p state)
			(when-not is-new?
				(end-session session-id subscriber))
			(if is-new?
				(do
					;; If we will be tracking this session, check that session
					;; does not already exist.
					(log/debugf "tracking session %s|%s"session-id subscriber )
					(start-new-session @session-data))
				(do
					;(set-session-data session-id subscriber @session-data)
					(db/updateSession {:session-id   session-id :subscriber subscriber
									   :session-data (str (reset! session-data (conj @session-data (when (= (:state @session-data) :menu-others)
																														 {:menu-3p true}))))}))))
		(log/debugf "Returning session for rendering %s" @session-data)
		;; Return session data for rendering purposes.
		@session-data))

;(navigate-menu subscriber session-id session-data input)


(defn proceedLend [{:keys [subscriber request-id loan-type amount-requested bundle-size] :as args}]
	(try
		(let [now (t/now)
					{:keys [date_part charging_method] :as loan_period} (db/get-loan-period loan-type amount-requested)
					_ (log/debugf "loan_period=%s|%s"subscriber loan_period)
					repay-time (f/unparse
								   (f/formatter "EEE, dd MMM yyyy") (t/plus now (t/seconds date_part)))]
			(lend (conj {} args {:repay-time repay-time :charging_method (keyword charging_method)})))
		(catch Exception e
			(log/error (format "unableToLend(%s,%s,%s,%s) -> %s" subscriber request-id amount-requested loan-type e) e)
			false)))


(defn- lend [{:keys [request-id subscriber service-class loan-type
					 amount-requested serviceq max-loanable channel
					 loan_count charging_method bundle-size] :as args}]
	(do
		(utils/increment-counter *countof-loan-requests* amount-requested)
		(let [_ (log/debugf "lending details %s"args)
					repay     (if (= charging_method :post) bundle-size amount-requested)
					principal (db/get-net-amount loan-type amount-requested charging_method)
					newargs (conj {} args {:to-repay repay :principal principal
										   :id request-id :name name
										   :original-sc service-class
										   :typ loan-type})]
			(new-loan-request request-id subscriber loan-type channel repay serviceq max-loanable (inc loan_count) charging_method)
			(log/infof "processingForVTU %s"newargs)
			(vtu/process-lend newargs))))


(defn- new-loan-request [request_id subscriber loan_type channel amount serviceq max_loanable loan_count charging_method]
	(let [[loan_type flag] (condp = loan_type
						:airtime ["airtime" true]
						:data    ["data" false]
										(throw (Exception. (format "UndefinedloanType(%s)" loan_type))))
				loan_flags (bit-or (if (db/serviceq-post-charged? charging_method) 2r00000001 0)
													 (if (db/serviceq-after-grace-period? charging_method) 2r00000010 0))
				channel (name channel)
				values {:request-id request_id :subscriber subscriber :loan_flags loan_flags :loan_type loan_type
								:channel channel :amount amount :serviceq serviceq :max_loanable max_loanable :loan_count loan_count
								:flag_done flag}
				_ (log/debugf "new loan request values %s" values)]
		(db/insert-tbl-loan-req values)))






(defn- get-start-state [session-data]
	(log/debugf "getStartState %s" session-data)
	(let [new-session-data (atom session-data)]
		[(let [menulist (menu/get-initial-state new-session-data)
					 _ (log/debugf "Menu => %s |%s"  menulist @new-session-data)]
			 menulist)
		 @new-session-data]))


