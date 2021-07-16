(ns ghas-acs.rmqutils
	(:require
		[langohr.core :as rmq]
		[langohr.basic :as lb]
		[langohr.channel :as lch]
		[langohr.queue :as lq]
		[langohr.exchange :as le]
		[langohr.consumers :as lc]
		[clojure.data.json :as json]
		[clojure.tools.logging :as log]
		[ghas-acs.utils :as utils]
		[ghas-acs.config :refer [env]]))


(declare initialize-publisher initialize-consumer create-consumer-handler)
;;sms queues
(def send-sms-details  (atom {}))


;;pe notification
(def send-pe-details  (atom {}))

;;for recovery
(def send-recovery-details (atom {}))

(def ^:dynamic value 1)

;;
(defn initialize-rabbitmq
	[args]
	(try
		(let [{:keys [queue-name queue-exchange queue-routing-key msg channel]} args
					_ (le/declare @channel queue-exchange "direct" {:durable true :auto-delete false})
					queue-name (:queue (lq/declare @channel queue-name {:durable true
																														 :auto-delete false}))]
			; bind queue to exchange

			(lq/bind @channel queue-name queue-exchange {:routing-key queue-routing-key})

			(lb/publish @channel queue-exchange queue-routing-key (json/write-str msg) {"Content-type" "text/json"})
			;(rmq/close channel)
			;(rmq/close rabbitmq-conn)
			true)
		(catch Exception e (log/error e (.getMessage e))
											 (throw (Exception. (format "unableToConnectToRabbitmq[%s]" (.getMessage e)))))))
;;


;[consumer] Received a message: {"msisdn":8156545907,"id":15981342110806992,"message":"Dear Customer, you have been credited with N43 airtime at N7 service charge. Please recharge by 2020-08-25 22:10:17 to repay your advance.","flash?":false,"from":"Borrow Me"}, delivery tag: 2, content type: null




(defn send-sms  [type args]
	(let [{:keys [request-id subscriber repay-time principal advance-name serviceq gross-amount]} args]
		(try
			(let [getMessage (fn [type gross-amount]
								 (binding [value gross-amount]
									 (let [sms-value (if (= type :airtime)
														 (get-in env [:as :msg :sms-airtime-lend-ok])
														 (get-in env [:as :msg :sms-data-lend-ok]))
										   ;_ (log/debugf "fqns value=%s|gross=%s" value gross-amount)
										   ]
										 (eval sms-value))))
						;sms-airtime-lend-ok (get-in env [:as :msg :sms-airtime-lend-ok])
						sms-airtime-lend-failed (get-in env [:as :msg :sms-airtime-lend-failed])
						sms-airtime-lend-unrecon (get-in env [:as :msg :sms-airtime-lend-unrecon])
						;sms-data-lend-ok (get-in env [:as :msg :sms-data-lend-ok])
						sms-data-lend-failed (get-in env [:as :msg :sms-data-lend-failed])
				  sms-data-lend-unrecon (get-in env [:as :msg :sms-data-lend-unrecon])
				  sms-data-lend-not-processed (get-in env [:as :msg :sms-data-lend-not-processed])
				  sms-sender-name (get-in env [:as :msg :sms-sender-name])
				  msg (utils/get-message (condp = type
											 :airtime-lend-ok (format (getMessage :airtime (+ principal serviceq))
																  (utils/cedis principal) (utils/cedis serviceq) repay-time)
											 ;(format sms-airtime-lend-ok (utils/cedis principal) (utils/cedis serviceq) repay-time)
											 :airtime-lend-failed (format sms-airtime-lend-failed (utils/cedis principal))
											 :airtime-lend-unrecon (format sms-airtime-lend-unrecon (utils/cedis principal))
											 :data-lend-ok	(format (getMessage :data (+ principal serviceq))
																  advance-name repay-time)
											 ;(format sms-data-lend-ok  advance-name repay-time)
											 :data-lend-failed (format sms-data-lend-failed advance-name)
											 :data-lend-not-processed (format sms-data-lend-not-processed advance-name)
											 :data-lend-unrecon (format sms-data-lend-unrecon (utils/cedis principal))
											 (throw (Exception. (format "UndefinedSMSType(%s)" type))))
						  args)
						_ (log/infof "sendSMS(%s,%s,%s) -> %s" request-id subscriber type msg)]
				(initialize-rabbitmq (assoc @send-sms-details :msg {:msisdn subscriber
																	:id request-id
																	:message msg
																	:flash? false
																	:from sms-sender-name})))
			(catch Exception ex
				(log/errorf ex "cannotSendSMS(%s,%s,%s) -> %s" request-id subscriber type (.getMessage ex))))))



(defn temp-send-sms [subscriber request-id msg]
	(initialize-rabbitmq (assoc @send-sms-details :msg {:msisdn subscriber
														:id request-id
														:message msg
														:flash? false
														:from nil})))

(defn trigger-recovery [args]
	(do
		(log/infof "sendTriggerRecovery(%s)" args)
		(initialize-rabbitmq (assoc @send-recovery-details :msg args))
		))
;; Initialize Interfaces


(defn shut-down []
	(let [queues [send-sms-details send-pe-details send-recovery-details]
				_ (log/infof "Shutting down Rabbitmq Connections")]
		(doseq [queue queues]
			(when (:conn @queue)
				(rmq/close (:channel @queue))
				(rmq/close (:conn @queue))
				(reset! (:channel @queue) nil)
				(reset! (:conn @queue) nil)))))

(defn initialize-queue [queue]
	(try
		(let [{:keys [queue-exchange queue-name queue-routing-key handler consumers conn channel retry-queue]
					 :or {consumers 10}} queue
				_ (reset! conn (rmq/connect queue))
				_ (reset! channel (lch/open @conn))
					_ (le/declare @channel queue-exchange "direct" {:durable true :auto-delete false})
					queue-name (:queue (lq/declare @channel queue-name {:durable true
																														 :auto-delete false :arguments nil #_(when retry-queue
																																														 {"x-dead-letter-exchange"    (:retry-exchange retry-queue)
																																															"x-dead-letter-routing-key" (:retry-routing-key retry-queue)})}))]
				; bind queue to exchange
				(lq/bind @channel queue-name queue-exchange {:routing-key queue-routing-key})
				;(lq/declare @channel queue-name {:durable true :auto-delete false})
				(when handler
					(doseq [count (repeat consumers "x")]
						;(lc/subscribe channel queue-name handler)
						(lc/subscribe @channel queue-name handler {:auto-ack false})))

				#_(when retry-queue
					(let [{:keys [retry-exchange retry-name retry-routing-key retry-delay retry-consumers]
								 :or   {retry-consumers 10}} retry-queue]
						_ (le/declare @channel retry-exchange "direct" {:durable true :auto-delete false})
						_ (log/infof "retry-queue=%s"retry-queue)
						retry-name (:queue (lq/declare @channel retry-name {:durable     true
																															 :auto-delete false :arguments {"x-dead-letter-exchange"    queue-exchange
																																															"x-dead-letter-routing-key" queue-routing-key
																																															"x-message-ttl" retry-delay}}))

						; (lq/declare @channel retry-name {:durable true :arguments {"x-dead-letter-exchange"    queue-exchange
						;																													"x-dead-letter-routing-key" queue-routing-key
						;																													"x-message-ttl"             retry-delay}})
						(lq/bind @channel retry-name retry-exchange {:routing-key retry-routing-key}))))
		(catch Exception e (log/error e (.getMessage e))
											 (throw (Exception. (format "unableToConnectToRabbitmq[%s] -> %s" queue (.getMessage e)))))))

(defn initialize-queue-interfaces []
	(let [queues [(reset! send-sms-details {:host    (get-in env [:queue :sms :host])
																				 :port     (get-in env [:queue :sms :port])
																				 :username (get-in env [:queue :sms :username])
																				 :password (get-in env [:queue :sms :password])
																				 :vhost    (get-in env [:queue :sms :vhost])
																					:conn            (atom nil)
																					:channel         (atom nil)
																				 ; queue info
																				 :queue-name (get-in env [:queue :sms :queue-name])
																				 :queue-exchange (get-in env [:queue :sms :queue-exchange])
																				 :queue-routing-key (get-in env [:queue :sms :queue-routing-key])})

								(reset! send-recovery-details {
																							:host     (get-in env [:queue :recovery :host])
																							:port     (get-in env [:queue :recovery :port])
																							:username (get-in env [:queue :recovery :username])
																							:password (get-in env [:queue :recovery :password])
																							:vhost    (get-in env [:queue :recovery :vhost])
																							 :conn            (atom nil)
																							 :channel         (atom nil)
																							; queue info
																							:queue-name (get-in env [:queue :recovery :queue-name])
																							:queue-exchange (get-in env [:queue :recovery :queue-exchange])
																							:queue-routing-key (get-in env [:queue :recovery :queue-routing-key])
																							})
								]]
		(doseq [queue queues]
			(initialize-queue queue))
		queues))
