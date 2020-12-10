(ns ghas-acs.sessions
	(:require [clojure.tools.logging :as log]
						[clj-time.local :as l]
						[clj-time.format :as f]
						[ghas-acs.utils :as utils]
						[ghas-acs.config :refer [env]]
						[clj-time.core :as t]
						[ghas-acs.db :as db])
	(:import (java.sql Date)))

;(def *session-data (atom {}))
(defn- %is-readonly-key? [key]
	(if (#{:session-id :subscriber :start-time :oldest-loan-time} key) true false))

(defn- %remove-readonly-keys [session-data]
	(dissoc session-data :session-id :subscriber :start-time :oldest-loan-time))

(defn- %get-readonly-entries [session-data]
	(reduce (fn [result e]
						(let [[k v] e]
							(if (%is-readonly-key? k)
								(assoc result k v)
								result)))
					{}
					session-data))



(defn session-data-update
	([session-data key value]
	 (if-not (%is-readonly-key? key)
		 (do
			 (log/debugf "session-data key value %s" (assoc session-data key value))
			 (assoc session-data key value))
		 (do
			 (log/debugf "session-data key-value %s" session-data)
			 session-data)))
	([session-data key-value-pairs]
	 (let [key-value-pairs (%remove-readonly-keys key-value-pairs)]
		 (if-not (empty? key-value-pairs)
			 (do
				 (log/debugf "session dataupdate key-value-pairs %s"(conj session-data key-value-pairs))
				 (conj session-data key-value-pairs))
			 (do
				 (log/debugf "session dataupdate %s"session-data)
				 session-data)))))

(defn get-session-data [session-id]
	(let [getSess (db/getSessionData {:session-id session-id})]
		getSess))


(defn start-new-session
	[session-data]
	"Start a new session. Return the result of the session start
operation."
	(let [max-age (get-in env [:as :ussd :ussd-session-max-age])
				{:keys [session-id subscriber]} session-data
				{:keys [subscriber_no session_data]} (get-session-data session-id)
				;;todo check value of allow-resume?
				allow-resume? true]
		;; Ensure that this is actually a new session.
		(when-not (nil? session_data)
			(log/errorf "duplicateSessionID(%s,msisdn=%s)" session-id subscriber)
			(throw (Exception. "duplicatedSessionID" )))
		(when-not (nil? subscriber_no)
			(when-not (= subscriber subscriber)
				(throw (Exception. (format "UnexpectedMsisdn(expected=%s,found=%s)" subscriber subscriber)))))
		;; ---
		(let [now (str (t/now))
					session-data (-> (dissoc session-data :oldest-loan-time)
													 (assoc :start-time now))]
			(log/debugf "inserting new session %s" {:session-id session-id :subscriber subscriber :session-data (str session-data), :max-age max-age, :allow-resume? allow-resume?})
			(db/newSession {:session-id session-id :subscriber subscriber :session-data (str session-data), :max-age max-age, :allow-resume? allow-resume?})
			;(swap! *session-data assoc session-id session-data)
			)
		true))



(defn clear-old-sessions []
	(let []
		(log/info "sessionCleanUpstart()")
		(db/clearExpiredSessions)
		#_(doseq [[session-id session-data] @*session-data]
				(try
					(let [start-time (session-data :start-time)]
						(when (t/after? now (t/plus start-time (t/seconds max-age)))
							(log/info (format "clearSession(id=%s,sub=%s)" session-id (session-data :subscriber)))
							(swap! *session-data dissoc session-id)))
					(catch Exception e
						(log/error (str "!processSession(" session-id ")") e))))
		(log/info "sessionCleanUpComplete()")))




(defn end-session [session-id subscriber]
	(db/closeSession {:session-id session-id :subscriber subscriber}))

