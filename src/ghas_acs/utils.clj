(ns ghas-acs.utils
	(:require
		[clojure.tools.logging :as log]
		[clojure.walk :as walk]
		[clojure.string :as str]
		[ghas-acs.config :refer [env]]
		[ghas-acs.counters :as counters]
		[clj-http.client :as http]
		[clojure.data.json :as json])
	(:import (java.nio.file FileSystem
													FileSystems
													Files
													Path)
					 java.nio.charset.Charset))

;


;;; -------------------------------------------------------------------
;;;  Paths.
;;; -------------------------------------------------------------------

(def ^:dynamic *file-system* (FileSystems/getDefault))

(defmulti make-path
					(fn [path] (type path)))

(defmethod make-path String [path]
	(.getPath #^FileSystem *file-system* path (make-array String 0)))

(defmethod make-path java.io.File [path]
	(.toPath #^java.io.File path))

(defmethod make-path Path [path]
	path)

(defmethod make-path clojure.lang.PersistentVector [path]
	(condp = (count path)
		0 (assert (> (count path) 0))
		1 (make-path (get path 0))
		(.getPath #^FileSystem *file-system*
							(str (first path)) (into-array String (map str (next path))))))

;;; --------------------------------------------------------------------
;;;  Counter functions.
;;; --------------------------------------------------------------------


(def ^:dynamic *currency-multiplier* (int (.intValue (Math/pow (bigint 10) 2))))
(defn increment-counter [the-atom value]
	(swap! the-atom (fn [values amount]
										(let [c (values amount)]
											(if c
												(assoc values amount (inc c))
												(assoc values amount 1))))
				 value))

(defn rename-nil-keys [m new-name]
	(into {}
				(map (fn [[k v]]
							 {(or k new-name)
								(if (map? v) (rename-nil-keys v new-name) v)})
						 m)))


(defn parse-ussd-request [input]
	(str/split input #"[*]"))

(defmacro funcall-logging [[function-name & args] & body]
	`(try
		 (do (log/info (format "%s(args=%s)" ~function-name [~@args]))
				 ~@body)
		 (catch Exception e#
			 (log/error (format "!%s(args=%s) -> %s" ~function-name [~@args]
													(.getMessage e#))
									e#)
			 (throw e#))))




(defmacro with-func-timed
	"macro to execute blocks that need to be timed and may throw exceptions"
	[tag fn-on-failure & body]
	`(let
		 [fn-name# (str "do" (.toUpperCase (subs ~tag 0 1)) (subs ~tag 1))
			start-time# (System/currentTimeMillis)
			e-handler# (fn [err# fname#] (log/errorf err# "!%s -> %s" fname# (.getMessage err#)) :error)
			return-val# (try
										~@body
										(catch Exception se#
											(if (fn? ~fn-on-failure)
												(~fn-on-failure se#)
												(e-handler# se# fn-name#))))]
		 (log/infof "callProf|%s|%s -> %s" (- (System/currentTimeMillis) start-time#) fn-name# return-val#)
		 return-val#))


(defn sqlify-keyword [kwd]
	(.replace (.toUpperCase (name kwd)) \- \_))



(defn cedis [amount]
	(let [ value (double (/ amount (env :denom-factor)))]
		(if (= (mod value 10) 0.0)
			(int value)
			value)))


(defn options->map [options]
	(if (and options (not (empty? options)))
		(apply array-map options)
		{}))

(defn increment-counter [the-atom value]
	(swap! the-atom (fn [current_atom_values new_value]
										(let [c (current_atom_values new_value)]
											(if c
												(assoc current_atom_values new_value (inc c))
												(assoc current_atom_values new_value 1))))
				 value))


(defn get-file-contents
	([file style #^String charset]
	 (let [file (if (instance? Path file) file (make-path file))]
		 (condp = style
			 :lines  (Files/readAllLines file (Charset/forName charset))
			 :string (String. #^"[B" (get-file-contents file :bytes) charset)
			 (Files/readAllBytes file))))
	([file style]
	 (get-file-contents file style "UTF-8"))
	([file]
	 (get-file-contents file :string "UTF-8")))


(defn get-message
	"Returns a string with parameter keys substituted with actual values"
	[message parameters]
	(when (empty? parameters)
		(throw (Exception. "!get-message() - > empty parameters passed.")))
	(let [parameters (into {} (map (fn [[k v]] {(str k) (str v)}) parameters))
				regex (re-pattern (apply str (interpose "|" (map #(str %) (keys parameters)))))]
		(str/replace message regex parameters)))


(defn generate-request-id
	"generate a unique identifier"
	[]
	(let [component-id "33"
				timestamp (System/currentTimeMillis)
				rand (format "%04d" (rand-int 9999))
				requestid (str component-id timestamp rand)]
		(biginteger requestid)))

(defn validate-sub [sub]
	"validates 'sub'"
	(let [sub (str sub)]
		(cond (and (str/starts-with? sub "233") (= (count sub) 12)) (biginteger sub)
					(and (str/starts-with? sub "0") (= (count sub) 10)) (biginteger (str "233" (subs sub 1)))
					(= (count sub) 9) (biginteger (str "233" sub))
					:else (throw (Exception. (format "Value unknown %s"sub))))))


(defn submsisdn
	([sub]
	 (submsisdn sub :default))
	([sub error]
	 (let [msisdn (str sub)
				 res (fn [val]
							 (biginteger val))]
		 (cond (nil? msisdn) nil
					 (= (count msisdn) 12) (res (subs msisdn 3))
					 (= (count msisdn) 9) (res msisdn)
					 :else (if (= error :default)
									 (throw (Exception.(format "cannot parse msisdn[%s]" sub)))
									 sub)))))


(defn toInt [s]
	(if (number? s) s (read-string s)))

(defn callURL
	[url msisdn timeout]
	(let [msisdn (validate-sub msisdn)
				options  {:socket-timeout timeout
									:conn-timeout timeout
									:timeout timeout             ; ms
									:query-params {"msisdn" msisdn}}
				_ (log/infof "callURL [url=%s, msisdn=%s, timeout=%s]" url msisdn timeout)
				{:keys [body ] :as trace}  (http/get url options)]
		(do
			(log/infof "handleMonitorRequest -> %s|%s" msisdn body)
			(if (empty? body)
				{:action :nack}
				{:action :ack}))))
