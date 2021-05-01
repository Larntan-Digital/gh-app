(ns ghas-acs.counters)

;;  Counters.
(def ^:dynamic *server-start-time*               (atom nil))


;; Lending-related.
(def ^:dynamic *countof-qualifiers*              (atom {}))
(def ^:dynamic *countof-loan-requests*           (atom {}))
(def ^:dynamic *countof-loan-requests-queued*    (atom {}))
(def ^:dynamic *countof-loan-requests-succeeded* (atom {}))
(def ^:dynamic *countof-pg-failed* (atom 0))
(def ^:dynamic *countof-pg-succeeded* (atom 0))

;; USSD-related.
(def #^:dynamic *countof-ussd-requests*           (atom {}))
(def ^:dynamic *countof-aborted-ussd-sessions*   (atom {}))
(def ^:dynamic *countof-timeout-ussd-sessions*   (atom 0))
(def ^:dynamic *countof-error-ussd-sessions*     (atom 0))
;; Requests by external services.
(def ^:dynamic *countof-ext-borrow-requests*     (atom {}))
(def ^:dynamic *countof-ext-borrow-error*        (atom 0))

(def ^:dynamic *amountof-successful-loans*				(atom 0))
(def ^:dynamic *amountof-failed-data-loans*				(atom 0))



(defn resetcounters [reset-time]
	(reset! *server-start-time*         reset-time)
	;; ---
	(reset! *countof-qualifiers*              {})
	(reset! *countof-loan-requests*           {})
	(reset! *countof-loan-requests-queued*    {})
	(reset! *countof-loan-requests-succeeded* {})
	(reset! *countof-pg-failed*    0)
	(reset! *countof-pg-succeeded* 0)
	;; ---
	(reset! *countof-ussd-requests*           {})
	(reset! *countof-aborted-ussd-sessions*   0)
	(reset! *countof-timeout-ussd-sessions*   0)
	(reset! *countof-error-ussd-sessions*     0)
	;; ---
	(reset! *countof-ext-borrow-requests*     {})
	(reset! *countof-ext-borrow-error*        0)

	(reset! *amountof-successful-loans*       0)
	(reset! *amountof-failed-data-loans* 	 0))



