(ns ghas-acs.ussd-menu
	(:require [clojure.tools.logging :as log]
						[ghas-acs.config :refer [env]]
						[ghas-acs.sessions :as sessions]
						[ghas-acs.utils :as utils]
						[ghas-acs.db :as db]
						[clojure.data.zip.xml :as zip-xml :only [xml1]]
						[clojure.zip :as zip :only [xml-zip]])
	(:import (clojure.lang PersistentList Keyword)
					 (java.io File StringReader PushbackReader)))

(declare define-nfa define-state-renderer define-state-initializer)






;; -------
;;  Definers.

(declare %nfa-automaton-def %ussd-renderer-def %state-initializer-def directstring-automaton-def)



(defn define-nfa
	"Define a non-deterministic finite automaton and related
	functions. The generated automaton accepts 2 parameters:
	a. the current session data (used to determine current state); and
	b. input from user
	and uses these to determine the next state. The function updates and
	returns the session data to the caller.
	---
	Transition table definition:
	state table ::= <state> transitions
	transitions ::= {transitions}*
	transition ::= (<input> <predicate> <state-if-true> <state-if-false> &optional <actions>)"
	[transition-table]
	[;(def state-direct (eval (directstring-automaton-def 'session-data transition-table)))
	 (def state-automaton (eval (%nfa-automaton-def 'session-data transition-table)))
	 (def ^:dynamic *valid-states* (let [result (atom #{})]
																	 (doseq [[state & transitions] transition-table]
																		 (when state (swap! result conj state))
																		 ;(log/debugf "transitions=%s"transitions)
																		 (doseq [[_ _ state-t state-f] transitions]
																			 ;(log/debugf "state-t=%s,state-f=%s"state-t state-f)
																			 (when state-t (swap! result conj state-t))
																			 (when state-f (swap! result conj state-f))))
																	 (log/debugf "*valid-states* definition=%s"@result)
																	 @result))
	 (def ^:dynamic *final-states* (let [transitions (reduce conj
																													 (map (fn [[state & transitions]]
																																	{state (let [result (atom #{})]
																																					 (doseq [[_ _ state-t state-f] transitions]
																																						 (when state-t (swap! result conj state-t))
																																						 (when state-f (swap! result conj state-f)))
																																					 @result)})
																																transition-table))
																			 outfinal (into #{} (doall (remove #(not (empty? (transitions %))) *valid-states*)))
																			 _ (log/debugf "*final-states* definition=%s"outfinal)]
																	 outfinal))
	 (defn valid-state-p [state] (if (*valid-states* state) true false))
	 (defn final-state-p [state] (if (*final-states* state) true false))])

(defn define-state-renderer [state-texts]
	(def render-state (eval (%ussd-renderer-def 'session-data state-texts))))


(defn define-state-initializer [initialization-table]
	(def get-initial-state (eval (%state-initializer-def 'session-data initialization-table))))

;; -------
;;  SEXP generation functions.

(defn %nfa-automaton-def [session-data-var transition-table]
	(let [input-var (gensym "input")]
		`(fn [~session-data-var ~input-var]
			 (let [~'input     ~input-var
						 ~'session   (fn [field#]
													 ((deref ~session-data-var) field#))

						 ~'check-menu? (fn [] (~'session :menu-3p))
						 ~'check-type? (fn [] (~'session :type))
						 ~'can-lend? (fn [amount#]
													 (and (>= (or (~'session :max-loanable) 0) amount#)
																(or (empty? @db/max-allowed-balances)
																		(let [min# (@db/max-allowed-balances amount#)
																					bal# (~'session :ma-balance)]
																			(if min#
																				(if bal#
																					(<= bal# min#)
																					(throw (Exception. (format "Main account balance unknown."))))
																				true)))))
						 ~'subflag?  (fn [bit#]
													 (bit-test (or (~'session :subscriber-flags) 0) bit#))
						 ~'session+  (fn [& args#]
													 (reset! ~session-data-var (sessions/session-data-update @~session-data-var (utils/options->map args#))))
						 ~'get-gross (fn [loan-type# amount# charging-method#] (db/get-gross-amount loan-type# amount# charging-method#))
						 ~'get-net   (fn [loan-type# amount# charging-method#] (db/get-net-amount loan-type# amount# charging-method#))
						 ~'gross     ~'get-gross
						 ~'net       ~'get-net
						 ~'fee       (fn [loan-type# amount#] (db/get-fee-amount loan-type# amount#))
						 ;; ---
						 {:keys [~'subscriber ~'state ~'language ~'subscriber-flags ~'account-status ~'age-on-network
										 ~'queue-count ~'queue-total ~'loan-balance ~'ma-balance ~'max-loanable ~'max-qualified
										 ~'amount-requested ~'amount-gross ~'amount-net ~'serviceq ~'ussd-string ~'outstanding ~'loan-type]}
						 @~session-data-var]
				 (let [current-state# (~'session :state)]
					 (condp = current-state#
						 ~@(mapcat (fn [[state & transitions]]
												 (list state
															 `(condp = ~input-var
																	~@(mapcat (fn [[%input predicate state-t state-f & actions]]
																							(log/debug (format "showingTransition(state=%s,input=%s,predicate=%s,next={%s,%s},actions=%s)"
																																 state %input predicate state-t state-f actions))
																							;; Validate transition definition.
																							(when-not (or (= %input :any) (string? %input))
																								(throw (RuntimeException. (format "State input improper for `%s'. Expected :any|string got `%s'" state %input))))
																							;(when (= state-t state) (log/warn (format "Potential cycle detected in [%s]%s/%s." %input state state-t)))
																							;(when (= state-f state) (log/warn (format "Potential cycle detected in [%s]%s/%s." %input state state-f)))
																							;; ---
																							(let [body `(do ~(if predicate
																																 `(if ~predicate
																																		(do ~@actions
																																				(swap! ~session-data-var assoc :state ~state-t))
																																		(swap! ~session-data-var assoc :state ~state-f))
																																 `(do ~@actions
																																			(swap! ~session-data-var assoc :state ~state-t)))
																															@~session-data-var)]
																								(if (string? %input)
																									`(~%input ~body)
																									(list body))))
																						transitions))))
											 transition-table)
						 (throw (Exception.  (format "unknownState(%s)" current-state#)))))))))


(defn- %ussd-renderer-def
	"Return the definition of the function to be used in rendering a
session on the USSD menu.
---
State text definition:
 state-texts ::= {state-text}+
 state-text ::= (<state> &rest {line}+)
 line ::= {<show-condition> {message}+}
 message ::= (<lang> <text>)
 text ::= (or string list)"
	[session-data-var state-texts]
	(let [known-languages (let [result (atom #{})]
													(doseq [[_ & texts] state-texts]
														;(log/debugf "state-texts %s"state-texts)
														;(log/debugf "texts %s"texts)
														(doseq [[_ & messages] texts]
															;(log/debugf "messages %s"messages)
															(doseq [[lang] messages]
																;(log/debugf "lang %s"lang)
																(swap! result conj lang))))
													@result)
				%generate-printer (fn [language]
														`(condp = ~'state
															 ~@(mapcat (fn [[state# & lines#]]
																					 (let [line-count# (count lines#)]
																						 (when (zero? line-count#)
																							 (throw (Exception. (format "!stateMessages(`%s')" state#))))
																						 (let [need-stream?# (atom (> line-count# 1))
																									 body# (doall
																													 (map (fn [[condition# & messages#] line-number#]
																																	(let [[_ text#] (first (filter (fn [[lang#]] (= lang# language)) messages#))]
																																		(when-not text#
																																			(log/warn (format "!translation(state=%s,lang=%s,line=%s)" state# language line-number#)))
																																		(let [more?#  (< line-number# line-count#)
																																					result# (condp = (type text#)
																																										nil `(do)
																																										String (if (= line-count# 1) text#
																																																								 (if more?# `(println ~text#) `(print ~text#)))
																																										PersistentList
																																										(do (reset! need-stream?# true)
																																												`(do ~@(let [result#
																																																		 (map (fn [elt#]
																																																						(condp = (type elt#)
																																																							String `(print ~elt#)
																																																							Keyword `(print (~'%param ~elt# (env :denom-factor)))
																																																							(throw (Exception. (format "unexpectedCompositeType(%s,text=%s)"
																																																																				 (type elt#) elt#)))))
																																																					text#)]
																																																 (if more?#
																																																	 `(~@result# (println))
																																																	 result#)))))]
																																			(if condition# `(when ~condition# ~result#) result#))))
																																lines# (range 1 (+ 1 line-count#))))
																									 body# (if @need-stream?# `((with-out-str ~@body#)) body#)]
																							 (list* state# body#))))
																				 state-texts)))]
		`(fn [~session-data-var]
			 (let [~'session  (fn [field#]
													;(log/debugf "session-data params" ~(~session-data-var field#))
													(~session-data-var field#))
						 ~'%param (fn [param# factor#]
												(let [value# (param# ~session-data-var)]
													(when (nil? value#)
														(log/errorf "unexpectedNullParam(%s, session=%s)" param# ~session-data-var))
													(if (param# #{:queue-total :loan-balance :max-loanable :max-qualified :amount-requested
																				:serviceq :amount-net :amount-gross :menu-3p :loan-type})
														(let [calval# (double (/ value# factor#))]
															(if (= (mod calval# 10) 0.0)
																(str (int calval#))
																(str calval#)))
														value#)))
						 {:keys [~'ma-balance ~'max-loanable]} ~session-data-var
						 ~'mabal-above-limit? (fn [amount#]
																		(if (empty? @db/max-allowed-balances)
																			false
																			(let [min# (@db/max-allowed-balances amount#)]
																				(if min#
																					(if ~'ma-balance
																						(> ~'ma-balance min#)
																						(throw (Exception. "Main account balance unknown.")))
																					false))))

						 ~'check-menu? (fn [] (if (~session-data-var :menu-3p) true false))
						 ~'check-type? (fn [] (if (= (~session-data-var :type) :lend-airtime) true false))
						 ~'can-lend? (fn [amount#]
													 (and (>= (or ~'max-loanable 0) amount#)
																(or (empty? @db/max-allowed-balances)
																		(let [min# (@db/max-allowed-balances amount#)]
																			(if min#
																				(if ~'ma-balance
																					(<= ~'ma-balance min#)
																					(throw (RuntimeException. "Main account balance unknown.")))
																				true)))))
						 ~'get-gross (fn [loan-type# amount# charging-method#] (db/get-gross-amount loan-type# amount# charging-method#))
						 ~'get-net   (fn [loan-type# amount# charging-method#] (db/get-net-amount loan-type# amount# charging-method#))
						 ~'gross     ~'get-gross
						 ~'net       ~'get-net
						 ~'fee       (fn [loan-type# amount#] (db/get-fee-amount loan-type# amount#))
						 ;; ---
						 {:keys [~'subscriber ~'state ~'language ~'subscriber-flags ~'account-status ~'age-on-network
										 ~'queue-count ~'queue-total ~'loan-balance ~'max-qualified ~'max-loanable
										 ~'amount-requested ~'amount-gross ~'amount-net ~'serviceq ~'ussd-string
										 ~'outstanding ~'loan-type]}
						 ~session-data-var
						 ;; ---
						 ~'subflag? (fn [bit#]
													(bit-test (or ~'subscriber-flags 0) bit#))]
				 ~(if (= (count known-languages) 1)
						(do
							(log/debugf "known-languages %s" (%generate-printer (first known-languages)))
							(%generate-printer (first known-languages)))
						`(condp = ~'language
							 ~@(mapcat (fn [lang#]
													 `(~lang# ~(%generate-printer lang#)))
												 known-languages)
							 (throw (Exception. (format "noTranslations(lang=%s)" ~'language)))))))))


(defn- %state-initializer-def
	"Return the definition of the function to be used in determining the
	starting state of a USSD session.
	---
	State initialization table definition:
	initialization-table ::= {initialization-entry}+
	initialization-entry ::= predicate actions state"
	[session-data-var initialization-table]
	(if (empty? initialization-table)
		(constantly nil)
		`(fn [~session-data-var]
			 (let [~'session  (fn [field#]
													((deref ~session-data-var) field#))
						 ~'session+ (fn [& args#]
													(reset! ~session-data-var (sessions/session-data-update @~session-data-var (utils/options->map args#))))
						 ~'get-gross (fn [loan-type# amount# charging-method#] (db/get-gross-amount loan-type# amount# charging-method#))
						 ~'get-net   (fn [loan-type# amount# charging-method#] (db/get-net-amount loan-type# amount# charging-method#))
						 ~'gross     ~'get-gross
						 ~'net       ~'get-net
						 ~'fee       (fn [loan-type# amount#] (db/get-fee-amount loan-type# amount#))
						 ;; ---
						 {:keys [~'subscriber ~'state ~'language ~'subscriber-flags ~'account-status ~'age-on-network
										 ~'queue-count ~'queue-total ~'loan-balance ~'ma-balance ~'max-loanable ~'max-qualified
										 ~'amount-requested ~'amount-gross ~'amount-net ~'serviceq ~'ussd-string
										 ~'outstanding ~'menu-3p ~'type ~'loan-type]}
						 @~session-data-var
						 ;; ---
						 ~'check-menu? (fn [] (if ~'menu-3p true false))

						 ~'check-type? (fn [] (if (= ~'type :lend-airtime) true false))
						 ~'can-lend? (fn [amount#]
													 (and (>= (or ~'max-loanable 0) amount#)
																(or (empty? @db/max-allowed-balances)
																		(let [min# (@db/max-allowed-balances amount#)]
																			(if min#
																				(if (when-not (= ~'ma-balance :error) ~'ma-balance)
																					(<= ~'ma-balance min#)
																					(throw (Exception. (format "Main account balance unknown."))))
																				true)))))
						 ~'subflag? (fn [bit#]
													(bit-test (or ~'subscriber-flags 0) bit#))]
				 (let [state# (cond ~@(mapcat (fn [[predicate# actions# state#]]
																				(log/debug (format "showingIntializing(state=%s,predicate=%s,actions=%s)"
																													 state# predicate# actions#))
																				[predicate# `(do ~@actions# ~state#)])
																			initialization-table))]
					 state#)))))
