(ns ghas-acs.handler
    (:require [compojure.core :refer :all]
              [compojure.route :as route]
              [mount.core :as mount]
              [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
              [ghas-acs.db :as db :refer [*db*]]
              [ghas-acs.config :refer [env]]
              [ghas-acs.service :as service]
              [ghas-acs.ussd-menu :as menu]
              [clojure.tools.logging :as log]
              [ghas-acs.utils :as utils]
              [ghas-acs.rmqutils :as rmqutils]
              [ghas-acs.sessions :as sessions]
              [ghas-acs.vtu :as vtu]
              [clojure.data.json :as json])
  (:import (java.io File PushbackReader StringReader)
           (java.util Timer TimerTask Date)))


(mount/defstate init
                :start (fn []
                         (log/info "\n-=[ghas-acs started successfully]=-"))
                :stop  (fn []
                         (log/info "\n-=[ghas-acs stopped successfully]=-")))
(def mythread (atom nil))


(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (doseq [component (:stopped (mount/stop))]
    (log/info component "stopped"))
  (when-not  (nil? @mythread)
    (.cancel @mythread)
    (reset! mythread nil))
  ;(rmqutils/shut-down)

  (shutdown-agents)

  (log/info "gloas-acs has shut down!"))

(defroutes app-routes
  (GET "/ussd/erl" request (service/handle-ussd-request request))
    #_(GET "/refund/erl/subs" request (let [params (:params request)
                                          amount (read-string (:amount params))
                                          sub (:sub params)
                                          pg (vtu/pg-credit-sub (utils/generate-request-id) sub :airtime amount)
                                          _ (log/infof "refund=>[%s]"pg)
                                          {:keys [status subscriber request-id]} pg
                                          _ (when (= status :ok)
                                                (rmqutils/temp-send-sms subscriber request-id (str "Dear Customer, you have been credited a refund of GHc"(utils/cedis amount) ". We apologize for any inconvenience caused. Thank you.")))]
                                        {:status  200
                                         :headers {"Content-Type" "text/plain"}
                                         :body    (json/write-str pg)}))
  (route/not-found "Not Found"))

(def app
  (wrap-defaults app-routes api-defaults))


(defn run-auto-recon [autorecon-interval]
  (utils/funcall-logging  ["runAutoRecon"]
                          (db/startLendingRecon) {:intervals autorecon-interval}))


(defn- start-thread-creator [ussd-session-cleanup-interval autorecon-interval]
  (let [interval (long (* ussd-session-cleanup-interval 1000))]
    (reset! mythread (doto (Timer. "thread-pool-1" true)
                       (.scheduleAtFixedRate (proxy [TimerTask] []
                                               (run []
                                                 (when-not (zero? interval)
                                                   (log/infof "startSessionCleaner(%s)"interval)
                                                   (sessions/clear-old-sessions))))
                                             (long 1000) (if (zero? interval) 1000 interval ))
                       (.scheduleAtFixedRate (proxy [TimerTask] []
                                               (run []
                                                 (when-not (zero? autorecon-interval)
                                                  (run-auto-recon autorecon-interval))))
                                             (Date.)
                                             (if (zero? autorecon-interval) 1000 (long autorecon-interval)))))))

(defn init-app []
  "init will be called once when
	app is deployed as a servlet on
	an app server such as Tomcat
	put any initialization code here"
  #_(reset! counters/*server-start-time* (f/unparse
                                         (f/formatters :mysql) (l/local-now)))
  (doseq [component (:started (mount/start))]
    (log/info component "started"))
  ;(log/infof "env=>%s"env)
  (let [ussd-session-cleanup-interval (get-in env [:as :ussd :ussd-session-cleanup-interval] 60)
        recon-lending-interval (get-in env [:pg :recon-lending-interval])
        factor (env :denom-factor)
        _ (db/load-denom-config)
        _ (when-not (nil? (get-in env [:as :ussd :ussd-menu-def]))
            (log/infof "ussd menu def %s" (get-in env [:as :ussd :ussd-menu-def]))
            (when-let [menu-def (File. (get-in env [:as :ussd :ussd-menu-def]))]
              (if (and (.exists #^File menu-def) factor)
                (let [{:keys [transition-table state-texts state-initializer]}
                      (with-open [s (PushbackReader. (StringReader. (utils/get-file-contents menu-def)))]
                        {:transition-table (read s) :state-texts (read s) :state-initializer (read s)})
                      _(menu/define-nfa transition-table)
                      _(menu/define-state-renderer state-texts)
                      _(menu/define-state-initializer state-initializer)])
                (throw (RuntimeException. (format "!found(menu-definition,path=%s or denom-factor=%s) " (str menu-def) factor))))
              (def ^:dynamic *language* :en)))
        _ (start-thread-creator ussd-session-cleanup-interval recon-lending-interval)
        _ (rmqutils/initialize-queue-interfaces)
        denoms (into [] @db/service-charge-rates)]
    (if (empty? denoms)
      (throw (Exception. (format "!denoms unpopulated -> %s" denoms)))
      (log/debugf "loan denoms %s" denoms))))
