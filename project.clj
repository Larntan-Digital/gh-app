(defproject ghas-acs "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [compojure "1.6.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [conman "0.8.6"]
                 [cprop "0.1.16"]
                 [org.clojure/data.json "0.2.6"]
                 [com.novemberain/langohr "5.1.0"]
                 [org.clojure/data.zip "0.1.3"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/core.async "1.1.587"]
                 [org.clojure/tools.cli "1.0.194"]
                 [clj-http "3.12.0"]
                 [clj-time "0.15.2"]
                 [org.clojure/tools.logging "1.0.0"]
                 [org.postgresql/postgresql "42.2.11"]
                 [aero "1.1.6"]
                 [mount "0.1.16"]
                 [hikari-cp "2.13.0"]
                 [seancorfield/next.jdbc "1.1.613"]]

  :source-paths ["src"]
  :test-paths ["test"]
  :resource-paths ["resources"]
  :target-path "target/%s/"
  :main ^:skip-aot ghas-acs.handler

  :plugins [[lein-ring "0.12.5"]
            [lein-uberwar "0.2.0"]]
  :ring {:handler ghas-acs.handler/app}
  :uberwar {:handler ghas-acs.handler/app
            :init ghas-acs.handler/init-app
            :destroy ghas-acs.handler/destroy
            :name "as.war"}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.2"]]}
   :uberjar {:omit-source true
             :aot :all
             :uberjar-name "as.jar"
             :source-paths ["env/prod/clj" ]
             :resource-paths ["env/prod/resources"]}})







