(ns ghas-acs.config
	(:require
		[aero.core :as aero]
		[mount.core :refer [args defstate]]
		[clojure.java.io :as io])
	(:import (java.util MissingResourceException)))

; (from-file (System/getProperty path-prop)))
;  ([path]
;   (let [path (expand-home path)
;         file (io/file path)]
;     (if (and file (.exists file))

(defn- from-file []
	(let [path (System/getProperty "erl.gh.acs")
				file (io/file path)]
		(if (and file (.exists file))
			path
			(throw (MissingResourceException.
							 (str "can't find a configuration file path: \"" path "\". ")
							 "" "")))))

(defn- read-config []
	(aero/read-config (from-file)))

(defstate env
					:start (read-config))

