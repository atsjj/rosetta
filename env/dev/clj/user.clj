(ns user
  (:require [mount.core :as mount]
            rosetta.core))

(defn start []
  (mount/start-without #'rosetta.core/repl-server))

(defn stop []
  (mount/stop-except #'rosetta.core/repl-server))

(defn restart []
  (stop)
  (start))


