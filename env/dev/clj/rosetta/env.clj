(ns rosetta.env
  (:require [selmer.parser :as parser]
            [clojure.tools.logging :as log]
            [rosetta.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (parser/cache-off!)
     (log/info "\n-=[rosetta started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[rosetta has shut down successfully]=-"))
   :middleware wrap-dev})
