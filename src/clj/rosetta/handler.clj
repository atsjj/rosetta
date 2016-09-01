(ns rosetta.handler
  (:require [compojure.core :refer [routes wrap-routes]]
            [rosetta.layout :refer [error-page]]
            [rosetta.routes.home :refer [home-routes]]
            [compojure.route :as route]
            [rosetta.env :refer [defaults]]
            [mount.core :as mount]
            [rosetta.middleware :as middleware]

            [summit.sap.routes :refer [sap-routes]]
            ))

(mount/defstate init-app
                :start ((or (:init defaults) identity))
                :stop  ((or (:stop defaults) identity)))

(def app-routes
  (routes
    (-> #'sap-routes
        ;; #'home-routes
        (wrap-routes middleware/wrap-csrf)
        (wrap-routes middleware/wrap-formats))
    (route/not-found
      (:body
        (error-page {:status 404
                     :title "page not found"})))))


(defn app [] (middleware/wrap-base #'app-routes))
