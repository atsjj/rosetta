(ns summit.sap.routes
  (:require [compojure.core :refer [defroutes GET routes wrap-routes context]]

            [summit.utils.core :refer [->int]]
            [summit.sap.project :as project]
            [summit.sap.order :as order]

            [summit.utils.core :as utils]))

(def default-server (atom :prd))

;; ----------------------  cache functions

(defonce ^:private rfc-caches (atom {}))

(defn- clear-cache []
  (reset! rfc-caches {}))
;; (clear-cache)

(defn- force-cache [& fn-var-and-args]
  (println fn-var-and-args)
  (let [key fn-var-and-args
        result (apply (first fn-var-and-args) (rest fn-var-and-args))]
    (swap! rfc-caches assoc key result)
    result))
;; (force-cache #'get-project :prd 3)

(defn- cache [& fn-var-and-args]
  (let [key fn-var-and-args
        result (@rfc-caches key)]
    (if result
      result
      (apply force-cache fn-var-and-args))))
;; (cache #'get-project :prd 3)

;; ------------------------------- cron jobs

(defn set-interval [callback ms]
  (future (while true (do (Thread/sleep ms) (callback)))))

(defn set-interval-named [name callback ms]
  (set-interval (fn []
                  (println "Processing cron-job " name)
                  (callback))
                ms))

;; (def job (set-interval #(println "hey") 1000))
;; (future-cancel job)
;; (def job (set-interval-named :boo #(println "hey") 1000))
;; (future-cancel job)


(defn gather-params [req]
  (merge
   (:params req)
   ;; (:filter req)))    ;; to accomodate json-api
   (:filter (:params req))))    ;; to accomodate json-api

(defn get-param
  "return named parameter from request"
  ([request param-name] (get-param request param-name nil))
  ([request param-name default]
   (utils/ppn "params" (:params request) "filter" (:filter request) "gathered" (gather-params request) param-name default)
   (or ((gather-params request) param-name) default)))
   ;; (let [params (:params request)
   ;;       filters (:filters params)
   ;;       param (keyword param-name)]
   ;;   ;; will be in filters for json-api
   ;;   (or (and filters (param filters)) (param params) default))))

(defn get-keyword-param
  ([request param-name] (get-keyword-param request param-name nil))
  ([request param-name default]
   (keyword (get-param request param-name default))))

(defn get-env-param
  ([request param-name] (get-env-param request param-name nil))
  ([request param-name default]
   (keyword (or (get-in (gather-params request) [:env param-name]) default))))

(defn- get-project [server project-id]
  (project/project server project-id))

(defn- get-projects [server account-num]
  (project/projects server account-num))

(defn- get-order [customer server order-id]
  (order/order->json-api (order/order server order-id)))

(defn debugged-body [m request debug-map]
  (let [body (or m {:errors ["not found"]})]
    (if true
      (merge body {:meta {:debug (merge debug-map {:request (utils/clean-request request)})}})
      body
      )))

(defn json-api-response
  ([m] (json-api-response m {} {}))
  ([m request] (json-api-response m request {}))
  ([m request debug-map]
   (let [body (debugged-body m request debug-map)]
     (if (nil? m)
       {:status 404
        :headers {"Content-Type" "application/vnd.api+json; charset=utf-8"}
        :body body}
       {:status 200
        :headers {"Content-Type" "application/vnd.api+json; charset=utf-8"}
        :body body}
       ))))

(defroutes sap-routes-v2
  (context "/api" []
    (context "/:version-num" []
      (GET "/clear-cache" req
        (clear-cache)
        {:status 200
         :headers {"Content-Type" "text/json"}
         :body {:cache-cleared true}})

      (GET "/default-server/:server-name" req
        (let [server-name (get-in req [:params :server-name])]
          (if (contains? #{"dev" "qas" "prd"} server-name)
            (do
              (reset! default-server (keyword server-name))
              {:status 200
               :headers {"Content-Type" "text/json"}
               :body {:new-server-name server-name}
               })
            {:status 400
             :headers {"Content-Type" "text/json"}
             :body {:no-such-server server-name}
             }
            )))

      (GET "/default-server" req
        {:status 200
         :headers {"Content-Type" "text/json"}
         :body {:server-name @default-server}
         })

      (GET "/accounts/:account-id/projects/:project-id" req
        (let [server (keyword (get-env-param req :server @default-server))
              id (->int (get-param req :project-id nil))
              ;; proj (get-project server id)
              proj (cache #'get-project server id)
              ]
          (json-api-response proj
                             req
                             {:server server :project-id id}
                             )
          ))

    (GET "/projects/:project-id" req
      (let [server (keyword (get-env-param req :server @default-server))
            id (->int (get-param req :project-id nil))
            ;; proj (get-project server id)
            proj (cache #'get-project server id)
            ]
        (json-api-response proj
                           req
                           {:server server :project-id id}
                           )
        ))

    (GET "/accounts/:account-id/projects" req
      (let [server (keyword (get-env-param req :server @default-server))
            account-num (get-param req :account-id nil)
            projs (get-projects server account-num)]
        (json-api-response projs
                           req
                           {:server server :account-num account-num}
                           )
        ))

    (GET "/projects" req
      (let [server (keyword (get-env-param req :server @default-server))
            account-num (get-param req :account nil)
            projs (get-projects server account-num)]
        (json-api-response projs
                           req
                           {:server server :account-num account-num}
                           )
        ))

    (GET "/orders/:id" req
      (let [customer nil
            server (keyword (get-env-param req :server @default-server))
            id (->int (get-param req :id nil))
            order (get-order customer server id)
            ;; order (cache #'get-order customer server id)
            ]
        (json-api-response order
                           req
                           {:server server :customer customer :order-id id}
                           )
        )))
    ))



(defonce cron-jobs (atom {}))

(def hourly-jobs
  {:project-id-3 #(force-cache #'get-project :prd 3)
   })

(defn start-cron-jobs []
  (let [duration (* 60 60 1000)]
    (for [[name job] hourly-jobs]
      (swap! cron-jobs assoc name (set-interval-named name job duration))
      )))

(defn stop-cron-jobs []
  (for [[name job] @cron-jobs]
    (do
      (future-cancel job)
      (swap! cron-jobs dissoc ))))

(stop-cron-jobs)
(start-cron-jobs)

