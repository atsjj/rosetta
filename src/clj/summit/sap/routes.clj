(ns summit.sap.routes
  (:require [compojure.core :refer [defroutes GET routes wrap-routes context]]

            [summit.utils.core :refer [->int]]
            [summit.sap.project :as project]
            [summit.sap.order :as order]

            [summit.utils.core :as utils]))

(def default-server :qas)

(defn get-param
  "return named parameter from request"
  ([request param-name] (get-param request param-name nil))
  ([request param-name default]
   (let [params (:params request)
         filters (:filters params)
         param (keyword param-name)]
     ;; will be in filters for json-api
     (or (and filters (param filters)) (param params) default))))

(defn get-keyword-param
  ([request param-name] (get-param request param-name nil))
  ([request param-name default]
   (keyword (get-param request param-name default))))

(defn get-project [customer server project-id]
  (project/project server project-id)
  )

(defn get-projects [customer server account-num]
  (project/projects server account-num)
  )

(defn get-order [customer server order-id]
  (order/order->json-api (order/order server order-id)))

(defn debugged-body [m request debug-map]
  (let [body (or m {:errors ["not found"]})]
    (if true
      (merge body {:debug (merge debug-map {:request (utils/clean-request request)})})
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

(defroutes sap-routes
  (context "/api" []
    (GET "/projects/:id" req
      (let [customer nil
            server (keyword (get-keyword-param req :server default-server))
            id (->int (get-param req :id nil))
            ;; id (->int (:id (:params req)))
            ;; id 1
            proj (get-project customer server id)
            ]
        (json-api-response proj
                           req
                           {:server server :customer customer :project-id id}
                           )
        ))

    (GET "/projects" req
      (let [server (keyword (get-keyword-param req :server default-server))
            customer nil
            account-num (get-param req :account "1002225")
            projs (get-projects customer server account-num)]
        (json-api-response projs
                           req
                           {:server server :customer customer :account-num account-num}
                           )
        ))

    (GET "/v2/orders/:id" req
      (let [customer nil
            ;; server (keyword (get-keyword-param req :server default-server))
            server (keyword (get-keyword-param req :server :prd))
            id (->int (get-param req :id nil))
            ;; id (->int (:id (:params req)))
            ;; id 1
            proj (get-order customer server id)
            ]
        (json-api-response proj
                           req
                           {:server server :customer customer :order-id id}
                           )
        ))
    ))
