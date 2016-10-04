(ns summit.sap.routes
  (:require [compojure.core :refer [defroutes GET routes wrap-routes context]]

            [summit.utils.core :refer [->int]]
            [summit.sap.project :as project]
            [summit.sap.order :as order]

            [summit.utils.core :as utils]))

(def default-server :qas)

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

(defn get-project [server project-id]
  (project/project server project-id)
  )

(defn get-projects [server account-num]
  (project/projects server account-num)
  )

(defn get-order [customer server order-id]
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
      (GET "/accounts/:account-id/projects/:project-id" req
        (let [server (keyword (get-env-param req :server default-server))
              id (->int (get-param req :project-id nil))
              proj (get-project server id)
              ]
          (json-api-response proj
                             req
                             {:server server :project-id id}
                             )
          ))

    (GET "/projects/:project-id" req
      (let [server (keyword (get-env-param req :server default-server))
            id (->int (get-param req :project-id nil))
            proj (get-project server id)
            ]
        (json-api-response proj
                           req
                           {:server server :project-id id}
                           )
        ))

    (GET "/accounts/:account-id/projects" req
      (let [server (keyword (get-env-param req :server default-server))
            account-num (get-param req :account-id nil)
            projs (get-projects server account-num)]
        (json-api-response projs
                           req
                           {:server server :account-num account-num}
                           )
        ))

    (GET "/projects" req
      (let [server (keyword (get-env-param req :server default-server))
            account-num (get-param req :account nil)
            projs (get-projects server account-num)]
        (json-api-response projs
                           req
                           {:server server :account-num account-num}
                           )
        ))

    (GET "/orders/:id" req
      (let [customer nil
            server (keyword (get-env-param req :server default-server))
            id (->int (get-param req :id nil))
            ;; id (->int (:id (:params req)))
            ;; id 1
            proj (get-order customer server id)
            ]
        (json-api-response proj
                           req
                           {:server server :customer customer :order-id id}
                           )
        )))
    ))
