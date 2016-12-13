(println "loading summit.sap.routes")

(ns summit.sap.routes
  (:require [compojure.core :refer [defroutes GET routes wrap-routes context]]

            [summit.utils.core :refer [->int]]
            [summit.sap.core :as erp]
            [summit.sap.project :as project]
            [summit.sap.spreadsheet :as spreadsheet]
            ;; [summit.sap.project2 :as project2]
            [summit.sap.order :as order]

            [summit.utils.core :as utils]
            [summit.sap.periodic :as periodic]

            [clojure.string :as str]
            [clojure.java.io :as io]))

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
;; (force-cache #'get-project :prd 322)
;; (apply #'get-project '(:prd 323))
;; (get-project :prd 324)

(defn- cache [& fn-var-and-args]
  (let [key fn-var-and-args
        result (@rfc-caches key)]
    (if result
      result
      (apply force-cache fn-var-and-args))))
;; (cache #'get-project :prd 3)
;; (keys (cache #'get-project :prd 3))


(defn gather-params [req]
  (merge
   (:params req)
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

#_(defn get-keyword-param
  ([request param-name] (get-keyword-param request param-name nil))
  ([request param-name default]
   (keyword (get-param request param-name default))))

(defn get-env-param
  ([request param-name] (get-env-param request param-name nil))
  ([request param-name default]
   (keyword (or (get-in (gather-params request) [:env param-name]) default))))

(defn- get-project [server project-id]
  (project/project server project-id))
;; (get-project :qas 1)
;; (get-project :qas 197)
;; (cache #'get-project :qas 1)
;; (cache #'get-project :qas 598)
;; (cache #'get-project :qas 498)
;; (keys (cache #'get-project :qas 1))
;; (json-api-response (cache #'get-project :qas 1) {:x 3} {:y 4})
;; (keys (json-api-response (cache #'get-project :qas 1) {:x 3} {:y 4}))

(defn- get-projects [server account-num]
  (project/projects server account-num))

(defn- get-order [customer server order-id]
  (order/order->json-api (order/order server order-id)))

(defn debugged-body [m request debug-map]
  (let [body (or m {:errors ["not found"]})]
    (if true
      ;; (merge body {:meta {:debug (merge debug-map {:request (utils/clean-request request)})}})
      body
      body
      )))

(defn json-api-response
  ([m] (json-api-response m {} {}))
  ([m request] (json-api-response m request {}))
  ([m request debug-map]
   (let [body (debugged-body m request debug-map)]
     (if (nil? m)
       {:status 404
        ;; :headers {"Content-Type" "application/vnd.api+json; charset=utf-8"}
        :headers {"Content-Type" "text/json; charset=utf-8"}
        :body body}
       {:status 200
        :headers {"Content-Type" "application/vnd.api+json; charset=utf-8"}
        ;; :headers {"Content-Type" "text/json; charset=utf-8"}
        :body body}
       ))))

(defroutes sap-routes-v2
  (context "/api" []
    (context "/:version-num" []

      (GET "/describe/:func-name" req
        (let [func-name (keyword (get-in req [:params :func-name]))]
          {:status  200
           :headers {"Content-Type" "text/json"}
           :body    (erp/function-interface (erp/find-function :qas func-name))}))

      ;; (GET "/projects2/:project-id" req
      ;;   (let [server (keyword (get-env-param req :server @default-server))
      ;;         id     (->int (get-param req :project-id nil))
      ;;         proj   (project2/project server id)
      ;;         ]
      ;;     (json-api-response proj
      ;;                        req
      ;;                        {:server server :project-id id}
      ;;                        )
      ;;     ))




      (GET "/clear-cache" req
        (clear-cache)
        {:status  200
         :headers {"Content-Type" "text/json"}
         :body    {:cache-cleared true}})

      (GET "/default-server/:server-name" req
        (let [server-name (get-in req [:params :server-name])]
          (if (contains? #{"dev" "qas" "prd"} server-name)
            (do
              (reset! default-server (keyword server-name))
              {:status  200
               :headers {"Content-Type" "text/json"}
               :body    {:new-server-name server-name}
               })
            {:status  400
             :headers {"Content-Type" "text/json"}
             :body    {:no-such-server server-name}
             }
            )))

      (GET "/default-server" req
        {:status  200
         :headers {"Content-Type" "text/json"}
         :body    {:server-name @default-server}
         })

      (GET "/accounts/:account-id/projects/:project-id" req
        (let [server (keyword (get-env-param req :server @default-server))
              id     (->int (get-param req :project-id nil))
              ;; proj   (cache #'get-project server id)
              proj   (get-project server id)
              ]
          (json-api-response proj
                             req
                             {:server server :project-id id}
                             )
          ))

      (GET "/project-raw/:project-id" req
        (println "in /project-raw")
        (let [server (keyword (get-env-param req :server @default-server))
              id     (->int (get-param req :project-id nil))
              proj   (project/project-raw-data server id)
              ]
          {:status 200
           :headers {"Content-Type" "text/json; charset=utf-8"}
           :body proj}
          ))

      (GET "/project-spreadsheet/:project-id" req
        (println "in /project-spreadsheet")
        (let [server (keyword (get-env-param req :server @default-server))
              id     (->int (get-param req :project-id nil))
              ;; proj   (project/project-spreadsheet-data server id)
              proj   (project/fetch-status-lines server id)
              filepath (spreadsheet/create-temp-spreadsheet "amps" "xlsx" (:headers proj) (:data proj))
              filename (last (str/split filepath #"/"))
              ;; file (spreadsheet/read-file filepath)
              ]
          {:status 200
           ;; :headers {"Content-Type" "text/json; charset=utf-8"}
           :headers {"Content-Type" spreadsheet/mime-spreadsheet
                     ;; "text/json; charset=utf-8"
                     ;; "Content-Disposition" (str "attachment; filename=\"" filename "\"")
                     "Content-Disposition" (str "inline; filename=\"" filename "\"")
                     ;; "Content-Length" (count file)
                     }
           :body (io/file filepath)}
          ))

      (GET "/project-spreadsheet-data/:project-id" req
        (println "in /project-spreadsheet-data")
        (let [server (keyword (get-env-param req :server @default-server))
              id     (->int (get-param req :project-id nil))
              ;; proj   (project/project-spreadsheet-data server id)
              proj   (project/fetch-status-lines server id)
              ]
          {:status 200
           :headers {"Content-Type" "text/json; charset=utf-8"}
           :body proj}
          ))

      (GET "/projects/:project-id" req
           (println "in /projects/:project-id")
        (let [server (keyword (get-env-param req :server @default-server))
              id     (->int (get-param req :project-id nil))
              ;; proj (get-project server id)
              ;; proj   (cache #'get-project server id)
              proj (get-project server id)
              ]
          ;; {:status 200
          ;;  :headers {"Content-Type" "application/json; charset=utf-8"}
          ;;  ;; :body proj
          ;;  :body {:a 3}
          ;;  }
          (json-api-response proj
          ;; (json-api-response {:a 4}  ;;proj
                             req
                             {:server server :project-id id}
                             )
          ))

      (GET "/accounts/:account-id/projects" req
        (let [server      (keyword (get-env-param req :server @default-server))
              account-num (get-param req :account-id nil)
              projs       (get-projects server account-num)]
          (json-api-response projs
                             req
                             {:server server :account-num account-num}
                             )
          ))

      (GET "/projects" req
        (let [server      (keyword (get-env-param req :server @default-server))
              account-num (get-param req :account nil)
              projs       (get-projects server account-num)]
          (json-api-response projs
                             req
                             {:server server :account-num account-num}
                             )
          ))

      (GET "/orders/:id" req
        (let [customer nil
              server   (keyword (get-env-param req :server @default-server))
              id       (->int (get-param req :id nil))
              order    (get-order customer server id)
              ]
          (json-api-response order
                             req
                             {:server server :customer customer :order-id id}
                             )
          ))
      )))


(def quarter-hourly-jobs
  {
   :project-id-3 #(force-cache #'get-project :prd 3)
   })
(def hourly-jobs
  {
   ;; :errors-are-caught #(do (println "hey") (/ 1 0))
   ;; :project-id-3 #(force-cache #'get-project :prd 3)
   })
(def daily-jobs
  {
   ;; :project-id-3 #(project/projects :prd 1037657)
   })


;; bapi for retrieving a project does not return the owning account number.
;; therefore when we retrieve the projects for an account number, we cache the
;; linkage between account number and projects (and the name, since it is not
;; available either).
;; but that means the projects bapi must be called first.
;; the call below is made to ensure we have the info on bootup.
(when (nil? (project/project-account-num 3))
  (println "getting projects for account 1037657")
  (project/projects :prd 1037657))

(def second 1000)
(def minute (* 60 second))
(def quarter-hour (* 15 minute))
(def hour (* 60 minute))
(def day (* 24 hour))

(periodic/stop-all-cron-jobs)
;; (periodic/start-cron-jobs second hourly-jobs)
(periodic/start-cron-jobs quarter-hour quarter-hourly-jobs)
;; (periodic/start-cron-jobs hour hourly-jobs)
;; (periodic/start-cron-jobs day daily-jobs)
;; (force-cache #'get-project :prd 3)

(println "done loading summit.sap.routes")
