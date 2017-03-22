(ns summit.ecommerce.product-info
  (:require [clojure.java.jdbc :as j]

            [summit.sap.price :as price]
            [summit.sap.conversions :as conv]
            ))

(def db-map (atom {:subprotocol "mysql"
                   :subname (or (System/getenv "DB_HOST") "//localhost:3306/blue_harvest_dev")
                   :user (or (System/getenv "DB_USER") "user")
                   :password (or (System/getenv "DB_PW") "pw")}))

(defn product-sql [matnr]
  (format "select * from products where matnr=\"%s\"" (conv/as-matnr matnr)))

(defn product-info [matnr]
  (merge
   (first (j/query @db-map [(product-sql matnr)]))
   {:price
    (->
     (price/internet-prices [matnr])
     first
     (nth 3)
     )}))
;; (product-info 1)
