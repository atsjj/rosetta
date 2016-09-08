(println "loading summit.sap.order")

(ns summit.sap.order
  (:require [summit.sap.core :refer :all]
            [clojure.string :as str]
            [summit.utils.core :as utils :refer [->int as-document-num as-short-document-num examples ppn]]
            ))

(defn transform-line-item [line-item]
  (let [order-id (as-short-document-num (:order line-item))]
    (let [line-item-id (as-short-document-num (:item line-item))]
      {:type "line-item"
       :id   (str order-id "-" line-item-id)
       :attributes
       {:delivery-status (:cust-cmpl-status line-item)
        :extendedPrice   0
        :quantity        0
        :price           0
        :shipping-type   (:shipping-type line-item)}
       :relationships
       {:account
        {:data
         {:type "account"
          :id   (as-short-document-num (:customer line-item))}}
        :job-account
        {:data
         {:type "job-account"
          :id   (:job-account line-item)}}
        :order
        {:data
         {:type "order"
          :id   order-id}}
        :product
        {:data
         {:type "product"
          :id   (as-short-document-num (:material line-item))}}}})))

(defn transform-line-items [line-items]
  (utils/ppn (str "transform-order-detail"))
  (map transform-line-item line-items))

(defn retrieve-maps [order-fn]
  (let [attr-defs (partition 3
                             [:line-items :et-orders-detail transform-line-items])]
    (into {}
          (for [attr-def attr-defs]
            (let [web-name (first attr-def)
                  sap-name (second attr-def)
                  name-transform-fn (nth attr-def 2)]
              [web-name (name-transform-fn (pull-map order-fn sap-name))])))))

(defn transform-order [order-fn]
  (let [maps (retrieve-maps order-fn)]
    maps))

(defn order
  ([order-id] (order :qas order-id))
  ([system order-id]
    (utils/ppn (str "getting order " order-id " on " system))
    (let [order-fn (find-function system :Z_O_ORDERS_QUERY) id-seq-num (atom 0)]
      (push order-fn {
        ; :i-customer "0001002225"
        :i-order (as-document-num order-id)
        :if-orders "X"
        :if-details "X"
        :if-addresses "X"
        :if-texts "X"})
      (execute order-fn)
      ;; (transform-order order-fn))))
      order-fn)))
      ; (execute order-fn)
      ; (let [result (transform-order order-fn)]
      ;   (if result (assoc-in result [:data :id] order-id))))))
; (def x (order "3970176"))
; (order "3970176")
(def f (order "3970176"))

;; (pull-map f :et-orders-detail)
(ppn (:line-items (retrieve-maps f))) 
;; (retrieve-maps f)
;; (ppn (pull-map (utils/ppl "function" f) :et-orders-detail))
;; (keys f)
;; (ppn (function-interface f))
;; (ppn (keys (function-interface f)))

(println "done loading summit.sap.order")
