(println "loading summit.sap.order")

(ns summit.sap.order
  (:require [summit.sap.core :refer :all]
            [clojure.string :as str]
            [summit.utils.core :as utils :refer [->int ->long as-document-num ppn]]
            [summit.sap.lookup-tables :as lookup-tables :refer [deliver-statuses shipping-types]]
            ))

(defn transform-address [address]
  {:id       (->int (:address-code address))
   :name     (:name address)
   :name2    (:name2 address)
   :city     (:city address)
   :county   (:county address)
   :state    (:state address)
   :street   (:street address)
   :street2  (:street2 address)
   :po-box   (:po-box address)
   :zip-code (:zip-code address)
   :country  (:country address)})

(defn transform-comment [comment]
  (let [order-id (->int (:order comment))]
    (let [comment-id (->int (:item comment))]
      {:id           (str order-id "-" comment-id)
       :line-item-id (str order-id "-" (->int (:item comment)))
       :order-id     order-id
       :value        (:tdline comment)})))

(defn transform-line-item [line-item]
  (let [order-id (->int (:order line-item))]
    (let [line-item-id  (->int (:item line-item))
          shipping-name (shipping-types (:shipping-type line-item))]
      {:id                 (str order-id "-" line-item-id)
       :account-id         (->int (:customer line-item))
       ;; :job-account-id     (:job-account line-item)
       :job-account-id     (->long (:job-account line-item))
       :order-id           order-id
       :product-id         (->int (:material line-item))
       :delivered-quantity (:delivered-qty line-item)
       :delivery-status    (deliver-statuses (:cust-cmpl-status line-item))
       :number-per-unit    (:num-per-unit line-item)
       :quantity           (:ordered-qty line-item)
       :shipping-type      shipping-name
       :total              (:total-item-price line-item)
       :unit-price         (:unit-price line-item)
       :uom                (:unit-of-measure line-item)})))

(defn transform-order-summary [order]
  {:id               (->int (:order order))
   :account-id       (->int (:customer order))
   :job-account-id   (->long (:job-account order))
   :bill-address-id  (->int (:bill-address-code order))
   :pay-address-id   (->int (:pay-address-code order))
   :ship-address-id  (->int (:ship-address-code order))
   :sold-address-id  (->int (:sold-address-code order))
   :created-at       (:created-date order)
   :expected-at      (:cust-expected order)
   :job-account-name (:job-account-name order)
   :line-item-count  (:number-of-items order)
   :purchase-order   (:cust-po order)
   :shipping-type    (shipping-types (:shipping-type order))
   :status           (deliver-statuses (:cust-cmpl-status order))
   :sub-total        (:subtotal order)
   :tax              (:sales-tax order)
   :total            (:total-cost order)})

(defn transform-addresses [addresses]
  (map transform-address addresses))

(defn transform-comments [comments]
  (map transform-comment comments))

(defn transform-line-items [line-items]
  (map transform-line-item line-items))

(defn transform-orders-summary [orders]
  (map transform-order-summary orders))

(defn retrieve-maps [order-fn]
  (let [attr-defs (partition 3
                             [:addresses :et-addresses transform-addresses
                              :comments :et-text transform-comments
                              :line-items :et-orders-detail transform-line-items
                              :orders :et-orders-summary transform-orders-summary])]
    (into {}
          (for [attr-def attr-defs]
            (let [web-name          (first attr-def)
                  sap-name          (second attr-def)
                  name-transform-fn (nth attr-def 2)]
              [web-name (name-transform-fn (pull-map order-fn sap-name))])))))

(defn- collect-addresses-for-order [order addresses]
  (let [ids (list
             (:bill-address-id order)
             (:ship-address-id order)
             (:sold-address-id order)
             (:pay-address-id order))]
    (filter (fn [x] (some #(= (:id x) %) ids)) addresses)))

(defn- collect-items-for-order [order-id items]
  (filter #(= order-id (:order-id %)) items))

(defn transform-order [maps order]
  (let [id (:id order)]
    {:order order
     :addresses (collect-addresses-for-order order (:addresses maps))
     :comments (collect-items-for-order id (:comments maps))
     :line-items (collect-items-for-order id (:line-items maps))}))

(defn transform-orders [order-fn]
  (let [maps (retrieve-maps order-fn)]
    (let [orders (:orders maps)]
      (map (partial transform-order maps) orders))))

(defn order-rfc
  ([order-id] (order-rfc :qas order-id))
  ([system order-id]
    (ppn (str "getting order " order-id " on " system))
    (let [order-fn (find-function system :Z_O_ORDERS_QUERY)]
      (push order-fn {
        :i-order (as-document-num order-id)
        :if-orders "X"
        :if-details "X"
        :if-addresses "X"
        :if-texts "X"})
      (execute order-fn)
      order-fn)))

(defn order
  ([order-id] (order :qas order-id))
  ([system order-id]
   (let [response (order-rfc system order-id)]
     (let [result (first (transform-orders response))] result))))

(defn line-item->json-api [line-item]
  {:type       "line-item"
   :id         (:id line-item)
   :attributes (dissoc line-item
                       :id
                       :account-id
                       :job-account-id
                       :order-id
                       :product-id)
   :relationships
   {:account
    {:data
     {:type "account"
      :id   (:account-id line-item)}}
    :job-account
    {:data
     {:type "job-account"
      :id   (:job-account-id line-item)}}
    :order
    {:data
     {:type "order"
      :id   (:order-id line-item)}}
    :product
    {:data
     {:type "product"
      :id   (:product-id line-item)}}}})

(defn line-items->json-api [line-items]
  (map line-item->json-api line-items))

(defn comment->json-api [comment]
  {:type       "comment"
   :id         (:id comment)
   :attributes (dissoc comment
                       :id
                       :line-item-id
                       :order-id)
   :relationships
   {:line-item
    {:data
     {:type "line-item"
      :id   (:line-item-id comment)}}
    :order
    {:data
     {:type "order"
      :id   (:order-id comment)}}}})

(defn comments->json-api [comments]
  (map comment->json-api comments))

(defn address->json-api [address]
  {:type       "address"
   :id         (:id address)
   :attributes (dissoc address
                       :id)})

(defn addresses->json-api [addresses]
  (map address->json-api addresses))

(defn json-api->stub [stub]
  (dissoc stub :attributes :relationships))

(defn json-api->stubs [stubs]
  (map json-api->stub stubs))

(defn order->json-api [result]
  (let [addresses  (:addresses result)
        comments   (:comments result)
        line-items (:line-items result)
        order      (:order result)]
    {:data
     {:type       "order"
      :id         (:id order)
      :attributes (dissoc order
                          :id
                          :account-id
                          :bill-address-id
                          :job-account-id
                          :ship-address-id
                          :sold-address-id
                          :pay-address-id)
      :relationships
      {:account
       {:data
        {:type "account"
         :id   (:account-id order)}}
       :bill-address
       {:data
        {:type "address"
         :id   (:bill-address-id order)}}
       :job-account
       {:data
        {:type "job-account"
         :id   (:job-account-id order)}}
       :ship-address
       {:data
        {:type "address"
         :id   (:ship-address-id order)}}
       :sold-address
       {:data
        {:type "address"
         :id   (:sold-address-id order)}}
       :pay-address
       {:data
        {:type "address"
         :id   (:pay-address-id order)}}
       :line-items
       {:data (json-api->stubs (line-items->json-api line-items))}
       :comments
       {:data (json-api->stubs (comments->json-api comments))}}}
     :included (flatten [(addresses->json-api addresses)
                         (comments->json-api comments)
                         (line-items->json-api line-items)])}))

;; (def f (order "3970176"))
;; (def f (order :prd "4152708"))
;; (def g (order-rfc :prd "4152708"))

;; (ppn f)
;; (ppn (order->json-api f))
;; (ppn (retrieve-maps g))
;; (ppn (function-interface g))

(println "done loading summit.sap.order")
