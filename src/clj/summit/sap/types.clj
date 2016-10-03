(ns summit.sap.types
  (:require [clojure.string :as str]
            [summit.utils.core :refer :all]
            [summit.sap.conversions :refer [as-matnr]]
            ;; [clojure.lang :refer [BigInt]]
            )
  )

(defrecord sap-type [name summit-type sap-type sap->summit summit->sap validations])

(def sap-types-array
  [
   [:char java.lang.String String identity clojure.string/upper-case []]
   [:matnr clojure.lang.BigInt String bigint as-matnr []]

   [:order-num clojure.lang.BigInt String bigint as-matnr [:range [0 499999999]]]
   [:quote-num clojure.lang.BigInt String bigint as-matnr [:range [2000000000 2499999999]]]
   [:job-account-num clojure.lang.BigInt String bigint as-matnr [:range [4000000000 4499999999]]]
   [:return-num clojure.lang.BigInt String bigint as-matnr [:range [6000000000 6499999999]]]
   [:debit-memo-num clojure.lang.BigInt String bigint as-matnr [:range [7000000000 7499999999]]]
   [:delivery-num clojure.lang.BigInt String bigint as-matnr [:range [8000000000 8399999999]]]
   [:return-delivery-num clojure.lang.BigInt String bigint as-matnr [:range [8400000000 8499999999]]]
   [:invoice-num clojure.lang.BigInt String bigint as-matnr [:range [9000000000 9499999999]]]
   ]
  )

