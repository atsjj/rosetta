(println "loading summit.sap.conversions")

(ns summit.sap.conversions
  (:require
   [clojure.string :as str]
   [summit.utils.core :as u]
   [summit.utils.log :as log]
   ))

(defn as-matnr [string]
  (u/zero-pad 18 string))

(defn as-document-num-str [string]
  (u/zero-pad 10 string))
;; (as-document-num-str "asdf")

(defn as-short-document-num-str [string]
  "remove leading zeros"
  (if string (str/replace string #"^0*" "")))
;; (as-short-document-num-str (as-document-num-str "00001"))

(defn as-customer-num [string]
  (as-document-num-str string))

(println "done loading summit.sap.conversions")
