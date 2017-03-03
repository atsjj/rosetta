(ns summit.sap.price
  (:require [summit.utils.core :as utils]
            [summit.sap.types :refer :all]
            [summit.sap.core :refer :all]
            [summit.sap.conversions :as conv]

            [mishmash.meta :as m]
            [incanter.core :as incant]
            [incanter.datasets :as d]
           ))


(def headings [:matnr :requested-qty :requested-price :unit-price :unit-type])
;; x


;; (def x (call-prices "1000736" "ALB1" [[2802 1] [2803 5] [2804 15]]))
;; ;; (pull x :et-price-output)
;; ;; (map transform (pull x :et-price-output))

;; ;; (def f (:function x))
;; ;; (def f (find-function :qas :z-o-complete-pricing))
;; ;; (def ff (:function f))
;; ;; (type ff)
;; ;; (.getName ff)
;; ;; (m/getNames f)
;; ;; (m/superclass ff)
;; ;; (m/subclasses ff)
;; ;; (keys x)
;; (def pdata (pull-with-schema x :et-price-output))
;; (def dd (incant/dataset headings (:data pdata)))
;; ;; (def dd (incant/dataset (:names pdata) (:data pdata)))
;; (:schema pdata)
;; (incant/view dd)
;; (type dd)
;; (first dd)
;; (count dd)
;; (incant/col-names dd)
;; (incant/nrow dd)
;; (incant/ncol dd)
;; (incant/$ [:matnr :netwr] dd)
;; (incant/$ 0 dd)
;; (incant/$ 0 :all dd)
;; (incant/$ (range 2) :all dd)
;; (incant/$ [0 1] :all dd)
;; (incant/$ 1 dd)

;; |             :matnr | :requested-qty | :requested-price | :unit-price | :unit-type |
;; |--------------------+----------------+------------------+-------------+------------|
;; | 000000000000002802 |          1.000 |             0.47 |         USD |        100 |
;; | 000000000000002803 |          5.000 |             2.56 |         USD |        100 |
;; | 000000000000002804 |         15.000 |             6.55 |         USD |        100 |

;; (def ddd (incant/to-matrix (incant/$ (:names pdata) dd)))
;; (type ddd)
;; (count ddd)
;; (first ddd)
;; (second ddd)
;; (reduce incant/plus ddd)
;; (incant/$= ddd * 4)




(declare transform make-matnr-qty)

(defn call-prices
  [account-number service-center-code matnr-qty-vec]
  (try
    (let [price-fn (find-function *sap-server* :z-o-complete-pricing)
          v (map make-matnr-qty matnr-qty-vec)]
      (->
       price-fn
       (push {:i-kunnr (conv/as-document-num-str account-number)
              :i-werks service-center-code
              :it-price-input v})
       execute
       ))
    (catch Throwable e
      (println "error in (prices " account-number " " service-center-code " " matnr-qty-vec ")")
      [])))

(defn prices
  [account-number service-center-code matnr-qty-vec]
  (try
    (let [price-fn (find-function *sap-server* :z-o-complete-pricing)
          v (map make-matnr-qty matnr-qty-vec)]
      (map transform
           (->
            price-fn
            (push {:i-kunnr (conv/as-document-num-str account-number)
                   :i-werks service-center-code
                   :it-price-input v})
            execute
            (pull :et-price-output)
            )))
    (catch Throwable e
      (println "error in (prices " account-number " " service-center-code " " matnr-qty-vec ")")
      [])))

(defn headed
  [v]
  {:headings [:matnr :requested-qty :requested-price :unit-price :unit-type]
   :data v})

(defn heading-prices
  [account-number service-center-code matnr-qty-vec]
  (headed (prices account-number service-center-code matnr-qty-vec)))

(defn- transform [p]
  (let [p (vec p)
        matnr (utils/as-integer (first p))
        unit-type (nth p 5)
        requested-price (nth p 6)
        requested-qty (nth p 1)
        unit-price 1
        unit-price (if (<= requested-qty 0M)
                     0
                     (.divide requested-price requested-qty 6 java.math.RoundingMode/HALF_UP))
        ]
    [matnr requested-qty requested-price unit-price unit-type]
    ))

(defn- make-matnr-qty [o]
  (let [mat-qty (if (sequential? o) o [o 1000])]
    [(conv/as-matnr (first mat-qty)) (second mat-qty)]))

(defn internet-prices [matnr-qty-vec]
  (prices internet-account-number internet-service-center-code matnr-qty-vec))

(defn- has-price?
  "does this pricing vector have a non-zero price?"
  [v]
  (< 0M (nth v 2)))

(defn request-oodles
  [account-number service-center-code matnr-qty-vec]
  (let [p (pmap #(prices account-number service-center-code %)
               (partition 10 10 [] matnr-qty-vec)
               )]
    (filter has-price? (apply concat p))))
;; (oddles-o-internet [20834 1 2 3 4 5 6 7 8 9 10 11 2718442 13])

(defn priced-internet
  [matnr-qty-vec]
  (request-oodles internet-account-number internet-service-center-code matnr-qty-vec))

(utils/examples
 (prices internet-account-number internet-service-center-code [[2718442 3]])
 (internet-prices
  [[2718442 3]
   [2718442 9]])
 (internet-prices [2718442 20834])
    ;; => {:headings [:matnr :requested-qty :requested-price :unit-price :unit-type], :data ([2718442 1000.000M 87.67M 0.087670M "FT"] [20834 1000.000M 0.00M 0.000000M ""])} 
 (def matnrs (->> (slurp "/home/bmd/Downloads/query_result.csv") clojure.string/split-lines (map utils/as-integer)))
 (first matnrs)
 (def p (priced-internet
         (->> matnrs (drop 400) (take 20))
                  ;; (->> (partition 10 10 [] matnrs) (drop 40) (take 2))
                  ))
 p
 (def p (priced-internet matnrs))
 (last p)
 (spit "prices.csv"
       (clojure.string/join "\n" (map #(str (first %) "," (nth % 3)) p))
       )

 (first prices)
 (type (first prices))
 (->> (internet-prices (range 8900 8910))
      :data
      (remove (fn [v] (= 0M (nth v 2))))
      )
 (internet-prices (range 10))
 (internet-prices (vec (range 10)))
 (internet-prices [1 2 3])
 (internet-prices (first prices))
 )
