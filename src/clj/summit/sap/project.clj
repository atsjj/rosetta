(println "loading summit.sap.project")

(ns summit.sap.project
  (:require [summit.sap.core :refer :all :as erp]
            [clojure.string :as str]
            [summit.utils.core :as utils :refer [->int examples]]
            [summit.sap.conversions :as conv]
            ;; [summit.sap.project :as project]

            [incanter.core :as i]
            [incanter.charts :as chart]
            [incanter.stats :as stats]
            [incanter.io :as io]
            [incanter.datasets :as d]
            )
  (:import [java.util.Date]))

;; http://incanter.org/docs/data-sorcery-new.pdf
;; (i/view (chart/histogram (stats/sample-normal 1000)))
;; (i/conj-rows [1 2] [3 4] [5 6])
;; (i/dataset ["a" "b"] [[1 2] [3 4] [5 6]])
;; (i/dataset [:a :b] [[1 2] [3 4] [5 6]])
;; (i/view (i/dataset ["a" "b"] [[1 2] [3 4] [5 6]]))
;; (i/to-dataset [{:a 1 :b 2} {:a 3 :b 4} {:a 5 :b 6 :c 7}])

;; (i/$ :speed (d/get-dataset :cars))
;; (i/with-data (d/get-dataset :cars)
;;   [(stats/mean (i/$ :speed))
;;    (stats/sd (i/$ :speed))])
;; (i/with-data (d/get-dataset :iris)
;;   (i/view i/$data)
;;   (i/view (i/$ [:Sepal.Length :Sepal.Width :Species]))
;;   (i/view (i/$ [:not :Petal.Width :Petal.Length]))
;;   (i/view (i/$ 0 [:not :Petal.Width :Petal.Length]))))

;; (i/$where {:Species "setosa"}
;;           (d/get-dataset :iris))
;; (i/$where {:Species "setosa"}
;;         (d/get-dataset :iris))
;; (i/$where {:Petal.Width {:lt 1.5}}
;;         (d/get-dataset :iris))
;; (i/$where {:Petal.Width {:gt 1.0, :lt 1.5}}
;;         (d/get-dataset :iris))
;; (i/$where {:Petal.Width {:gt 1.0, :lt 1.5} :Species {:in #{"virginica" "setosa"}}}
;;         (d/get-dataset :iris))
;; (i/$where (fn [row]
;;             (or (< (row :Petal.Width) 1.0)
;;                 (> (row :Petal.Length) 5.0)))
;;           (d/get-dataset :iris))

;; (i/with-data (d/get-dataset :hair-eye-color)
;;   (i/view i/$data)
;;   (i/view (i/$order :count :desc))
;;   (i/view (i/$order [:hair :eye] :desc)))

;; rollup
;; (->> (d/get-dataset :iris)
;;      (i/$rollup stats/mean :Petal.Length :Species)
;;      i/view)
;; (->> (d/get-dataset :iris)
;;      (i/$rollup #(/ (stats/sd %) (count %))
;;               :Petal.Length :Species)
;;      i/view)
;; (->> (d/get-dataset :hair-eye-color)
;;      (i/$rollup i/sum :count [:hair :eye])
;;      (i/$order :count :desc)
;;      i/view)



(defn date->str [date]
  (last (clojure.string/split (print-str date) #"\"")))
;; (date->str (java.util.Date.))

;; ----------------------------------
;;     All projects for an account





;;;    really want to "correct" schema, not end up with just the col names.


(defrecord schema-store [schemas])
(defrecord store [schema-store data])
(defrecord persistence [create-fn read-fn update-fn delete-fn])

(defrecord schema-def [definitions])
(defrecord store-schema [store schema])
(defrecord record-def [schema definitions])
;; (defrecord record-def [schema definitions])
(defrecord struct-def [schema definitions])
(defrecord field-def  [schema key display-name descript type len sublen default])

(defrecord entity [field data])

;; database terminology (wikipedia, Database_schema):
;;   table field relationship view index package procedure function queue trigger type sequence materialized-view synonym database-link directory xml-schema



(defonce ^:private et-project-fields [:client :id :sold-to :name :title :start-date :end-date :service-center-code :status :last-modifier :modified-on])

;; cached during call to projects (plural)
(defonce ^:private cached-projects (atom {}))
(defn project-name [id]
  (:name (@cached-projects id)))

(defn project-account-num [id]
  (:account-num (@cached-projects id)))
;; (project-account-num 3)

;; cached during call to project (singular)
(defonce ^:private project-json-cache (atom {}))
(defonce ^:private project-status-lines-cache (atom {}))

(def extractions
  {:order-info [;; :id (swap! id-seq-num inc)
                :project-id :projid ->int "Project ID"
                :order-num :vbeln-va ->int "Order #"
                :drawing-num :bstkd identity "Drawing #"
                :expected-at :bstdk identity "Expected Date"
                ]
   :line-item  [:item-num :posnr-va ->int "Item #"
                :matnr :matnr ->int "Mat #"
                :customer-matnr :kdmat identity "Cust Mat #"
                :descript :arktx identity "Description"
                :circuit-id :circ-id identity "Circuit ID"
                :requested-qty :kwmeng double "Requested Quantity"
                :delivered-qty :tot-gi-qty double "Total Delivered Quantity"    ;; "Total Goods Issue Quantity"
                :remaining-qty :remaining double "Remaining To Deliver"  ;; still to be delivered
                :reserved-qty :resv-qty double "Reserved Quantity"
                :uom :vrkme identity "UOM"
                :inventory-loc :inv-loc identity "Inventory Loc"
                :storage-loc :lgort identity "Storage Loc"
                :service-center :werks identity "Service Center"
                :trailer-atp :cust-loc-atp double "Trailer ATP"
                :service-center-atp :main-loc-atp double "Service Center ATP"
                :schedule-at :edatu identity "Schedule Date"
                :entered-at :audat identity "Entered Date"
                :available-at :edatu identity "Available Date"
                ]
   :delivery   [:delivery :vbeln-vl identity "Delivery"
                :released-qty :lfimg double "Released Quantity (Pending)"   ;; means "printed" in the warehouse
                :delivered-qty :picked double "Delivered Quantity"     ;; picked is the closest to what the customer would call delivered
                :delivery-item-num :posnr-vl identity "Delivery Item #"]})

(defn- fetch-project-json [system project-id]
  (@project-json-cache [system project-id]))
(defn- cache-project-json [system project-id maps]
  (swap! project-json-cache assoc [system project-id] maps))

(defn fetch-status-lines [system project-id]
  (@project-status-lines-cache [system project-id]))
;; (:headers (fetch-status-lines :prd 3))

(defn- cache-status-lines
  "cache status lines. used for spreadsheet data"
  [system project-id status-lines]
  (let [headers (:headers status-lines)
        data (:data status-lines)
        good-cols (remove nil?
                          (map-indexed (fn [i name] (if (empty? name) nil i)) headers))
        f (fn [col-data] (map #(get col-data %) good-cols))
        h (f (vec headers))
        d (map #(f (vec %)) data)
        status-lines {:headers h :data d}
        ]
    (swap! project-status-lines-cache assoc [system project-id]
           (merge {:account-num (project-account-num project-id)
                   :project-id project-id
                   :project-name (project-name project-id)}
                  status-lines))))
;; (get-project :prd 3)
;; (get-project :qas 1)

(defn- raw-project-data [f]
  (pull-with-schemas f
                     [:et-status-lines :status-lines]
                     [:et-vbak-atts :order-attr-defs]
                     [:et-vbap-atts :line-item-attr-defs]
                     [:et-likp-atts :delivery-attr-defs]
                     ))
;; (raw-project-data y)

(defn- static-project-col-names
  "additional attributes will retain their german funky names"
  [status-lines]
  (let [bad-names (:names status-lines)
        good-names
        (into {}
              (map (fn [x] [(second x) (nth x 3)])
                   (partition 4 (concat (:order-info extractions) (:line-item extractions) (:delivery extractions)))))
        ]
    (map #(if-let [n (% good-names)] n %) bad-names)))
;; (static-project-col-names (pull-with-schema (find-function :qas :z-o-zvsemm-project-cube) :et-status-lines))


(defn- attr-name-conv [key-prefix defs]
  (let [append #(str %2 %1)
        def-hash (into {} (map (fn [def] [(-> def (nth 2) (str/split #"_") last (append key-prefix) keyword) (nth def 3)]) defs))]
    def-hash))
;; (attr-name-conv "zz-zvsemm-vbap-attr-" (:data (:line-item-attr-defs (raw-project-data y))))

(defn- project-col-names
  "proper names for all status line columns, including additional attributes"
  [raw-data]
  (let [colnames (static-project-col-names (:status-lines raw-data))
        order-conversions (attr-name-conv "zz-zvsemm-vbak-attr-" (:data (:order-attr-defs raw-data)))
        item-conversions (attr-name-conv "zz-zvsemm-vbap-attr-" (:data (:line-item-attr-defs raw-data)))
        delivery-conversions (attr-name-conv "zz-zvsemm-likp-attr-" (:data (:delivery-attr-defs raw-data)))
        attr-conversions (merge order-conversions item-conversions delivery-conversions)]
    (map #(if (keyword? %)
            (if-let [v (% attr-conversions)]
              v
              "")
            %)
         colnames)
    ))
;; (def y (execute-project-query :qas 1))
;; (project-col-names (raw-project-data y))



(defn- transform-quartets [m attr-defs]
  (let [attr-defs (partition 4 attr-defs)]
    (into {}
          (for [attr-def attr-defs]
            (let [web-name (first attr-def)  sap-name (second attr-def)  name-transform-fn (nth attr-def 2)]
              [web-name (name-transform-fn (m sap-name))])))))

(defn- transform-project-summary [v]
  (let [m (into {} (map vector et-project-fields v))]
    (->
     m
     (dissoc :client)
     (assoc :sold-to (->int (:sold-to m))
            :id (->int (:id m))
            :status (case (:status m)
                      "A" :active
                      "P" :planning
                      "C" :complete
                      "Z" :cancelled
                      (:status m))))))

(defn- transform-ship-to [projs v]
  (swap! projs update-in [(->int (second v)) :ship-to-ids] conj (->int (nth v 2))))

(defn- project-link [id]
  {:self (str "/api/projects/" id)})

(defn- project->json-api [account-id proj]
  (let [id (:id proj)]
    {:type "project"
     :id id
     :links (project-link id)
     ;; :attributes (dissoc proj :id :sold-to :ship-to-ids)
     :attributes {:id id :name (:name proj)}
     :relationships
     {:account
      {:data
       {:type "account"
        :id account-id}}}
     }))

(defn projects
  ([account-num] (projects :prd account-num))
  ([server account-num]
   (println "getting projects for account number: " account-num " on server " server)
   (let [f (find-function server :Z_O_ZVSEMM_KUNAG_ASSOC_PROJ)]
     (push f {:i_kunag (conv/as-document-num-str account-num)})
     (execute f)
     (let [projs (map transform-project-summary (pull f :et-projects))]
       (doseq [proj projs]
         (swap! cached-projects assoc (:id proj)
                {:id (:id proj)
                 :account-num account-num
                 :name (:name proj)}))
       {:data
        (map (partial project->json-api account-num) projs)}
       ))))


;; ----------------------------------------------------
;;      a single project

(defn transform-attribute-definition [index m]
  (let [id (-> (:attr-assign m) str/lower-case (str/replace #"_" "-"))]
    [id {:id id
         :seq (inc index)
         :title (:attr-title m)
         :len (->int (:attr-len m))
         :required? (= "X" (:attr-req m))
         :batch-only? (= "X" (:attr-batch-only m))
         :conv (:attr-conv m)}]))

(defn transform-attribute-definitions [v]
  (into {}
        (map-indexed (fn [idx attr] (transform-attribute-definition idx attr)) v)))

(defn pair-key-vals [proj attr-prefix attr-defs]
  (map #(let [attr-name (str "attr-" (inc %))]
          (vector
           (:title (attr-defs attr-name))
           ((keyword (str attr-prefix attr-name)) proj)))
       (range (count attr-defs))))

(defn transform-message-lists [sap-messages]
  (let [msgs
        (for [msg sap-messages]
          {:order-num (->int (:vbeln msg))
           :item-num (->int (:posnr msg))
           :message-type (:msgtyp msg)
           :text (:message msg)})]
    (group-by :order-num msgs)))
;; (ppn (process-message-lists (pull-map projects-fn :et-message-list)))
;; ((process-message-lists (pull-map projects-fn :et-message-list)) 16811)

(defn- status-line-col-names []
  ;; (map first
       (concat (:order-info extractions) (:line-item extractions) (:delivery extractions))
       ;; )
  )
;; (status-line-col-names)

(defn- line-item-id [m]
  (str (-> m :order :order-num) "-" (-> m :line-item :item-num)))

(defn- delivery-line-item-id [m]
  (str (-> m :delivery :delivery utils/->long) "-" (-> m :delivery :delivery-item-num utils/->long) "-" (line-item-id m)))

(defn- collect-same [v id]
  (filter #(= id (:id %)) v))

(defn- extract-attr-vals
  ([m begin-str] (extract-attr-vals m begin-str 1 {}))
  ([m begin-str index v]
   (let [key (keyword (str begin-str index))
         value (key m)]
     (if (nil? value)
       v
       (extract-attr-vals m begin-str (inc index) (conj v [(keyword (str "attr-" index)) value]))))))

(defn transform-status-line [m]
  {:order (transform-quartets m (:order-info extractions))
   :line-item (transform-quartets m (:line-item extractions))
   :delivery (transform-quartets m (:delivery extractions))
   :order-attr-vals (extract-attr-vals m "zz-zvsemm-vbak-attr-")
   :line-item-attr-vals (extract-attr-vals m "zz-zvsemm-vbap-attr-")
   :delivery-attr-vals (extract-attr-vals m "zz-zvsemm-likp-attr-")
   ;; :raw m
   })

(defn transform-status-lines [m]
  (map transform-status-line m))

(defn pull-project-json-api-maps [project-fn]
  (let [attr-defs (partition 3
                             [:status-lines :et-status-lines transform-status-lines
                              :messages :et-message-list transform-message-lists
                              :delivery-attr-defs :et-likp-atts transform-attribute-definitions
                              :order-attr-defs :et-vbak-atts transform-attribute-definitions
                              :line-item-attr-defs :et-vbap-atts transform-attribute-definitions])]
    (into {}
          (for [attr-def attr-defs]
            (let [web-name (first attr-def)
                  sap-name (second attr-def)
                  name-transform-fn (nth attr-def 2)]
              [web-name (name-transform-fn (pull-map project-fn sap-name))])))))

(defn- transform-raw-order [m]
  (let [order (:order m)]
    (assoc
     (clojure.set/rename-keys order {:order-num :id})
     :line-item-id (line-item-id m)
     :available-at (-> m :line-item :available-at)
     :circuit-id (-> m :line-item :circuit-id)
     :attrs (:order-attr-vals m))))

(defn- merge-order [orders order]
  (let [line-item-ids (apply conj [] (map :line-item-id orders))
        available-ats (apply conj [] (remove nil? (map :available-at orders)))
        available-ats (map #(.getTime %) available-ats)
        max-available-ats (if (not-empty available-ats) (apply max available-ats))
        circuit-ids (apply conj [] (map :circuit-id orders))]
    (merge
     order
     {:line-item-ids (-> line-item-ids set sort)
      :max-available-at (if max-available-ats (java.util.Date. max-available-ats))
      :circuit-ids (disj (set circuit-ids) "")
      :attributes (:attrs (first orders))
      }
     )))

(defn- join-like-orders [orders]
  (let [unique-orders (set (map #(dissoc % :line-item-id :available-at :circuit-id :attrs) orders))]
    (map #(merge-order (collect-same orders (:id %)) %) unique-orders)
    ))

(defn- order->json-api [order]
  (let [order-id (:id order)
        line-items (map (fn [x] {:type :project-line-item :id x}) (:line-item-ids order))
        circuits (map (fn [x] {:type :circuits :id x}) (:circuit-ids order))
        project-id (:project-id order)
        proj-order-id (str project-id "-" order-id)]
    {:type :project-order
     :id order-id
     :max-available-at (:max-available-at order)
     :attributes (dissoc order :id :project-id :line-item-ids :circuit-ids)
     :relationships {:order
                     {:links
                      {:related (str "/api/v2/orders/" order-id)}
                      :data {:type :order :id order-id}}
                     :circuits {:data circuits}
                     :project {:data {:type :project :id project-id}}
                     :project-line-items {:data line-items}}}))

(defn- extract-orders [maps]
  (->> maps
       (map transform-raw-order)
       join-like-orders
       (map order->json-api)
       ))

(defn- transform-raw-item [m]
  (let [item (:line-item m)
        order-id (-> m :order :order-num)
        delivery (-> m :delivery)]
    (assoc item
           :id (line-item-id m)
           :order-id order-id
           :delivery delivery
           :attrs (:line-item-attr-vals m))))

(defn- merge-item [items item]
  ;; (let [delivery-ids (apply conj [] (filter #(not-empty %) (map :delivery items)))
  (let [deliveries (apply conj [] (filter #(not-empty (:delivery %)) (map :delivery items)))
        released-qtys (->> deliveries (map :released-qty) (remove nil?))
        released-qty (apply + released-qtys)
        ;; released-qty (apply + (map :released-qty items))
        ;; delivered-qty (apply + (map :delivered-qty items))
        ;; picked-qty (apply + (map :picked-qty items))
        ]
    (merge
     item
     ;; {:delivery-ids delivery-ids
     {:deliveries deliveries
      :released-qty released-qty
      :remaining-to-release (- (:requested-qty item) released-qty)
      ;; :delivered-qty delivered-qty
      ;; :picked-qty picked-qty
      :attributes (:attrs (first items))})))

(defn- join-like-items [items]
  ;; (let [unique-items (set (map #(dissoc % :delivery :delivered-qty :picked-qty :attrs) items))]
  (let [unique-items (set (map #(dissoc % :delivery :released-qty :picked-qty :attrs) items))]
    (map #(merge-item (collect-same items (:id %)) %) unique-items)))

(defn- line-item->json-api [item]
  (let [closed-deliveries (remove #(<= (:delivered-qty %) 0) (:deliveries item))
        deliveries
        (map (fn [x] {:type :project-line-item-delivery
                      :id (str (utils/->long (:delivery x)) "-" (utils/->long (:delivery-item-num x))
                               "-" (:id item))})
             closed-deliveries)]
    {:type :project-line-item
     :id (:id item)
     :links {:self (str "/api/v2/project-line-item/" (:id item))}
     :attributes (dissoc item :order-id :delivery-ids :deliveries)
     :relationships (cond->
                        {:project-order {:data {:type :project-order :id (:order-id item)}}
                         :line-item {:data {:type :line-item :id (:id item)}}}
                      deliveries (assoc :project-line-item-deliveries {:data deliveries})
                      )}))

;; (defn- deliveries->json-api [item]
;;   (map (fn [x] {:type :project-delivery
;;                 :id (utils/->long x)
;;                 :relationships
;;                 (map
;;                  (fn [x]
;;                    {:type :project-line-item-delivery
;;                     :id
;;                     (str (utils/->long (:delivery x)) "-" (utils/->long (:delivery-item-num x)))}
;;                    :attributes {:qty (:delivered-qty x)}
;;                    )
;;                  (filter #(= x (:delivery %)) (:deliveries item))
;;                  )})
;;        (set (map :delivery (:deliveries item)))))

(defn- delivery-line-item->json-api [status-line]
  (let [line-item-id (line-item-id status-line)
        delivery (:delivery status-line)
        delivery-id (-> delivery :delivery utils/->long str)
        delivery-line-item-id (delivery-line-item-id status-line)
        ]
    {:type :project-line-item-delivery
     :id delivery-line-item-id
     :relationships {:project-delivery {:data {:type :project-delivery :id delivery-id}}
                     :project-line-item {:data {:type :project-line-item :id line-item-id}}}
     :attributes {:qty (:delivered-qty delivery)
                  :released-qty (:released-qty delivery)}}))

(defn- delivery-line-items->json-api [status-lines]
  (map delivery-line-item->json-api
       (->>
        status-lines
        (filter #(-> % :delivery :delivery not-empty))
        (remove #(<= (-> % :delivery :delivered-qty) 0))  ;; remove open items (ticket has dropped but not pgi'ed)
        ;; (remove #(and (not-empty (-> % :delivery :deliver)) (<= (-> % :delivery :delivered-qty) 0)))  ;; remove open items (ticket has dropped but not pgi'ed)
        )))

(defn- extract-unique-line-items [maps]
  (->> maps
       (map transform-raw-item)
       join-like-items
       ))

(defn- extract-line-items [unique-items]
  (map line-item->json-api unique-items))

(defn- add-delivery->line-item-relationships [maps deliveries]
  (map (fn [delivery]
         (let [delivery-id (:id delivery)
               matching (filter #(= (get-in % [:delivery :delivery]) delivery-id) maps)]
           (assoc delivery :relationships
                  {:project-line-item-deliveries
                   {:data
                    (map (fn [m]
                           {:type :project-line-item-delivery
                            :id (delivery-line-item-id m)})
                         matching)}})))
       deliveries))

(defn- extract-deliveries [maps]
  ;; (->> maps
  ;; (->> (remove #(and (not-empty (-> % :delivery :delivery)) (<= (-> % :delivery :delivered-qty) 0)) maps)
  (->> (remove #(<= (-> % :delivery :delivered-qty) 0) maps)
       (map (fn [m] {:type :project-delivery
                     :id (-> m :delivery :delivery)
                     :attributes
                     {:attributes (:delivery-attr-vals m)
                      ;; :delivered-qty (-> m :delivery :delivered-qty)
                      }
                     }))
       (filter #(not-empty (:id %)))
       set
       (add-delivery->line-item-relationships maps)))

(defn- attrize [attribute-maps]
  (->>
   attribute-maps
   (map (fn [[key attr]]
          {:key (:id attr)
           :value (:title attr)
           :sequence (->int (last (str/split (:id attr) #"-")))}))
   (remove #(= "--DELETE--" (:value %)))
   ))

(defn- drawings->json-api [project-id drawings]
  (for [[id order-nums] drawings]
    {:type :drawing
     :id id
     :relationships
     {:project {:data {:type :project :id project-id}}
      :project-orders
      (map (fn [x] {:type :project-order :id x})
           order-nums)}}))

(defn- circuits->json-api [project-id circuits]
  (for [[id order-nums] circuits]
    {:type :circuit
     :id id
     :relationships
     {:project {:data {:type :project :id project-id}}
      :project-orders
      (map (fn [x] {:type :project-order :id x})
           order-nums)}}))

(defn- extract-drawings [status-lines]
  (utils/collect-by
   #(-> % :order :drawing-num)
   #(-> % :order :order-num)
   status-lines))

(defn- extract-circuits [status-lines]
  (dissoc
   (utils/collect-by #(-> % :line-item :circuit-id) #(-> % :order :order-num) status-lines)
   ""))

(defn transform-project [project-id maps]
  (let [status-lines (:status-lines maps)
        order-ids    (set (map #(-> % :order :order-num) status-lines))
        unique-items (extract-unique-line-items status-lines)
        items        (extract-line-items unique-items)
        orders       (extract-orders status-lines)
        deliveries   (extract-deliveries status-lines)
        drawings     (extract-drawings status-lines)
        circuits     (extract-circuits status-lines)
        json-orders  (set (map (fn [id] {:type :project-order :id id}) order-ids))]
    {:data
     {:type          :project
      :id            project-id
      :attributes    {:name                         (project-name project-id)
                      :project-order-attributes     (attrize (:order-attr-defs maps))
                      :project-line-item-attributes (attrize (:line-item-attr-defs maps))
                      :project-delivery-attributes  (attrize (:delivery-attr-defs maps))
                      }
      :relationships {:project-orders {:data json-orders}
                      ;; TODO: could there be duplicates here? If so, put the drawing nums into a set first
                      :drawings       {:data
                                       (for [[id _] drawings]
                                         {:type :drawing :id id})}
                      :circuits       {:data
                                       (for [[id _] circuits]
                                         {:type :circuit :id id})}
                      :account        {:data
                                       {:type "account" :id (project-account-num project-id)}}}}
     :included
     (concat
      orders
      items
      deliveries
      (drawings->json-api project-id drawings)
      (circuits->json-api project-id circuits)

      ;; (deliveries->json-api unique-items)
      (delivery-line-items->json-api status-lines)
      )
     ;; :raw maps
     }))

;; (defn- get-project-schema-force [system project-id]
;;   (let [project-fn (find-function system :Z_O_ZVSEMM_PROJECT_CUBE)]
;;     (push project-fn {:i-proj-id (conv/as-document-num-str project-id)})
;;     (execute project-fn)
;;     (pull-with-schema project-fn :et-status-lines)))
;;     ;; (let [maps (pull-project-maps project-fn)]
;;     ;;   (swap! cached-raw-projects assoc [system project-id] maps)
;;     ;;   maps)))
;; ;; (get-project-schema-force :qas 1)

;; (defn- combine-line [line]
;;   (let [l (atom (merge (:order line) (:line-item line) (:delivery line)))]
;;     @l))

(defn- pull-status-lines [f]
  (let [;; p (pull-with-schema f :et-status-lines)
        p (pull f :et-status-lines)
        ;; lines (:status-lines p)
        ;; order-attr-defs (:order-attr-defs p)
        ;; line-attr-defs (:line-item-attr-defs p)
        ;; delivery-attr-defs (:delivery-attr-defs p)
        ;; order-attrs (map :title order-attr-defs)
        ;; line-attrs (map :title line-attr-defs)
        ;; delivery-attrs (map :title delivery-attr-defs)
        ]
    ;; (map combine-line p)
    p
    ;; {:attrs 3
    ;;  :orig p}
    ))


;; (pull-status-lines y)

;; (def y (execute-project-query :qas 1))
;; (keys y)
;; (pull y :et-status-lines)
;; (:schema (pull-with-schema y :et-status-lines))
;; (:names (pull-with-schema y :et-status-lines))
;; (:schema (pull-with-schema y :et-vbak-atts)) ;; order
;; (:data (pull-with-schema y :et-vbak-atts)) ;; order
;; (:names (pull-with-schema y :et-vbak-atts)) ;; order
;; (:names (pull-with-schema y :et-vbap-atts)) ;; line item
;; (:names (pull-with-schema y :et-likp-atts)) ;; delivery





;; (transform-status-line )

;; (:mandt :proj-id :attr-assign :attr-title :attr-len :attr-req :attr-conv)
;; (:mandt :proj-id :attr-assign :attr-title :attr-len :attr-req :attr-conv :attr-batch-only)
;; (:mandt :proj-id :attr-assign :attr-title :attr-len :attr-req :attr-conv)

;; (defn project-spreadsheet-data [system project-id]
;;   (get-project-schema-force system project-id))

(defn- execute-project-query [system project-id]
  (let [project-fn (find-function system :Z_O_ZVSEMM_PROJECT_CUBE)]
    (push project-fn {:i-proj-id (conv/as-document-num-str project-id)})
    (execute project-fn)
    project-fn))

(defn get-project
  "force get project data from sap and cache for spreadsheet and project"
  [system project-id]
  (let [f (execute-project-query system project-id)
        raw-data (raw-project-data f)   ;; this is schema + vector data, not maps
        status-lines {:headers (project-col-names raw-data)
                      :data (-> raw-data :status-lines :data)}
        maps (pull-project-json-api-maps f)]
    (cache-status-lines system project-id status-lines)
    (cache-project-json system project-id maps)
    ;; status-lines
    maps
    ))

;; (cache-raw-project project-fn system project-id)

;; (defn- get-project [system project-id]
;;   (let [key [system project-id]
;;         p (@cached-raw-projects key)]
;;     (if p
;;       p
;;       (get-project-force system project-id))))

;; TODO: delete these? they are used in routes.clj
(defn project-raw-data [system project-id]
  )
(defn project-spreadsheet-data [system project-id]
  )
;; (defn project-raw-data [system project-id]
;;   (@cached-raw-projects [system project-id]))

  ;; (let [p (project-raw-data system project-id)
  ;;       lines (:status-lines p)
  ;;       order-attr-defs (:order-attr-defs p)
  ;;       line-attr-defs (:line-item-attr-defs p)
  ;;       delivery-attr-defs (:delivery-attr-defs p)
  ;;       order-attrs (map :title order-attr-defs)
  ;;       line-attrs (map :title line-attr-defs)
  ;;       delivery-attrs (map :title delivery-attr-defs)]
  ;;   (pull-with-schema )
  ;;   (map combine-line lines)
  ;;   ;; {:attrs 3
  ;;   ;;  :orig p}
  ;;   ))

;; (keys @cached-raw-projects)
;; (-> @cached-raw-projects first second keys)
;; (-> @cached-raw-projects first second :status-lines first keys)
;; (-> @cached-raw-projects first second :status-lines first :order keys)

(defn- normalize-project [maps]
  )





;; (def rawproj (get-project :prd 3))
;; (keys rawproj)
;; (-> rawproj :status-lines first keys)
;; (def normproj (normalize-project rawproj))



(defn ensure-project
  "return project. retrieve if not already cached"
  [system project-id]
  (if-let [p (fetch-project-json system project-id)]
    p
    (get-project system project-id)))


(defn project
  ([project-id] (project :qas project-id))
  ([system project-id]
   (utils/ppn (str "getting project " project-id " on " system))
   (transform-project project-id (ensure-project system project-id))
   ))
(examples
 (project :qas 1)
 (project :qas 28)
 (project :prd 3)
 (def x (project :prd 3))
 (def x (project :qas 28))
 (get-project :qas 1)
 (get-project :qas 28)
 (do
   (get-project :prd 3) nil)
 (count (project 3))
 (def x (get-project :prd 3))
 (map #(type %) x)
 (-> x keys)
 (def y (project :prd 3))
 (count y)
 (map type y)
 (-> y keys)

 (println "hey")
 (projects :qas 1002225)
 (projects :prd 1037657)
 (java.text.SimpleDateFormat. "YYY-MM-ddTHH:mm:ss")
 (java.util.Date.)
 (def x (print-str (java.util.Date.)))
 )
;; (def p1 (project :qas 1))
;; (keys (-> p1 :data))
;; (keys (-> p1 :data :attributes))
;; (map keys (-> p1 :data :attributes :project-order-attributes))
;; (keys (-> p1 :data :relationships))

(println "done loading summit.sap.project")

;; (-> x keys)
;; (->> x :included (filter #(= (:type %) :project-order)))
;; (->> x :included (map #(keys %)))
;; (->> x :included (map #(% :type)))
;; (->> x :included (filter #(= (% :type) :project-line-item)) first :attributes)
;; (->> x :included (filter #(= (% :type) :project-delivery)) first )
