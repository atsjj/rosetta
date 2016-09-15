(println "loading summit.sap.project")

(ns summit.sap.project
  (:require [summit.sap.core :refer :all]
            [clojure.string :as str]
            [summit.utils.core :as utils :refer [->int as-document-num examples]]
            ))


;; ----------------------------------
;;     All projects for an account

(def ^:private et-project-fields [:client :id :sold-to :name :title :start-date :end-date :service-center-code :status :last-modifier :modified-on])
(def ^:private cached-projects (atom {}))
(defn- project-name [id]
  (:name (@cached-projects id)))
(defn- project-account-num [id]
  (:account-num (@cached-projects id)))


(defn- transform-triplets [m attr-defs]
  (let [attr-defs (partition 3 attr-defs)]
    (into {}
          (for [attr-def attr-defs]
            (let [web-name (first attr-def)  sap-name (second attr-def)  name-transform-fn (nth attr-def 2)]
              [web-name (name-transform-fn (m sap-name))])))))

(defn- transform-project-summary [v]
  (let [m (into {} (map vector et-project-fields v))
        transformed-m
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
                          (:status m))))]
    ;; [(:id transformed-m) transformed-m]))
     transformed-m))

(defn- transform-ship-to [projs v]   ;; v => [:client :project :ship-to]
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
     ;; :relationships
     ;; {:account {:data {:type "sold-to" :id (:sold-to proj)}}}
     }))

(defn projects
  ([account-num] (projects :qas account-num))
  ([server account-num]
   (println "getting projects for accont number: " account-num " on server " server)
   (let [f (find-function server :Z_O_ZVSEMM_KUNAG_ASSOC_PROJ)]
     (push f {:i_kunag (as-document-num account-num)})
     (execute f)
     (let [projs (map transform-project-summary (pull f :et-projects))
           ;; project-relationships (map (fn [m] {:type "project"
           ;;                                     :id (:id m)
           ;;                                     :links (project-link (:id m))
           ;;                                     :relationships {:data {:type "account" :id (:id m)}}
           ;;                                     }) projs)
           ]
       (doseq [proj projs]
         ;; (swap! cached-project-names assoc (:id proj) (:name proj)))
         (swap! cached-projects assoc (:id proj)
                {:id (:id proj)
                 :account-num account-num
                 :name (:name proj)}))
       {:data
        (map (partial project->json-api account-num) projs)
        }
       ;; {:data
       ;;  {:type "account"
       ;;   :id account-num
       ;;   :relationships {:projects project-relationships}
       ;;   }
       ;;  :included (map project->json-api projs)
       ;;  }
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

(def extractions
  {:order-info [;; :id (swap! id-seq-num inc)
                :project-id :projid ->int
                :order-num :vbeln-va ->int
                ;; :id 
                ;; :has-messages? (contains? messages order-num)
                :drawing-num :bstkd identity
                :schedule-date :edatu identity
                :expected-date :bstdk identity
                :entered-date :audat identity
                ;; ])]
]
   :line-item  [:item-num :posnr-va ->int
                :matnr :matnr ->int
                :customer-matnr :kdmat identity
                ;; :delivery-item :posnr-vl ->int
                :descript :arktx identity
                :circuit-id :circ-id identity
                :requested-qty :kwmeng double
                :delivered-qty :lfimg double
                :picked-qty :picked double
                :total-goods-issue-qty :tot-gi-qty double
                :remaining-qty :remaining double  ;; still to be delivered
                :reserved-qty :resv-qty double
                :uom :vrkme identity
                :inventory-loc :inv-loc identity
                :storage-loc :lgort identity
                :service-center :werks identity
                :trailer-atp :cust-loc-atp double
                :service-center-atp :main-loc-atp double]
   :delivery   [:delivery :vbeln-vl identity]})

(defn- line-item-id [m]
  (str (-> m :order :order-num) "-" (-> m :line-item :item-num)))

(defn- collect-same [v id]
  ;; (defn- collect-same [v id]
  ;;   (filter #(= id (:id %)) v))

  (filter #(= id (:id %)) v))

(defn- extract-attr-vals
  ([m begin-str] (extract-attr-vals m begin-str 1 {}))
  ([m begin-str index v]
   (let [key (keyword (str begin-str index))
         val (key m)]
     (if (nil? val)
       v
       (extract-attr-vals m begin-str (inc index) (conj v [(keyword (str "attr-" index)) val]))))))

(defn transform-status-line [m]
  {:order (transform-triplets m (:order-info extractions))
   :line-item (transform-triplets m (:line-item extractions))
   :delivery (transform-triplets m (:delivery extractions))
   :order-attr-vals (extract-attr-vals m "zz-zvsemm-vbak-attr-")
   :line-item-attr-vals (extract-attr-vals m "zz-zvsemm-vbap-attr-")
   :delivery-attr-vals (extract-attr-vals m "zz-zvsemm-likp-attr-")
   :raw m})

;; (project 2)
(defn transform-status-lines [m]
  (let [lines (map transform-status-line m)]
    lines))

(defn retrieve-maps [project-fn]
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
              [web-name (name-transform-fn (pull-map project-fn sap-name))]))))
  )

;; (project 1)

(defn- transform-raw-order [m]
  (let [order (:order m)
        ;; line-item-id (-> m :line-item )
        ]
    (assoc
     (clojure.set/rename-keys order {:order-num :id})
     :line-item-id (line-item-id m)
     :attrs (:order-attr-vals m)
     )
    ))

(defn- merge-order [orders order]
  (let [line-item-ids (apply conj [] (map :line-item-id orders))]
    (merge
     order
     {:line-item-ids (-> line-item-ids set sort)
      :attributes (:attrs (first orders))})))

(defn- join-like-orders [orders]
  (let [unique-orders (set (map #(dissoc % :line-item-id :attrs) orders))]
    (map #(merge-order (collect-same orders (:id %)) %) unique-orders)))
    ;; (set orders)))

(defn- order->json-api [order]
  {:type :project-order
   :id (:id order)
   :attributes (dissoc order :id :project-id :line-item-ids)
   :relationships {:order
                   {:links
                    {:related (str "/api/v2/orders/" (:id order))}
                    :data {:type :order :id (:id order)}}
                   :project {:data {:type :project :id (:project-id order)}}
                   :project-line-items {:data (map (fn [x] {:type :project-line-item :id x}) (:line-item-ids order))}
                   }
   })

(defn- extract-orders [maps]
  (->> maps
      (map transform-raw-order)
      join-like-orders
      (map order->json-api)
      ))

(defn- transform-raw-item [m]
  (let [item (:line-item m)
        order-id (-> m :order :order-num)
        delivery (-> m :delivery :delivery)
        ]
    (assoc item
           :id (line-item-id m)
           :order-id order-id
           :delivery delivery
           :attrs (:line-item-attr-vals m)
           )))

(defn- merge-item [items item]
  (let [
        delivery-ids (apply conj [] (filter #(not-empty %) (map :delivery items)))
        delivered-qty (apply + (map :delivered-qty items))
        picked-qty (apply + (map :picked-qty items))
        ]
    (merge
     item
     {:delivery-ids delivery-ids
      :delivered-qty delivered-qty
      :picked-qty picked-qty
      :attributes (:attrs (first items))}
     )))

(defn- join-like-items [items]
  (let [unique-items (set (map #(dissoc % :delivery :delivered-qty :picked-qty :attrs) items))]
    (map #(merge-item (collect-same items (:id %)) %) unique-items)))

(defn- line-item->json-api [item]
  {:type :project-line-item
   :id (:id item)
   :attributes (dissoc item :order-id :delivery-ids)
   :relationships {:project-order {:data {:type :project-order :id (:order-id item)}}
                   :line-item {:data {:type :line-item :id (:id item)}}
                   :project-deliveries {:data (map (fn [x] {:type :project-delivery :id x}) (:delivery-ids item))}
                   }
   })

(defn- extract-line-items [maps]
  (->> maps
      (map transform-raw-item)
      join-like-items
      (map line-item->json-api)
      ))


(defn- delivery->json-api [id]
  {:type "delivery"
   :id id})

(defn- extract-deliveries [maps]
  (->> maps
       (map (fn [m] {:type :project-delivery
                     :id (-> m :delivery :delivery)
                     :attributes (:delivery-attr-vals m)}))
       (filter #(not-empty (:id %)))
       set
       ))

(defn- attrize [attribute-maps]
  ;; attribute-maps)
  (map (fn [[key attr]]
         {:key (:id attr)
          :value (:title attr)
          :sequence (->int (last (str/split (:id attr) #"-")))}
         )
       attribute-maps))

(defn- drawings->json-api [project-id drawings]
  (for [[id order-nums] drawings]
    {:type :drawing
     :id id
     :relationships
     {:project {:data {:type :project :id project-id}}
      :project-orders
      (map (fn [x] {:type :project-order :id x})
           order-nums)
      }
     }))

(defn- circuits->json-api [project-id circuits]
  (for [[id order-nums] circuits]
    {:type :circuit
     :id id
     :relationships
     {:project {:data {:type :project :id project-id}}
      :project-orders
      (map (fn [x] {:type :project-order :id x})
           order-nums)
      }
     }))

(defn- extract-drawings [status-lines]
  (utils/collect-by
   #(-> % :order :drawing-num)
   #(-> % :order :order-num)
   status-lines))
  ;; (disj (set (map #(-> % :order :drawing-num) status-lines)) ""))

(defn- extract-circuits [status-lines]
  (dissoc
   (utils/collect-by #(-> % :line-item :circuit-id) #(-> % :order :order-num) status-lines)
   ""))
  ;; (disj (set (map #(-> % :line-item :circuit-id) status-lines)) ""))

(defn transform-project [project-id project-fn]
  (let [maps (retrieve-maps project-fn)
        ]
    (let [
          status-lines (:status-lines maps)
          order-ids (map #(-> % :order :order-num) status-lines)
          items (extract-line-items status-lines)
          orders (extract-orders status-lines)
          deliveries (extract-deliveries status-lines)
          drawings (extract-drawings status-lines)
          circuits (extract-circuits status-lines)
          json-orders (map (fn [id] {:type :project-order :id id}) order-ids)
          ]
      {:data
       {:type :project
        :id project-id
        :attributes {:project-order-attributes (attrize (:order-attr-defs maps))
                     :project-line-item-attributes (attrize (:line-item-attr-defs maps))
                     :project-delivery-attributes (attrize (:delivery-attr-defs maps))}
        :relationships {:project-orders {:data json-orders}
                        :drawings {:data
                                   (for [[id _] drawings]
                                     {:type :drawing :id id})}
                        :circuits {:data
                                   (for [[id _] circuits]
                                     {:type :circuit :id id})}
                        }
        }
       :included
       (concat orders items deliveries
                (drawings->json-api project-id drawings)
                (circuits->json-api project-id circuits)
               )
       ;; :included
       ;; {
       ;;  :project-orders orders
       ;;  :project-line-items items
       ;;  :project-deliveries deliveries
       ;;  :drawings (drawings->json-api project-id drawings)
       ;;  :circuits (circuits->json-api project-id circuits)
       ;;  }
       :raw maps
       })
    ))

(defn project
  ([project-id] (project :qas project-id))
  ([system project-id]
   (utils/ppn (str "getting project " project-id " on " system))
   (let [project-fn (find-function system :Z_O_ZVSEMM_PROJECT_CUBE)
         id-seq-num (atom 0)]
       ;; note: :attr-conv will tell us the attribute type
       ;; (ppn (function-interface project-fn))
     (push project-fn {:i-proj-id (as-document-num project-id)})
     (execute project-fn)
     (let [result (transform-project project-id project-fn)]
       (when result
         (->
          result
          (assoc-in
           [:data :id] project-id)
          (assoc-in
           [:data :name] (project-name project-id))
          (assoc-in
           [:data :relationships :account]
           {:data {:type "account" :id (project-account-num project-id)}})
          )))
     )))
;; (project 1)
;; (project 2)
;; (projects 1002224)

(println "done loading summit.sap.project")
