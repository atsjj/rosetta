(println "loading summit.sap.core")

(ns summit.sap.core
  (:require [clojure.string :as str]

            ;; [com.rpl.specter :as s]

            [summit.utils.core :refer [examples ->str ->keyword ->int ppl]]
            [summit.sap.types :refer :all]
            [summit.sap.conversions :refer :all]
            )
  (:import  [com.sap.conn.jco
             JCo
             AbapException JCoContext JCoDestination
             JCoDestinationManager JCoException JCoField
             JCoFunction JCoFunctionTemplate JCoStructure
             JCoTable
             ]))

(println "in summit.sap.core, after ns declaration")

(def internet-account-number "0001027925")
(def internet-service-center-code "ZZZZ")

(def servers
  {:dev "dev"
   :qas "qas"
   :prd "prd"
   :prod "prd"
   })

(def ^:dynamic *sap-server* :prd)

(defmacro with-sap-server [server-keyword & body]
  `(binding [*sap-server* ~server-keyword]
     ~@body))

(defn classMethodNames [klass]
  (sort
   (map #(.getName %)
        (.getDeclaredMethods
         klass))))
(defn methodNames [obj]
  (sort
   (map #(.getName %)
        (.getDeclaredMethods
         (type obj)))))
(defn allMethodNames [obj]
  (sort
   (reduce (fn [accum o] (concat accum (map #(.getName %) (.getDeclaredMethods o)))) [] (conj (disj (supers (type obj)) java.lang.Object) (type obj)))))



(defn find-destination [server-name]
  (if (keyword? server-name)
    (JCoDestinationManager/getDestination (servers server-name))
    server-name))

(defn find-repository [destination]
  (.getRepository destination))

(defn find-function [server-name function-name]
  (let [dest (find-destination server-name)
        repo (find-repository dest)
        func (.getFunction repo (->str function-name))
        ]
    {:server server-name :function-name (->keyword function-name) :destination dest :repository repo :function func})
  )
;; (find-destination :qas)
;; (def dev-bapi-avail (find-function :dev :bapi-material-availability))





(defn find-field-in [param-list name]
  (some #(if (= name (.getName %)) %) param-list))

(defn find-top-field [f name]
  (let [name (->str name)]
    (if-let [fld (find-field-in (.getImportParameterList f) name)]
      fld
      (if-let [fld (find-field-in (.getExportParameterList f) name)]
        fld
        (find-field-in (.getTableParameterList f) name)))))

(defn find-field
  ([f name] (find-top-field (:function f) (->str name)))
  ([f name1 name2] (find-field-in (find-field f name1) (->str name2))))

(defn table-field-names [fld]
  (map #(.getName %) (.getTable fld)))



(defn get-cols
  ([tbl]
   (get-cols tbl nil))
  ([tbl colnames]
   (doall (if colnames
            (map #(.getValue tbl (->str %)) colnames)
            (map #(.getValue %) tbl))))
  ([tbl colnames rownum]
   (.setRow tbl rownum)
   (get-cols tbl colnames)))

(defn get-structure
  ([fld] (get-structure fld nil))
  ([fld names]
   (get-cols (.getStructure fld) names)))

(defn get-table
  ([fld] (get-table fld nil))
  ([fld names]
   (let [tbl (.getTable fld)]
     (map #(get-cols tbl names %) (range (.getNumRows tbl))))))

(defn get-data [fld & args]
  (cond
    (.isTable fld) (apply get-table fld args)
    (.isStructure fld) (apply get-structure fld args)
    :else (.getValue fld)))






(defn set-cols
  ([fld v] (set-cols fld nil v))
  ([fld colnames v]
   (let [colnames (if colnames (map ->str colnames) (table-field-names fld))
         tbl (.getTable fld)]
     (doall (map #(.setValue tbl %1 %2) colnames v))))
     ;; (doall (map #(vector  (->str %1) %2) colnames v))))
  ([fld colnames rownum v]
   (let [tbl (.getTable fld)]
     (.appendRow tbl)
     ;; note: can't recursively call because pointer is now at newly appended row
     ;;       but a recursive call will perform another getTable, which resets the pointer
     (doall (map #(.setValue tbl %1 %2) colnames v)))))

(defn set-table
  ([fld v] (set-table fld nil v))
  ([fld names v]
   (let [tbl (.getTable fld)
         names (if names names (table-field-names fld))
         ]
     (doall (map #(set-cols fld names 1 %) v))
     )))

(defn set-data [fld & args]
  (cond
    (.isTable fld) (apply set-table fld args)
    (.isStructure fld) 3
    :else (.setValue fld (first args))))

(defn push [f m]
  ;; (map (fn [[k v]] (vector k v)) m))
  ;; (map (fn [[k v]] (vector (find-field f k) v)) m))
  (doall (map (fn [[k v]] (set-data (find-field f k) v)) m)))


;; (defn table-field-names [fld]              ;; defined above
;;   (map #(.getName %) (.getTable fld)))

(defn field-names [param-list]
  (map #(.getName %) param-list))

(defn has-field? [param-list fldname]
  (contains? (set (field-names param-list)) (->str fldname)))

(defn composite-field? [fld]
  (or (.isTable fld) (.isStructure fld)))

(defn table-or-structure [fld]
  (if (.isTable fld)
    (.getTable fld)
    (.getStructure fld)))

(defn parameter-field? [fld]
  (= (type fld) com.sap.conn.jco.rt.DefaultParameterField))

(declare composite-field-definitions)

(defn field-definition [fld]
  (let [param-field? (parameter-field? fld)
        title (.getName fld)
        keyword-name (->keyword title)
        typ (->keyword (.getTypeAsString fld))
        descript (.getDescription fld)
        len (.getLength fld)
        where (if param-field? (cond (.isImport fld) :import (.isExport fld) :export (.isChanging fld) :changing (.isTable fld) :table))
        required? (if param-field? (not (.isOptional fld)))
        active? (if param-field? (.isActive fld))
        default-val (if param-field? (.getDefault fld) :no-default)
        columns (if (composite-field? fld)
                  (composite-field-definitions fld))]
    (let [arr [keyword-name title descript typ len where required? default-val]]
      (if columns
        (conj arr columns)
        (conj arr [])
        ))))
;; (function-interface dev-jarred-avail)
;; (field-definition x)
;; (field-definition y)
;; [:mat-atp "MAT_ATP" "Structure for Z_ISA_MAT_AVAILABILITY" :table com.sap.conn.jco.rt.DefaultParameterField ([[:matnr "MATNR" "Material Number" :char com.sap.conn.jco.rt.DefaultRecordField :default]] [[:werks "WERKS" "Plant" :char com.sap.conn.jco.rt.DefaultRecordField :default]] [[:name1 "NAME1" "Name" :char com.sap.conn.jco.rt.DefaultRecordField :default]] [[:com-qty "COM_QTY" "Committed Quantity" :bcd com.sap.conn.jco.rt.DefaultRecordField :default]])]
;; (.isOptional y)
;; (spit "junk"
;;       (with-out-str (clojure.pprint/pprint (field-definition y))))
;; (.isTable y)

(defn composite-field-definitions [fld]
  (map (fn [x] (field-definition x)) (table-or-structure fld) ))

(defn field-definitions [param-list]
  (map field-definition param-list))

(defn get-field [f fldname]
  (let [im (.getImportParameterList f)
        fld (->str fldname)]
    (if (has-field? im fld)
      (.getValue im fld)
      (.getValue (.getTableParameterList f) fld))))

(defn get-value [f fldname]
  (let [im (.getImportParameterList f)
        fld (->str fldname)]
    (if (has-field? im fld)
      (.getValue im fld)
      (.getValue (.getTableParameterList f) fld))))

(defn pull [f name & args]
  (apply get-data (find-field f name) args))

(defn pull-with-schema [f name & args]
  (let [fld (find-field f name)
        names (map first (last (field-definition fld)))
        vals (apply get-data fld args)
        ]
    {:schema (last (field-definition fld))
     :names names
     :data vals}
    ))
;; (pull (find-function :qas :z-o-zvsemm-project-cube) :et-status-lines)
;; (def x (pull-with-schema (find-function :qas :z-o-zvsemm-project-cube) :et-status-lines))
;; x
;; (keys x)
;; (:data x)

(defn pull-with-schemas [f & names]
  (into {}
        (map
         #(let [key (if (coll? %) (first %) %)
                return-key (if (coll? %) (second %) %)]
            [return-key (pull-with-schema f key)])
         names)))

(defn pull-map [f name & args]
  (let [fld (find-field f name)
        names (map first (last (field-definition fld)))
        vals (apply get-data fld args)
        ]
    (if (empty? (first vals))
      {}
      (if (seq? (first vals))
        (map #(apply assoc {} (interleave names %)) vals)
        (apply assoc {} (interleave names vals))
        ))
    ))

;; (defrecord sap-data-translation
;;     [web-name sap-name descript type length required? active? default columns])

;; (def sap-data-translations (atom {}))

;; (defn create-sap-translation [v]
;;   (case (nth v 3)
;;                                            ; web-name  sap-name   descript  type      len       required? active?  default  columns
;;     (:table :struct) (->sap-data-translation (first v) (second v) (nth v 2) (nth v 3) nil       nil       nil      nil      (map create-sap-translation (nth v 4)))
;;     (let [cnt (count v)]
;;       (->sap-data-translation                (first v) (second v) (nth v 2) (nth v 3) (nth v 4) (nth v 5) (nth v 6) (nth v 7) nil)
;;       )
;;     ))
;; ;; (field-definition y)

;; (create-sap-translation
;;   [:atps "MAT_ATP" "availability" :table
;;    [[:matnr          "MATNR" "material number" :char 18 :matnr]
;;     [:service-center "WERKS" "plant"           :char 5 nil]
;;     [:qty            "COM_QTY" "quantity"      :bcd 15 :bigint-bcd]
;;     ]])

;; {:z-isa-mat-availabiliity
;;  {:inputs [:matnr "MATNR"]
;;   :tables [:atps ...]}}

;; (->sap-data-translation :atps "MAT_ATP" :table nil
;;                    [])

(defn function-interface [func]
  (let [f (:function func)]
    {:name (:function-name func)
     :imports (field-definitions (.getImportParameterList f))
     :exports (field-definitions (.getExportParameterList f))
     :tables (field-definitions (.getTableParameterList f))
     }))





(defn execute [f]
  (.execute (:function f) (:destination f))
  f)

;; (defn set-data
;;   ([f key val]
;;    (let [key (->str key)
;;          fld (find-field (:function f) key)]
;;      (.setValue fld val)))
;;   ([f key key2 val])
;;   )

;; (defn get-data
;;   ([f key]
;;    )
;;   ([f & keys]
;;    )
;;   )

(defprotocol SapEntity
  (val [this])
  (definition [this])
  )

(extend com.sap.conn.jco.rt.AbapFunction
  SapEntity
  {:val (fn [this] 3)
   :definition (fn [this] 4)
   }
  )

(extend com.sap.conn.jco.rt.DefaultTable
  SapEntity
  {:val (fn [this] 3)
   :definition (fn [this] 4)
   }
  )

(extend com.sap.conn.jco.rt.DefaultParameterField
  SapEntity
  {:val (fn [this] 3)
   :definition (fn [this] 4)
   }
  )


(examples

(def has-data-qas-avail (find-function :dev :z-isa-mat-availability))
(def f (:function has-data-qas-avail))
(.setValue (.getImportParameterList (:function has-data-qas-avail)) "MATNR" (as-matnr "33323"))
(.execute (:function has-data-qas-avail) (:destination has-data-qas-avail))
(def t (.getTable (.getTableParameterList (:function has-data-qas-avail)) "MAT_ATP"))

(get-value f "MAT_ATP")
(pull has-data-qas-avail "MAT_ATP")
(pull-with-schema has-data-qas-avail "MAT_ATP")

(type t)
(.getFieldCount t)
(.getNumColumns t)
(.getNumRows t)
(.hasField t "MATNR")
(.getValue t "MATNR")
(.getValue t (->str :matnr))
(.isTable t)

(.getType f)
(.getType f 1)
(.getTabLength f)
(.getDescription f 1)
(.indexOf f 1)

(val (find-field dev-jarred-avail "MAT_ATP"))
(definition (find-field dev-jarred-avail "MAT_ATP"))


(def t (.getValue (.getImportParameterList (:function has-data-qas-avail)) "MATNR"))

(.setValue (.getImportParameterList (:function has-data-qas-avail)) "MATNR" (as-matnr "33323"))
(.execute (:function has-data-qas-avail) (:destination has-data-qas-avail))
(.getValue f "MATNR")

(get-data f)
(get-table f :mat-atp [:matnr :werks :com-qty :name1])
(get-table f :mat-atp [:matnr :werks :com-qty :name1])
(get-table f :mat-atp (range 4))
(get-table f :mat-atp nil)
(count (get-table f :mat-atp [:matnr :werks :com-qty :name1]))


)



(examples



(def dev-bapi-avail (find-function :dev :bapi-material-availability))
(def qas-avail (find-function :default :z-isa-mat-availability))
(def qas-avail (find-function :qas :z-isa-mat-availability))
(def dev-jarred-avail (find-function :dev :z-o-material-availability))

(find-field dev-jarred-avail "MAT_ATP")

(find-field (:function dev-jarred-avail) "MAT_ATP")
(find-field (:function dev-jarred-avail) "I_SERVICE_CENTER")
(find-field (:function dev-jarred-avail) "IT_MATERIAL")
(function-interface dev-jarred-avail)




(function-interface (find-function :qas :z-o-zvsemm-project-cube))
(:tables (function-interface (find-function :qas :z-o-zvsemm-project-cube)))
(filter #(= :et-status-lines (first %)) (:tables (function-interface (find-function :qas :z-o-zvsemm-project-cube))))
(first (filter #(= :et-status-lines (first %)) (:tables (function-interface (find-function :qas :z-o-zvsemm-project-cube)))))
(nth (first (filter #(= :et-status-lines (first %)) (:tables (function-interface (find-function :qas :z-o-zvsemm-project-cube))))) 8)
(map first (nth (first (filter #(= :et-status-lines (first %)) (:tables (function-interface (find-function :qas :z-o-zvsemm-project-cube))))) 8))
(pull (find-function :qas :z-o-zvsemm-project-cube) :et-status-lines)
(pull-with-schema (find-function :qas :z-o-zvsemm-project-cube) :et-status-lines)

(:table (first (filter #(= :et-status-lines (first %)) (:tables (function-interface (find-function :qas :z-o-zvsemm-project-cube))))))
(field-definitions (find-function :qas :z-o-zvsemm-project-cube))
(field-definitions (find-field (:function (find-function :qas :z-o-zvsemm-project-cube)) "ET_STATUS_LINES"))
(find-field (:function (find-function :qas :z-o-zvsemm-project-cube)) "ET_STATUS_LINES")
(field-names (.getTableParameterList (:function (find-function :qas :z-o-zvsemm-project-cube))))
(field-names ((.getTableParameterList (:function (find-function :qas :z-o-zvsemm-project-cube))) "ET_STATUS_LINES"))
(find-field (.getTableParameterList (:function (find-function :qas :z-o-zvsemm-project-cube))) "ET_STATUS_LINES")
(find-field (find-function :qas :z-o-zvsemm-project-cube) "ET_STATUS_LINES")
(field-names (find-field (find-function :qas :z-o-zvsemm-project-cube) "ET_STATUS_LINES"))

(get-field (:function (find-function :qas :z-o-zvsemm-project-cube)) :et-status-lines)
(get-field (find-function :qas :z-o-zvsemm-project-cube) :et-status-lines)
(get-field (find-function :qas :z-o-zvsemm-project-cube) "ET_STATUS_LINES")




(def has-data-qas-avail (find-function :default :z-isa-mat-availability))
(def has-data-qas-avail (find-function :dev :z-isa-mat-availability))
(.setValue (.getImportParameterList (:function has-data-qas-avail)) "MATNR" (as-matnr "33323"))
(.execute (:function has-data-qas-avail) (:destination has-data-qas-avail))
(def t (.getTable (.getTableParameterList (:function has-data-qas-avail)) "MAT_ATP"))
t 

(function-interface qas-avail)
(function-interface dev-bapi-avail)
(function-interface dev-jarred-avail)
(allMethodNames (:function dev-bapi-avail))
(.getName (:function dev-bapi-avail))
(spit "junk"
      (with-out-str (clojure.pprint/pprint
                     (function-interface dev-bapi-avail)
                     )))

(def x (first (.getImportParameterList (:function qas-avail))))
(def y (first (.getTableParameterList (:function qas-avail))))
(field-definition x)
(allMethodNames x)
(.getExtendedFieldMetaData x)
(.getDescription x)
(.getDescription y)
(.getName x)
(.getTypeAsString x)
;; (.getTypeName x)
;; (.getTypeParameters x)
(.getLength x)
(.getUnicodeByteLength x)

(methodNames (first (.getImportParameterList (:function qas-avail))))





(field-names (.getImportParameterList f))
(field-names (.getTableParameterList f))
(table-field-definition (.getTableParameterList f))
(field-definitions (.getTableParameterList f))
(field-definitions (.getImportParameterList f))
(field-definitions (.getExportParameterList f))
(concat
 (field-definitions (.getTableParameterList f))
 (field-definitions (.getImportParameterList f))
 (field-definitions (.getExportParameterList f)))

(.setValue (.getImportParameterList f) "MATNR" (as-matnr "33323"))
(.execute f d)
(def t (.getTable (.getTableParameterList f) "MAT_ATP"))
(get-table f :mat-atp [:matnr :werks :com-qty :name1])
(get-table f :mat-atp (range 4))
(get-table f :mat-atp nil)
(count (get-table f :mat-atp [:matnr :werks :com-qty :name1]))


(composite-field-definitions x)

(map table-field-definition fld)

(field-definitions (.getTableParameterList f))
(field-definitions (.getImportParameterList f))
(field-definitions (.getExportParameterList f))


)

(println "done loading summit.sap.core")
