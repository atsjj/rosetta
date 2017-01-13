(ns summit.sap.spreadsheet
  (:require [clojure.java.io :as io]
            [dk.ative.docjure.spreadsheet :as xls]

            [summit.utils.core :as u]
            ))

(defn- temp-filename [prefix suffix]
  (format "/tmp/%s-%s-%s%s"
          prefix
          (System/currentTimeMillis)
          (long (rand 0x100000000))
          (if suffix (str "." suffix))
          ))
;; (temp-filename "abc" "xlsx")

(defn delete-col-nums
  ([delete-cols] (fn [row] (delete-col-nums delete-cols row)))
  ([delete-cols row]
   delete-cols
   (->>
    (map #(if (contains? delete-cols %2) :delete-it %1) row (range))
    (remove #(= % :delete-it))
    )))
;; (delete-col-nums #{2 9} (range 15))
;; (->> (remove-deleted-columns all-data) (take 3))

(defn remove-deleted-columns
  [data]
  (let [deleted-col-nums (->>
                          (map (fn [col colnum] (if (= col "--DELETE--") colnum))
                               (first data)
                               (range))
                          (remove nil?)
                          set)
        ]
    (if (empty? deleted-col-nums)
      data
      (map (delete-col-nums deleted-col-nums) data)))
  )

(defn create-temp-spreadsheet
  "returns the filename of the created spreadsheet"
  [prefix wb-name colnames data]
  (let [filename (temp-filename prefix "xlsx")
        data (concat [(map u/->str colnames)] data)
        data (remove-deleted-columns data)
        ;; _ (println data)
        wb (xls/create-workbook wb-name
                                data)]
  (def all-data data)
    (xls/save-workbook! filename wb)
    filename))
;; (create-temp-spreadsheet "status-lines" "Status Lines"
;;                          ;; ["A" "b" "Col C"]
;;                          [:ab :cd :ef]
;;                          [[1 1 1]
;;                           [2 2 2]
;;                           [3 3 33]])

(def mime-spreadsheet
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

(defn read-file [filename]
  (with-open [reader (io/input-stream filename)]
    (let [length (.length (io/file filename))
          buffer (byte-array length)]
      (.read reader buffer 0 length)
            buffer)))

