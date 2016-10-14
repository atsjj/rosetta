(ns summit.utils.errors
  (:require [summit.utils.core :as u])
  )

(defn handle-error [err msg]
  (println "\n\n******    ERROR: " msg "," (java.util.Date.) "*******\n" err))
