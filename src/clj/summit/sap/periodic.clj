(ns summit.sap.periodic
  (:require [summit.utils.errors :as err])
  )

;; ------------------------------- cron jobs

(defn set-interval [callback ms & msgs]
  (future (while true
            (try
              (callback)
              (catch Exception e
                (err/handle-error e msgs))
              )
            (Thread/sleep ms))))

(defn set-interval-named [name callback ms]
  (set-interval (fn []
                  (println "\n\nProcessing cron-job " name (java.util.Date.))
                  (callback)
                  (println "Completed cron-job " name (java.util.Date.))
                  )
                ms))

(defonce cron-jobs (atom {}))

(defn add-cron-job [name f duration]
  (swap! cron-jobs assoc name (set-interval-named name f duration)))

(defn stop-cron-job [name]
  (let [job-fn (@cron-jobs name)]
    (when job-fn
      (future-cancel job-fn)
      (swap! cron-jobs dissoc name))))

(defn start-cron-jobs [duration named-jobs]
  (doseq [[name job-fn] named-jobs]
    (when-not (@cron-jobs name)  ;; leave existing jobs untouched
      (add-cron-job name job-fn duration))))

(defn stop-all-cron-jobs []
  (doseq [name (keys @cron-jobs)]
    (stop-cron-job name)))
