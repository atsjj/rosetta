(ns summit.utils.log
  (:require [clojure.pprint :refer [pprint]]
           [clojure.core.async
            :as
            a
            :refer
            [<!
             <!!
             >!
             >!!
             alts!
             alts!!
             buffer
             chan
             close!
             dropping-buffer
             go
             go-loop
             sliding-buffer
             thread
             timeout
             ]]
           ))

(defn- create-soon-chan []
  (chan 30))
(defn- create-slow-chan []
  (chan (sliding-buffer 3)))

(def ^:private log-chan (atom (create-soon-chan)))
(def ^:private log-slowly-chan (atom (create-slow-chan)))
(def log-delay 500)

(defn- log
  [args]
  (binding [clojure.pprint/*print-miser-width* 120
            clojure.pprint/*print-right-margin* 120]
    (doseq [arg args] (pprint arg))))

(defn- log-loop [chan-atom post-log-fn]
  (go-loop []
    (let [args (<! @chan-atom)]
      (when args
        (log args)
        (post-log-fn)
        (recur))
      )))

(defn- chan-open? [c]
  ;; Without keeping state, seems to be no other way to determine if channel is
  ;; open other than writing to it.
  (>!! c "."))

(defn- start-loop [chan-atom create-chan-fn post-log-fn]
  (when-not (chan-open? @chan-atom)
    (reset! chan-atom (create-chan-fn))
    (log-loop chan-atom post-log-fn)
    ))

(defn start []
  (println "starting log-loops")
  (start-loop log-chan create-soon-chan #())
  (start-loop log-slowly-chan create-slow-chan #(Thread/sleep log-delay)))

(defn stop []
  (println "stopping logging")
  (close! @log-chan)
  (close! @log-slowly-chan)
  )

(stop)
(start)


;; These are the primary interface functions:

(defn log-soon [& args]
  (when-not (>!! @log-chan args)
    (log args))
  (last args))
;; (>!! @log-chan [3 4 5])
;; (<!! @log-chan)

(defn log-slowly [& args]
  (when-not (>!! @log-slowly-chan args)
    (log args))
  (last args))



;; (log-soon 3 4)
;; (log-slowly 5 6)

;; (dotimes [n 50]
;;   (log-soon n 4 (rand)))
;; (dotimes [n 50]
;;   (log-slowly n 4 (rand)))

