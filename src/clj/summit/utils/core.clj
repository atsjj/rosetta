(ns summit.utils.core
  (:require
    [clojure.string :as str]
    [summit.utils.log :as log]
    ))

(defmacro examples [& forms]
  )

;; ;; Conversions

(defn ->str [a-name]
  (if (string? a-name)
    a-name
    (if (number? a-name)
      a-name
      (str/replace
       (str/upper-case
        (if (keyword? a-name)
          (name a-name)
          (str a-name)))
       "-" "_"))))

(defn ->keyword [a-string]
  (if (keyword? a-string)
    a-string
    (keyword
     (str/lower-case (str/replace (str a-string) "_" "-")))))

(defn ->int [v]
  (if (nil? v)
    nil
    (if (string? v)
      (let [v (str/trim v)]
        (if (empty? v)
          nil
          (-> v Double/parseDouble int)))
      (int v))))

(defn ->float [v]
  (if (nil? v)
    nil
    (if (string? v)
      (let [v (str/trim v)]
        (if (empty? v)
          nil
          (Double/parseDouble v)))
      (double v))))

(defn ->long [v]
  (if (nil? v)
    nil
    (if (string? v)
      (let [v (str/trim v)]
        (if (empty? v)
          nil
          (-> v Double/parseDouble long)))
      (long v))))

(defn as-integer [string]
  (->int string))


;; Logging

(defn ppn
  "pprint, returning nil"
  [& args]
  (apply log/log-soon args)
  nil)

(defn ppl
  "pprint, returning last arg"
  [& args]
  (apply log/log-soon args))

(defn ppd
  "pprint, but when called quickly, drop most calls. Return last arg"
  [& args]
  (apply log/log-slowly args))

(defn ppdn
  "pprint, but when prints a lot, drop some of them. Return nil"
  [& args]
  (apply log/log-slowly args)
  nil)


;; SAP conversions

(defn zero-pad [width string]
  (if string
    (let [s (str (apply str (repeat width "0")) string)]
      (subs s (- (count s) width)))))

(defn as-matnr [string]
  (zero-pad 18 string))

(defn as-document-num [string]
  (zero-pad 10 string))
;; (as-document-num "asdf")

(defn as-short-document-num [string]
  "remove leading zeros"
  (if string (str/replace string #"^0*" "")))
;; (as-short-document-num (as-document-num "00001"))

(defn as-customer-num [string]
  (as-document-num string))


;; Clean http request to enable printing

(defn req-sans-unprintable [req]
  #_["compojure.api.middleware/options",
     "cookies",
     "remote-addr",
     "ring.swagger.middleware/data",
     "params",
     "flash",
     "route-params",
     "headers",
     "async-channel",
     "server-port",
     "content-length",
     "form-params",
     "compojure/route",
     "websocket?",
     "session/key",
     "query-params",
     "content-type",
     "path-info",
     "character-encoding",
     "context",
     "uri",
     "server-name",
     "query-string",
     "body",
     "multipart-params",
     "scheme",
     "request-method",
     "session"]
  (let [bad-params [                        ; these throw errors when json-izing
                    ;; :compojure.api.middleware/options
                    ;; :async-channel
                    ;; :compojure/route
                    :route-middleware
                    :route-handler
                    :server-exchange
                    ;; :context
                    :body
                    ]
        x (apply dissoc (concat [req] bad-params))]
    x))



(defn clean-request [req]
  ;; (clean-all (req-sans-unprintable req)))
  (req-sans-unprintable req))


(defn collect-by [key-fn val-fn maps]
  (let [result (atom {})]
    (doseq [m maps]
      (let [key      (key-fn m)
            value    (val-fn m)
            orig-val (@result key)
            new-val  (if (nil? orig-val) (set [value]) (conj orig-val value))
            ]
        (swap! result assoc key new-val)))
    @result))
(examples
 (collect-by :a :b [{:a 3 :b 7} {:a 4 :b 5} {:a 3 :b 9}])
 (let [x [{:a 3 :b 7} {:a 4 :b 5} {:a 3 :b 9}]]
   (assert (=
            (collect-by :a :b x)
            ;; {3 [7 9] 4 [5]})))
            {3 #{7 9} 4 #{5}})))
 )
