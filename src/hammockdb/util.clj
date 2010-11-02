(ns hammockdb.util)

(defn parse-int [int-str]
  (Integer/parseInt int-str))

(defmacro switch [data & cases]
  `(let [{:keys ~(vec (take-nth 2 cases))} ~data]
    (cond
      ~@cases)))
