(ns hammockdb.util)

(defn parse-int [int-str]
  (Integer/parseInt int-str))

(defmacro switch [data & cases]
  `(let [{:keys ~(vec (take-nth 2 cases))} ~data]
    (cond
      ~@cases)))

(defn update-at [o k f & args]
  (apply update-in o [k] f args))

(defmacro if-not-let [[as to] n y]
  `(let [~as ~to]
     (if ~as ~y ~n)))
