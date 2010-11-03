(ns hammockdb.data-test
  (:use clojure.test)
  (:require [hammockdb.data :as data]))

(deftest test-set-fn
  (let [assocer (data/set-fn (fn [m k v] [(assoc m k v) v]))
        ident   (atom {})
        ret     (assocer ident :k :v)]
    (is (= :v ret))
    (is (= {:k :v} @ident))))
