(ns hammockdb.data-test
  (:use clojure.test)
  (:require [hammockdb.data :as data]))

(deftest test-set-fn
  (let [assocer (data/set-fn (fn [m k v] [v (assoc m k v)]))
        ident   (atom {})
        ret     (assocer ident :k :v)]
    (is (= :v ret))
    (is (= {:k :v} @ident))))

(deftest test-db-lifecycle
  (let [state {}]
    (is (= {:dbids []} (data/db-index state)))
    (let [[ret state] (data/db-put state "db1")]
      (is (= {:db {:seq 0 :doc-count 0 :by-docid {} :by-seq {}}} ret))
      (is (= {:dbids ["db1"]} (data/db-index state)))
      (let [[ret state] (data/db-put state "db2")]
        (is (:db ret))
        (is (= (set ["db1" "db2"]) (set (:dbids (data/db-index state)))))))))
