(ns hammockdb.data-test
  (:use clojure.test)
  (:require [hammockdb.data :as data]))

(deftest test-set-fn
  (let [assocer (data/set-fn (fn [m k v] [v (assoc m k v)]))
        ident   (atom {})
        ret     (assocer ident :k :v)]
    (is (= :v ret))
    (is (= {:k :v} @ident))))

(deftest test-db-put
  (let [state {}]
    (is (= {:dbids []} (data/db-list state)))
    (let [[ret state] (data/db-put state "db1")]
      (is (= {:db {:seq 0 :doc-count 0 :by-docid {} :by-seq {}}} ret))
      (is (= {:dbids ["db1"]} (data/db-list state)))
      (let [[ret state] (data/db-put state "db2")]
        (is (:db ret))
        (is (= (set ["db1" "db2"]) (set (:dbids (data/db-list state)))))
        (let [[ret state] (data/db-put state "db1")]
          (is (:existing-db ret))
          (is (nil? state)))))))

(deftest test-db-get
  (let [state (data/state-new)
        [_ state] (data/db-put state "db1")
        ret1 (data/db-get state "db1")
        ret2 (data/db-get state "db2")]
    (is (= "db1" (get-in ret1 [:db "db_name"])))
    (is (:no-db ret2))))
