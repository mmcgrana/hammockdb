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
  (let [state (data/state-new)]
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

(deftest test-db-delete
  (let [state (data/state-new)
        [_ state] (data/db-put state "db1")
        [ret state] (data/db-delete state "db1")]
    (is (:ok ret))
    (is (= [] (:dbids (data/db-list state))))
    (let [[ret state] (data/db-delete state "db1")]
      (is (:no-db ret))
      (is (nil? state)))))

(deftest test-doc-validate-keys
  (is (nil? (data/doc-validate-keys {"foo" "bar"})))
  (is (= {:invalid-key "_wot"} (data/doc-validate-keys {"_wot" "bat"}))))

(deftest test-doc-update-errors
  (is (:id-mismatch (data/doc-update {:id "doc1"} {"k" "v"})))
  (is (:id-mismatch (data/doc-update {:id "doc1"} {"_id" "doc2" "k" "v"})))
  (is (:conflict (data/doc-update {:id "doc1" :rev "1-a"}
                                      {"_id" "doc1" "_rev" "2-b"})))
  (is (:conflict (data/doc-update {:id "doc1" :rev "1-a"} {"_id" "doc1"})))
  (is (:invalid-key (data/doc-update {:id "doc1"} {"_id" "doc1" "_wot" "v"}))))

(def test-doc-update-new
  (let [ret (data/doc-update {:id "doc1" :conflicts []} {"_id" "doc1" "k" "v"})]
    (is (:update ret))
    (let [{:keys [info doc]} (:update ret)]
      (is (= "doc1" (:id info)))
      (is (:rev info))
      (is (= "doc1") (:id doc))
      (is (.startsWith (:rev doc) "1-"))
      (is (not (:deleted doc)))
      (is (= "doc1" (get-in doc [:body "_id"])))
      (is (.startsWith (get-in doc [:body "_rev"]) "1-"))
      (is (= "v" (get-in doc [:body "k"]))))))

(def test-doc-update-existing
  (let [ret (data/doc-update {:id "doc1" :conflicts []}
                             {"_id" "doc1" "k" "v1"})]
    (let [doc (:doc (:update ret))
          rev (:rev doc)
          ret (data/doc-update doc {"_id" "doc1" "_rev" rev "k" "v2"})]
      (is (:update ret))
      (let [{:keys [info doc]} (:update ret)]
        (is (= "doc1" (:id info)))
        (is (.startsWith (:rev info) "2-"))
        (is (= "doc1" (:id doc)))
        (is (.startsWith (:rev doc) "2-"))
        (is (not (:deleted doc)))
        (is (= "doc1" (get-in doc [:body "_id"])))
        (is (.startsWith (get-in doc [:body "_rev"]) "2-"))
        (is (= "v2" (get-in doc [:body "k"])))))))

(deftest test-doc-update-delete
  (let [ret (data/doc-update {:id "doc1" :conflicts []}
                             {"_id" "doc1" "k" "v1"})]
    (let [doc (:doc (:update ret))
          rev (:rev doc)
          ret (data/doc-update doc {"_id" "doc1" "_rev" rev "_deleted" true})]
      (is (:deleted (:doc (:update ret)))))))

(deftest test-doc-new
  (let [doc (data/doc-new "id1" {"_id" "id1" "k" "v"})]
    (is (:id doc))
    (is (:rev doc))
    (is (:body doc))))

(deftest test-doc-put-no-db
  (let [state (data/state-new)]
    (let [[ret state] (data/doc-put state "db1" "doc1" {})]
      (is (:no-db ret))
      (is (nil? state)))))

(deftest test-doc-put-bad-doc-new
  (let [state (data/state-new)
        [_ state] (data/db-put state "db1")
        [ret state] (data/doc-put state "db1" "doc1" {})]
    (prn ret)
    (prn state)
    (is (:bad-doc ret))
    (is (nil? state))))

(deftest test-doc-put-bad-doc-existing)

; (deftest test-doc-find-rev-fail
;   (is (nil? (data/doc-find-rev {:conflicts [] :rev "foo"} "bar"))))
; 
; (deftest test-doc-get
;   (let [ret (data/doc-update {:id "doc1" :conflicts []}
;                              {"_id" "doc1" "k" "v1"})]
;     )
