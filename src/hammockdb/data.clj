(ns hammockdb.data
  (:use [hammockdb.util :only (update-at if-not-let)])
  (:require [hammockdb.util :as util])
  (:require [clojure.string :as str])
  (:import java.util.UUID))

(defn state-new [] {})
(defn ident-new [] (atom (state-new)))

(defn set-fn [pure-write]
  (fn [ident & args]
    (let [state @ident
          [ret state-new] (apply pure-write state args)]
      (when state-new
        (assert (compare-and-set! ident state state-new)))
      ret)))

(defn uuid []
  (str (UUID/randomUUID)))

(defn uuids [c]
  (take c (repeatedly uuid)))

(defn db-new [dbid]
  {:seq 0
   :doc-count 0
   :by-docid {}
   :by-seq {}})

(defn db-list
  "dbids"
  [state]
  {:dbids (or (keys state) [])})

(defn db-inflate [db dbid]
  {"db_name" dbid
   "update_seq" (:seq db)
   "doc_count" (:doc-count db)})

(defn db-get
  "no-db, db"
  [state dbid]
  (if-not-let [db (get state dbid)]
    {:no-db true}
    {:db (db-inflate db dbid)}))

(defn db-put
  "existing-db, db"
  [state dbid]
  (if (get state dbid)
    [{:existing-db true} nil]
    (let [db (db-new dbid)]
      [{:db db} (assoc state dbid db)])))

(def db-put! (set-fn db-put))

(defn db-delete
  "no-db, ok"
  [state dbid]
  (if-not-let [db (get state dbid)]
    [{:no-db true} nil]
    [{:ok true} (dissoc state dbid)]))

(def db-delete! (set-fn db-delete))

(defn doc-get
  "no-db, no-doc, doc"
  [state dbid docid]
  (if-not-let [db (get state dbid)]
    {:no-db true}
    (if-not-let [doc (get (:by-docid db) docid)]
      {:no-doc true}
      {:doc doc :db db})))

(defn doc-new-rev [& [old-rev]]
  (if old-rev
    (let [num (util/parse-int (first (str/split old-rev #"\-")))]
      (str (inc num) "-" (uuid)))
    (str 1 "-" (uuid))))

(defn doc-update
  "conflict, update"
  [doc new-doc]
  (let [rev (get doc "_rev")
        check-rev (get new-doc "_rev")]
    (if (and rev (not= rev check-rev))
      {:conflict true}
      (let [new-rev (if rev (doc-new-rev rev) (doc-new-rev))
            info {:id (get doc "_id") :rev rev}
            new-doc (assoc new-doc "_rev" new-rev)]
        {:update {:doc new-doc :info info}}))))

(defn doc-put
  "no-db, doc"
  [state dbid docid new-doc rev]
  (if-not-let [db (get state dbid)]
    [{:no-db true} nil]
    (let [new-seq (inc (:seq db))]
      (if-let [doc (get-in db [:by-docid docid])]
        (let [res (doc-update doc (assoc new-doc "_rev" rev))]
          (if-not-let [update (:update res)]
            [res nil]
            (let [db (assoc db :seq new-seq)
                  db (update-at db :by-docid assoc docid (:doc update))
                  db (update-at db :by-seq assoc new-seq (:info update))]
              [{:doc (:doc update)} (assoc state dbid db)])))
        (let [update (:update (doc-update {"_id" docid}
                                          (assoc new-doc "_id" docid)))]
          (let [db (assoc db :seq new-seq)
                db (update-at db :doc-count inc)
                db (update-at db :by-docid assoc docid (:doc update))
                db (update-at db :by-seq assoc new-seq (:info update))]
            [{:doc (:doc update)} (assoc state dbid db)]))))))

(def doc-put! (set-fn doc-put))

(defn doc-post
  "no-db, doc"
  [state dbid doc]
  (let [docid (or (get doc "_id") (uuid))
        doc (assoc doc "_id" docid)]
    (doc-put state dbid docid doc)))

(def doc-post! (set-fn doc-post))

(defn doc-delete
  "no-db, no-doc, conflict, doc"
  [state dbid docid rev]
  (let [ret (doc-get state dbid docid)]
    (if-not-let [doc (:doc ret)]
      [ret nil]
      (if-not (= rev (get doc "_rev"))
        [{:conflict true} nil]
        (let [new-rev (doc-new-rev rev)
              new-doc (assoc doc "_rev" new-rev "_deleted" true)
              db (:db ret)
              db (update-at db :seq inc)
              db (update-at db :doc-count dec)
              db (update-at db :by-docid dissoc docid)
              db (update-at db :by-seq assoc (:seq db)
                   {:id (get doc "_id") :rev new-rev :deleted true})]
          [{:doc new-doc} (assoc state dbid db)])))))

(def doc-delete! (set-fn doc-delete))
