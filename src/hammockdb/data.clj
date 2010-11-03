(ns hammockdb.data
  (:use [hammockdb.util :only (update if-not-let)])
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

(defn db-put
  "existing-db, db"
  [state dbid]
  (if (get state dbid)
    [{:existing-db true} nil]
    (let [db (db-new dbid)]
      [{:db db} (assoc state dbid db)])))

(def db-put! (set-fn db-put))

(defn db-inflate [db dbid]
  {"db_name" dbid
   "doc_count" (:doc-count db)
   "doc_size" (* 339.2 (:doc-count db))
   "purge_seq" 0
   "update_seq" (:seq db)})

(defn db-get
  "no-db, db"
  [state dbid]
  (if-not-let [db (get state dbid)]
    {:no-db true}
    {:db (db-inflate db dbid)}))

(defn db-delete
  "no-db, ok"
  [state dbid]
  (if-not-let [db (get state dbid)]
    [{:no-db true} nil]
    [{:ok true} (dissoc state dbid)]))

(def db-delete! (set-fn db-delete))

(def doc-special-keys
  #{"_id" "_rev" "_deleted" "_attachments"})

(defn doc-validate-keys [body]
  (some
    (fn [[k v]]
      (and (.startsWith k "_") (not (doc-special-keys k)))
        {:invalid-key k})
    body))

(defn doc-new-rev [& [old-rev]]
  (if old-rev
    (let [num (util/parse-int (first (str/split old-rev #"\-")))]
      (str (inc num) "-" (uuid)))
    (str 1 "-" (uuid))))

(defn doc-update
  "id-mismatch, write-conflict, rev-mismatch, invalid-key, update"
  [doc body & [opts]]
  (if (not= (body "_id") (:id doc))
    {:id-mismatch true}
    (if (and (not (:delected doc))
             (:rev doc)
             (not= (:rev doc) (body "_rev")))
      {:rev-mismatch true} ; no write-conflict yet
      (let [err? (doc-validate-keys body)]
        (if (:invalid-key err?)
          err?
          (let [rev (if (:rev doc) (doc-new-rev (:rev doc))
                                   (or (body "_rev") (doc-new-rev)))
                deleted (body "_deleted")
                r {:info {:id (:id doc) :rev rev}}
                r (if (:seq doc) (assoc r :old-seq (:seq doc)) r)
                doc (assoc doc :rev rev :deleted deleted :body body)]
            {:update {:doc doc :r r}}))))))

(defn doc-new [docid body]
  (:doc (:update (doc-update {:id docid :conflicts []} body))))

(defn doc-find-rev [doc rev]
  (some
    (fn [doc] (and (= rev (:rev doc)) doc))
    (:conflicts doc)))

(defn doc-conflict-revs [doc]
  (map :rev (or (:conflicts doc))))

(defn doc-inflate [doc opts]
  (if (and (:rev opts) (not= (:rev opts) (:rev doc)))
    (doc-inflate (doc-find-rev doc (:rev opts)))
    (let [doci {"_rev" (:rev doc) "_id" (:id doc)}
          doci (if (:conflicts opts)
                 (assoc doci "_conflicts" (doc-conflict-revs doc))
                 doci)
          doci (merge (:body doc) doci)]
      doci)))

(defn doc-get
  "no-db, no-doc, del-doc, doc"
  [state dbid docid & [opts]]
  (if-not-let [db (get state dbid)]
    {:no-db true}
    (if-not-let [doc (get (:by-docid db) docid)]
      {:no-doc true}
      (if (:deleted doc)
        {:del-doc true}
        {:doc (doc-inflate doc opts)}))))

(defn doc-put
  "no-db, bad-doc, doc"
  [state dbid docid body & [opts]]
  (if-not-let [db (get state dbid)]
    [nil {:no-db true}]
    (let [new-seq (inc (:seq db))]
      (if-let [doc (get-in db [:by-docid docid])]
        (let [res (doc-update doc body opts)]
          (if-not-let [update (:update res)]
            [nil {:bad-doc true}]
            (let [doc (:doc update)
                  doc (assoc doc :seq new-seq)
                  db (if-let [old-seq (:old-seq (:r update))]
                       (update db :by-seq assoc old-seq nil)
                       db)
                  db (assoc db :seq new-seq)
                  db (update db :by-seq assoc new-seq (:info (:r update)))]
              [db doc])))
        (let [doc (doc-new docid body)
              doc (assoc doc :seq new-seq)
              db (assoc db :seq new-seq)
              db (update db :doc-count inc)
              db (update db :by-docid assoc docid doc)
              db (update db :by-seq assoc new-seq
                   {:id docid :rev (:rev doc)})]
          [db doc])))))

(def doc-put! (set-fn doc-put))

(defn doc-post
  "no-db, bad-doc, doc"
  [state dbid body]
  (let [docid (or (get body "_id") (uuid))
        body (assoc body "_id" docid)]
    (doc-put state dbid docid body)))

(def doc-post! (set-fn doc-post))

(defn doc-delete
  "no-db, no-doc, doc"
  [state dbid docid rev]
  (let [body {"_id" docid "_rev" rev "_deleted" true}
        [db doc] (doc-put state dbid docid body)
        db (update db :doc-count dec)]
    [db doc]))

(def doc-delete! (set-fn doc-delete))
