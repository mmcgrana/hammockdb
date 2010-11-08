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
   :by-seq (sorted-map)})

(defn db-list
  ":dbids"
  [state]
  {:dbids (or (keys state) [])})

(defn db-inflate [db dbid]
  {"db_name" dbid
   "update_seq" (:seq db)
   "doc_count" (:doc-count db)})

(defn db-get
  ":no-db, :db"
  [state dbid]
  (if-not-let [db (get state dbid)]
    {:no-db true}
    {:db (db-inflate db dbid)}))

(defn db-put
  ":existing-db, :db"
  [state dbid]
  (if (get state dbid)
    [{:existing-db true} nil]
    (let [db (db-new dbid)]
      [{:db db} (assoc state dbid db)])))

(def db-put! (set-fn db-put))

(defn db-delete
  ":no-db, :ok"
  [state dbid]
  (if-not-let [db (get state dbid)]
    [{:no-db true} nil]
    [{:ok true} (dissoc state dbid)]))

(def db-delete! (set-fn db-delete))

(defn db-change-item [db [seq info] include-doc]
  (let [docid (:id info)
        item {"seq" seq "id" docid "changes" [{"rev" (:rev info)}]}
        item (if (:deleted item)
               (assoc item "deleted" true)
               item)
        item (if include-doc
               (assoc item "doc" (get-in db [:by-docid docid]))
               item)]
    item))

(defn db-changes
  ":no-db, :changes"
  [state dbid idocs since]
  (if-not-let [db (get state dbid)]
    {:no-db true}
    (let [last-seq (:seq db)
          index (:by-seq db)
          elems (if since (subseq index > since) index)
          results (map #(db-change-item db % idocs) elems)]
      {:changes
        {"results" results
         "last_seq" last-seq}})))

(defn call-once [f]
  (let [called (atom false)]
    (fn [& args]
      (if (compare-and-set! called false true)
        (try
          (apply f args)
          (catch Exception e
            (.printStackTrace e)))))))

(defn db-changes-subscribe [ident sid f]
  (add-watch ident sid (fn [_ _ _ state] (f state))))

(defn db-changes-unsubscribe [ident sid]
  (remove-watch ident sid))

(defn db-changes-subscribe-longpoll
  ":no-db, :changes. called no more than once."
  [ident dbid idocs since lpid db-emit]
  (let [db-emit-once (call-once db-emit)
        callback
          (fn [state]
            (if-not-let [db (get state dbid)]
              (do
                (db-changes-unsubscribe ident lpid)
                (db-emit-once {:type :no-db}))
              (when (> (:seq db) since)
                (db-changes-unsubscribe ident lpid)
                (let [ret (db-changes state dbid idocs since)]
                  (db-emit-once {:type :changes :data (:changes ret)})))))]
    (db-changes-subscribe ident lpid callback)
    (callback @ident)))

(defn doc-get
  ":no-db, :no-doc, :doc"
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
  ":conflict, :update"
  [doc new-doc]
  (let [rev (get doc "_rev")
        check-rev (get new-doc "_rev")]
    (if (and rev (not= rev check-rev))
      {:conflict true}
      (let [new-rev (if rev (doc-new-rev rev) (doc-new-rev))
            info {:id (get doc "_id") :rev new-rev}
            new-doc (assoc new-doc "_rev" new-rev)]
        {:update {:doc new-doc :info info}}))))

(defn doc-put
  ":no-db, :doc"
  [state dbid docid new-doc & [rev]]
  (if-not-let [db (get state dbid)]
    [{:no-db true} nil]
    (let [new-seq (inc (:seq db))]
      (if-let [doc (get-in db [:by-docid docid])]
        (let [res (doc-update doc new-doc)]
          (if-not-let [update (:update res)]
            [res nil]
            (let [db (assoc db :seq new-seq)
                  db (update-at db :by-docid assoc docid (:doc update))
                  db (update-at db :by-seq assoc new-seq (:info update))]
              [doc-update (assoc state dbid db)])))
        (let [update (:update (doc-update {"_id" docid}
                                          (assoc new-doc "_id" docid)))]
          (let [db (assoc db :seq new-seq)
                db (update-at db :doc-count inc)
                db (update-at db :by-docid assoc docid (:doc update))
                db (update-at db :by-seq assoc new-seq (:info update))]
            [{:doc (:doc update)} (assoc state dbid db)]))))))

(def doc-put! (set-fn doc-put))

(defn doc-post
  ":no-db, :doc"
  [state dbid doc]
  (doc-put state dbid (uuid) doc))

(def doc-post! (set-fn doc-post))

(defn doc-delete
  ":no-db, :no-doc, :conflict, :doc"
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
