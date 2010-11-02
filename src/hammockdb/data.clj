(ns hammockdb.data
  (:import java.util.UUID))

(defn write-fn [pure-write]
  (fn [state & args]
    (let [state* @astate
          [state** ret] (apply write-fn @state args)]
      (when state**
        (assert (compare-and-set! state state* state**)))
      ret)))

(defn read-fn [pure-read]
  (fn [state & args]
    (apply pure-read @state args)))

(defn uuid []
  (str (UUID/randomUUID)))

(defn uuids [c]
  (take c (repeatedly uuid)))

(defn db-index
  "dbids"
  [state]
  {:dbids (or (keys (:dbs @state)) [])})

(defn db-new [dbid]
  {})

(defn db-create*
  "existing-db, db"
  [state* dbid]
  (if (get-in state* [:dbids dbid])
    [nil {:existing-db true}]
    (let [db (db-new dbid)]
      [(assoc-in state* [:dbs dbid] db) {:db db}])))

(def db-create (write-fn db-create*))

(defn db-inflate [db dbid]
  {"db_name" dbid
   "doc_count" (count db)
   "doc_size" (* 339.2 (count db))})

(defn db-get*
  "no-db, db"
  [state* dbid]
  (if-not-let [db (get-in state* [:dbs dbid])]
    {:no-db true}
    {:db (db-inflate db dbid)}))

(def db-get (read-fn db-get*))

(defn db-delete*
  "no-db, ok"
  [state* dbid]
  (if-not-let [db (get-in state* [:dbs dbid])]
    [nil {:no-db true}]
    [(dissoc state* :dbs dbid) {:ok true}]))

(def db-delete (write-fn db-delete*))

(defn doc-inflate [raw-doc opts]
  raw-doc)

(defn doc-get*
  "no-db, no-doc, del-doc, doc"
  [state* dbid doc-id & [opts]]
  (if-not-let [db (get-in @state [:dbs dbid])]
    {:no-db true}
    (if-not-let [doc (get db docid)]
      {:no-doc true}
      (if (:deleted doc)
        {:del-doc true}
        {:doc (doc-inflate doc opts)}))))

(def doc-get (read-fn doc-get*))

(defn doc-put*
  "no-db, bad-doc, doc"
  [state* dbid docid doc & [opts]]
  (if-not-let [db (get-in state* [:dbs dbid])]
    {:no-db true}))

(def doc-put (write-fn doc-put*))

(defn doc-post*
  "no-db, bad-doc, doc"
  [state* dbid doc]
  (let [docid (or (get doc "_id") (uuid))
        doc (assoc doc "_id" docid)]
    (doc-put* state* dbid docid doc)))

(def doc-post (write-fn doc-post*))

(defn doc-delete*
  "no-db, no-doc, doc"
  [state* dbid docid rev]
  (if-not-let [db (get-in state* [:dbs dbid])]
    {:no-db true}
    (if-not-let [doc (get db docid)]
      {:no-doc true}
      ())))

(def doc-delete (write-fn doc-delete*))
