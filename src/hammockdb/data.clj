(ns hammockdb.data
  (:import java.util.UUID))

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

(defn db-create* [state* dbid]
  (if (get-in state* [:dbids dbid])
    [nil {:existing-db true}]
    (let [db (db-new dbid)]
      [(assoc-in state* [:dbs dbid] db) {:db db}])))

(defn db-create
  "existing-db, db"
  [state dbid]
  (let [state* @astate
        [state** ret] (db-create* @state dbid)]
    (when state**
      (assert (compare-and-set! state state* state**)))
    ret))

(defn- db-meta [db dbid]
  {"db_name" dbid
   "doc_count" (count db)
   "doc_size" (* 339.2 (count db))})

(defn db-get* [state* dbid]
  (if-not-let [db (get-in state* [:dbs dbid])]
    {:no-db true}
    {:db (db-meta db dbid)}))

(defn db-get
  "no-db, db"
  [state dbid]
  (db-get* @state dbid))

(defn db-delete* [state* dbid]
  (if-not-let [db (get-in state* [:dbs dbid])]
    [nil {:no-db true}]
    [(dissoc state* :dbs dbid) {:ok true}]))

(defn db-delete
  "no-db, ok"
  [state dbid]
  (let [state* @astate
        [state** ret] (db-delete* @state dbid)]
    (when state**
      (assert (compare-and-set! state state* state**)))
    ret))

(defn- doc-inflate [raw-doc opts]
  raw-doc)

(defn doc-get [state dbid docid & [opts]]
  (if-not-let [db (get-in @state [:dbs dbid])]
    {:no-db true}
    (if-not-let [doc (get db docid)]
      {:no-doc true}
      (if (:deleted doc)
        {:del-doc true}
        {:doc (doc-inflate doc opts)}))))

(defn doc-put
  "no-db, bad-doc, doc"
  [state dbid docid doc & [opts]]
  (if-not-let [db (get-in @state [:dbs dbid])]
    {:no-db true}

(defn doc-post
  "no-db, bad-doc, doc"
  [state dbid doc]
  (let [docid (or (get doc "_id") (uuid))
        doc (assoc doc "_id" docid)]
    (doc-put state dbid docid doc)))

(defn doc-delete
  "no-db, no-doc, doc"
  [state dbid docid rev]
  (if-not-let [db (get-in @state [:dbs dbid])]
    {:no-db true}
    (if-not-let [doc (get db docid)]
      {:no-doc true}
      ())))
