(ns hammockdb.data
  (:import java.util.UUID))

(defn uuid []
  (str (UUID/randomUUID)))

(defn uuids [c]
  (take c (repeatedly uuid)))

(defn db-index
  "Returns a seq of dbids."
  [state]
  (or (keys (:dbs @state)) []))

(defn- db-new [dbid]
  {})

(defn db-create
  "Create a new databasee. Returns true if a database was created, false
   if a database already existed with the given dbid."
   [state dbid]
    (if (get-in @state [:dbs dbid])
      false
      (do
        (swap! state assoc-in [:dbs dbid] (db-new dbid))
        true)))

(defn- db-meta [db dbid]
  {"db_name" dbid
   "doc_count" (count db)
   "doc_size" (* 339.2 (count db))})

(defn db-get
  "Returns metadata about the named db if it exists, or nil otherwise."
  [state dbid]
  (if-let [db (get-in @state [:dbs dbid])]
    (db-meta db dbid)))

(defn db-delete
  "Deletes the named db. Returns true if deleted, false if db did not exist."
  [state dbid]
  (if-let [db (get-in @state [:dbs dbid])]
    (do
      (swap! state dissoc :dbs dbid)
      true)
    false))

(defn db-doc-bulk-update
  ""
  [state docs all-or-nothing]
   (let [docs (get params "docs")
            all-or-nothing (contains? params "all-or-nothing")
            results (map
                      (fn [doc] (data/doc-put state doc))
                      docs)]))

(defn- db-doc-inflate [raw-doc opts]
  raw-doc)

(defn db-doc-get [state dbid docid & [opts]]
  (if-not-let [db (get-in @state [:dbs dbid])]
    {:no-db true}
    (if-not-let [raw-doc (get db docid)]
      {:no-doc true}
      (if (:deleted raw-doc)
        {:del-doc true}
        (db-doc-inflate raw-doc opts)))))

(defn db-doc-get* [db])

(defn db-doc-put [state dbid jdoc & [opts]]
  (if-not-let [db (get-in @state [:dbs dbid])]
    {:no-db true}
    (if-let [doc (db-doc-get* db (jdoc "_id"))]
      (let [doc2 (db-doc-update )]))))

(defn db-doc-post [state dbid doc]
  (let doc [(contains? doc "_id") doc (assoc doc "_id" (uuid))]
    (db-doc-put state dbid doc)))
