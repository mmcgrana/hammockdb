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
