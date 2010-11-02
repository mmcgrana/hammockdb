(ns hammockdb.data
  (:import java.util.UUID))

(defn write-fn [pure-write]
  (fn [state & args]
    (let [state* @state
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

(defn db-new [dbid]
  {:seq 0
   :doc_count
   :by_docid {}
   :by_seq {}})

(defn db-index*
  "dbids"
  [state*]
  {:dbids (or (keys state*) [])})

(def db-index (read-fn db-index))

(defn db-create*
  "existing-db, db"
  [state* dbid]
  (if (get state* dbid)
    [nil {:existing-db true}]
    (let [db (db-new dbid)]
      [(assoc state* dbid db) {:db db}])))

(def db-create (write-fn db-create*))

(defn db-inflate [db dbid]
  {"db_name" dbid
   "doc_count" (count db)
   "doc_size" (* 339.2 (count db))})

(defn db-get*
  "no-db, db"
  [state* dbid]
  (if-not-let [db (get state* dbid)]
    {:no-db true}
    {:db (db-inflate db dbid)}))

(def db-get (read-fn db-get*))

(defn db-delete*
  "no-db, ok"
  [state* dbid]
  (if-not-let [db (get state* dbid)]
    [nil {:no-db true}]
    [(dissoc state* dbid) {:ok true}]))

(def db-delete (write-fn db-delete*))

(defn doc-new [docid body]
  {:id docid
   :body body
   :conflicts []})

(defn doc-find-rev [doc rev]
  (some
    (fn [doc] (and (= rev (:rev doc)) doc))
    (:conflicts doc)))

(def doc-conflict-revs [doc]
  (map :rev (or (:conflicts doc))))

(defn doc-inflate [doc opts]
  (if (and (:rev opts) (not= (:rev opts) (:rev doc)))
    (doc-inflate (doc-find-rev doc (:rev opts)))
    (let [doci {"_rev" (:rev doc) "_id" (:id doc)}
          doci (if (:conflicts opts)
                 (assoc doci "_conflicts" (doc-conflict-revs doc))
                 doci)]
      (merge (:body doc) doci))))

(defn doc-get*
  "no-db, no-doc, del-doc, doc"
  [state* dbid docid & [opts]]
  (if-not-let [db (get state* dbid)]
    {:no-db true}
    (if-not-let [doc (get (:by-docid db) docid)]
      {:no-doc true}
      (if (:deleted doc)
        {:del-doc true}
        {:doc (doc-inflate doc opts)}))))

(def doc-get (read-fn doc-get*))

(defn doc-update
  "id-mismatch, write-conflict, rev-mismatch, bad-field, rev"
  [doc jdoc & [opts]]
  (if (or (not (get jdoc "_id")) (not= (get jdoc "_id") (get doc "_id")))
    {:id-mismatch true}
    (if (not (:deleted))))

(defn doc-put*
  "no-db, bad-doc, doc"
  [state* dbid docid body & [opts]]
  (if-not-let [db (get state* dbid)]
    {:no-db true}
    (if-let [doc (get (:by-docid db) docid)]
      ()
      (let [doc (assoc (doc-new docid body) :seq (inc (:seq db)))
            db (assoc db
                 :seq (inc (:seq db))
                 :doc-count (inc (:doc-count db))
                 :by-docid (assoc (:by-docid db) (:id doc) doc)
                 :by-seq (assoc (:by-seq db) (inc (:seq db))
                                             {:id (:id doc) :rev (:rev doc)}))]
         [(assoc state* dbid db) doc]))))

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
  (if-not-let [db (get state* dbid)]
    {:no-db true}
    (if-not-let [doc (get db docid)]
      {:no-doc true}
      ())))

(def doc-delete (write-fn doc-delete*))
