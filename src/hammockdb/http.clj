(ns hammockdb.http
  (:use compojure.core)
  (:use ring.middleware.json-params)
  (:use ring.middleware.stacktrace)
  (:use ring.middleware.reload)
  (:require [clj-json.core :as json])
  (:require [hammockdb.data :as data])
  (:require [hammockdb.state :as state]))

; utilities
(defn parse-int [int-str]
  (Integer/parseInt int-str))

; json requests and responses

(defn jbody? [data]
  (and data (not (vector? data))))

(defn jr [status data & [headers]]
  {:status status
   :headers (merge headers {"Content-Type" "application/json"})
   :body (json/generate-string data)})

(defn je [status error reason]
  (jr status {"error" error "reason" reason}))

(defn je-no-db [dbid]
  (je 404 "not_found" (format "No database: %s" dbid)))

(defn je-no-doc [docid]
  (je 404 "not_found" "No doc with id: " docid))

; http api
(defroutes handler

  ; welcome
  (GET "/" []
    (jr 200 {"couchdb" "Welcome" "version" "0"}))

  ; config stubs
  (POST "/:db/_ensure_full_commit" [] (jr 200 {"ok" true}))
  (POST "/_restart" [] (jr 200 {"ok" true}))
  (PUT "/_config/*" [] (jr 200 {"ok" true}))
  (GET "/_config/*" [] (jr 200 {"ok" true}))

  ; uuid service
  (GET "/_uuids" {{c "count"} :params}
    (let [c (parse-int (or c "1"))
          etag (data/uuid)
          uuids (data/uuids c)]
      (jr 200 {"uuids" uuids}
        {"Cache-Control" "no-cache"
         "Pragma" "no-cache"
         "Etag" etag})))

  ; list dbs
  (GET "/_all_dbs" []
    (let [{:keys [dbids] (data/db-index state/state)}]
      (jr 200 dbids)))

  ; create db
  (PUT "/:dbid" [dbid]
    (let [{:keys [existing-db db]} (data/db-create state/state dbid)]
      (cond
        existing-db (je 412 "db_exists" "The database already exists")
        db (jr 201 {"ok" true} {"Location" (format "/%s" dbid)}))))

  ; get db info
  (GET "/:dbid" [dbid]
    (let [{:keys [no-db db]} (data/db-get state/state dbid)]
      (cond
        no-db (je-no-db dbid)
        db (jr 200 dbinfo))))

  ; delete db
  (DELETE "/:dbid" [dbid]
    (let [{:keys [no-db ok]} (data/db-delete state/state dbid)]
      (cond
        no-db (jr 200 {"ok" true})
        ok (je-no-db dbid))))

  ; create db docs
  (POST "/:dbid/_bulk_docs" {{:strs [dbid docs all-or-nothing]} :params}
    (let [{:keys [no-db results]}
          (data/db-bulk-update state dbid docs all-or-nothing)]
      (cond
        no-db (je-no-db dbid)
        results (jr 200 results))))

  ;; view db query results
  ;(GET "/:db/_all_docs" [db]
  ;  (if-let [db (get-in @state [:dbs db])]
  ;   (let [rows (map
  ;                (fn [docid doc]
  ;                  {"id" docid "key" docid "value" {"rev" (get doc "rev")}})
  ;                (data/doc-all (view/with-params params)))]
  ;    (jr 200 {"rows" rows "total_rows" (data/db-doc-count db)}))
  ;  (je 404 "not_found" (str "No database: " db))))
  ;
  ;; database changes feed
  ;(GET "/:db/_changes" [db]
  ;  (je 500 "not_implemented" "Getting there"))

  ; get doc
  (GET "/:dbid/:docid" {{:strs [dbid docid rev attachements conflicts]}}
    (let [{:keys [no-db no-doc del-doc doc]} (data/db-doc-get state dbid docid
                                               {:rev rev
                                                :attachements attachements
                                                :conflicts conflicts})]
      (cond
        no-db   (je-no-db dbid)
        no-doc  (je-no-doc docid)
        del-doc (je 404 "not_found" (format "Deleted doc with id: %s" docid))
        doc     (jr 200 doc))))

  ; create unkeyed doc
  (POST "/:dbid" {{doc :json-params dbid "dbid"} :params}
    (let [{:keys [no-db bad-doc doc]} (data/db-doc-post state dbid doc)]
      (cond
        no-db (je-no-db dbid)
        bad-doc (je 400 "bad_request" "Request body must be a JSON object")
        doc (jr 201 (assoc doc "ok" true)
                    {"Location" (format "/%s/%s" dbid (doc "_id"))}))))

  ;; created keyed doc or update doc
  ;(PUT "/:db/:docid" [db docid]
  ;  (if-let [db (get-in @state [:dbs db])]
  ;    (let [doc json-params
  ;          doc (assoc doc "_id" docid)
  ;          resp (data/db-put state doc)
  ;          resp (assoc resp "ok" true)]
  ;      (jr 201 resp {"Location" (format "/%s/%s" db docid)}))
  ;    (je 404 "not_found" (str "no database: " db))
  ;
  ;; delete doc
  ;(DELETE "/:db/:docid" [{{:strs [db docid rev]} :params}]
  ;  (if-let [db (get-in @state [:dbs db])]
  ;    (if-let [doc (data/doc-get state db docid)]
  ;      (let [new_rev (data/dec-del state db docid)]
  ;        (jr 200 {"ok" true "id" docid "rev" new_rev}))
  ;      (3 404 "not_found" (str "No doc with id: " docid)))
  ;    (je 404 "not_found" (str "no database: " db))))
)

(defn wrap-internal-error [handler]
  (fn [req]
    (try
      (handler req)
      (catch Exception e
        (je 500 "internal_error" (.getMessage e))))))

(defn wrap-request-log [handler]
  (fn [req]
    (println (format "%s %s" (.toUpperCase (name (:request-method req)))
                             (:uri req)))
    (handler req)))

(def app
  (-> #'handler
    wrap-json-params
    wrap-stacktrace-log
    wrap-internal-error
    wrap-request-log
    (wrap-reload '(hammockdb.http hammockdb.data))))
