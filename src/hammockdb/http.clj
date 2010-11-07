(ns hammockdb.http
  (:use compojure.core)
  (:use ring.middleware.json-params)
  (:use ring.middleware.stacktrace)
  (:use ring.middleware.reload)
  (:use [hammockdb.util :only (switch)])
  (:require [clj-json.core :as json])
  (:require [hammockdb.util :as util])
  (:require [hammockdb.data :as data]))

; server identity
(defonce ident (data/ident-new))

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
  (je 404 "not_found" (format "No doc with id: %s" docid)))

(defn je-bad-doc []
  (je 400 "bad_request" "Request body must be a JSON object"))

; http api
(defroutes handler
  (GET "/" []
    (jr 200 {"couchdb" "Welcome" "version" "0"}))

  ; config stubs
  (POST "/:db/_ensure_full_commit" [] (jr 200 {"ok" true}))
  (PUT "/_config/*" [] (jr 200 {"ok" true}))
  (GET "/_config/*" [] (jr 200 {"ok" true}))

  ; abuse restart to clear state
  (POST "/_restart" []
    (swap! ident (constantly (data/state-new)))
    (jr 200 {"ok" true}))

  ; uuid service
  (GET "/_uuids" {{c "count"} :params}
    (let [c (util/parse-int (or c "1"))]
      (jr 200 {"uuids" (data/uuids c)}
        {"Cache-Control" "no-cache"
         "Pragma" "no-cache"
         "Etag" (data/uuid)})))

  ; list dbs
  (GET "/_all_dbs" []
    (switch (data/db-list @ident)
      dbids (jr 200 dbids)))

 ; get db info
  (GET "/:dbid" [dbid]
    (switch (data/db-get @ident dbid)
      no-db (je-no-db dbid)
      db (jr 200 db)))

  ; create db
  (PUT "/:dbid" [dbid]
    (switch (data/db-put! ident dbid)
      existing-db (je 412 "db_exists" "The database already exists")
      db (jr 201 {"ok" true})))

  ; delete db
  (DELETE "/:dbid" [dbid]
    (switch (data/db-delete! ident dbid)
      no-db (je-no-db dbid)
      ok (jr 200 {"ok" true})))

  ; changes feed
  (GET "/:dbid/_changes" [dbid]
    (switch (data/db-changes @ident dbid)
      no-db (je-no-db dbid)
      changes (jr 200 changes)))

  ; get doc
  (GET "/:dbid/:docid" {{:strs [dbid docid]} :params}
    (switch (data/doc-get @ident dbid docid)
      no-db   (je-no-db dbid)
      no-doc  (je-no-doc docid)
      doc     (jr 200 doc)))

  ; create unkeyed doc
  (POST "/:dbid/" {new-doc :json-params {dbid "dbid"} :params}
    (switch (data/doc-post! ident dbid new-doc)
      no-db (je-no-db dbid)
      bad-doc (je-bad-doc)
      doc (jr 201 {"ok" true "id" (get doc "_id") "rev" (get doc "_rev")})))

  ; created keyed doc or update doc
  (PUT "/:dbid/:docid" {new-doc :json-params
                        {dbid "dbid" docid "docid" rev "rev"} :params}
    (switch (data/doc-put! ident dbid docid new-doc rev)
      no-db (je-no-db dbid)
      bad-doc (je-bad-doc)
      conflict (je 408 "conflict" "Document update conflict")
      doc (jr 201 {"ok" true "id" (get doc "_id") "rev" (get doc "_rev")})))

  ; delete doc
  (DELETE "/:dbid/:docid" {{:strs [dbid docid rev]} :params}
    (switch (data/doc-delete! ident dbid docid rev)
      no-db (je-no-db dbid)
      no-doc (je-no-doc docid)
      conflict (je 408 "confilct" "Document deletion conflict.")
      doc (jr 200 {"ok" true "rev" (get doc "_rev")}))))

; middlewares
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

; http service
(def app
  (-> #'handler
    wrap-json-params
    wrap-stacktrace-log
    wrap-internal-error
    wrap-request-log
    (wrap-reload '(hammockdb.data hammockdb.http))))
