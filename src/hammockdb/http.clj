(ns hammockdb.http
  (:use compojure.core)
  (:use ring.middleware.json-params)
  (:use ring.middleware.stacktrace)
  (:use ring.middleware.reload)
  (:use [hammockdb.util :only (switch)])
  (:require [clj-json.core :as json])
  (:require [hammockdb.util :as util])
  (:require [hammockdb.data :as data])
  (:require [hammockdb.state :as state]))

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

(defn je-bad-doc []
  (je 400 "bad_request" "Request body must be a JSON object"))

; http api
(defroutes handler
  (GET "/" []
    (jr 200 {"couchdb" "Welcome" "version" "0"}))

  ; config stubs
  (POST "/:db/_ensure_full_commit" [] (jr 200 {"ok" true}))
  (POST "/_restart" [] (jr 200 {"ok" true}))
  (PUT "/_config/*" [] (jr 200 {"ok" true}))
  (GET "/_config/*" [] (jr 200 {"ok" true}))

  ; uuid service
  (GET "/_uuids" {{c "count"} :params}
    (let [c (util/parse-int (or c "1"))]
      (jr 200 {"uuids" (data/uuids c)}
        {"Cache-Control" "no-cache"
         "Pragma" "no-cache"
         "Etag" (data/uuid)})))

  ; list dbs
  (GET "/_all_dbs" []
    (switch (data/db-index state/state)
      dbids (jr 200 dbids)))

  ; create db
  (PUT "/:dbid" [dbid]
    (switch (data/db-create state/state dbid)
      existing-db (je 412 "db_exists" "The database already exists")
      db (jr 201 {"ok" true} {"Location" (format "/%s" dbid)})))

  ; get db info
  (GET "/:dbid" [dbid]
    (switch (data/db-get state/state dbid)
      no-db (je-no-db dbid)
      db (jr 200 dbinfo)))

  ; delete db
  (DELETE "/:dbid" [dbid]
    (switch (data/db-delete state/state dbid)
      no-db (jr 200 {"ok" true})
      ok (je-no-db dbid)))

  ; get doc
  (GET "/:dbid/:docid" {{:strs [dbid docid rev attachements conflicts]}}
    (switch (data/db-doc-get state dbid docid {:rev rev
                                               :attachements attachements
                                               :conflicts conflicts})
      no-db   (je-no-db dbid)
      no-doc  (je-no-doc docid)
      del-doc (je 404 "not_found" (format "Deleted doc with id: %s" docid))
      doc     (jr 200 doc)))

  ; create unkeyed doc
  (POST "/:dbid" {{doc :json-params dbid "dbid"} :params}
    (switch (data/db-doc-post state dbid doc)
      no-db (je-no-db dbid)
      bad-doc (je-bad-doc)
      doc (jr 201 (assoc doc "ok" true)
                  {"Location" (format "/%s/%s" dbid (doc "_id"))})))

  ; created keyed doc or update doc
  (PUT "/:dbid/:docid" {{body :json-params dbid "dbid" docid "docid"} :params}
    (switch (data/db-doc-put state dbid docid body)
      no-db (je-no-db dbid)
      bad-doc (je-bad-doc)
      doc (jr 201 doc {"Location" (format "/%s/%s" db docid)})))

  ; delete doc
  (DELETE "/:dbid/:docid" {{:strs [dbid docid rev]} :params}
    (switch (db-doc-delete state dbid docid rev)
      no-db (je-no-db dbid)
      no-doc (je-no-doc docid)
      doc (jr 200 (assoc doc "ok" true))))

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
    (wrap-reload '(hammockdb.http hammockdb.data))))
