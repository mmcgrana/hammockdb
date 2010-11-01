(ns hammocdb.web
  (:use compojure.core)
  (:use ring.middleware.json-params)
  (:use ring.middleware.stacktrace))

; state
(def state
  (atom {}))

; utilities
(defn parse-int [int-str]
  (Integer/parseInt int-str))

(defn uuid-gen []
  ...)

; json response handling
(defn jr [status data & [headers]]
  {:status status
   :headers (merge headers {"Content-Type" "application/json"})
   :body (json/generate-string data)})

(defn je [status error reason]
  (jr status {"error" error "reason" reason}))


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
  (GET "/_uuids" [{p :params}]
    (let [c (parse-int (or (:count p) "1"))
          uuids (take c (repeatedly uuid-gen))]
      (jr 200 {"uuids" uuids}
        {"Cache-Control" "no-cache"
         "Pragma" "no-cache"
         "Etag" (uuid-gen)})))

  ; list dbs
  (GET "/_all_dbs" []
    (jr 200 (keys (:dbs @state))))

  ; create db
  (PUT "/:db" [db]
    ...)

  ; get db info
  (GET "/:db" [db]
    ...)

  ; delete db
  (DELETE "/:db" [db]
    ...)

  ; create db docs
  (POST "/:db/_bulk_docs" [db]
    ...)

  ; view db query results
  (GET "/:db/_all_docs" [db]
    ...)

  ; create db queries
  (POST "/:db/_all_docs" [db]
    ...)

  ; database changes feed
  (GET "/:db/_changes" [db]
    ...)

  ; get doc
  (GET "/:db/:docid" [db docid]
    ...)

  ; create unkeyed doc
  (POST "/:db" [db]
    ...)

  ; created keyed doc
  (PUT "/:db/:docid" [db docid]
    ...)

  ; delete doc
  (DELETE "/:db/:docid" [db docid]
    ...)

  ; run temp view
  (POST "/:db/_temp_view" [db]
    ...))

(def app
  (-> handler
    wrap-json-params
    wrap-stacktrace))

(defn -main [& args]
  (run-jetty-async app {:port 5984}))
