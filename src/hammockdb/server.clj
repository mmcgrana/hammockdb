(ns hammockdb.server
  (:require [hammockdb.http :as http])
  (:use ring.adapter.jetty-async))

(defn -main [& args]
  (let [s (run-jetty-async http/app {:port 5984 :join? false})]
    (println "Hammock time.")
    (.join s)))
