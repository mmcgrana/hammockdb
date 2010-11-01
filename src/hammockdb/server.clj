(ns hammockdb.server
  (:require [hammockdb.http :as http])
  (:use ring.adapter.jetty-async))

(defn -main [& args]
  (run-jetty-async http/app {:port 5984}))
