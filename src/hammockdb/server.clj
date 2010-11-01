(ns hammockdb.server
  (:use ring.adapter.jetty-async))

(defn -main [& args]
  (run-jetty-async app {:port 5984}))
