(ns hammockdb.util-test
  (:require [hammockdb.util :as util])
  (:use clojure.test))

(deftest test-parse-int
  (is (= 3 (util/parse-int "3"))))

(deftest test-switch
  (is (= "FOO!"
    (util/switch {:foo "FOO"}
      bar "bar"
      foo (str foo "!")))))

(deftest test-update
  (is (= {:foo 2} (util/update {:foo 1} :foo inc))))

(deftest test-if-not-let
  (is (= :yes (util/if-not-let [foo "f"] :no :yes)))
  (is (= :no  (util/if-not-let [foo nil] :no :yes))))
