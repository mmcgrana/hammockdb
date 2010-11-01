# HammockDB

An Implementation of the [CouchDB](http://couchdb.apache.org/) [HTTP API](http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference) in Clojure, inspired by the Ruby equivalent [Booth](http://github.com/jchris/booth). 

## Why

Clojure's support for immutable data structures, functional programming, and sane concurrency align perfectly with CouchDB's design and enable a strait-forward and elegant HammockDB implementation.

HammockDB also serves as a good, non-trivial example of a data-based web application composed with [Ring](http://github.com/mmcgrana/ring) and taking advantage of that library's synchronous and asynchronous operation modes.

The name "HammockDB" comes from Rich Hickey's talk at the first [clojure-conj](http://clojure-conj.org), where he emphasized the importance of hard thinking on hard problems and suggested that hammocks might facilitate such thinking.

## Usage

    $ lein deps
    $ lein run -m hammockdb.server
    $ curl http://localhost:5984/_all_dbs 

## Testing

HammockDB has some unit tests:

    $ lein test

HammockDB also tries to pass as much of the [Futon](http://wiki.apache.org/couchdb/Getting_started_with_Futon) browser test suite as possible:

    $ ???
