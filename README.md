# couchc

Simple CouchDB API internal wrapper.


## Build

    $ export COUCHDB_SRC=/path/to/sources/src/couchdb
    $ erlc -I $COUCHDB_SRC *.erl

## Test it

    $ ERL_FLAGS="-pa /path/to/couchc" ./util/run -i
    
    Eshell V5.8.4  (abort with ^G)
    1> Apache CouchDB 1.2.0a-0cd6405-git (LogLevel=info) is starting.
    Apache CouchDB has started. Time to relax.
    [info] [<0.36.0>] Apache CouchDB has started on http://127.0.0.1:5984/

    1> couchc_test:test().
    [info] [<0.214.0>] checkpointing view update at seq 2 for couchc_testdb _design/test
    [info] [<0.214.0>] checkpointing view update at seq 3 for couchc_testdb _design/test
    [info] [<0.214.0>] checkpointing view update at seq 4 for couchc_testdb _design/test
    [info] [<0.214.0>] Shutting down view group server, monitored db is closing.
      All 5 tests passed.
    ok
    2> 
    


