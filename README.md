# couchc

Simple CouchDB API internal wrapper. 

This library wrap CouchDB intenal API in simple call to help you in
building CouchDB plugings.

This api is compatible with CouchDb 1.1.x and trunk (1.2.x) .

## Build

    $ export COUCHDB_SRC=/path/to/sources/src/couchdb
    $ erlc -I $COUCHDB_SRC *.erl

Note: Move it in couchdb beam folder if you want to use it easily.

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
    


