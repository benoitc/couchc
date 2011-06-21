# couchc

Simple CouchDB API internal wrapper.


## Build

    $ export COUCHDB_SRC=/path/to/sources/src/couchdb
    $ erlc -I $COUCHDB_SRC couchc.erl

## Test it

    $ ERL_FLAGS="-pa /path/to/couchc" ./util/run -i
    $ couchc_test:all().


