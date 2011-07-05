# couchc

Simple CouchDB API internal wrapper. 

This library wrap CouchDB intenal API in simple call to help you in
building CouchDB plugings.

This api is compatible with CouchDb 1.1.x and trunk (1.2.x) .

## Build

    $ make COUCHDB_SRC=/path/to/sources/src/couchdb

Note: Move it in couchdb beam folder if you want to use it easily.

## Getting started

### Create a database

    Options = [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}],
    {ok, Db} = couchc:create_db("couchc_testdb", Options).


### Save a document

    Doc = {[{<<"_id">>, <<"doc1">>}, {<<"v">>, 1}]},
    {pk, DocID, DocRev} =  couchc:save_doc(Db, Doc).

### Open a document

    {ok, Doc1} = couchc:open_doc(Db, DocId).

### Delete a document

    {ok, DocID, NewDocRev} = couchc:delete_doc(Doc, {DocId, DocRev}).

### Save multiple documents

    Docs = [
            {[{<<"_id">>, <<"a">>}, {<<"v">>, 1}, {<<"t">>, <<"test">>}]},
            {[{<<"_id">>, <<"b">>}, {<<"v">>, 2}, {<<"t">>, <<"test">>}]}],
    Results = couchc:save_docs(Db, Docs),

### Get all documents

    {ok, {TotalRowsCount, Offset, Results1}} = couchc:all(Db).

### Views

    DesignDoc = {[
            {<<"_id">>, <<"_design/test">>},
            {<<"language">>,<<"javascript">>},
            {<<"views">>,
                {[{<<"v1">>,
                    {[{<<"map">>,
                        <<"function (doc) {\n if (doc.t == \"test\") {\n emit(doc._id, doc);\n}\n}">>
                    }]}
        }]}}]},
    
    {ok, _, _} = couchc:save_doc(Db, DesignDoc),
    {ok, {TotalRowsCount, Offset, Results1}} = couchc:all(Db, {<<"test">>, <<"v1">>})

This function get all documents in the view. You can use fold instead of
all to fold documents. the all function is written like this:


    collect_results(Row, Acc) ->
        {ok, [Row | Acc]}.

    all(Db, ViewName, Options) ->
        fold(Db, ViewName, fun collect_results/2, Options).

That's it. More documentation is coming soon.

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
    


