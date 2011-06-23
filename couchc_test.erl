 -module(test_couchc).
 -compile(export_all).
 -include_lib("eunit/include/eunit.hrl").

-include("couch_db.hrl").

createdb_test() ->
    {ok, Db} = couchc:create_db("couchc_testdb"),
    {ok, Info} = couchc:db_info(Db),
    ?assert(proplists:get_value(db_name, Info) == <<"couchc_testdb">>),
    {ok, Db1} = couchc:open_db("couchc_testdb"),
    {ok, Info1} = couchc:db_info(Db1),
    ?assert(proplists:get_value(db_name, Info1) == <<"couchc_testdb">>),
    ok = couchc:delete_db("couchc_testdb"),
    Result = couchc:open_db("couchc_testdb"),
    ?assert(Result == {error,{not_found,no_db_file}}).
