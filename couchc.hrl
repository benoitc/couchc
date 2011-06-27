%%% -*- erlang -*-
%%%
%%% This file is part of couchcc released under the MIT license. 
%%% See the NOTICE for more information.

-type db_options() :: {user_ctx, #user_ctx{}} | sys_db.

-type ejson() :: ejson_object() | ejson_array().

-type ejson_array() :: [ejson_term()].
-type ejson_object() :: {[{ejson_key(), ejson_term()}]}.

-type ejson_key() :: binary() | atom().

-type ejson_term() :: ejson_array() 
    | ejson_object() 
    | ejson_string() 
    | ejson_number() 
    | true | false | null.

-type ejson_string() :: binary().

-type ejson_number() :: float() | integer().

-record(cdb, {
        name,
        options :: db_options()}).
