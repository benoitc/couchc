%%% -*- erlang -*-
%%%
%%% This file is part of couchcc released under the MIT license. 
%%% See the NOTICE for more information.

-type db_option() :: {user_ctx, #user_ctx{}} | sys_db.
-type db_options() :: list(db_option()).

-type db_name() :: binary() | string().
-type docid() :: binary() | string().

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

-type doc_option() :: {revs, list(binary())} | revs_infos | conflict |
deleted_conflicts | local_seq | latest | att_encoding_info| {atts_since, RevsList::list(binary())} | attachments.

-type doc_options() :: list(doc_option()).

-type update_type() :: replicated_changes | interactive_edit.

-type update_option() :: {update_type, update_type()} | full_commit | delay_commit.

-type update_options() :: list(update_option).

-type copy_option() :: {rev, binary()} | {target_rev, binary()}.
-type copy_options() :: list(copy_option()).


-type view_option() :: {keys, list(binary())}
    | {query_args, #view_query_args{}}.

-type view_options() :: list(view_option()).

-type attachment_options() :: list(doc_options
    | {ranges, list({From::integer(), To::integer()})}
    | {accepted_encoding, list(gzip | encoding)}).

-type body() :: iolist() | function().

-record(cdb, {
        name,
        options :: db_options()}).

-type db() :: #cdb{}.
