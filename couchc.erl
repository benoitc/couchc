%%% -*- erlang -*-
%%%
%%% This file is part of couchc released under the MIT license. 
%%% See the NOTICE for more information.

-module(couchc).

-include("couch_db.hrl").
-include("couchc.hrl").

-export([all_dbs/0, all_dbs/1, all_dbs/2,
         create_db/1, create_db/2, 
         delete_db/1, delete_db/2,
         open_db/1, open_db/2,
         open_or_create_db/1, open_or_create_db/2,
         db_info/1,
         purge_docs/2,
         db_set_security/2, db_get_security/1,
         db_set_revs_limit/2, db_get_revs_limit/1,
         db_missing_revs/2, db_revs_diff/2,
         compact/1, compact/2,
         view_cleanup/1,
         ensure_full_commit/1, ensure_full_commit/2,
         get_uuid/0, get_uuids/1,
         open_doc/2, open_doc/3, open_doc_rev/4,
         save_doc/2, save_doc/3,
         delete_doc/2, delete_doc/3,
         copy_doc/3, copy_doc/4,
         save_docs/2, save_docs/3,
         delete_docs/2, delete_docs/3,
         db_exec/2, db_admin_exec/2,
         all/1, all/2, all/3,
         fold/2, fold/3, fold/4,
         open_attachment/3, open_attachment/4,
         open_attachment_async/3, open_attachment_async/4,
         get_attachment_part/1, get_attachment_part/2,
         collect_attachment/1, collect_attachment/2,
         collect_attachment_ranges/1, collect_attachment_ranges/2,
         save_attachment/4, save_attachment/5,
         delete_attachment/3, delete_attachment/4]).

-spec get_uuid() -> binary().
%% @doc get a generated uuid
get_uuid() ->
    couch_uuids:new().

-spec get_uuids(Count::integer()) -> list(binary()).
%% @doc return a list of generated uuid
get_uuids(Count) ->
    [couch_uuids:new() || _ <- lists:seq(1, Count)].

-spec all_dbs() -> list(binary()).
%% @doc return list of database names
all_dbs() ->
    couch_server:all_databases().

-spec all_dbs(Fun::function()) -> iolist().
%% @equiv  all_dbs(Fun, [])
all_dbs(Fun) ->
    all_dbs(Fun, []).

-spec all_dbs(Fun::function(), Options::db_options()) -> iolist().
%% @doc fold list of database names in function Fun
%% ex:
%%      Fun = fun({PInfo} = Info, Acc) ->
%%          DbName = proplist:get(db_name, PInfo),
%%          [{DbName, Info}|Acc]
%%      end,
%%      all_dbs(Fun, [])
%% will return a list of tuple (DbName, DbInfo)
all_dbs(Fun, Options) ->
    {ok, DbNames} = couch_server:all_databases(),
    lists:foldl(fun(DbName, Acc) ->
        case couch_db:open(dbname(DbName), Options) of
            {ok, Db} ->
                {ok, Info} = couch_db:get_db_info(Db),
                NewAcc = Fun({Info}, Acc),
                couch_db:close(Db),
                NewAcc;
            _Error ->
                Acc
        end
    end, [], DbNames).

-spec create_db(DbName::db_name()) -> {ok, Db::db()}| {error, term()}.
%% @equiv create_db(DbName, [])
create_db(DbName) ->
    create_db(DbName, []).

-spec create_db(DbName::db_name(), Options::db_options()) -> {ok, Db::db()}| {error, term()}.
%% @doc create a database
create_db(DbName, Options) ->
    case couch_server:create(dbname(DbName), Options) of
        {ok, Db} ->
            couch_db:close(Db),
            {ok, #cdb{name=DbName, options=Options}};
        Error ->
            {error, Error}
    end.

-spec delete_db(DbName::db_name()) -> ok | {error, term()}.
%% @equiv delete_db(DbName, [])
delete_db(DbName) ->
    delete_db(DbName, []).

-spec delete_db(DbName::db_name(), Options::db_options()) -> ok| {error, term()}.
%% @doc delete a database
delete_db(DbName, Options) ->
    case couch_server:delete(dbname(DbName), Options) of
        ok ->
            ok;
        Error ->
            {error, Error}
    end.

-spec open_db(DbName::db_name()) -> {ok, Db::db()}| {error, term()}.
%% @equiv open_db(DbName, [])
open_db(DbName) ->
    open_db(DbName, []).

-spec open_db(DbName::db_name(), Options::db_options()) -> {ok, Db::db()}| {error, term()}.
%% @doc open an existing database
open_db(DbName, Options) ->
    case couch_db:open(dbname(DbName), Options) of
        {ok, Db} ->
            couch_db:close(Db),
            {ok, #cdb{name=DbName, options=Options}};
        Error ->
            {error, Error}
    end.


-spec open_or_create_db(DbName::db_name()) -> {ok, Db::db()}| {error, term()}.
    %% @equiv open_db(DbName, [])
open_or_create_db(DbName) ->
    open_or_create_db(DbName, []).

-spec open_or_create_db(DbName::db_name(), Options::db_options()) -> {ok, Db::db()}| {error, term()}.
%% @doc open or create a database
open_or_create_db(DbName, Options) ->
    case create_db(DbName, Options) of
        {ok, Db} ->
            Db;
        {error, file_exists} ->
            open_db(DbName, Options);
        Error ->
            Error
    end.

-spec db_info(Db::db()) -> {ok, ejson_object()}.
%% @doc return db info object
db_info(Db) ->
    db_exec(Db, fun(Db0) ->
                {ok, Info} = couch_db:get_db_info(Db0),
                %% we return a Json object
                {ok, {Info}}
        end).

-spec ensure_full_commit(Db::db()) -> {ok, StartTime::integer()} | {error, term()}.
%% @equiv ensure_full_commit(Db, [])
ensure_full_commit(Db) ->
    ensure_full_commit(Db, []).

-spec ensure_full_commit(Db::db(), Options::db_options()) -> {ok, StartTime::integer()} | {error, term()}.
%% @doc commit all docs in memory
ensure_full_commit(Db, Options) ->
    db_exec(Db, fun(Db0) ->
        UpdateSeq = couch_db:get_update_seq(Db0),
        CommittedSeq = couch_db:get_committed_update_seq(Db0),
        case proplists:get_value(seq, Options) of
            undefined ->
                couch_db:ensure_full_commit(Db0);
            RequiredSeq ->
                RequiredSeq0 = couch_util:to_integer(RequiredSeq),
                if 
                    RequiredSeq0 > UpdateSeq ->
                        {error, {bad_request, 
                                "can't do a full commit ahead of current update_seq"}};
                    RequiredSeq > CommittedSeq ->
                        couch_db:ensure_full_commit(Db0);
                    true ->
                        {ok, Db0#db.instance_start_time}
                end
        end
    end).

-spec purge_docs(Db::db(), IdsRevs::list({DocId::binary(),
            Rev::binary()})) -> {ok, PurgeSeq::integer(),
            PurgedIdsRevs2::list({DocId1::binary(),
                    Rev1::binary()})} | {error, term()}.
%% @doc purge a list of revision per documents
purge_docs(Db, IdsRevs) ->
    IdsRevs2 = parse_ids_revs(IdsRevs),
    db_exec(Db, fun(Db0) ->
        case couch_db:purge_docs(Db0, IdsRevs2) of
            {ok, PurgeSeq, PurgedIdsRevs} ->
                PurgedIdsRevs2 = [{Id, couch_doc:revs_to_strs(Revs)} || 
                    {Id, Revs} <- PurgedIdsRevs],
                {ok, PurgeSeq, PurgedIdsRevs2};
            Error ->
                {error, Error}
        end
    end).


-spec db_set_security(Db::db(), SecObj::ejson_object()) -> ok | {error,
        term()}.
%% @doc set database security object
db_set_security(Db, SecObj) ->
    db_admin_exec(Db, fun(Db0) ->
                try
                    couch_db:set_security(Db0, SecObj)
                catch
                    _:Error -> {error, Error}
                end
        end).

-spec db_get_security(Db::db()) -> ejson_object().
%% @doc return database security object
db_get_security(Db) ->
    db_exec(Db, fun(Db0) ->
                couch_db:get_security(Db0)
        end).

-spec db_set_revs_limit(Db::db(), Limit::integer()) -> ok | {error,
        term()}.
%% @doc set number of revision / doc to keep in database
db_set_revs_limit(Db, Limit) ->
    db_admin_exec(Db, fun(Db0) ->
                try
                    couch_db:set_revs_limit(Db0, Limit)
                catch
                    _:Error -> {error, Error}
                end
        end).

-spec db_get_revs_limit(Db::db()) -> integer().
%% @doc get number of revision / doc to keep in database
db_get_revs_limit(Db) ->
    db_exec(Db, fun(Db0) ->
                couch_db:get_revs_limit(Db0)
        end).

-spec db_missing_revs(Db::db(), IdsRevs::list({DocId::binary,
            DocRev::binary})) -> {ok, term()}.
%% @doc get missing revs
db_missing_revs(Db, IdsRevs) ->
    IdsRevs2 = parse_ids_revs(IdsRevs),
    db_exec(Db, fun(Db0) ->
        {ok, Results} = couch_db:get_missing_revs(Db0, IdsRevs2),
        Results2 = [{Id, couch_doc:revs_to_strs(Revs)} || {Id, Revs, _}
            <- Results],
        {ok, Results2}
    end).

-spec db_revs_diff(Db::db(), IdsRevs::list({DocId::binary,
            DocRev::binary})) -> {ok, term()}.
db_revs_diff(Db, IdsRevs) ->
    IdsRevs2 = parse_ids_revs(IdsRevs),
    db_exec(Db, fun(Db0) ->
        {ok, Results} = couch_db:get_missing_revs(Db0, IdsRevs2),
        Results2 = 
        lists:map(fun({Id, MissingRevs, PossibleAncestors}) ->
            {Id,
                {[{missing, couch_doc:revs_to_strs(MissingRevs)}] ++
                    if PossibleAncestors == [] ->
                        [];
                    true ->
                        [{possible_ancestors,
                            couch_doc:revs_to_strs(PossibleAncestors)}]
                end}}
        end, Results),
        {ok, Results2}
    end).
       
-spec compact(Db::db()) -> ok.
%% @doc start to compact db.
compact(Db) ->
    db_admin_exec(Db, fun(Db0) ->
        couch_db:start_compact(Db0)
    end).

-spec compact(Db::db(), DName::binary()) -> ok.
%% @doc start to compact view in ddoc.
compact(#cdb{name=DbName}=Db, DName) ->
    db_admin_exec(Db, fun(_) ->
        couch_view_compactor:start_compact(dbname(DbName),
                    couch_util:to_binary(DName))
    end).

-spec view_cleanup(Db::db()) -> ok.
%% @doc clean up index files of a db
view_cleanup(Db) ->
    db_admin_exec(Db, fun(Db0) ->
        couch_view:cleanup_index_files(Db0)
    end).

-spec open_doc(Db::db(), DocId::docid()) -> {ok,Doc::ejson_object()} |
    {error, term()}.
%% @equiv open_doc(Db, DocId, [])
open_doc(Db, DocId) ->
    open_doc(Db, DocId, []).



-spec open_doc(Db::db(), DocId::docid(), Options::doc_options()) -> {ok,
        Doc::ejson_object()} | {error, term()}.
%% @doc open a document with DocId, return DocId and last revision.
%% Options = [revs, revs_info, conflicts, deleted_conflicts, local_seq,
%%            latest, att_encoding_info, {atts_since, RevList}, 
%%            attachments]
open_doc(Db, DocId, Options) ->
    DocId1 = couch_util:to_binary(DocId),
    Rev = proplists:get_value(rev, Options, nil),
    Revs =  proplists:get_value(open_revs, Options, []),

    case Revs of
        [] ->
            open_doc_rev(Db, DocId1, Rev, Options);
        _ ->
            db_exec(Db, fun(Db0) ->
                Options1 = proplists:delete(open_revs, Options),
                case couch_db:open_doc_revs(Db0, DocId1, Revs, Options1) of
                    {ok, Results} ->
                        Results1 = lists:foldl(fun(Result, Acc) ->
                                    case Result of
                                        {ok, Doc} ->
                                            JsonDoc = couch_doc:to_json_obj(Doc,
                                                Options1),
                                            [JsonDoc|Acc];
                                        {{not_found, missing}, RevId} ->
                                            RevStr =
                                            couch_doc:rev_to_str(RevId),
                                            [{not_found, missing}, RevStr]
                                end
                        end, [], Results),
                        {ok, Results1};
                    Error ->
                        {error, Error}
                end
            end)
    end.

-spec open_doc_rev(Db::db(), DocId::docid(), Rev::nil | binary(),
    Options::doc_options()) -> {ok, Doc::ejson_object()} | {error,
        term()}.
%% @doc open a doc with its Doc id and revision.
open_doc_rev(Db, DocId, nil, Options) ->
    db_exec(Db, fun(Db0) ->
        case couch_db:open_doc(Db0, DocId, Options) of
            {ok, Doc} ->
                {ok, couch_doc:to_json_obj(Doc, Options)};
            Error ->
                {error, Error}
        end
    end);
open_doc_rev(Db, DocId, Rev, Options) ->
    db_exec(Db, fun(Db0) ->
        Rev1 = couch_doc:parse_rev(Rev),
        Options1 = proplists:delete(rev, Options),
        case couch_db:open_doc_revs(Db0, DocId, [Rev1], Options1) of
            {ok, [{ok, Doc}]} ->
                {ok, Doc};
            {ok, [{{not_found, missing}, Rev}]} ->
                {error, [{{not_found, missing}, Rev}]};
            {ok, Else} ->
                {error, Else}
        end
    end).

-spec save_doc(Db::db(), Doc::ejson_object()) -> {ok, DocId::binary(),
        Rev::binary()} | {error, term()}.
%% @equiv save_doc(Db, Doc, []).
save_doc(Db, Doc) ->
    save_doc(Db, Doc, []).


-spec save_doc(Db::db(), Doc::ejson_object(), 
    Options::update_options()) -> {ok, DocId::binary(), Rev::binary()} | {error, term()}.
%% @doc create or update a document. 
%% A document is a Json object like this one:
%%      
%%      ```{[
%%          {<<"_id">>, <<"myid">>},
%%          {<<"title">>, <<"test">>}
%%      ]}'''
%%
%% Options are arguments passed to the request. This function return a
%% new document with last revision and a docid. If _id isn't specified in
%% document it will be created. Id is created by extracting an uuid from
%% the couchdb node.
%%
%% update_options [batch, {update_type, UpdateType}, full_commit, delay_commit]
%% updare_types = replicated_changes, interactive_edit
%%
save_doc(Db, {DocProps}=Doc, Options) ->
    %% get a new doc id or validate the existing.
    DocId = couch_util:get_value(<<"_id">>, DocProps),
    case DocId of
        undefined ->
            save_doc(Db, {[{<<"_id">>, get_uuid()}|DocProps]}, Options);
        _ ->
            try couch_doc:validate_docid(DocId) of
                ok ->
                    db_exec(Db, fun(Db0) ->
                        Doc0 = couch_doc:from_json_obj(Doc),
                        validate_attachment_names(Doc0),
                        case proplists:get_value(batch, Options) of
                            true ->
                                spawn(fun() ->
                                    case catch(couch_db:update_doc(Db0, Doc0, [])) of
                                        {ok, _} -> 
                                            ok;
                                        Error2 ->
                                            ?LOG_INFO("Batch doc error (~s): ~p",
                                                [DocId, Error2])
                                    end
                                end);
                            _ ->
                                UpdateType = proplists:get_value(update_type, Options),
                                {ok, NewRev} = case UpdateType of
                                    undefined ->
                                        couch_db:update_doc(Db0, Doc0, Options,
                                            interactive_edit);

                                    UpdateType ->
                                        Options1 = proplists:delete(update_type, Options),
                                        couch_db:update_doc(Db0, Doc0, Options1, UpdateType)
                                end,
                                NewRevStr = couch_doc:rev_to_str(NewRev),
                                {ok, DocId, NewRevStr}
                        end
                    end)
            catch
                _:Error -> {error, Error}
            end
    end.

-spec delete_doc(Db::db(), Doc::ejson_object() | {DocId::binary(),
        DocRev::binary()}) -> {ok, DocId::binary(),
        Rev::binary()} | {error, term()}.
%% @equiv delete_doc(Db, Doc, []).
delete_doc(Db, Doc) ->
    delete_doc(Db, Doc, []).

-spec delete_doc(Db::db(), Doc::ejson_object() | {DocId::binary(),
        DocRev::binary()}, Options::update_options()) -> {ok,
        DocId::binary(), Rev::binary()} | {error, term()}.
%% delete a document
delete_doc(Db, {DocId, DocRev}, Options)
        when is_binary(DocId) andalso is_binary(DocRev) ->
    Doc = {[
            {<<"_id">>, DocId},
            {<<"_rev">>, DocRev},
            {<<"_deleted">>, true}]},
    save_doc(Db, Doc, Options);
delete_doc(Db, {DocProps}, Options) ->
    Doc = {[{<<"_deleted">>, true}|DocProps]},
    save_doc(Db, Doc, Options).

-spec copy_doc(Db::db(), SourceDocId::binary(), 
    TargetDocId::binary()) -> {ok, TargetDocId::binary(),
        TargetRev::binary()} | {error, term()}.
%% @equiv copy_doc(Db, SourceDocId, TargetDocId, []).
copy_doc(Db, SourceDocId, TargetDocId) ->
    copy_doc(Db, SourceDocId, TargetDocId, []).

-spec copy_doc(Db::db(), SourceDocId::binary(), 
    TargetDocId::binary(), Options::copy_options()) -> {ok, TargetDocId::binary(),
        TargetRev::binary()} | {error, term()}.
%% @doc copy a document from DocId to TargetId
copy_doc(Db, SourceDocId, TargetDocId, Options) ->
    SourceRev = proplists:get_value(rev, Options, nil),
    TargetRev = proplists:get_value(target_rev, Options),

    case open_doc_rev1(Db, SourceDocId, SourceRev, Options) of
        {ok, Doc} ->
            db_exec(Db, fun(Db0) ->
                TargetRev1 = case TargetRev of
                    undefined ->
                        {0, []};
                    _ ->
                        {Pos, RevId} = couch_doc:parse_rev(TargetRev),
                        {Pos, [RevId]}
                end,

                {ok, NewTargetRev} = couch_db:update_doc(Db0,
                    Doc#doc{id=TargetDocId, revs=TargetRev1}, []),

                NewTargetRevStr = couch_doc:rev_to_str(NewTargetRev),
                {ok, TargetDocId, NewTargetRevStr}
            end);
        Error ->
            Error
    end.


-spec save_docs(Db::db(), Docs::list(ejson_object())) -> {ok,
        list({DocId::binary(), DocRev::binary()})} | {error, term()}.
%% @equiv save_docs(Db, Docs, []).
save_docs(Db, Docs) ->
    save_docs(Db, Docs, []).

-spec save_docs(Db::db(), Docs::list(ejson_object()),
    Options::update_options()) -> {ok, list({DocId::binary(), 
                DocRev::binary()})} | {error, term()}.
%% @doc save multiple docs
%% options = replicated_changes, all_or_nothing, delay_commit,
%% full_commmit
save_docs(Db, Docs, Options) ->
    db_exec(Db, fun(Db0) ->
        case proplists:get_value(replicated_changes, Options) of
            true ->
                Docs1 = lists:map(fun(JsonObj) ->
                        Doc = couch_doc:from_json_obj(JsonObj),
                        validate_attachment_names(Doc),
                        Doc
                    end, Docs),
                Options1 = proplists:delete(all_or_nothing, Options),
                {ok, Errors} = couch_db:update_docs(Db0, Docs1, Options1, 
                    replicated_changes),
                lists:map(fun couch_httpd_db:update_doc_result_to_json/1, 
                    Errors);
            _ ->
                Docs1 = lists:map(fun({ObjProps} = JsonObj) ->
                        Doc = couch_doc:from_json_obj(JsonObj),
                        validate_attachment_names(Doc),
                        Id = case Doc#doc.id of
                            <<>> -> couch_uuids:new();
                            Id0 -> Id0
                        end,
                        case couch_util:get_value(<<"_rev">>, ObjProps) of
                        undefined ->
                           Revs = {0, []};
                        Rev  ->
                            {Pos, RevId} = couch_doc:parse_rev(Rev),
                            Revs = {Pos, [RevId]}
                        end,
                        Doc#doc{id=Id,revs=Revs}
                    end, Docs),
                case couch_db:update_docs(Db0, Docs1, Options) of
                    {ok, Results} ->
                        lists:zipwith(fun couch_httpd_db:update_doc_result_to_json/2,
                            Docs1, Results);
                    {aborted, Errors} ->
                        lists:map(fun couch_httpd_db:update_doc_result_to_json/1, 
                            Errors)
                end
        end
    end).

-spec delete_docs(Db::db(), Docs::list(ejson_object())) -> {ok,
        list({DocId::binary(), DocRev::binary()})} | {error, term()}.
%% @equiv save_docs(Db, Docs, []).
delete_docs(Db, Docs) ->
    delete_docs(Db, Docs, []).

-spec delete_docs(Db::db(), Docs::list(ejson_object() | {DocId::binary(),
            DocRev::binary()}), Options::update_options()) -> {ok, list({DocId::binary(),
            DocRev::binary()})} | {error, term()}.
%% @doc like save_docs but delete multiples docs.
delete_docs(Db, Docs, Options) ->
    Docs0 = lists:map(fun(Doc) ->
            case Doc of 
                {DocId, DocRev} 
                when is_binary(DocId) andalso is_binary(DocRev) ->
                    {[
                    {<<"_id">>, DocId},
                    {<<"_rev">>, DocRev},
                    {<<"_deleted">>, true}]};
                {DocProps} ->
                    {[{<<"_deleted">>, true}|DocProps]}
            end
        end, Docs),
    save_docs(Db, Docs0, Options).

-spec all(Db::db()) -> {ok, Result::ejson_object()} | {error, term()}.
%% @doc get all docs
all(Db) ->
    all(Db, 'docs').

-spec all(Db::db(), ViewName:: 'docs' | {DesignName::binary(),
        ViewName::binary()}) -> {ok, Result::ejson_object()} | {error, term()}.
%% @doc get all docs or all results from a view

all(Db, ViewName) ->
    all(Db, ViewName, []).

-spec all(Db::db(), ViewName:: 'docs' | {DesignName::binary(),
        ViewName::binary()}, Options::view_options()) -> {ok, Result::ejson_object()} | {error, term()}.
%% @doc get all docs or all results from a view
all(Db, ViewName, Options) ->
    fold(Db, ViewName, fun collect_results/2, Options).


-spec fold(Db::db(), Fun::function()) -> {ok, Result::ejson_object()} | {error, term()}.
%% @doc fold all docs in a function fun(Doc, Acc)
fold(Db, Fun) ->
    fold(Db, 'docs', Fun, []).

-spec fold(Db::db(), ViewName::'docs' | {DesignName::binary(),
        ViewName::binary()}, Fun::function()) -> {ok, Result::ejson_object()} | {error, term()}.
%% @equiv fold(Db, ViewName, Fun, []).
fold(Db, ViewName, Fun) ->
    fold(Db, ViewName, Fun, []).

-spec fold(Db::db(), ViewName::'docs' | {DesignName::binary(),
        ViewName::binary()}, Fun::function(), Options::view_options()) -> {ok, Result::ejson_object()} | {error, term()}.
%% @doc fold all docs in a function fun(Doc, Acc)
fold(Db, 'docs', Fun, Options) ->
    Keys = proplists:get_value(keys, Options, nil),
    QueryArgs = proplists:get_value(query_args, Options,
        #view_query_args{}),

    #view_query_args{
            limit = Limit,
            skip = SkipCount,
            stale = _Stale,
            direction = Dir,
            group_level = _GroupLevel,
            start_key = StartKey,
            start_docid = StartDocId,
            end_key = EndKey,
            end_docid = EndDocId,
            inclusive_end = Inclusive
    } = QueryArgs,
    
    db_exec(Db, fun(Db0) ->
        {ok, Info} = couch_db:get_db_info(Db0),
        TotalRowCount = proplists:get_value(doc_count, Info),
        StartId = if is_binary(StartKey) -> StartKey;
                true -> StartDocId
            end,
        EndId = if is_binary(EndKey) -> EndKey;
                true -> EndDocId
            end,
        FoldAccInit = {Limit, SkipCount, undefined, []},
        UpdateSeq = couch_db:get_update_seq(Db0),

        case Keys of
        nil ->
            FoldlFun = couch_httpd_view:make_view_fold_fun(nil, QueryArgs,
                <<"">>, Db0,
                UpdateSeq, TotalRowCount, #view_fold_helper_funs{
                    reduce_count = fun couch_db:enum_docs_reduce_to_count/1,
                    start_response = fun start_map_view_fold_fun/6,
                    send_row = make_docs_row_fold_fun(Fun)
                }),
            AdapterFun = fun(#full_doc_info{id=Id}=FullDocInfo, Offset, Acc) ->
                case couch_doc:to_doc_info(FullDocInfo) of
                    #doc_info{revs=[#rev_info{deleted=false}|_]} = DocInfo ->
                        FoldlFun({{Id, Id}, DocInfo}, Offset, Acc);
                    #doc_info{revs=[#rev_info{deleted=true}|_]} ->
                        {ok, Acc}
                end
            end,
            {ok, LastOffset, FoldResult} = couch_db:enum_docs(Db0,
                AdapterFun, FoldAccInit, [{start_key, StartId}, {dir, Dir},
                    {if Inclusive -> end_key; true -> end_key_gt end, EndId}]),
            finish_view_fold(TotalRowCount, LastOffset, FoldResult);
        _ ->
            FoldlFun = couch_httpd_view:make_view_fold_fun(nil, QueryArgs,
                <<"">>, Db0,
                UpdateSeq, TotalRowCount, #view_fold_helper_funs{
                    reduce_count = fun(Offset) -> Offset end,
                    start_response = fun start_map_view_fold_fun/6,
                    send_row = make_docs_row_fold_fun(Fun)
                }),
            KeyFoldFun = case Dir of
            fwd ->
                fun lists:foldl/3;
            rev ->
                fun lists:foldr/3
            end,

            FoldResult = KeyFoldFun(
                fun(Key, FoldAcc) ->
                    DocInfo = (catch couch_db:get_doc_info(Db0, Key)),
                    Doc = case DocInfo of
                    {ok, #doc_info{id = Id} = Di} ->
                        {{Id, Id}, Di};
                    not_found ->
                        {{Key, error}, not_found};
                    _ ->
                        ?LOG_ERROR("Invalid DocInfo: ~p", [DocInfo]),
                        throw({error, invalid_doc_info})
                    end,
                    {_, FoldAcc2} = FoldlFun(Doc, 0, FoldAcc),
                    FoldAcc2
                end, FoldAccInit, Keys),
            finish_view_fold(TotalRowCount, 0, FoldResult)
        end

    end);
%% options [{reduce, true}, {keys, nil}, 
%%          {query_args, #view_query_args{}}]
fold(Db, {DName, VName}, Fun, Options) ->
    DName1 = couch_util:to_binary(DName),
    VName1 = couch_util:to_binary(VName),
    DesignId = <<"_design/", DName1/binary>>,
    Keys = proplists:get_value(keys, Options, nil),
    QueryArgs = proplists:get_value(query_args, Options,
        #view_query_args{}),

    #view_query_args{
        stale=Stale} = QueryArgs,
    Reduce = get_reduce_type(Options),
    db_exec(Db, fun(Db0) ->
        case couch_view:get_map_view(Db0, DesignId, VName1, Stale) of
            {ok, View, Group} ->
                fold_map_view(View, Group, Fun, Db0, QueryArgs, Keys);
            {not_found, Reason} ->
                case couch_view:get_reduce_view(Db0, DesignId, VName1, Stale) of
                {ok, ReduceView, Group} ->
                    case Reduce of
                    false ->
                        MapView =
                        couch_view:extract_map_view(ReduceView),
                        fold_map_view(MapView, Group, Fun, Db0, QueryArgs, Keys);
                    _ ->
                        fold_reduce_view(ReduceView, Group, Fun, Db0,
                            QueryArgs, Keys)
                    end;
                _ ->
                    {error, {not_found, Reason}}
                end
        end
    end);
fold(_, _, _, _) ->
    {error, {bad_view_name, <<"bad view name">>}}.

-spec open_attachment(Db::db(), DocId::docid(), FileName::binary()) ->
    {ok, Bin::binary(), term()} | {ok, Bin::binary()} | {ok,
        list({From::integer(), To::integer(), Type::binary(),
                Bin::binary})} | {error, term()}.
%% @equiv open_attachment(Db, DocId, FileName, [])
open_attachment(Db, DocId, FileName) ->
    open_attachment(Db, DocId, FileName, []).

-spec open_attachment(Db::db(), DocId::docid(), FileName::binary(),
    Options::attachment_options()) ->
    {ok, Bin::binary(), term()} | {ok, Bin::binary()} | {ok,
        list({From::integer(), To::integer(), Type::binary(),
                Bin::binary})} | {error, term()}.
%% @doc open an attachment FileName in Doc with DocId
open_attachment(Db, DocId, FileName, Options) ->
    case open_attachment_async(Db, DocId, FileName, Options) of
        {ok, Pid, ranges} ->
            {ok, collect_attachment_ranges(Pid)};
        {ok, Pid, {content_md5, _}=Md5} ->
            {ok, collect_attachment(Pid), Md5};
        {ok, Pid} ->
            {ok, Pid, collect_attachment(Pid)};
        Error ->
            Error
    end.

-spec open_attachment_async(Db::db(), DocId::docid(), FileName::binary()) ->
    {ok, Pid::pid(), term()} | {ok, Pid::pid()} | {error, term()}.
%% @equiv open_attachment_async(Db, DocId, FileName, [])
open_attachment_async(Db, DocId, FileName) ->
    open_attachment_async(Db, DocId, FileName, []).


-spec open_attachment_async(Db::db(), DocId::docid(),
    FileName::binary(), Options::attachment_options()) ->
     {ok, Pid::pid(), ranges} | {ok, Pid::pid(), term()} | {ok,
         Pid::pid()} | {error, term()}.
%% @doc get an attachment asynchronously
%% Use get_attachment_part(Pid::pid()) to get attachment part. If you
%% specified ranges in options, the returned response is {ok, Pid,
%% ranges}
open_attachment_async(Db, DocId, FileName, Options) ->
    FileName1 = couch_util:to_binary(FileName),
    Rev = proplists:get_value(rev, Options, nil),
    DocId1 = couch_util:to_binary(DocId),
    case open_doc_rev1(Db, DocId1, Rev, Options) of
        {ok, Doc} ->
            #doc{atts=Atts} = Doc,
            case [A || A <- Atts, A#att.name == FileName1] of
                [] ->
                    {error, {not_found, "Document is missing attachment"}};
                [#att{type=Type, encoding=Enc, disk_len=DiskLen, 
                        att_len=AttLen}=Att] ->
                    case acceptable_encoding(Options) of
                    {error, Error} ->
                        {error, Error};
                    EncList ->
                        AcceptsAttEnc = lists:member(Enc, EncList),
                        Len = case {Enc, AcceptsAttEnc} of
                            {identity, _} ->
                                DiskLen;
                            {_, false} when DiskLen =/= AttLen ->
                                DiskLen;
                            {_, true} ->
                                AttLen;
                            _ ->
                                undefined
                        end,
                        AttFun = case AcceptsAttEnc of
                            false ->
                                fun couch_doc:att_foldl_decode/3;
                            true ->
                                fun couch_doc:att_foldl/3
                        end,
                        case Len of
                            undefined ->
                                Pid = spawn(fun() -> 
                                            stream_attachment(AttFun, Att) 
                                    end),
                                {ok, Pid, []};
                            _ ->
                                Ranges = parse_ranges(proplists:get_value(ranges, Options), Len),
                                case {Enc, Ranges} of
                                    {identity, Ranges} when is_list(Ranges) ->
                                        Pid = spawn(fun() ->
                                                    stream_attachment_ranges(Type,
                                                        Len, Att, Ranges) 
                                            end),
                                        {ok, Pid, ranges};
                                    _ ->
                                        Pid = spawn(fun() ->
                                                    stream_attachment(AttFun, Att) 
                                            end),
                                        if Enc =:= identity orelse AcceptsAttEnc =:= true ->
                                            {ok, Pid, {content_md5,
                                                    base64:encode(Att#att.md5)}};
                                        true ->
                                            {ok, Pid}
                                        end
                                end
                        end

                end
            end;
        {error, conflict} ->
            {error, not_found};
        Error ->
            Error
    end.


-spec get_attachment_part(Pid::pid()) -> {ok, done}
    | {ok, {new_range, {Type::binary(), From::integer(), To::integer(),Len::integer}}} 
    | {ok, {range_part, {{From::integer(), To::integer(), Type::binary(), Len::integer()}, Bin::binary(), BinSize::integer()}}}
    | {ok, {Bin::binary(), BinSize::binary()}}.
%% @equiv get_attachment_part(Pid, infinity).
get_attachment_part(Pid) ->
    get_attachment_part(Pid, infinity).


-spec get_attachment_part(Pid::pid(), Timeout::infinity | integer()) -> {ok, done}
    | {ok, {new_range, {Type::binary(), From::integer(), To::integer(),  Len::integer}}} 
    | {ok, {range_part, {{From::integer(), To::integer(), Type::binary(), Len::integer()}, Bin::binary(), BinSize::integer()}}}
    | {ok, {Bin::binary(), BinSize::binary()}}.
%% @doc stream fetch attachment to a Pid. Messages  
%%      will be of the form `{reference(), message()}',
%%      where `message()' is one of:
%%      <dl>
%%          <dt>{ok, done}</dt>
%%              <dd>You got all the attachment</dd>
%%          <dt>{ok, {Bin::binary(), BinSize::binary()}}</dt>
%%              <dd>Part of the attachment with its size</dd>
%%          <dt>{ok, {new_range, {Type::binary(), From::integer(), To::integer(),  Len::integer}}}</dt>
%%              <dd>Get a new range info (when a range start</dd>
%%          <dt>{ok, {range_part, {{From, To, Type, Len}, Bin::binary(), BinSize::integer()}}}</dt>
%%              <dd>Get a new range part</dd>
%%          <dt>{error, term()}</dt>
%%              <dd>n error occurred</dd>
%%      </dl>
%% @spec stream_fetch_attachment(Db::db(), DocId::string(), Name::string(), 
%%                               ClientPid::pid(), Options::list(), Timeout::integer())
get_attachment_part(Pid, Timeout) ->
    Pid ! {ack, self()},
    receive
        {attachment_done, Pid} ->
            {ok, done};
        {attachment_range, Pid, {Type, From, To, Len}} ->
            Pid ! {ack, self()},
            {ok, {new_range, {Type, From, To, Len}}};
        {attachment_range_part, RangePart} ->
            Pid ! {ack, self()},
            {ok, {range_part, RangePart}};
        {attachment_part, Pid, Part} ->
            Pid ! {ack, self()},
            {ok, Part}
    after Timeout ->
            kill_attachment_streamer(Pid)
    end.

-spec save_attachment(Db::db(), DocId::binary(), FileName::binary(),
    Payload::body()) -> {ok, DocId::binary(), DocRev::binary()} |
    {error, term}.
%% @equiv save_attachment(Db, DocId, FileName, Payload, []).
save_attachment(Db, DocId, FileName, Payload) ->
    save_attachment(Db, DocId, FileName, Payload, []).

-spec save_attachment(Db::db(), DocId::binary(), FileName::binary(),
    Payload::body(), Options::update_options()) -> {ok, DocId::binary(), DocRev::binary()} |
    {error, term}.
%% @doc save an attachment
save_attachment(Db, DocId, FileName, Payload, Options) ->
    FileName1 = couch_util:to_binary(FileName),
    CType = couch_util:to_binary(proplists:get_value(content_type, 
            Options, <<"application/octet-stream">>)),
    CLen = proplists:get_value(content_len, Options),
    Md5 = couch_util:to_binary(proplists:get_value(content_md5, Options,
            <<>>)),
    Encoding = proplists:get_value(encoding, Options, identity),
    NewAtt = [#att{
                name = FileName1,
                type = CType,
                data = Payload,
                att_len = CLen,
                md5 = Md5,
                encoding = Encoding}],

    db_exec(Db, fun(Db0) ->
        Result = case proplists:get_value(rev, Options) of
            undefined ->
                case couch_db:open_doc(Db0, DocId, Options) of
                    {ok, Doc0} ->
                        Doc0;
                    _ ->
                        #doc{id=DocId}
                end;
            Rev ->
                Rev1 = couch_doc:parse_rev(Rev),
                case couch_db:open_doc_revs(Db0, DocId, [Rev1], []) of
                    {ok, [{ok, Doc0}]} -> 
                        Doc0;
                    {ok, [{{not_found, missing}, Rev1}]} -> 
                        {error, conflict};
                    {ok, [Error]} -> 
                        {error, Error}
                end
        end,
        case Result of
            {error, _} = Error1 ->
                Error1;
            Doc ->
                #doc{atts=Atts} = Doc,
                DocEdited = Doc#doc{
                    atts = NewAtt ++ [A || A <- Atts, A#att.name /=
                        FileName1]
                },
                {ok, UpdatedRev} = couch_db:update_doc(Db0, DocEdited, []),
                UpdatedRevStr = couch_doc:rev_to_str(UpdatedRev),
                {ok, DocId, UpdatedRevStr}
        end
    end).

-spec delete_attachment(Db::db(), DocId::binary(), FileName::binary()) ->
    {ok, DocId::binary(), DocRev::binary()} | {error, term}.
%% @equiv delete_attachment(Db, DocId, FileName, []).
delete_attachment(Db, DocId, FileName) ->
    delete_attachment(Db, DocId, FileName, []).

-spec delete_attachment(Db::db(), DocId::binary(), FileName::binary(),
    Options::update_options()) ->
    {ok, DocId::binary(), DocRev::binary()} | {error, term}.
%% @doc delete an attachment
delete_attachment(Db, DocId, FileName, Options) ->
    FileName1 = couch_util:to_binary(FileName),
    Rev = proplists:get_value(rev, Options, nil),
    DocId1 = couch_util:to_binary(DocId),
    NewAtt = [],
    
    case open_doc_rev1(Db, DocId1, Rev, Options) of
        {ok, Doc} ->
            #doc{atts=Atts} = Doc,
            DocEdited = Doc#doc{
                atts = NewAtt ++ [A || A <- Atts, A#att.name /=
                FileName1]
            },
            db_exec(Db, fun(Db0) ->
                {ok, UpdatedRev} = couch_db:update_doc(Db0, DocEdited, []),
                UpdatedRevStr = couch_doc:rev_to_str(UpdatedRev),
                {ok, DocId, UpdatedRevStr}
            end);
        Error ->
            Error
    end.

%% utility functionss

db_exec(#cdb{name=DbName,options=Options}, Fun) ->
    case couch_db:open(dbname(DbName), Options) of
        {ok, Db} ->
            try
                Fun(Db)
            after
                catch couch_db:close(Db)
            end;
        Error ->
            {error, {db, Error}}
    end.

db_admin_exec(Db, Fun) ->
    db_exec(Db, fun(Db0) ->
        try couch_db:check_is_admin(Db0) of
            ok -> Fun(Db0)
        catch
            _:Error -> {error, Error}
        end
    end).

collect_attachment(Pid) ->
    collect_attachment(Pid, []).

collect_attachment(Pid, Acc) ->
    case get_attachment_part(Pid) of
        {ok, done} ->
            iolist_to_binary(lists:reverse(Acc));
        {ok, {Bin, _}} ->
            collect_attachment(Pid, [Bin|Acc]);
        Error ->
            Error
    end.


collect_attachment_ranges(Pid) ->
    collect_attachment_ranges(Pid, orddict:new()).

collect_attachment_ranges(Pid, Ranges) ->
    case get_attachment_part(Pid) of
        {ok, done} ->
            RangesList = dict:to_list(Ranges),
            lists:map(fun({_, {From, To, Type, Len, Acc}}) ->
                            Bin = iolist_to_binary(lists:reverse(Acc)),
                            {From, To, Type, Len, Bin}
                    end, RangesList);
        {ok, range_part, {{From, To, Type, Len}, Bin, _}}->
            NewRanges = case dict:find({From, To}, Ranges) of
                {ok, {Type, From, To, Len, Acc}} ->
                    dict:store({From, To},  {Type, From, To, Len,
                            [Bin|Acc]}, Ranges);
                _ ->
                    dict:store({From, To},  {Type, From, To, Len,
                            [Bin]}, Ranges)
            end,
            collect_attachment_ranges(Pid, NewRanges);
        Error ->
            Error
    end.

%% private functions

stream_attachment(AttFun, Att) ->
    Self = self(),
    AttFun(Att, fun(Bins, _) ->
        BinSegment = list_to_binary(Bins),
        receive
            {ack, From} ->
                From ! {attachment_part, Self, {BinSegment,
                        size(BinSegment)}}
        end,
        {ok, []}
    end, []),
    receive
        {ack, From} ->
            From ! {attachment_done, Self}
    end.

stream_attachment_ranges(Type, Len, Att, Ranges) ->
    Self = self(),
    lists:foreach(fun({From, To}) ->
        receive
            {ack, From} ->
                From ! {attachment_range, Self, {Type,
                        From, To, Len}}
        end,
        couch_doc:range_att_foldl(Att, From, To + 1, fun(Bins, _) ->
            BinSegment = list_to_binary(Bins),
            receive
                {ack, CFrom} ->
                    CFrom ! {attachment_range_part, Self, {{From, To,
                                Type, Len}, BinSegment,
                            size(BinSegment)}}
            end
        end, {ok, []})
    end, Ranges),
    receive
        {ack, From} ->
            From ! {attachment_done, Self}
    end.
        
kill_attachment_streamer(Pid) ->
    erlang:monitor(Pid),
    unlink(Pid),
    exit(Pid, timeout),
    receive
        {'DOWN', _, process, Pid, timeout} ->
            {error, timeout};
        {'DOWN', _, process, Pid, Reason} ->
            erlang:error(Reason)
    end.


parse_ranges(undefined, _Len) ->
    undefined;
parse_ranges(Ranges, Len) ->
    parse_ranges(Ranges, Len, []).

parse_ranges([], _Len, Acc) ->
    lists:reverse(Acc);
parse_ranges([{From, To}|_], _Len, _Acc) when is_integer(From) andalso is_integer(To) andalso To < From ->
    throw(requested_range_not_satisfiable);
parse_ranges([{From, To}|Rest], Len, Acc) when is_integer(To) andalso To >= Len ->
    parse_ranges([{From, Len-1}] ++ Rest, Len, Acc);
parse_ranges([{none, To}|Rest], Len, Acc) ->
    parse_ranges([{Len - To, Len - 1}] ++ Rest, Len, Acc);
parse_ranges([{From, none}|Rest], Len, Acc) ->
    parse_ranges([{From, Len - 1}] ++ Rest, Len, Acc);
parse_ranges([{From,To}|Rest], Len, Acc) ->
    parse_ranges(Rest, Len, [{From, To}] ++ Acc).


acceptable_encoding(Options) ->
    case proplists:get_value(accepted_encoding, Options) of
        undefined ->
            [identity];
        EncList  ->
            EncList1 = lists:foldl(fun(Enc, Acc) ->
                        case lists:member(Enc, [gzip, identity]) of
                            true ->
                                [Enc|Acc];
                            _ ->
                                Acc
                        end
                end, [], EncList),
            case EncList1 of
                [] ->
                    {error, bad_accept_encoding_value};
                _ ->
                    EncList1
            end
    end.

open_doc_rev1(Db, DocId, nil, Options) ->
    db_exec(Db, fun(Db0) ->
        case couch_db:open_doc(Db0, DocId, Options) of
            {ok, Doc} ->
                {ok, Doc};
            Error ->
                {error, Error}
        end
    end);
open_doc_rev1(Db, DocId, Rev, Options) ->
    db_exec(Db, fun(Db0) ->
        Rev1 = couch_doc:parse_rev(Rev),
        Options1 = proplists:delete(rev, Options),
        case couch_db:open_doc_revs(Db0, DocId, [Rev1], Options1) of
            {ok, [{ok, Doc}]} ->
                {ok, Doc};
            {ok, [{{not_found, missing}, Rev}]} ->
                {error, [{{not_found, missing}, Rev}]};
            {ok, Else} ->
                {error, Else}
        end
    end).


fold_map_view(View, Group, Fun, Db, QueryArgs, nil) ->
    #view_query_args{
        limit = Limit,
        skip = SkipCount
    } = QueryArgs,
    CurrentEtag = couch_httpd_view:view_etag(Db, Group, View),
    {ok, RowCount} = couch_view:get_row_count(View),
    FoldlFun = couch_httpd_view:make_view_fold_fun(nil, QueryArgs,
        CurrentEtag, Db, Group#group.current_seq, RowCount, 
        #view_fold_helper_funs{
            reduce_count=fun couch_view:reduce_to_count/1,
            start_response = fun start_map_view_fold_fun/6,
            send_row = make_map_row_fold_fun(Fun)}),
    FoldAccInit = {Limit, SkipCount, undefined, []},
    {ok, LastReduce, FoldResult} = couch_view:fold(View,
        FoldlFun, FoldAccInit, couch_httpd_view:make_key_options(QueryArgs)),
    finish_view_fold(RowCount,
                couch_view:reduce_to_count(LastReduce), FoldResult);
fold_map_view(View, Group, Fun, Db, QueryArgs, Keys) ->
    #view_query_args{
        limit = Limit,
        skip = SkipCount
    } = QueryArgs,
    CurrentEtag = couch_httpd_view:view_etag(Db, Group, View, Keys),
    {ok, RowCount} = couch_view:get_row_count(View),
    FoldAccInit = {Limit, SkipCount, undefined, []},
    {LastReduce, FoldResult} = lists:foldl(fun(Key, {_, FoldAcc}) ->
        FoldlFun = couch_httpd_view:make_view_fold_fun(nil, QueryArgs#view_query_args{},
                CurrentEtag, Db, Group#group.current_seq, RowCount,
                #view_fold_helper_funs{
                    reduce_count = fun couch_view:reduce_to_count/1,
                    start_response = fun start_map_view_fold_fun/6,
                    send_row = make_map_row_fold_fun(Fun)
                }),
        {ok, LastReduce, FoldResult} = couch_view:fold(View, FoldlFun,
                FoldAcc, couch_httpd_view:make_key_options(
                     QueryArgs#view_query_args{start_key=Key, end_key=Key})),
        {LastReduce, FoldResult}
    end, {{[],[]}, FoldAccInit}, Keys),
    finish_view_fold(RowCount, couch_view:reduce_to_count(LastReduce),
                FoldResult).

fold_reduce_view(View, Group, Fun, Db, QueryArgs, nil) ->
    #view_query_args{
        limit = Limit,
        skip = Skip,
        group_level = GroupLevel
    } = QueryArgs,
    CurrentEtag = couch_httpd_view:view_etag(Db, Group, View),
    {ok, GroupRowsFun, RespFun} = couch_httpd_view:make_reduce_fold_funs(nil, GroupLevel,
                QueryArgs, CurrentEtag, Group#group.current_seq,
                #reduce_fold_helper_funs{
                    start_response = fun start_reduce_view_fold_fun/4,
                    send_row = make_reduce_row_fold_fun(Fun)
                }),
    FoldAccInit = {Limit, Skip, undefined, []},
    {ok, {_, _, _, AccResult}} = couch_view:fold_reduce(View,
        RespFun, FoldAccInit, [{key_group_fun, GroupRowsFun} | 
            couch_httpd_view:make_key_options(QueryArgs)]),
    {ok, AccResult};
fold_reduce_view(View, Group, Fun, Db, QueryArgs, Keys) ->
    #view_query_args{
        limit = Limit,
        skip = Skip,
        group_level = GroupLevel
    } = QueryArgs,
    CurrentEtag = couch_httpd_view:view_etag(Db, Group, View, Keys),
    {ok, GroupRowsFun, RespFun} = couch_httpd_view:make_reduce_fold_funs(nil, GroupLevel,
                QueryArgs, CurrentEtag, Group#group.current_seq,
                #reduce_fold_helper_funs{
                    start_response = fun start_reduce_view_fold_fun/4,
                    send_row = make_reduce_row_fold_fun(Fun)
                }),
    {_, RedAcc3} = lists:foldl(
        fun(Key, {Resp, RedAcc}) ->
            % run the reduce once for each key in keys, with limit etc
            % reapplied for each key
            FoldAccInit = {Limit, Skip, Resp, RedAcc},
            {_, {_, _, Resp2, RedAcc2}} = couch_view:fold_reduce(View,
                    RespFun, FoldAccInit, [{key_group_fun, GroupRowsFun} |
                    couch_httpd_view:make_key_options(QueryArgs#view_query_args{
                        start_key=Key, end_key=Key})]),
            % Switch to comma
            {Resp2, RedAcc2}
        end,
        {undefined, []}, Keys),
     {ok, RedAcc3}.


get_reduce_type(Options) ->
    case proplists:get_value(reduce, Options) of
        false ->
            false;
        _ ->
            true
    end.

start_reduce_view_fold_fun(_Req, _Etag, _Acc0, _UpdateSeq) ->
    {ok, nil, []}.

make_reduce_row_fold_fun(ViewFoldFun) ->
    fun(_Resp, {Key, Value}, Acc) ->
        {Go, NewAcc} = ViewFoldFun({[{key, Key}, {value, Value}]}, Acc),
        {Go, NewAcc}
    end.

finish_view_fold(TotalRows, Offset, FoldResult) ->
    Result = case FoldResult of
        {_, _, _,[]} ->
            [];
        {_, _, _, {_, _, AccResult}} ->
            AccResult
    end,
    {ok, {TotalRows, Offset, Result}}.

start_map_view_fold_fun(_Req, _Etag, TotalViewCount, Offset, _Acc, _UpdateSeq) ->
    {ok, nil, {TotalViewCount, Offset, []}}.


make_map_row_fold_fun(ViewFoldFun) ->
    fun(_Resp, Db, KV, IncludeDocs, Conflicts, {TotalViewCount, Offset, Acc}) ->
        JsonObj = couch_httpd_view:view_row_obj(Db, KV, IncludeDocs,
            Conflicts),
        {Go, NewAcc} = ViewFoldFun(JsonObj, Acc),
        {Go, {TotalViewCount, Offset, NewAcc}}
    end.

make_docs_row_fold_fun(ViewFoldFun) ->
    fun(_Resp, Db, KV, IncludeDocs, Conflicts, {TotalViewCount, Offset, Acc}) ->
        JsonObj = all_docs_view_row_obj(Db, KV, IncludeDocs,
            Conflicts),
        {Go, NewAcc} = ViewFoldFun(JsonObj, Acc),
        {Go, {TotalViewCount, Offset, NewAcc}}
    end.

all_docs_view_row_obj(_Db, {{DocId, error}, Value}, _IncludeDocs, _Conflicts) ->
    {[{key, DocId}, {error, Value}]};
all_docs_view_row_obj(Db, {_KeyDocId, DocInfo}, true, Conflicts) ->
    case DocInfo of
    #doc_info{revs = [#rev_info{deleted = true} | _]} ->
        {all_docs_row(DocInfo) ++ [{doc, null}]};
    _ ->
        {all_docs_row(DocInfo) ++ couch_httpd_view:doc_member(
            Db, DocInfo, if Conflicts -> [conflicts]; true -> [] end)}
    end;
all_docs_view_row_obj(_Db, {_KeyDocId, DocInfo}, _IncludeDocs, _Conflicts) ->
    {all_docs_row(DocInfo)}.

all_docs_row(#doc_info{id = Id, revs = [RevInfo | _]}) ->
    #rev_info{rev = Rev, deleted = Del} = RevInfo,
    [ {id, Id}, {key, Id},
        {value, {[{rev, couch_doc:rev_to_str(Rev)}] ++ case Del of
            true -> [{deleted, true}];
            false -> []
            end}} ].


collect_results(Row, Acc) ->
    {ok, [Row | Acc]}.
    

parse_ids_revs(IdsRevs) ->
    [{Id, couch_doc:parse_revs(Revs)} || {Id, Revs} <- IdsRevs].

dbname(DbName) ->
    couch_util:to_binary(DbName).

validate_attachment_names(Doc) ->
    lists:foreach(fun(#att{name=Name}) ->
        validate_attachment_name(Name)
    end, Doc#doc.atts).

validate_attachment_name(Name) when is_list(Name) ->
    validate_attachment_name(list_to_binary(Name));
validate_attachment_name(<<"_",_/binary>>) ->
    throw({bad_request, <<"Attachment name can't start with '_'">>});
validate_attachment_name(Name) ->
    case couch_util:validate_utf8(Name) of
        true -> Name;
        false -> throw({bad_request, <<"Attachment name is not UTF-8 encoded">>})
    end.
