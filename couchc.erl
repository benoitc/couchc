%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license. 
%%% See the NOTICE for more information.

-module(couchc).

-include("couch_db.hrl").

-record(cdb, {
        name,
        options}).

-export([create_db/1, create_db/2, 
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
         save_docs/2, save_docs/3,
         delete_docs/2, delete_docs/3,
         db_exec/2, db_admin_exec/2,
         all/1, all/2, all/3,
         fold/2, fold/3, fold/4]).


get_uuid() ->
    couch_uuids:new().

get_uuids(Count) ->
    [couch_uuids:new() || _ <- lists:seq(1, Count)].

create_db(DbName) ->
    create_db(DbName, []).

create_db(DbName, Options) ->
    case couch_server:create(dbname(DbName), Options) of
        {ok, Db} ->
            couch_db:close(Db),
            {ok, #cdb{name=DbName, options=Options}};
        Error ->
            {error, Error}
    end.

delete_db(DbName) ->
    delete_db(DbName, []).

delete_db(DbName, Options) ->
    case couch_server:delete(dbname(DbName), Options) of
        ok ->
            ok;
        Error ->
            {error, Error}
    end.


open_db(DbName) ->
    open_db(DbName, []).

open_db(DbName, Options) ->
    case couch_db:open(dbname(DbName), Options) of
        {ok, Db} ->
            couch_db:close(Db),
            {ok, #cdb{name=DbName, options=Options}};
        Error ->
            {error, Error}
    end.

open_or_create_db(DbName) ->
    open_or_create_db(DbName, []).

open_or_create_db(DbName, Options) ->
    case create_db(DbName, Options) of
        {ok, Db} ->
            Db;
        {error, file_exists} ->
            open_db(DbName, Options);
        Error ->
            Error
    end.

db_info(Db) ->
    db_exec(Db, fun(Db0) ->
                couch_db:get_db_info(Db0)
        end).

ensure_full_commit(Db) ->
    ensure_full_commit(Db, []).

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


db_set_security(Db, SecObj) ->
    db_exec(Db, fun(Db0) ->
                couch_db:set_security(Db0, SecObj)
        end).

db_get_security(Db) ->
    db_exec(Db, fun(Db0) ->
                couch_db:get_security(Db0)
        end).

db_set_revs_limit(Db, Limit) ->
    db_exec(Db, fun(Db0) ->
                couch_db:set_revs_limit(Db0, Limit)
        end).

db_get_revs_limit(Db) ->
    db_exec(Db, fun(Db0) ->
                couch_db:get_revs_limit(Db0)
        end).


db_missing_revs(Db, IdsRevs) ->
    IdsRevs2 = parse_ids_revs(IdsRevs),
    db_exec(Db, fun(Db0) ->
        {ok, Results} = couch_db:get_missing_revs(Db0, IdsRevs2),
        Results2 = [{Id, couch_doc:revs_to_strs(Revs)} || {Id, Revs, _}
            <- Results],
        {ok, Results2}
    end).

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
       

compact(Db) ->
    db_admin_exec(Db, fun(Db0) ->
        couch_db:start_compact(Db0)
    end).

compact(#cdb{name=DbName}=Db, DName) ->
    db_admin_exec(Db, fun(_) ->
        couch_view_compactor:start_compact(dbname(DbName),
                    couch_util:to_binary(DName))
    end).

view_cleanup(Db) ->
    db_admin_exec(Db, fun(Db0) ->
        couch_view:cleanup_index_files(Db0)
    end).

open_doc(Db, DocId) ->
    open_doc(Db, DocId, []).

%% @doc

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

save_doc(Db, Doc) ->
    save_doc(Db, Doc, []).

%% options [batch, {update_type, UpdateType}, full_commit, delay_commit]
%% updare_types = replicated_changes, interactive_edit, 
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


delete_doc(Db, Doc) ->
    delete_doc(Db, Doc, []).


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


save_docs(Db, Docs) ->
    save_docs(Db, Docs, []).

%% @doc
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

delete_docs(Db, Docs) ->
    delete_docs(Db, Docs, []).

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

all(Db) ->
    all(Db, 'docs').

all(Db, ViewName) ->
    all(Db, ViewName, []).

all(Db, ViewName, Options) ->
    fold(Db, ViewName, fun collect_results/2, Options).


fold(Db, Fun) ->
    fold(Db, 'docs', Fun, []).

fold(Db, ViewName, Fun) ->
    fold(Db, ViewName, Fun, []).

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
                case couch_view:get_reduce_view(Db, DesignId, VName1, Stale) of
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

%% utility functions

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

%% private functions
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
            couch_httpd_view:viewmake_key_options(QueryArgs)]),
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
        {reduce, false} ->
            false;
        _ ->
            true
    end.

start_reduce_view_fold_fun(_Req, _Etag, _Acc0, _UpdateSeq) ->
    {ok, nil, []}.

make_reduce_row_fold_fun(ViewFoldFun) ->
    fun(_Resp, {Key, Value}, Acc) ->
        {Go, NewAcc} = ViewFoldFun({Key, Value}, Acc),
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
