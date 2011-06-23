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
         db_exec/2, db_admin_exec/2]).


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
