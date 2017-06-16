% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(ddoc_cache_opener).
-behaviour(gen_server).
-vsn(1).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-export([
    start_link/0
]).
-export([
    init/1,
    terminate/2,

    handle_call/3,
    handle_cast/2,
    handle_info/2,

    code_change/3
]).

-export([
    open_doc/2,
    open_doc/3,
    open_validation_funs/1,
    lookup/1,
    match_newest/1,
    recover_doc/2,
    recover_doc/3,
    recover_validation_funs/1
]).
-export([
    fetch_doc_data/1
]).

-include("ddoc_cache.hrl").


-define(LRU, ddoc_cache_lru).


-record(st, {
    db_ddocs
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec open_doc(dbname(), docid()) -> {ok, #doc{}}.
open_doc(DbName, DocId) ->
    Resp = gen_server:call(?MODULE, {open, {DbName, DocId}}, infinity),
    handle_open_response(Resp).

-spec open_doc(dbname(), docid(), revision()) -> {ok, #doc{}}.
open_doc(DbName, DocId, Rev) ->
    Resp = gen_server:call(?MODULE, {open, {DbName, DocId, Rev}}, infinity),
    handle_open_response(Resp).

-spec open_validation_funs(dbname()) -> {ok, [fun()]}.
open_validation_funs(DbName) ->
    Resp = gen_server:call(?MODULE, {open, {DbName, validation_funs}}, infinity),
    handle_open_response(Resp).


lookup(Key) ->
    try ets:lookup(?CACHE, Key) of
        [#entry{key = Key, val = Val}] ->
            ddoc_cache_lru:accessed(Key),
            {ok, Val};
        _ ->
            missing
    catch
        error:badarg ->
            recover
    end.

match_newest(Key) ->
    Pattern = #entry{
        key = Key,
        val = '_',
        _ = '_'
    },
    try ets:match_object(?CACHE, Pattern) of
        [] ->
            missing;
        Entries ->
            Docs = lists:map(fun(#entry{key = K, val = V}) ->
                ddoc_cache_lru:accessed(K),
                V
            end, Entries),
            Sorted = lists:sort(
                fun (#doc{deleted=DelL, revs=L}, #doc{deleted=DelR, revs=R}) ->
                    {not DelL, L} > {not DelR, R}
                end, Docs),
            {ok, hd(Sorted)}
    catch
        error:badarg ->
            recover
    end.

recover_doc(DbName, DDocId) ->
    fabric:open_doc(DbName, DDocId, [ejson_body, ?ADMIN_CTX]).

recover_doc(DbName, DDocId, Rev) ->
    {ok, [Resp]} = fabric:open_revs(DbName, DDocId, [Rev], [ejson_body, ?ADMIN_CTX]),
    Resp.

recover_validation_funs(DbName) ->
    {ok, DDocs} = fabric:design_docs(mem3:dbname(DbName)),
    Funs = lists:flatmap(fun(DDoc) ->
        case couch_doc:get_validate_doc_fun(DDoc) of
            nil -> [];
            Fun -> [Fun]
        end
    end, DDocs),
    {ok, Funs}.


init(_) ->
    process_flag(trap_exit, true),
    {ok, #st{}}.

terminate(_Reason, _St) ->
    ok.

handle_call({open, OpenerKey}, From, St) ->
    case ets:lookup(?OPENERS, OpenerKey) of
        [#opener{clients=Clients}=O] ->
            ets:insert(?OPENERS, O#opener{clients=[From | Clients]}),
            {noreply, St};
        [] ->
            Pid = spawn_link(?MODULE, fetch_doc_data, [OpenerKey]),
            ets:insert(?OPENERS, #opener{key=OpenerKey, pid=Pid, clients=[From]}),
            {noreply, St}
    end;

handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


% The do_evict clauses are upgrades while we're
% in a rolling reboot.
handle_cast({do_evict, _} = Msg, St) ->
    gen_server:cast(?LRU, Msg),
    {noreply, St};

handle_cast({do_evict, _, _}, St) ->
    gen_server:cast(?LRU, Msg)
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info({'EXIT', _Pid, {open_ok, OpenerKey, Resp}}, St) ->
    respond(OpenerKey, {open_ok, Resp}),
    {noreply, St};

handle_info({'EXIT', _Pid, {open_error, OpenerKey, Type, Error}}, St) ->
    respond(OpenerKey, {open_error, Type, Error}),
    {noreply, St};

handle_info({'EXIT', Pid, Reason}, St) ->
    Pattern = #opener{pid=Pid, _='_'},
    case ets:match_object(?OPENERS, Pattern) of
        [#opener{key=OpenerKey, clients=Clients}] ->
            [gen_server:reply(C, {error, Reason}) || C <- Clients],
            ets:delete(?OPENERS, OpenerKey),
            {noreply, St};
        [] ->
            {stop, {unknown_pid_died, {Pid, Reason}}, St}
    end;

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec fetch_doc_data({dbname(), validation_funs}) -> no_return();
                    ({dbname(), atom()}) -> no_return();
                    ({dbname(), docid()}) -> no_return();
                    ({dbname(), docid(), revision()}) -> no_return().
fetch_doc_data({DbName, validation_funs}=OpenerKey) ->
    {ok, Funs} = recover_validation_funs(DbName),
    ok = ddoc_cache_lru:insert(OpenerKey, Funs),
    exit({open_ok, OpenerKey, {ok, Funs}});
fetch_doc_data({DbName, Mod}=OpenerKey) when is_atom(Mod) ->
    % This is not actually a docid but rather a custom cache key.
    % Treat the argument as a code module and invoke its recover function.
    try Mod:recover(DbName) of
        {ok, Result} ->
            ok = ddoc_cache_lru:insert(OpenerKey, Result),
            exit({open_ok, OpenerKey, {ok, Result}});
        Else ->
            exit({open_ok, OpenerKey, Else})
    catch
        Type:Reason ->
            exit({open_error, OpenerKey, Type, Reason})
    end;
fetch_doc_data({DbName, DocId}=OpenerKey) ->
    try recover_doc(DbName, DocId) of
        {ok, Doc} ->
            {RevDepth, [RevHash| _]} = Doc#doc.revs,
            Rev = {RevDepth, RevHash},
            ok = ddoc_cache_lru:insert({DbName, DocId, Rev}, Doc),
            exit({open_ok, OpenerKey, {ok, Doc}});
        Else ->
            exit({open_ok, OpenerKey, Else})
    catch
        Type:Reason ->
            exit({open_error, OpenerKey, Type, Reason})
    end;
fetch_doc_data({DbName, DocId, Rev}=OpenerKey) ->
    try recover_doc(DbName, DocId, Rev) of
        {ok, Doc} ->
            ok = ddoc_cache_lru:insert({DbName, DocId, Rev}, Doc),
            exit({open_ok, OpenerKey, {ok, Doc}});
        Else ->
            exit({open_ok, OpenerKey, Else})
    catch
        Type:Reason ->
            exit({open_error, OpenerKey, Type, Reason})
    end.

handle_open_response(Resp) ->
    case Resp of
        {open_ok, Value} -> Value;
        {open_error, throw, Error} -> throw(Error);
        {open_error, error, Error} -> erlang:error(Error);
        {open_error, exit, Error} -> exit(Error)
    end.

respond(OpenerKey, Resp) ->
    [#opener{clients=Clients}] = ets:lookup(?OPENERS, OpenerKey),
    _ = [gen_server:reply(C, Resp) || C <- Clients],
    ets:delete(?OPENERS, OpenerKey).
