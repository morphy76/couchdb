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
    open/1
]).

-include("ddoc_cache.hrl").


-define(LRU, ddoc_cache_lru).


-record(st, {
    db_ddocs
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


open(Key) ->
    case ets:lookup(?CACHE, Key) of
        [#entry{val = Val}] ->
            couch_stats:increment_counter([ddoc_cache, hit]),
            ddoc_cache_lru:accessed(Key),
            {ok, Val};
        [] ->
            couch_stats:increment_counter([ddoc_cache, miss]),
            Resp = gen_server:call(?MODULE, {open, Key}, infinity),
            ddoc_cache_entry:handle_resp(Resp);
        recover ->
            couch_stats:increment_counter([ddoc_cache, recovery]),
            ddoc_cache_entry:open(Key)
    end.


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
            Pid = ddoc_cache_entry:spawn_link(OpenerKey),
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

handle_cast({do_evict, _, _} = Msg, St) ->
    gen_server:cast(?LRU, Msg),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info({'EXIT', _Pid, {open_ok, OpenerKey, Resp}}, St) ->
    respond(OpenerKey, {open_ok, Resp}),
    {noreply, St};

handle_info({'EXIT', _Pid, {open_error, OpenerKey, Type, Reason, Stack}}, St) ->
    respond(OpenerKey, {open_error, Type, Reason, Stack}),
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


respond(OpenerKey, Resp) ->
    [#opener{clients=Clients}] = ets:lookup(?OPENERS, OpenerKey),
    [gen_server:reply(C, Resp) || C <- Clients],
    ets:delete(?OPENERS, OpenerKey).
