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

-module(ddoc_cache_refresher).
-behaviour(gen_server).
-vsn(1).


-export([
    spawn/1,
    stop/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include("ddoc_cache.hrl").


-record(st, {
    key
}).


-define(REFRESH_TIMEOUT, 67000).


spawn(Key) ->
    proc_lib:spawn(?MODULE, init, [{self(), Key}]).


stop(Pid) ->
    gen_server:cast(Pid, stop).


init({Parent, Key}) ->
    process_flag(trap_exit, true),
    erlang:monitor(process, Parent),
    gen_server:enter_loop(?MODULE, [], #st{key = Key}, ?REFRESH_TIMEOUT).


terminate(_Reason, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast(stop, St) ->
    {stop, normal, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(timeout, St) ->
    ddoc_cache_entry:spawn_link(St#st.key),
    {noreply, St};

handle_info({'EXIT', _, {open_ok, Key, Resp}}, #st{key = Key} = St) ->
    Self = self(),
    case Resp of
        {ok, Val} ->
            case ets:lookup(?CACHE, Key) of
                [] ->
                    % We were evicted
                    {stop, normal, St};
                [#entry{key = Key, val = Val, pid = Self}] ->
                    % Value hasn't changed, do nothing
                    {noreply, St};
                [#entry{key = Key, pid = Self}] ->
                    % Value changed, update cache
                    case ddoc_cache_lru:refresh(Key, Val) of
                        ok ->
                            {noreply, St};
                        evicted ->
                            {stop, normal, St}
                    end
            end;
        _Else ->
            ddoc_cache_lru:remove(Key),
            {stop, normal, St}
    end;

handle_info({'EXIT', _, _}, #st{key = Key} = St) ->
    % Somethign went wrong trying to refresh the cache
    % so bail in the interest of safety.
    ddoc_cache_lru:remove(Key),
    {stop, normal, St};

handle_info({'DOWN', _, _, _, _}, St) ->
    % ddoc_cache_lru died, so we will as well
    {stop, normal, St};

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
