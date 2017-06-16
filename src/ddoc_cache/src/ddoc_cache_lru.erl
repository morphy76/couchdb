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

-module(ddoc_cache_lru).
-behaviour(gen_server).
-vsn(1).


-export([
    start_link/0,

    insert/2,
    accessed/1,
    evict/2
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
    handle_db_event/3
]).


-include("ddoc_cache.hrl").


-record(st, {
    keys, % key -> time
    dbs, % dbname -> docid -> key -> []
    time,
    max_size,
    evictor
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


insert(Key, Val) ->
    gen_server:call(?MODULE, {insert, Key, Val}).


accessed(Key) ->
    gen_server:cast(?MODULE, {accessed, Key}).


-spec evict(dbname(), [docid()]) -> ok.
evict(DbName, DDocIds) ->
    gen_server:cast(?MODULE, {evict, DbName, DDocIds}).


init(_) ->
    {ok, Keys} = khash:new(),
    {ok, Dbs} = khash:new(),
    {ok, Evictor} = couch_event:link_listener(
            ?MODULE, handle_db_event, nil, [all_dbs]
        ),
    MaxSize = config:get_integer("ddoc_cache", "max_size", 1000),
    {ok, #st{
        keys = Keys,
        dbs = Dbs,
        time = 0,
        max_size = MaxSize,
        evictor = Evictor
    }}.


terminate(_Reason, St) ->
    case is_pid(St#st.evictor) of
        true -> exit(St#st.evictor, kill);
        false -> ok
    end,
    ok.


handle_call({insert, Key, Val}, _From, St) ->
    #st{
        keys = Keys,
        dbs = Dbs,
        time = Time
    } = St,
    NewTime = Time + 1,
    true = ets:insert(?CACHE, #entry{key = Key, val = Val}),
    true = ets:insert(?ATIMES, {NewTime, Key}),
    ok = khash:put(Keys, NewTime),
    store_key(Dbs, Key),
    {reply, ok, trim(St#st{time = NewTime})};

handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast({accessed, Key}, St) ->
    #st{
        keys = Keys,
        time = Time
    } = St,
    NewTime = Time + 1,
    case khash:lookup(Keys, Key) of
        {value, OldTime} ->
            true = ets:delete(?ATIMES, OldTime),
            true = ets:insert(?ATIMES, {NewTime, Key}),
            ok = khash:put(Keys, NewTime);
        not_found ->
            % Likely a client read from the cache while an
            % eviction message was in our mailbox
            ok
    end,
    {noreply, St};

handle_cast({evict, _} = Msg, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, Msg),
    {noreply, St};

handle_cast({evict, _, _} = Msg, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, Msg),
    {noreply, St};

handle_cast({do_evict, DbName}, St) ->
    #st{
        keys = KeyTimes,
        dbs = Dbs
    } = St,
    case khash:lookup(Dbs, DbName) of
        {value, DDocIds} ->
            khash:fold(DDocIds, fun(_, Keys, _) ->
                khash:fold(Keys, fun(Key, _, _) ->
                    {value, Time} = khash:lookup(KeyTimes, Key),
                    remove(St, Time)
                end, nil)
            end, nil),
            khash:del(Dbs, DbName);
        not_found ->
            ok
    end,
    {noreply, St};

handle_cast({do_evict, DbName, DDocIds}, St) ->
    #st{
        keys = KeyTimes,
        dbs = Dbs
    } = St,
    case khash:lookup(Dbs, DbName) of
        {value, DDocIds} ->
            lists:foreach(fun(DDocId) ->
                case khash:lookup(DDocIds, DDocId) of
                    {value, Keys} ->
                        khash:fold(Keys, fun(Key, _, _) ->
                            {value, Time} = khash:lookup(KeyTimes, Key),
                            remove(St, Time)
                        end, nil);
                    not_found ->
                        ok
                end,
                khash:del(DDocIds, DDocId)
            end, [no_ddocid | DDocIds]),
            case khash:size(DDocIds) of
                0 -> khash:del(Dbs, DbName);
                _ -> ok
            end;
        not_found ->
            ok
    end,
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info({'EXIT', Pid, Reason}, #st{evictor=Pid}=St) ->
    couch_log:error("ddoc_cache_opener evictor died ~w", [Reason]),
    {ok, Evictor} = couch_event:link_listener(
            ?MODULE, handle_db_event, nil, [all_dbs]
        ),
    {noreply, St#st{evictor=Evictor}};

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


handle_db_event(ShardDbName, created, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};

handle_db_event(ShardDbName, deleted, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};

handle_db_event(_DbName, _Event, St) ->
    {ok, St}.


store_key(Dbs, Key) ->
    DbName = ddoc_cache_entry:dbname(Key),
    DDocId = ddoc_cache_entry:ddocid(Key),
    case khash:lookup(Dbs, DbName) of
        {value, DDocIds} ->
            case khash:lookup(DDocIds, DDocId) of
                {value, Keys} ->
                    khash:put(Keys, Key, []);
                not_found ->
                    {ok, Keys} = khash:from_list([{Key, []}]),
                    khash:put(DDocIds, DDocId, Keys)
            end;
        not_found ->
            {ok, Keys} = khash:from_list([{Key, []}]),
            {ok, DDocIds} = khash:from_list([{DDocId, Keys}]),
            khash:put(Dbs, DDocId, DDocIds)
    end.


trim(St) ->
    #st{
        keys = Keys,
        max_size = MaxSize
    } = St,
    case khash:size(Keys) > MaxSize of
        true ->
            case ets:first(?ATIMES) of
                '$end_of_table' ->
                    St;
                ATime ->
                    trim(remove(St, ATime))
            end;
        false ->
            St
    end.


remove(St, ATime) ->
    #st{
        keys = Keys
    } = St,
    {value, Key} = khash:lookup(Keys, ATime),
    true = ets:delete(?CACHE, Key),
    true = ets:delete(?ATIMES, ATime),
    ok = khash:del(Keys, Key),
    St.
