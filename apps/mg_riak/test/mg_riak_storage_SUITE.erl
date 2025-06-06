%%%
%%% Copyright 2024 Valitydev
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

%%%
%%% Тесты всех возможных бэкендов хранилищ.
%%%
%%% TODO:
%%%  - сделать проверку, что неймспейсы не пересекаются
%%%
-module(mg_riak_storage_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

%% base group tests
-export([base_test/1]).
-export([batch_test/1]).
-export([indexes_test/1]).
-export([key_length_limit_test/1]).
-export([indexes_test_with_limits/1]).
-export([stress_test/1]).

-export([riak_pool_stable_test/1]).
-export([riak_pool_overload_test/1]).
-export([riak_pool_misbehaving_connection_test/1]).

-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, riak}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {riak, [],
            tests() ++
                [
                    riak_pool_stable_test,
                    riak_pool_overload_test,
                    riak_pool_misbehaving_connection_test
                ]}
    ].

-spec tests() -> [test_name()].
tests() ->
    [
        base_test,
        batch_test,
        % incorrect_context_test,
        indexes_test,
        key_length_limit_test,
        indexes_test_with_limits,
        stress_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({riakc_pb_socket, 'get_index_eq', '_'}, x),
    % dbg:tpl({riakc_pb_socket, 'get_index_range', '_'}, x),
    Apps = mg_cth:start_applications([msgpack, gproc, riakc, pooler]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_cth:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(Group, C) ->
    [{storage_type, Group} | C].

-spec end_per_group(group_name(), config()) -> ok.
end_per_group(_, _C) ->
    ok.

%%
%% base group tests
%%
-spec base_test(config()) -> _.
base_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"base_test">>),
    Pid = start_storage(Options),
    base_test(<<"1">>, Options),
    ok = stop_storage(Pid).

-spec base_test(mg_core:id(), mg_core_storage:options()) -> _.
base_test(Key, Options) ->
    Value1 = #{<<"hello">> => <<"world">>},
    Value2 = [<<"hello">>, 1],

    undefined = mg_core_storage:get(Options, Key),
    Ctx1 = mg_core_storage:put(Options, Key, undefined, Value1, []),
    {Ctx1, Value1} = mg_core_storage:get(Options, Key),
    Ctx2 = mg_core_storage:put(Options, Key, Ctx1, Value2, []),
    {Ctx2, Value2} = mg_core_storage:get(Options, Key),
    ok = mg_core_storage:delete(Options, Key, Ctx2),
    undefined = mg_core_storage:get(Options, Key),
    ok.

-spec batch_test(config()) -> _.
batch_test(C) ->
    {Mod, StorageOpts} = storage_options(?config(storage_type, C), <<"batch_test">>),
    Options = {Mod, StorageOpts#{bathing => #{concurrency_limit => 3}}},
    Pid = start_storage(Options),
    Keys = lists:map(fun genlib:to_binary/1, lists:seq(1, 10)),
    Value = #{<<"hello">> => <<"world">>},

    PutBatch = lists:foldl(
        fun(Key, Batch) ->
            mg_core_storage:add_batch_request({put, Key, undefined, Value, []}, Batch)
        end,
        mg_core_storage:new_batch(),
        Keys
    ),
    PutResults = mg_core_storage:run_batch(Options, PutBatch),
    Ctxs = lists:zipwith(
        fun(Key, Result) ->
            {{put, Key, undefined, Value, _}, Ctx} = Result,
            Ctx
        end,
        Keys,
        PutResults
    ),

    GetBatch = lists:foldl(
        fun(Key, Batch) ->
            mg_core_storage:add_batch_request({get, Key}, Batch)
        end,
        mg_core_storage:new_batch(),
        Keys
    ),
    GetResults = mg_core_storage:run_batch(Options, GetBatch),
    _ = lists:zipwith3(
        fun(Key, Ctx, Result) ->
            {{get, Key}, {Ctx, Value}} = Result
        end,
        Keys,
        Ctxs,
        GetResults
    ),

    ok = stop_storage(Pid).

-spec indexes_test(config()) -> _.
indexes_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"indexes_test">>),
    Pid = start_storage(Options),

    K1 = <<"Key_24">>,
    I1 = {integer, <<"index1">>},
    IV1 = 1,

    K2 = <<"Key_42">>,
    I2 = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    [] = mg_core_storage:search(Options, {I1, IV1}),
    [] = mg_core_storage:search(Options, {I1, {IV1, IV2}}),
    [] = mg_core_storage:search(Options, {I2, {IV1, IV2}}),

    Ctx1 = mg_core_storage:put(Options, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),

    [K1] = mg_core_storage:search(Options, {I1, IV1}),
    [{IV1, K1}] = mg_core_storage:search(Options, {I1, {IV1, IV2}}),
    [K1] = mg_core_storage:search(Options, {I2, IV2}),
    [{IV2, K1}] = mg_core_storage:search(Options, {I2, {IV1, IV2}}),

    Ctx2 = mg_core_storage:put(Options, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    [K1] = mg_core_storage:search(Options, {I1, IV1}),
    [{IV1, K1}, {IV2, K2}] = mg_core_storage:search(Options, {I1, {IV1, IV2}}),
    [K1] = mg_core_storage:search(Options, {I2, IV2}),
    [{IV1, K2}, {IV2, K1}] = mg_core_storage:search(Options, {I2, {IV1, IV2}}),

    ok = mg_core_storage:delete(Options, K1, Ctx1),

    [{IV2, K2}] = mg_core_storage:search(Options, {I1, {IV1, IV2}}),
    [{IV1, K2}] = mg_core_storage:search(Options, {I2, {IV1, IV2}}),

    ok = mg_core_storage:delete(Options, K2, Ctx2),

    [] = mg_core_storage:search(Options, {I1, {IV1, IV2}}),
    [] = mg_core_storage:search(Options, {I2, {IV1, IV2}}),

    ok = stop_storage(Pid).

-spec key_length_limit_test(config()) -> _.
key_length_limit_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"key_length_limit">>),
    Pid = start_storage(Options),

    {logic, {invalid_key, {too_small, _}}} =
        (catch mg_core_storage:get(Options, <<"">>)),
    {logic, {invalid_key, {too_small, _}}} =
        (catch mg_core_storage:add_batch_request({get, <<"">>}, mg_core_storage:new_batch())),

    {logic, {invalid_key, {too_small, _}}} =
        (catch mg_core_storage:put(Options, <<"">>, undefined, <<"test">>, [])),
    {logic, {invalid_key, {too_small, _}}} =
        (catch mg_core_storage:add_batch_request(
            {put, <<"">>, undefined, <<"test">>, []},
            mg_core_storage:new_batch()
        )),

    _ = mg_core_storage:get(Options, binary:copy(<<"K">>, 1024)),

    {logic, {invalid_key, {too_big, _}}} =
        (catch mg_core_storage:get(Options, binary:copy(<<"K">>, 1025))),

    {logic, {invalid_key, {too_big, _}}} =
        (catch mg_core_storage:add_batch_request(
            {get, binary:copy(<<"K">>, 1025)},
            mg_core_storage:new_batch()
        )),

    _ = mg_core_storage:put(
        Options,
        binary:copy(<<"K">>, 1024),
        undefined,
        <<"test">>,
        []
    ),

    {logic, {invalid_key, {too_big, _}}} =
        (catch mg_core_storage:put(
            Options,
            binary:copy(<<"K">>, 1025),
            undefined,
            <<"test">>,
            []
        )),

    ok = stop_storage(Pid).

-spec indexes_test_with_limits(config()) -> _.
indexes_test_with_limits(C) ->
    Options = storage_options(?config(storage_type, C), <<"indexes_test_with_limits">>),
    Pid = start_storage(Options),

    K1 = <<"Key_24">>,
    I1 = {integer, <<"index1">>},
    IV1 = 1,

    K2 = <<"Key_42">>,
    I2 = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    Ctx1 = mg_core_storage:put(Options, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),
    Ctx2 = mg_core_storage:put(Options, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    {[{IV1, K1}], Cont1} = mg_core_storage:search(Options, {I1, {IV1, IV2}, 1, undefined}),
    {[{IV2, K2}], Cont2} = mg_core_storage:search(Options, {I1, {IV1, IV2}, 1, Cont1}),
    {[], undefined} = mg_core_storage:search(Options, {I1, {IV1, IV2}, 1, Cont2}),

    [{IV1, K2}, {IV2, K1}] = mg_core_storage:search(Options, {I2, {IV1, IV2}, inf, undefined}),

    ok = mg_core_storage:delete(Options, K1, Ctx1),
    ok = mg_core_storage:delete(Options, K2, Ctx2),

    ok = stop_storage(Pid).

-spec stress_test(_C) -> ok.
stress_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"stress_test">>),
    Pid = start_storage(Options),
    ProcessCount = 20,
    Processes = [
        stress_test_start_process(ID, ProcessCount, Options)
     || ID <- lists:seq(1, ProcessCount)
    ],

    timer:sleep(5000),
    ok = stop_wait_all(Processes, shutdown, 5000),
    ok = stop_storage(Pid).

-spec stress_test_start_process(integer(), pos_integer(), mg_core_storage:options()) -> pid().
stress_test_start_process(ID, ProcessCount, Options) ->
    erlang:spawn_link(fun() -> stress_test_process(ID, ProcessCount, 0, Options) end).

-spec stress_test_process(integer(), pos_integer(), integer(), mg_core_storage:options()) ->
    no_return().
stress_test_process(ID, ProcessCount, RunCount, Options) ->
    % Добавляем смещение ID, чтобы не было пересечения ID машин
    ok = base_test(erlang:integer_to_binary(ID), Options),

    receive
        {stop, Reason} ->
            ct:print("Process: ~p. Number of runs: ~p", [self(), RunCount]),
            exit(Reason)
    after 0 -> stress_test_process(ID + ProcessCount, ProcessCount, RunCount + 1, Options)
    end.

-spec stop_wait_all([pid()], _Reason, timeout()) -> ok.
stop_wait_all(Pids, Reason, Timeout) ->
    OldTrap = process_flag(trap_exit, true),

    lists:foreach(
        fun(Pid) -> send_stop(Pid, Reason) end,
        Pids
    ),

    lists:foreach(
        fun(Pid) ->
            case stop_wait(Pid, Reason, Timeout) of
                ok -> ok;
                timeout -> exit(stop_timeout)
            end
        end,
        Pids
    ),

    true = process_flag(trap_exit, OldTrap),
    ok.

-spec send_stop(pid(), _Reason) -> ok.
send_stop(Pid, Reason) ->
    Pid ! {stop, Reason},
    ok.

-spec stop_wait(pid(), _Reason, timeout()) -> ok | timeout.
stop_wait(Pid, Reason, Timeout) ->
    receive
        {'EXIT', Pid, Reason} -> ok
    after Timeout -> timeout
    end.

%%

-spec riak_pool_stable_test(_C) -> ok.
riak_pool_stable_test(_C) ->
    Namespace = <<"riak_pool_stable_test">>,
    InitialCount = 1,
    RequestCount = 10,
    Options = riak_options(Namespace, #{
        init_count => InitialCount,
        max_count => RequestCount div 2,
        idle_timeout => 1000,
        cull_interval => 1000,
        queue_max => RequestCount * 2
    }),
    Storage = {mg_riak_storage, Options},
    Pid = start_storage(Storage),

    % Run multiple requests concurrently
    _ = genlib_pmap:map(
        fun(N) ->
            base_test(genlib:to_binary(N), Storage)
        end,
        lists:seq(1, RequestCount)
    ),

    % Give pool 3 seconds to get back to initial state
    ok = timer:sleep(3000),

    {ok, Utilization} = mg_riak_storage:pool_utilization(Options),
    ?assertMatch(
        #{
            in_use_count := 0,
            free_count := InitialCount
        },
        maps:from_list(Utilization)
    ),

    ok = stop_storage(Pid).

-spec riak_pool_overload_test(_C) -> ok.
riak_pool_overload_test(_C) ->
    Namespace = <<"riak_pool_overload_test">>,
    RequestCount = 40,
    Options = riak_options(
        Namespace,
        #{
            init_count => 1,
            max_count => 4,
            queue_max => RequestCount div 4
        }
    ),
    Storage = {mg_riak_storage, Options},
    Pid = start_storage(Storage),

    ?assertThrow(
        {transient, {storage_unavailable, no_pool_members}},
        genlib_pmap:map(
            fun(N) ->
                base_test(genlib:to_binary(N), Storage)
            end,
            lists:seq(1, RequestCount)
        )
    ),

    ok = stop_storage(Pid).

-spec riak_pool_misbehaving_connection_test(_C) -> ok.
riak_pool_misbehaving_connection_test(_C) ->
    Namespace = <<"riak_pool_overload_test">>,
    WorkersCount = 4,
    RequestCount = 4,
    Options = riak_options(
        Namespace,
        #{
            init_count => 1,
            max_count => WorkersCount div 2,
            queue_max => WorkersCount * 2
        }
    ),
    Storage = {mg_riak_storage, Options},
    Pid = start_storage(Storage),

    _ = genlib_pmap:map(
        fun(RequestID) ->
            Key = genlib:to_binary(RequestID),
            case RequestID of
                N when (N rem WorkersCount) == (N div WorkersCount) ->
                    % Ensure that request fails occasionally...
                    ?assertThrow(
                        {transient, {storage_unavailable, _}},
                        mg_core_storage:put(Storage, Key, <<"NOTACONTEXT">>, <<>>, [])
                    );
                _ ->
                    % ...And it will not affect any concurrently running requests.
                    ?assertEqual(
                        undefined,
                        mg_core_storage:get(Storage, Key)
                    )
            end
        end,
        lists:seq(1, RequestCount * WorkersCount),
        #{proc_limit => WorkersCount}
    ),

    ok = stop_storage(Pid).

%%

-spec storage_options(atom(), binary()) -> mg_core_storage:options().
storage_options(riak, Namespace) ->
    {mg_riak_storage,
        riak_options(
            Namespace,
            #{
                init_count => 1,
                max_count => 10,
                idle_timeout => 1000,
                cull_interval => 1000,
                auto_grow_threshold => 5,
                queue_max => 100
            }
        )}.

-spec riak_options(mg_core:ns(), map()) -> mg_riak_storage:options().
riak_options(Namespace, PoolOptions) ->
    #{
        name => storage,
        pulse => ?MODULE,
        host => "riakdb",
        port => 8087,
        bucket => Namespace,
        pool_options => PoolOptions
    }.

-spec start_storage(mg_core_storage:options()) -> pid().
start_storage(Options) ->
    mg_utils:throw_if_error(
        genlib_adhoc_supervisor:start_link(
            #{strategy => one_for_all},
            [mg_core_storage:child_spec(Options, storage)]
        )
    ).

-spec stop_storage(pid()) -> ok.
stop_storage(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec handle_beat(_, mpulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
