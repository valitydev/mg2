%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_core_ct_helper).

-define(CLIENT, mg_core_kafka_client).
-define(BROKERS, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]).

-export([config/1]).

-export([start_application/1]).
-export([start_applications/1]).

-export([stop_applications/1]).
-export([assert_wait_expected/3]).

-export([build_storage/2]).
-export([bootstrap_machine_storage/3]).
-export([bootstrap_events_storage/2]).

-export([stop_wait_all/3]).

-export([handle_beat/2]).

-export([assert_poll_minimum_time/2]).
-export([poll_for_value/3]).
-export([poll_for_exception/3]).

-type appname() :: atom().

-type option() ::
    kafka_client_name.

-spec config(option()) -> _.

config(kafka_client_name) ->
    ?CLIENT.

-spec start_application(appname() | {appname(), [{atom(), _Value}]}) -> _Deps :: [appname()].

start_application(consuela) ->
    genlib_app:start_application_with(consuela, [
        {registry, #{
            nodename => "consul0",
            namespace => <<"mg">>
        }}
    ]);
start_application(brod) ->
    genlib_app:start_application_with(brod, [
        {clients, [
            {config(kafka_client_name), [
                {endpoints, ?BROKERS},
                {auto_start_producers, true}
            ]}
        ]}
    ]);
start_application({AppName, Env}) ->
    genlib_app:start_application_with(AppName, Env);
start_application(AppName) ->
    genlib_app:start_application(AppName).

-spec start_applications([appname()]) -> _Deps :: [any()].

start_applications(Apps) ->
    lists:foldl(fun(App, Deps) -> Deps ++ start_application(App) end, [], Apps).

-spec stop_applications([appname()]) -> ok.

stop_applications(AppNames) ->
    lists:foreach(fun application:stop/1, lists:reverse(AppNames)).

%%

-spec assert_wait_expected(any(), function(), mg_core_retry:strategy()) -> ok.

assert_wait_expected(Expected, Fun, Strategy) when is_function(Fun, 0) ->
    case Fun() of
        Expected ->
            ok;
        Other ->
            case genlib_retry:next_step(Strategy) of
                {wait, Timeout, NextStrategy} ->
                    timer:sleep(Timeout),
                    assert_wait_expected(Expected, Fun, NextStrategy);
                finish ->
                    error({assertion_failed, Expected, Other})
            end
    end.

-spec build_storage(mg_core:ns(), mg_core_utils:mod_opts()) -> mg_core_utils:mod_opts().
build_storage(NS, Module) when is_atom(Module) ->
    build_storage(NS, {Module, #{}});
build_storage(NS, {Module, Options}) ->
    {Module, Options#{name => erlang:binary_to_atom(NS, utf8)}}.

-spec bootstrap_machine_storage(cql | memory, mg_core:ns(), module()) ->
    mg_core_machine_storage:options().
bootstrap_machine_storage(cql, NS, Processor) ->
    Options = build_machine_storage_cql_options(Processor, #{
        node => {"scylla0", 9042},
        keyspace => mg
    }),
    ok = mg_core_machine_storage_cql:teardown(Options, NS),
    ok = mg_core_machine_storage_cql:bootstrap(Options, NS),
    {mg_core_machine_storage_cql, Options};
bootstrap_machine_storage(memory, _NS, _Processor) ->
    {mg_core_machine_storage_kvs, #{kvs => mg_core_storage_memory}}.

build_machine_storage_cql_options(Processor, Options) when
    Processor == mg_core_events_machine;
    Processor == mg_core_machine_tags
->
    Options#{processor => Processor};
build_machine_storage_cql_options(Processor, Options) ->
    % NOTE
    % Assuming bootstrapping performed in the same module which is usual for test code.
    Options#{schema => Processor}.

-spec bootstrap_events_storage(cql | memory, mg_core:ns()) ->
    mg_core_events_storage:options().
bootstrap_events_storage(cql, NS) ->
    Options = #{
        node => {"scylla0", 9042},
        keyspace => mg
    },
    ok = mg_core_events_storage_cql:teardown(Options, NS),
    ok = mg_core_events_storage_cql:bootstrap(Options, NS),
    {mg_core_events_storage_cql, Options};
bootstrap_events_storage(memory, _NS) ->
    {mg_core_events_storage_kvs, #{kvs => mg_core_storage_memory}}.

-spec stop_wait_all([pid()], _Reason, timeout()) -> ok.
stop_wait_all(Pids, Reason, Timeout) ->
    FlagWas = erlang:process_flag(trap_exit, true),
    TRef = erlang:start_timer(Timeout, self(), stop_timeout),
    ok = lists:foreach(fun(Pid) -> erlang:exit(Pid, Reason) end, Pids),
    ok = await_stop(Pids, Reason, TRef),
    _ = erlang:process_flag(trap_exit, FlagWas),
    ok.

-spec await_stop([pid()], _Reason, reference()) -> ok.
await_stop([Pid | Rest], Reason, TRef) ->
    receive
        {'EXIT', Pid, Reason} ->
            await_stop(Rest, Reason, TRef);
        {timeout, TRef, Error} ->
            erlang:exit(Error)
    end;
await_stop([], _Reason, TRef) ->
    _ = erlang:cancel_timer(TRef),
    receive
        {timeout, TRef, _} -> ok
    after 0 -> ok
    end.

%%

-type category() :: atom().

-spec handle_beat
    (consuela_client:beat(), {client, category()}) -> ok;
    (consuela_session_keeper:beat(), {keeper, category()}) -> ok;
    (consuela_zombie_reaper:beat(), {reaper, category()}) -> ok.

handle_beat(Beat, {Producer, Category}) ->
    ct:pal(Category, "[~p] ~p", [Producer, Beat]);
handle_beat(_Beat, _) ->
    ok.

-spec assert_poll_minimum_time({ok, pos_integer()} | {error, timeout}, non_neg_integer()) ->
    boolean() | {error, timeout}.
assert_poll_minimum_time({error, timeout}, _TargetCutoff) ->
    {error, timeout};
assert_poll_minimum_time({ok, TimeSpent}, TargetCutoff) when TimeSpent >= TargetCutoff ->
    true;
assert_poll_minimum_time({ok, TimeSpent}, TargetCutoff) when TimeSpent =< TargetCutoff ->
    _ = ct:pal(
        error,
        "Polling took ~p seconds, which is shorter then the target ~p seconds.",
        [TimeSpent, TargetCutoff]
    ),
    false.

-spec poll_for_value(fun(), term(), pos_integer()) -> {ok, pos_integer()} | {error, timeout}.
poll_for_value(Fun, Wanted, MaxTime) ->
    poll_for_value(Fun, Wanted, MaxTime, 0).

-spec poll_for_value(fun(), term(), pos_integer(), non_neg_integer()) ->
    {ok, pos_integer()} | {error, timeout}.
poll_for_value(_Fun, _Wanted, MaxTime, TimeAcc) when TimeAcc > MaxTime ->
    {error, timeout};
poll_for_value(Fun, Wanted, MaxTime, TimeAcc) ->
    Time0 = erlang:system_time(millisecond),
    case Fun() of
        Wanted ->
            {ok, TimeAcc};
        Other ->
            _ = ct:pal("poll_for_value: ~p", [Other]),
            _ = timer:sleep(100),
            poll_for_value(Fun, Wanted, MaxTime, TimeAcc + (erlang:system_time(millisecond) - Time0))
    end.

-spec poll_for_exception(fun(), term(), pos_integer()) -> {ok, pos_integer()} | {error, timeout}.
poll_for_exception(Fun, Wanted, MaxTime) ->
    poll_for_exception(Fun, Wanted, MaxTime, 0).

-spec poll_for_exception(fun(), term(), pos_integer(), non_neg_integer()) ->
    {ok, pos_integer()} | {error, timeout}.
poll_for_exception(_Fun, _Wanted, MaxTime, TimeAcc) when TimeAcc > MaxTime ->
    {error, timeout};
poll_for_exception(Fun, Wanted, MaxTime, TimeAcc) ->
    Time0 = erlang:system_time(millisecond),
    try Fun() of
        Value ->
            _ = ct:pal("poll_for_exception: ~p", [Value]),
            _ = timer:sleep(100),
            poll_for_exception(Fun, Wanted, MaxTime, TimeAcc + (erlang:system_time(millisecond) - Time0))
    catch
        throw:Wanted ->
            {ok, TimeAcc}
    end.
