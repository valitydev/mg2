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

-module(mg_cth).

-include_lib("mg_cth/include/mg_cth.hrl").

-export([config/1]).
-export([kafka_client_config/1]).

-export([start_application/1]).
-export([start_applications/1]).
-export([stop_applications/1]).

%%

-export([await_ready/1]).
-export([riak_ready/0]).

-export([assert_wait_ok/2]).
-export([assert_wait_expected/3]).

-export([build_storage/2]).

-export([stop_wait_all/3]).
-export([flush/0]).

-export([assert_poll_minimum_time/2]).
-export([poll_for_value/3]).
-export([poll_for_exception/3]).

-export([trace_testcase/3]).
-export([maybe_end_testcase_trace/1]).

%%

-define(READINESS_RETRY_STRATEGY, genlib_retry:exponential(10, 2, 1000, 10000)).

-type appname() :: atom().
-type app() :: appname() | {appname(), [{atom(), _Value}]}.
-type config() :: [{atom(), _}].
-type option() :: kafka_client_name.

-spec config(option()) -> _.
config(kafka_client_name) ->
    ?CLIENT.

-spec kafka_client_config([{string(), pos_integer()}]) -> [{atom(), _Value}].
kafka_client_config(Brokers) ->
    [
        {clients, [
            {config(kafka_client_name), [
                {endpoints, Brokers},
                {auto_start_producers, true}
            ]}
        ]}
    ].

-spec epg_connector_config() -> _.
epg_connector_config() ->
    [
        {databases, #{
            progressor_db => #{
                host => "postgres",
                port => 5432,
                database => "progressor_db",
                username => "progressor",
                password => "progressor"
            }
        }},
        {pools, #{
            default_pool => #{
                database => progressor_db,
                size => 10
            }
        }}
    ].

-spec progressor_config() -> _.
progressor_config() ->
    [
        {namespaces, #{
            binary_to_atom(?NS) => #{
                processor => #{client => null},
                storage => #{
                    client => prg_pg_backend,
                    options => #{pool => default_pool}
                },
                worker_pool_size => 0
            }
        }}
    ].

-spec start_application(app()) -> _Deps :: [appname()].
start_application(brod) ->
    genlib_app:start_application_with(brod, kafka_client_config(?BROKERS));
start_application(epg_connector) ->
    genlib_app:start_application_with(epg_connector, epg_connector_config());
start_application(progressor) ->
    genlib_app:start_application_with(progressor, progressor_config());
start_application({AppName, Env}) ->
    genlib_app:start_application_with(AppName, Env);
start_application(AppName) ->
    genlib_app:start_application(AppName).

-spec start_applications([app()]) -> _Deps :: [appname()].
start_applications(Apps) ->
    lists:foldl(fun(App, Deps) -> Deps ++ start_application(App) end, [], Apps).

-spec stop_applications([appname()]) -> ok.
stop_applications(AppNames) ->
    lists:foreach(fun application:stop/1, lists:reverse(AppNames)).

%%

-spec await_ready(fun(() -> ok | _NotOk)) -> ok.
await_ready(Fun) ->
    assert_wait_ok(
        fun() ->
            try
                Fun()
            catch
                C:E:Stacktrace -> {C, E, Stacktrace}
            end
        end,
        ?READINESS_RETRY_STRATEGY
    ).

-spec riak_ready() -> ok | {error, _}.
riak_ready() ->
    case riakc_pb_socket:start("riakdb", 8087) of
        {ok, Ref} ->
            pong = riakc_pb_socket:ping(Ref),
            ok = riakc_pb_socket:stop(Ref),
            ok;
        Error ->
            Error
    end.

%%

-spec assert_wait_ok(fun(() -> ok | _NotOk), genlib_retry:strategy()) -> ok.
assert_wait_ok(Fun, Strategy) ->
    assert_wait_expected(ok, Fun, Strategy).

-spec assert_wait_expected(any(), function(), genlib_retry:strategy()) -> ok.
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

-spec build_storage(mg_core:ns(), mg_utils:mod_opts()) -> mg_utils:mod_opts().
build_storage(NS, Module) when is_atom(Module) ->
    build_storage(NS, {Module, #{}});
build_storage(NS, {Module, Options}) ->
    {Module, Options#{name => erlang:binary_to_atom(NS, utf8)}}.

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

-spec flush() -> [term()].
flush() ->
    receive
        Anything -> [Anything | flush()]
    after 0 -> []
    end.

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

-spec trace_testcase(module(), atom(), config()) -> config().
trace_testcase(Mod, Name, C) ->
    SpanName = iolist_to_binary([atom_to_binary(Mod), ":", atom_to_binary(Name), "/1"]),
    SpanCtx = otel_tracer:start_span(opentelemetry:get_application_tracer(Mod), SpanName, #{kind => internal}),
    %% NOTE This also puts otel context to process dictionary
    _ = otel_tracer:set_current_span(SpanCtx),
    [{span_ctx, SpanCtx} | C].

-spec maybe_end_testcase_trace(config()) -> ok.
maybe_end_testcase_trace(C) ->
    case lists:keyfind(span_ctx, 1, C) of
        {span_ctx, SpanCtx} ->
            _ = otel_span:end_span(SpanCtx),
            ok;
        _ ->
            ok
    end.
