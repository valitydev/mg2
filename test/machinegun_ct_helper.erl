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

-module(machinegun_ct_helper).

-define(CLIENT, mg_kafka_client).
-define(BROKERS, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]).

-define(READINESS_RETRY_STRATEGY, genlib_retry:exponential(10, 2, 1000, 10000)).

-export([config/1]).

-export([start_application/1]).
-export([start_applications/1]).
-export([stop_applications/1]).

-export([await_ready/1]).
-export([riak_ready/0]).

-export([assert_wait_ok/2]).
-export([assert_wait_expected/3]).

-export([build_storage/2]).

-export([stop_wait_all/3]).
-export([flush/0]).

-export([handle_beat/2]).

-type appname() :: atom().

-type option() ::
    kafka_client_name.

-spec config(option()) -> _.
config(kafka_client_name) ->
    ?CLIENT.

-spec start_application(appname() | {appname(), [{atom(), _Value}]}) -> _Deps :: [appname()].
start_application(consuela) ->
    start_application_with(consuela, [
        {registry, #{
            nodename => "consul0",
            namespace => <<"mg">>
        }}
    ]);
start_application(brod) ->
    start_application_with(brod, [
        {clients, [
            {config(kafka_client_name), [
                {endpoints, ?BROKERS},
                {auto_start_producers, true}
            ]}
        ]}
    ]);
start_application({AppName, Env}) ->
    start_application_with(AppName, Env);
start_application(AppName) ->
    genlib_app:start_application(AppName).

-spec start_applications([{appname(), _Config} | appname()]) -> _Deps :: [appname()].
start_applications(Apps) ->
    lists:foldl(fun(App, Deps) -> Deps ++ start_application(App) end, [], Apps).

-spec stop_applications([appname()]) -> ok.
stop_applications(AppNames) ->
    lists:foreach(fun application:stop/1, lists:reverse(AppNames)).

%%

-spec await_ready(fun(() -> ok | _NotOk)) -> ok.
await_ready(Fun) ->
    assert_wait_ok(Fun, ?READINESS_RETRY_STRATEGY).

-spec riak_ready() -> ok | {error, _}.
riak_ready() ->
    case riakc_pb_socket:start_link("riakdb", 8087) of
        {ok, Ref} ->
            pong = riakc_pb_socket:ping(Ref),
            ok = riakc_pb_socket:stop(Ref),
            ok;
        Error ->
            Error
    end.

%%

-spec assert_wait_ok(fun(() -> ok | _NotOk), mg_core_retry:strategy()) -> ok.
assert_wait_ok(Fun, Strategy) ->
    assert_wait_expected(ok, Fun, Strategy).

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

-spec start_application_with(Application, genlib_opts:opts()) -> [Application] when Application :: atom().
start_application_with(App, Env) ->
    _ = application:load(App),
    _ = set_app_env(App, Env),
    start_application(App, temporary).

-spec set_app_env(atom(), genlib_opts:opts()) -> ok.
set_app_env(App, Env) ->
    R = [application:set_env(App, K, V) || {K, V} <- Env],
    _ = lists:all(fun(E) -> E =:= ok end, R) orelse exit(setenv_failed),
    ok.

-spec start_application(Application :: atom(), Type :: atom()) -> [Application] when Application :: atom().
start_application(AppName, Type) ->
    case application:start(AppName, Type) of
        ok ->
            [AppName];
        {error, {already_started, AppName}} ->
            [];
        {error, {Status, DepName}} when Status =:= not_started; Status =:= not_running ->
            start_application(DepName, Type) ++ start_application(AppName, Type);
        {error, Reason} ->
            exit(Reason)
    end.
