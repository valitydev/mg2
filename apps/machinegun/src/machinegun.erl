-module(machinegun).

%% API
-export([start/0]).
-export([stop/0]).

%% application callbacks
-behaviour(application).

-export([start/2]).
-export([stop/1]).

-spec start() -> {ok, _}.
start() ->
    application:ensure_all_started(?MODULE).

-spec stop() -> ok.
stop() ->
    application:stop(?MODULE).

-spec start(_, _) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
    Config = maps:from_list(genlib_app:env(?MODULE)),
    ok = setup_metrics(),
    ChildSpecs = mg_conf:construct_child_specs(Config, additional_routes(Config)),
    genlib_adhoc_supervisor:start_link({local, ?MODULE}, #{strategy => rest_for_one}, ChildSpecs).

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%% Internals

-spec setup_metrics() -> ok.
setup_metrics() ->
    ok = woody_ranch_prometheus_collector:setup(),
    ok = woody_hackney_prometheus_collector:setup(),
    ok = mg_pulse_prometheus:setup(),
    ok = mg_riak_pulse_prometheus:setup(),
    ok = mg_riak_prometheus:setup(),
    ok = mg_event_sink_kafka_prometheus_pulse:setup().

%% TODO Maybe move those to `mg_conf'.

-spec additional_routes(mg_conf:config()) -> [woody_server_thrift_http_handler:route(any())].
additional_routes(Config) ->
    HealthChecks = maps:get(health_check, Config, #{}),
    [
        get_startup_route(),
        get_health_route(HealthChecks),
        get_prometheus_route()
    ].

-spec get_startup_route() -> {iodata(), module(), _Opts :: any()}.
get_startup_route() ->
    EvHandler = {erl_health_event_handler, []},
    Check = #{
        startup => #{
            runner => {mg_health_check, startup, []},
            event_handler => EvHandler
        }
    },
    erl_health_handle:get_startup_route(Check).

-spec get_health_route(erl_health:check()) -> {iodata(), module(), _Opts :: any()}.
get_health_route(Check0) ->
    EvHandler = {erl_health_event_handler, []},
    Check = maps:map(fun(_, V = {_, _, _}) -> #{runner => V, event_handler => EvHandler} end, Check0),
    erl_health_handle:get_route(Check).

-spec get_prometheus_route() -> {iodata(), module(), _Opts :: any()}.
get_prometheus_route() ->
    {"/metrics/[:registry]", prometheus_cowboy2_handler, []}.
