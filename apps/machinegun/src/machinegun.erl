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
    ChildSpecs = mg_configurator:construct_child_specs(Config),
    genlib_adhoc_supervisor:start_link(
        {local, ?MODULE},
        #{strategy => rest_for_one},
        ChildSpecs
    ).

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%% Internals

-spec setup_metrics() -> ok.
setup_metrics() ->
    ok = woody_ranch_prometheus_collector:setup(),
    ok = woody_hackney_prometheus_collector:setup(),
    ok = mg_pulse_prometheus:setup(),
    ok = mg_event_sink_kafka_prometheus_pulse:setup(),
    ok = mg_riak_prometheus:setup().
