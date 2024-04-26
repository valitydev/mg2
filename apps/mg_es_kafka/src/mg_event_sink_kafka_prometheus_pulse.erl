-module(mg_event_sink_kafka_prometheus_pulse).

-include_lib("mg_es_kafka/include/pulse.hrl").

-export([setup/0]).

%% mg_pulse handler
-behaviour(mg_core_pulse).
-export([handle_beat/2]).

%% internal types
-type beat() :: #mg_event_sink_kafka_sent{} | mg_core_pulse:beat().
-type options() :: #{}.
-type metric_name() :: prometheus_metric:name().
-type metric_label_value() :: term().

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.
handle_beat(_Options, Beat) ->
    ok = dispatch_metrics(Beat).

%%
%% management API
%%

%% Sets all metrics up. Call this when the app starts.
-spec setup() -> ok.
setup() ->
    %% Event sink / kafka
    true = prometheus_counter:declare([
        {name, mg_event_sink_produced_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of Machinegun event sink events."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_event_sink_kafka_produced_duration_seconds},
        {registry, registry()},
        {labels, [namespace, name, action]},
        {buckets, duration_buckets()},
        {duration_unit, seconds},
        {help, "Machinegun event sink addition duration."}
    ]),
    ok.

%% Internals

-spec dispatch_metrics(beat()) -> ok.
% Event sink operations
dispatch_metrics(#mg_event_sink_kafka_sent{
    name = Name,
    namespace = NS,
    encode_duration = EncodeDuration,
    send_duration = SendDuration
}) ->
    ok = inc(mg_event_sink_produced_total, [NS, Name]),
    ok = observe(mg_event_sink_kafka_produced_duration_seconds, [NS, Name, encode], EncodeDuration),
    ok = observe(mg_event_sink_kafka_produced_duration_seconds, [NS, Name, send], SendDuration);
% Unknown
dispatch_metrics(_Beat) ->
    ok.

-spec inc(metric_name(), [metric_label_value()]) -> ok.
inc(Name, Labels) ->
    _ = prometheus_counter:inc(registry(), Name, Labels, 1),
    ok.

-spec observe(metric_name(), [metric_label_value()], number()) -> ok.
observe(Name, Labels, Value) ->
    _ = prometheus_histogram:observe(registry(), Name, Labels, Value),
    ok.

-spec registry() -> prometheus_registry:registry().
registry() ->
    default.

-spec duration_buckets() -> [number()].
duration_buckets() ->
    [
        0.001,
        0.005,
        0.010,
        0.025,
        0.050,
        0.100,
        0.250,
        0.500,
        1,
        2.5,
        5,
        10
    ].
