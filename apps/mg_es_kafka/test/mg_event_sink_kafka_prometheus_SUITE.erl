-module(mg_event_sink_kafka_prometheus_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("mg_es_kafka/include/pulse.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

-export([event_sink_kafka_sent_test/1]).

-define(NS, <<"NS">>).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, beats}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {beats, [parallel], [
            event_sink_kafka_sent_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    C.

-spec end_per_suite(config()) -> ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) -> config().
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> ok.
end_per_group(_, _C) ->
    ok.

%% Tests

-spec event_sink_kafka_sent_test(config()) -> _.
event_sink_kafka_sent_test(_C) ->
    Buckets = test_millisecond_buckets(),
    Name = kafka,
    _ = maps:fold(
        fun(DurationMs, BucketIdx, {Counter, BucketAcc}) ->
            ok = test_beat(#mg_event_sink_kafka_sent{
                name = Name,
                namespace = ?NS,
                machine_id = <<"ID">>,
                request_context = null,
                deadline = undefined,
                encode_duration = erlang:convert_time_unit(DurationMs, millisecond, native),
                send_duration = erlang:convert_time_unit(DurationMs, millisecond, native),
                data_size = 0,
                partition = 0,
                offset = 0
            }),
            ?assertEqual(prometheus_counter:value(mg_event_sink_produced_total, [?NS, Name]), Counter),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_event_sink_kafka_produced_duration_seconds, [?NS, Name, encode]),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_event_sink_kafka_produced_duration_seconds, [?NS, Name, send]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, BucketAcc, 0) + 1, BucketHit),
            {Counter + 1, BucketAcc#{BucketIdx => BucketHit}}
        end,
        {1, #{}},
        Buckets
    ).

%% Metrics utils

-spec test_beat(term()) -> ok.
test_beat(Beat) ->
    mg_event_sink_kafka_prometheus_pulse:handle_beat(#{}, Beat).

-spec test_millisecond_buckets() -> #{non_neg_integer() => pos_integer()}.
test_millisecond_buckets() ->
    #{
        0 => 1,
        1 => 1,
        5 => 2,
        10 => 3
    }.
