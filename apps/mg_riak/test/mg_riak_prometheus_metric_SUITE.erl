%%%
%%% Copyright 2020 RBKmoney
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

-module(mg_riak_prometheus_metric_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("mg_riak/include/pulse.hrl").
-include_lib("prometheus/include/prometheus_model.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([riak_client_get_start_test/1]).
-export([riak_client_get_finish_test/1]).
-export([riak_client_put_start_test/1]).
-export([riak_client_put_finish_test/1]).
-export([riak_client_search_start_test/1]).
-export([riak_client_search_finish_test/1]).
-export([riak_client_delete_start_test/1]).
-export([riak_client_delete_finish_test/1]).
-export([riak_pool_no_free_connection_errors_test/1]).
-export([riak_pool_queue_limit_reached_errors_test/1]).
-export([riak_pool_killed_free_connections_test/1]).
-export([riak_pool_killed_in_use_connections_test/1]).
-export([riak_pool_connect_timeout_errors_test/1]).

-export([riak_pool_collector_test/1]).

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
        {group, beats},
        {group, collectors}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {beats, [parallel], [
            riak_client_get_start_test,
            riak_client_get_finish_test,
            riak_client_put_start_test,
            riak_client_put_finish_test,
            riak_client_search_start_test,
            riak_client_search_finish_test,
            riak_client_delete_start_test,
            riak_client_delete_finish_test,
            riak_pool_no_free_connection_errors_test,
            riak_pool_queue_limit_reached_errors_test,
            riak_pool_killed_free_connections_test,
            riak_pool_killed_in_use_connections_test,
            riak_pool_connect_timeout_errors_test
        ]},
        {collectors, [], [
            riak_pool_collector_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = mg_cth:start_applications([mg_riak]),
    ok = mg_riak_pulse_prometheus:setup(),
    ok = mg_riak_prometheus:setup(),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_cth:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) -> ok.
end_per_group(_, _C) ->
    ok.

%% Tests

-spec riak_client_get_start_test(config()) -> _.
riak_client_get_start_test(_C) ->
    ok = test_beat(#mg_riak_client_get_start{
        name = {?NS, caller, type}
    }).

-spec riak_client_get_finish_test(config()) -> _.
riak_client_get_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_riak_client_get_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_riak_client_operation_duration_seconds, [?NS, type, get]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec riak_client_put_start_test(config()) -> _.
riak_client_put_start_test(_C) ->
    ok = test_beat(#mg_riak_client_put_start{
        name = {?NS, caller, type}
    }).

-spec riak_client_put_finish_test(config()) -> _.
riak_client_put_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_riak_client_put_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_riak_client_operation_duration_seconds, [?NS, type, put]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec riak_client_search_start_test(config()) -> _.
riak_client_search_start_test(_C) ->
    ok = test_beat(#mg_riak_client_search_start{
        name = {?NS, caller, type}
    }).

-spec riak_client_search_finish_test(config()) -> _.
riak_client_search_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_riak_client_search_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_riak_client_operation_duration_seconds, [?NS, type, search]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec riak_client_delete_start_test(config()) -> _.
riak_client_delete_start_test(_C) ->
    ok = test_beat(#mg_riak_client_delete_start{
        name = {?NS, caller, type}
    }).

-spec riak_client_delete_finish_test(config()) -> _.
riak_client_delete_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_riak_client_delete_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_riak_client_operation_duration_seconds, [?NS, type, delete]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec riak_pool_no_free_connection_errors_test(config()) -> _.
riak_pool_no_free_connection_errors_test(_C) ->
    ok = test_beat(#mg_riak_connection_pool_state_reached{
        name = {?NS, caller, type},
        state = no_free_connections
    }),
    ?assertEqual(
        1,
        prometheus_counter:value(mg_riak_pool_no_free_connection_errors_total, [?NS, type])
    ).

-spec riak_pool_queue_limit_reached_errors_test(config()) -> _.
riak_pool_queue_limit_reached_errors_test(_C) ->
    ok = test_beat(#mg_riak_connection_pool_state_reached{
        name = {?NS, caller, type},
        state = queue_limit_reached
    }),
    ?assertEqual(
        1,
        prometheus_counter:value(mg_riak_pool_queue_limit_reached_errors_total, [?NS, type])
    ).

-spec riak_pool_killed_free_connections_test(config()) -> _.
riak_pool_killed_free_connections_test(_C) ->
    ok = test_beat(#mg_riak_connection_pool_connection_killed{
        name = {?NS, caller, type},
        state = free
    }),
    ?assertEqual(
        1,
        prometheus_counter:value(mg_riak_pool_killed_free_connections_total, [?NS, type])
    ).

-spec riak_pool_killed_in_use_connections_test(config()) -> _.
riak_pool_killed_in_use_connections_test(_C) ->
    ok = test_beat(#mg_riak_connection_pool_connection_killed{
        name = {?NS, caller, type},
        state = in_use
    }),
    ?assertEqual(
        1,
        prometheus_counter:value(mg_riak_pool_killed_in_use_connections_total, [?NS, type])
    ).

-spec riak_pool_connect_timeout_errors_test(config()) -> _.
riak_pool_connect_timeout_errors_test(_C) ->
    ok = test_beat(#mg_riak_connection_pool_error{
        name = {?NS, caller, type},
        reason = connect_timeout
    }),
    ?assertEqual(
        1,
        prometheus_counter:value(mg_riak_pool_connect_timeout_errors_total, [?NS, type])
    ).

%%

-spec riak_pool_collector_test(config()) -> _.
riak_pool_collector_test(_C) ->
    ok = mg_cth:await_ready(fun mg_cth:riak_ready/0),
    Storage =
        {mg_riak_storage, #{
            name => {?NS, caller, type},
            host => "riakdb",
            port => 8087,
            bucket => ?NS,
            pool_options => #{
                init_count => 0,
                max_count => 10,
                queue_max => 100
            },
            pulse => undefined,
            sidecar => {mg_riak_prometheus, #{}}
        }},

    {ok, Pid} = genlib_adhoc_supervisor:start_link(
        #{strategy => one_for_all},
        [mg_core_storage:child_spec(Storage, storage)]
    ),

    Collectors = prometheus_registry:collectors(default),
    ?assert(lists:member(mg_riak_prometheus_collector, Collectors)),

    Self = self(),
    ok = prometheus_collector:collect_mf(
        default,
        mg_riak_prometheus_collector,
        fun(MF) -> Self ! MF end
    ),
    MFs = mg_cth:flush(),
    MLabels = [
        #'LabelPair'{name = <<"namespace">>, value = <<"NS">>},
        #'LabelPair'{name = <<"name">>, value = <<"type">>}
    ],
    ?assertMatch(
        [
            #'MetricFamily'{
                name = <<"mg_riak_pool_connections_free">>,
                metric = [#'Metric'{label = MLabels, gauge = #'Gauge'{value = 0}}]
            },
            #'MetricFamily'{
                name = <<"mg_riak_pool_connections_in_use">>,
                metric = [#'Metric'{label = MLabels, gauge = #'Gauge'{value = 0}}]
            },
            #'MetricFamily'{
                name = <<"mg_riak_pool_connections_limit">>,
                metric = [#'Metric'{label = MLabels, gauge = #'Gauge'{value = 10}}]
            },
            #'MetricFamily'{
                name = <<"mg_riak_pool_queued_requests">>,
                metric = [#'Metric'{label = MLabels, gauge = #'Gauge'{value = 0}}]
            },
            #'MetricFamily'{
                name = <<"mg_riak_pool_queued_requests_limit">>,
                metric = [#'Metric'{label = MLabels, gauge = #'Gauge'{value = 100}}]
            }
        ],
        lists:sort(MFs)
    ),

    ok = proc_lib:stop(Pid, normal, 5000).

%% Metrics utils

-spec test_beat(term()) -> ok.
test_beat(Beat) ->
    mg_riak_pulse_prometheus:handle_beat(#{}, Beat).

-spec test_millisecond_buckets() -> #{non_neg_integer() => pos_integer()}.
test_millisecond_buckets() ->
    #{
        0 => 1,
        1 => 1,
        5 => 2,
        10 => 3
    }.
