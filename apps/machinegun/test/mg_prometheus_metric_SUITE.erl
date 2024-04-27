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

-module(mg_prometheus_metric_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("mg_scheduler/include/pulse.hrl").
-include_lib("mg_core/include/pulse.hrl").
-include_lib("mg_es_kafka/include/pulse.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

-export([machine_lifecycle_loaded_test/1]).
-export([machine_lifecycle_unloaded_test/1]).
-export([machine_lifecycle_created_test/1]).
-export([machine_lifecycle_removed_test/1]).
-export([machine_lifecycle_failed_test/1]).
-export([machine_lifecycle_committed_suicide_test/1]).
-export([machine_lifecycle_loading_error_test/1]).
-export([machine_lifecycle_transient_error_test/1]).
-export([machine_process_started_test/1]).
-export([machine_process_finished_test/1]).
-export([timer_lifecycle_created_test/1]).
-export([timer_lifecycle_rescheduled_test/1]).
-export([timer_lifecycle_rescheduling_error_test/1]).
-export([timer_lifecycle_removed_test/1]).
-export([timer_process_started_test/1]).
-export([timer_process_finished_test/1]).
-export([scheduler_search_success_test/1]).
-export([scheduler_search_error_test/1]).
-export([scheduler_task_error_test/1]).
-export([scheduler_new_tasks_test/1]).
-export([scheduler_task_started_test/1]).
-export([scheduler_task_finished_test/1]).
-export([scheduler_quota_reserved_test/1]).
-export([worker_call_attempt_test/1]).
-export([worker_start_attempt_test/1]).
-export([storage_get_start_test/1]).
-export([storage_get_finish_test/1]).
-export([storage_put_start_test/1]).
-export([storage_put_finish_test/1]).
-export([storage_search_start_test/1]).
-export([storage_search_finish_test/1]).
-export([storage_delete_start_test/1]).
-export([storage_delete_finish_test/1]).

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
            machine_lifecycle_loaded_test,
            machine_lifecycle_unloaded_test,
            machine_lifecycle_created_test,
            machine_lifecycle_removed_test,
            machine_lifecycle_failed_test,
            machine_lifecycle_committed_suicide_test,
            machine_lifecycle_loading_error_test,
            machine_lifecycle_transient_error_test,
            machine_process_started_test,
            machine_process_finished_test,
            timer_lifecycle_created_test,
            timer_lifecycle_rescheduled_test,
            timer_lifecycle_rescheduling_error_test,
            timer_lifecycle_removed_test,
            timer_process_started_test,
            timer_process_finished_test,
            scheduler_search_success_test,
            scheduler_search_error_test,
            scheduler_task_error_test,
            scheduler_new_tasks_test,
            scheduler_task_started_test,
            scheduler_task_finished_test,
            scheduler_quota_reserved_test,
            worker_call_attempt_test,
            worker_start_attempt_test,
            storage_get_start_test,
            storage_get_finish_test,
            storage_put_start_test,
            storage_put_finish_test,
            storage_search_start_test,
            storage_search_finish_test,
            storage_delete_start_test,
            storage_delete_finish_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = mg_cth:start_applications([
        gproc,
        {machinegun, mg_config()}
    ]),
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

-spec machine_lifecycle_loaded_test(config()) -> _.
machine_lifecycle_loaded_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_loaded{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null
    }).

-spec machine_lifecycle_unloaded_test(config()) -> _.
machine_lifecycle_unloaded_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_unloaded{
        namespace = ?NS,
        machine_id = <<"ID">>
    }).

-spec machine_lifecycle_created_test(config()) -> _.
machine_lifecycle_created_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_created{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null
    }).

-spec machine_lifecycle_removed_test(config()) -> _.
machine_lifecycle_removed_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_removed{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null
    }).

-spec machine_lifecycle_failed_test(config()) -> _.
machine_lifecycle_failed_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_failed{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        deadline = undefined,
        exception = {throw, thrown, []}
    }).

-spec machine_lifecycle_committed_suicide_test(config()) -> _.
machine_lifecycle_committed_suicide_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_committed_suicide{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        suicide_probability = undefined
    }).

-spec machine_lifecycle_loading_error_test(config()) -> _.
machine_lifecycle_loading_error_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_loading_error{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        exception = {throw, thrown, []}
    }).

-spec machine_lifecycle_transient_error_test(config()) -> _.
machine_lifecycle_transient_error_test(_C) ->
    ok = test_beat(#mg_core_machine_lifecycle_transient_error{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        exception = {throw, thrown, []},
        retry_strategy = genlib_retry:new_strategy({linear, infinity, 1}),
        retry_action = finish
    }).

-spec machine_process_started_test(config()) -> _.
machine_process_started_test(_C) ->
    ok = test_beat(#mg_core_machine_process_started{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        processor_impact = timeout,
        deadline = undefined
    }).

-spec machine_process_finished_test(config()) -> _.
machine_process_finished_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_core_machine_process_finished{
                namespace = ?NS,
                machine_id = <<"ID">>,
                request_context = null,
                processor_impact = timeout,
                deadline = undefined,
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_machine_processing_duration_seconds, [?NS, timeout]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec timer_lifecycle_created_test(config()) -> _.
timer_lifecycle_created_test(_C) ->
    ok = test_beat(#mg_core_timer_lifecycle_created{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        target_timestamp = 1
    }).

-spec timer_lifecycle_rescheduled_test(config()) -> _.
timer_lifecycle_rescheduled_test(_C) ->
    ok = test_beat(#mg_core_timer_lifecycle_rescheduled{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        deadline = undefined,
        target_timestamp = 1,
        attempt = 0
    }).

-spec timer_lifecycle_rescheduling_error_test(config()) -> _.
timer_lifecycle_rescheduling_error_test(_C) ->
    ok = test_beat(#mg_core_timer_lifecycle_rescheduling_error{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        deadline = undefined,
        exception = {throw, thrown, []}
    }).

-spec timer_lifecycle_removed_test(config()) -> _.
timer_lifecycle_removed_test(_C) ->
    ok = test_beat(#mg_core_timer_lifecycle_removed{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null
    }).

-spec timer_process_started_test(config()) -> _.
timer_process_started_test(_C) ->
    ok = test_beat(#mg_core_timer_process_started{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        queue = normal,
        target_timestamp = 1,
        deadline = undefined
    }).

-spec timer_process_finished_test(config()) -> _.
timer_process_finished_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_core_timer_process_finished{
                namespace = ?NS,
                machine_id = <<"ID">>,
                request_context = null,
                queue = normal,
                target_timestamp = 1,
                deadline = undefined,
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_timer_processing_duration_seconds, [?NS, normal]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec scheduler_search_success_test(config()) -> _.
scheduler_search_success_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_skd_search_success{
                namespace = ?NS,
                scheduler_name = name,
                delay = 0,
                tasks = [],
                limit = 0,
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_scheduler_scan_duration_seconds, [?NS, name]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec scheduler_search_error_test(config()) -> _.
scheduler_search_error_test(_C) ->
    ok = test_beat(#mg_skd_search_error{
        namespace = ?NS,
        scheduler_name = name,
        exception = {throw, thrown, []}
    }).

-spec scheduler_task_error_test(config()) -> _.
scheduler_task_error_test(_C) ->
    ok = test_beat(#mg_skd_task_error{
        namespace = ?NS,
        machine_id = <<"ID">>,
        scheduler_name = name,
        exception = {throw, thrown, []}
    }).

-spec scheduler_new_tasks_test(config()) -> _.
scheduler_new_tasks_test(_C) ->
    ok = test_beat(#mg_skd_new_tasks{
        namespace = ?NS,
        scheduler_name = name,
        new_tasks_count = 0
    }).

-spec scheduler_task_started_test(config()) -> _.
scheduler_task_started_test(_C) ->
    ok = test_beat(#mg_skd_task_started{
        namespace = ?NS,
        scheduler_name = name,
        machine_id = <<"ID">>,
        task_delay = 0
    }).

-spec scheduler_task_finished_test(config()) -> _.
scheduler_task_finished_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_skd_task_finished{
                namespace = ?NS,
                scheduler_name = name,
                machine_id = <<"ID">>,
                task_delay = 0,
                process_duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_scheduler_task_processing_duration_seconds, [?NS, name]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec scheduler_quota_reserved_test(config()) -> _.
scheduler_quota_reserved_test(_C) ->
    ok = test_beat(#mg_skd_quota_reserved{
        namespace = ?NS,
        scheduler_name = name,
        active_tasks = 0,
        waiting_tasks = 0,
        quota_name = unlimited,
        quota_reserved = 0
    }).

-spec worker_call_attempt_test(config()) -> _.
worker_call_attempt_test(_C) ->
    ok = test_beat(#mg_core_worker_call_attempt{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        deadline = undefined
    }).

-spec worker_start_attempt_test(config()) -> _.
worker_start_attempt_test(_C) ->
    ok = test_beat(#mg_core_worker_start_attempt{
        namespace = ?NS,
        machine_id = <<"ID">>,
        request_context = null,
        msg_queue_len = 0,
        msg_queue_limit = 0
    }).

-spec storage_get_start_test(config()) -> _.
storage_get_start_test(_C) ->
    ok = test_beat(#mg_core_storage_get_start{
        name = {?NS, caller, type}
    }).

-spec storage_get_finish_test(config()) -> _.
storage_get_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_core_storage_get_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_storage_operation_duration_seconds, [?NS, type, get]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec storage_put_start_test(config()) -> _.
storage_put_start_test(_C) ->
    ok = test_beat(#mg_core_storage_put_start{
        name = {?NS, caller, type}
    }).

-spec storage_put_finish_test(config()) -> _.
storage_put_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_core_storage_put_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_storage_operation_duration_seconds, [?NS, type, put]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec storage_search_start_test(config()) -> _.
storage_search_start_test(_C) ->
    ok = test_beat(#mg_core_storage_search_start{
        name = {?NS, caller, type}
    }).

-spec storage_search_finish_test(config()) -> _.
storage_search_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_core_storage_search_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_storage_operation_duration_seconds, [?NS, type, search]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

-spec storage_delete_start_test(config()) -> _.
storage_delete_start_test(_C) ->
    ok = test_beat(#mg_core_storage_delete_start{
        name = {?NS, caller, type}
    }).

-spec storage_delete_finish_test(config()) -> _.
storage_delete_finish_test(_C) ->
    Buckets = test_millisecond_buckets(),
    _ = maps:fold(
        fun(DurationMs, BucketIdx, Acc) ->
            ok = test_beat(#mg_core_storage_delete_finish{
                name = {?NS, caller, type},
                duration = erlang:convert_time_unit(DurationMs, millisecond, native)
            }),
            {BucketsHits, _} =
                prometheus_histogram:value(mg_storage_operation_duration_seconds, [?NS, type, delete]),
            BucketHit = lists:nth(BucketIdx, BucketsHits),
            %% Check that bucket under index BucketIdx received one hit
            ?assertEqual(maps:get(BucketIdx, Acc, 0) + 1, BucketHit),
            Acc#{BucketIdx => BucketHit}
        end,
        #{},
        Buckets
    ).

%% Metrics utils

-spec test_beat(term()) -> ok.
test_beat(Beat) ->
    mg_pulse_prometheus:handle_beat(#{}, Beat).

-spec mg_config() -> list().
mg_config() ->
    [
        {woody_server, #{ip => {0, 0, 0, 0}, port => 8022}},
        {namespaces, #{}},
        {event_sink_ns, #{
            storage => mg_core_storage_memory,
            registry => mg_core_procreg_global
        }},
        {pulse, {mg_pulse, #{}}}
    ].

-spec test_millisecond_buckets() -> #{non_neg_integer() => pos_integer()}.
test_millisecond_buckets() ->
    #{
        0 => 1,
        1 => 1,
        5 => 2,
        10 => 3
    }.
