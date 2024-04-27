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

-module(mg_riak_pulse_prometheus).

-include_lib("mg_riak/include/pulse.hrl").

-export([setup/0]).
-export([handle_beat/2]).

%% internal types
-type beat() :: mg_riak_pulse:beat().
-type options() :: #{}.
-type metric_name() :: prometheus_metric:name().
-type metric_label_value() :: term().

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat() | _OtherBeat) -> ok.
handle_beat(_Options, Beat) ->
    ok = dispatch_metrics(Beat).

%%
%% management API
%%

%% Sets all metrics up. Call this when the app starts.
-spec setup() -> ok.
setup() ->
    % Riak client operations
    true = prometheus_counter:declare([
        {name, mg_riak_client_operation_changes_total},
        {registry, registry()},
        {labels, [namespace, name, operation, change]},
        {help, "Total number of Machinegun riak client operations."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_riak_client_operation_duration_seconds},
        {registry, registry()},
        {labels, [namespace, name, operation]},
        {buckets, duration_buckets()},
        {duration_unit, seconds},
        {help, "Machinegun riak client operation duration."}
    ]),
    %% Riak pool events
    true = prometheus_counter:declare([
        {name, mg_riak_pool_no_free_connection_errors_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of no free connection errors in Machinegun riak pool."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_queue_limit_reached_errors_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of queue limit reached errors in Machinegun riak pool."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_connect_timeout_errors_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of connect timeout errors in Machinegun riak pool."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_killed_free_connections_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of killed free Machinegun riak pool connections."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_killed_in_use_connections_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of killed used Machinegun riak pool connections."}
    ]),
    ok.

%% Internals

-spec dispatch_metrics(beat() | _Other) -> ok.
% Riak client operations
dispatch_metrics(#mg_riak_client_get_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, get, start]);
dispatch_metrics(#mg_riak_client_get_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, get, finish]),
    ok = observe(mg_riak_client_operation_duration_seconds, [NS, Type, get], Duration);
dispatch_metrics(#mg_riak_client_put_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, put, start]);
dispatch_metrics(#mg_riak_client_put_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, put, finish]),
    ok = observe(mg_riak_client_operation_duration_seconds, [NS, Type, put], Duration);
dispatch_metrics(#mg_riak_client_search_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, search, start]);
dispatch_metrics(#mg_riak_client_search_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, search, finish]),
    ok = observe(mg_riak_client_operation_duration_seconds, [NS, Type, search], Duration);
dispatch_metrics(#mg_riak_client_delete_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, delete, start]);
dispatch_metrics(#mg_riak_client_delete_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_riak_client_operation_changes_total, [NS, Type, delete, finish]),
    ok = observe(mg_riak_client_operation_duration_seconds, [NS, Type, delete], Duration);
% Riak pool events
dispatch_metrics(#mg_riak_connection_pool_state_reached{
    name = {NS, _Caller, Type},
    state = no_free_connections
}) ->
    ok = inc(mg_riak_pool_no_free_connection_errors_total, [NS, Type]);
dispatch_metrics(#mg_riak_connection_pool_state_reached{
    name = {NS, _Caller, Type},
    state = queue_limit_reached
}) ->
    ok = inc(mg_riak_pool_queue_limit_reached_errors_total, [NS, Type]);
dispatch_metrics(#mg_riak_connection_pool_connection_killed{name = {NS, _Caller, Type}, state = free}) ->
    ok = inc(mg_riak_pool_killed_free_connections_total, [NS, Type]);
dispatch_metrics(#mg_riak_connection_pool_connection_killed{name = {NS, _Caller, Type}, state = in_use}) ->
    ok = inc(mg_riak_pool_killed_in_use_connections_total, [NS, Type]);
dispatch_metrics(#mg_riak_connection_pool_error{name = {NS, _Caller, Type}, reason = connect_timeout}) ->
    ok = inc(mg_riak_pool_connect_timeout_errors_total, [NS, Type]);
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
