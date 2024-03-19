%%%
%%% Copyright 2020 Valitydev
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

-module(mg_woody_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_core_events_machine handler
-behaviour(mg_core_events_machine).
-export_type([options/0]).
-export([processor_child_spec/1, process_signal/4, process_call/4, process_repair/4]).

%%
%% mg_core_events_machine handler
%%
-type options() :: woody_client:options().

-spec processor_child_spec(options()) -> supervisor:child_spec().
processor_child_spec(Options) ->
    woody_client:child_spec(Options).

-spec process_signal(Options, ReqCtx, Deadline, SignalArgs) -> Result when
    Options :: options(),
    ReqCtx :: mg_core_events_machine:request_context(),
    Deadline :: mg_core_deadline:deadline(),
    SignalArgs :: mg_core_events_machine:signal_args(),
    Result :: mg_core_events_machine:signal_result().
process_signal(Options, ReqCtx, Deadline, {Signal, Machine}) ->
    {ok, SignalResult} =
        call_processor(
            Options,
            ReqCtx,
            Deadline,
            'ProcessSignal',
            {mg_woody_packer:pack(signal_args, {Signal, Machine})}
        ),
    mg_woody_packer:unpack(signal_result, SignalResult).

-spec process_call(Options, ReqCtx, Deadline, CallArgs) -> mg_core_events_machine:call_result() when
    Options :: options(),
    ReqCtx :: mg_core_events_machine:request_context(),
    Deadline :: mg_core_deadline:deadline(),
    CallArgs :: mg_core_events_machine:call_args().
process_call(Options, ReqCtx, Deadline, {Call, Machine}) ->
    {ok, CallResult} =
        call_processor(
            Options,
            ReqCtx,
            Deadline,
            'ProcessCall',
            {mg_woody_packer:pack(call_args, {Call, Machine})}
        ),
    mg_woody_packer:unpack(call_result, CallResult).

-spec process_repair(Options, ReqCtx, Deadline, RepairArgs) ->
    mg_core_events_machine:repair_result()
when
    Options :: options(),
    ReqCtx :: mg_core_events_machine:request_context(),
    Deadline :: mg_core_deadline:deadline(),
    RepairArgs :: mg_core_events_machine:repair_args().
process_repair(Options, ReqCtx, Deadline, {Args, Machine}) ->
    RepairResult =
        call_processor(
            Options,
            ReqCtx,
            Deadline,
            'ProcessRepair',
            {mg_woody_packer:pack(repair_args, {Args, Machine})}
        ),
    case RepairResult of
        {ok, Result} ->
            {ok, mg_woody_packer:unpack(repair_result, Result)};
        {error, Error} ->
            {error, {failed, mg_woody_packer:unpack(repair_error, Error)}}
    end.

%% TODO Investigate into this value and design of corresponding guard-timer.
-define(KILL_TIMEOUT, 3000).

-spec call_processor(
    options(),
    mg_core_events_machine:request_context(),
    mg_core_deadline:deadline(),
    atom(),
    woody:args()
) -> {ok, term()} | {error, mg_proto_state_processing_thrift:'RepairFailed'()}.
call_processor(Options, ReqCtx, Deadline, Function, Args) ->
    % TODO сделать нормально!
    {ok, TRef} = timer:kill_after(call_duration_limit(Options, Deadline) + ?KILL_TIMEOUT),
    try
        woody_client:call(
            {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
            Options,
            mg_woody_utils:set_deadline(Deadline, request_context_to_woody_context(ReqCtx))
        )
    of
        {ok, _} = Result ->
            Result;
        {exception, Reason} ->
            {error, Reason}
    catch
        error:Reason = {woody_error, {_, resource_unavailable, _}} ->
            throw({transient, {processor_unavailable, Reason}});
        error:Reason = {woody_error, {_, result_unknown, _}} ->
            throw({transient, {processor_unavailable, Reason}})
    after
        {ok, cancel} = timer:cancel(TRef)
    end.

-spec request_context_to_woody_context(mg_core_events_machine:request_context()) ->
    woody_context:ctx().
request_context_to_woody_context(#{<<"woody">> := OpaqueWoodyCtx}) ->
    mg_woody_utils:opaque_to_woody_context(OpaqueWoodyCtx);
request_context_to_woody_context(_Other) ->
    woody_context:new().

-spec call_duration_limit(options(), mg_core_deadline:deadline()) -> timeout().
call_duration_limit(Options, undefined) ->
    TransportOptions = maps:get(transport_opts, Options, #{}),
    %% use default values from hackney:request/5 options
    ConnectTimeout = maps:get(connect_timeout, TransportOptions, 8000),
    % not documented option
    SendTimeout = maps:get(connect_timeout, TransportOptions, 5000),
    RecvTimeout = maps:get(recv_timeout, TransportOptions, 5000),
    RecvTimeout + ConnectTimeout + SendTimeout;
call_duration_limit(_Options, Deadline) ->
    mg_core_deadline:to_timeout(Deadline).
