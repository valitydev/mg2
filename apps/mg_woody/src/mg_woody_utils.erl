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

-module(mg_woody_utils).
-include_lib("mg_woody/include/pulse.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export([handle_error/3]).
-export([opaque_to_woody_context/1]).
-export([woody_context_to_opaque/1]).
-export([get_deadline/2]).
-export([set_deadline/2]).

% woody
-export_type([woody_handlers/0]).
-export_type([woody_handler/0]).

%%
%% API
%%
-type ctx() :: #{
    namespace := mg_core:ns() | undefined,
    machine_id := mg_core_events_machine:id(),
    deadline => mg_core_deadline:deadline(),
    request_context := mg_core:request_context()
}.
-type pulse() :: mg_core_pulse:handler().

%%
%% Woody
%%
-type woody_handlers() :: [woody_handler()].
-type woody_handler() :: _.

-spec handle_error(ctx(), fun(() -> R), pulse()) -> R.
handle_error(Ctx, F, Pulse) ->
    try
        F()
    catch
        throw:Error:ST ->
            Exception = {throw, Error, ST},
            #{
                namespace := NS,
                machine_id := ID,
                request_context := ReqCtx
            } = Ctx,
            ok = mg_core_pulse:handle_beat(Pulse, #woody_request_handle_error{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                deadline = maps:get(deadline, Ctx, undefined),
                exception = Exception
            }),
            handle_error(Error, ST)
    end.

-spec handle_error(mg_core_machine:thrown_error() | {logic, namespace_not_found}, [_StackItem]) ->
    no_return().
handle_error({logic, Reason}, _ST) ->
    erlang:throw(handle_logic_error(Reason));
% TODO может Reason не прокидывать дальше?
handle_error({transient, Reason}, _ST) ->
    BinaryDescription = erlang:list_to_binary(io_lib:format("~9999p", [Reason])),
    woody_error:raise(system, {internal, resource_unavailable, BinaryDescription});
handle_error({timeout, _} = Reason, _ST) ->
    BinaryDescription = erlang:list_to_binary(io_lib:format("~9999p", [Reason])),
    woody_error:raise(system, {internal, result_unknown, BinaryDescription});
handle_error(UnknownError, ST) ->
    erlang:raise(error, UnknownError, ST).

-spec handle_logic_error(_) -> _.
handle_logic_error(machine_not_found) -> #mg_stateproc_MachineNotFound{};
handle_logic_error(machine_already_exist) -> #mg_stateproc_MachineAlreadyExists{};
handle_logic_error(machine_failed) -> #mg_stateproc_MachineFailed{};
handle_logic_error(machine_already_working) -> #mg_stateproc_MachineAlreadyWorking{};
handle_logic_error(namespace_not_found) -> #mg_stateproc_NamespaceNotFound{};
handle_logic_error(event_sink_not_found) -> #mg_stateproc_EventSinkNotFound{};
% TODO обработать случай создания машины c некорректным ID в рамках thrift
handle_logic_error({invalid_machine_id, _}) -> #mg_stateproc_MachineNotFound{}.

%%
%% packer to opaque
%%
-spec opaque_to_woody_context(mg_core_storage:opaque()) -> woody_context:ctx().
opaque_to_woody_context([1, RPCID, ContextMeta]) ->
    #{
        rpc_id => opaque_to_woody_rpc_id(RPCID),
        meta => ContextMeta,
        %% FIXME
        deadline => undefined
    };
opaque_to_woody_context([1, RPCID]) ->
    #{
        rpc_id => opaque_to_woody_rpc_id(RPCID),
        %% FIXME
        deadline => undefined
    }.

-spec woody_context_to_opaque(woody_context:ctx()) -> mg_core_storage:opaque().
woody_context_to_opaque(#{rpc_id := RPCID, meta := ContextMeta}) ->
    [1, woody_rpc_id_to_opaque(RPCID), ContextMeta];
woody_context_to_opaque(#{rpc_id := RPCID}) ->
    [1, woody_rpc_id_to_opaque(RPCID)].

-spec woody_rpc_id_to_opaque(woody:rpc_id()) -> mg_core_storage:opaque().
woody_rpc_id_to_opaque(#{span_id := SpanID, trace_id := TraceID, parent_id := ParentID}) ->
    [SpanID, TraceID, ParentID].

-spec opaque_to_woody_rpc_id(mg_core_storage:opaque()) -> woody:rpc_id().
opaque_to_woody_rpc_id([SpanID, TraceID, ParentID]) ->
    #{span_id => SpanID, trace_id => TraceID, parent_id => ParentID}.

%%
%% Woody deadline utils
%%
-spec get_deadline(woody_context:ctx(), mg_core_deadline:deadline()) ->
    mg_core_deadline:deadline() | no_return().
get_deadline(Context, Default) ->
    case woody_context:get_deadline(Context) of
        undefined ->
            Default;
        Deadline ->
            %% MG and woody deadline formats are different
            mg_core_deadline:from_unixtime_ms(woody_deadline:to_unixtime_ms(Deadline))
    end.

-spec set_deadline(mg_core_deadline:deadline(), woody_context:ctx()) -> woody_context:ctx().
set_deadline(undefined, Context) ->
    Context;
set_deadline(Deadline, Context) ->
    WoodyDeadline = woody_deadline:from_unixtime_ms(mg_core_deadline:to_unixtime_ms(Deadline)),
    woody_context:set_deadline(WoodyDeadline, Context).
