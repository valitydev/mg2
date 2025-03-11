%%%
%%% Copyright 2025 Valitydev
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

-module(mg_woody_automaton).

%% API
-export_type([options/0]).
-export_type([machine_simple/0]).
-export([handler/1]).

%% woody handler
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% API types
-type options() :: #{mg_core:ns() => ns_options()}.
-type ns_options() :: #{
    machine := mg_core_events_machine:options(),
    modernizer => mg_core_events_modernizer:options()
}.
-type machine_status_simple() :: working | {failed, Reason :: binary()}.
%% Exactly mg_core_events_machine:machine(), except some information is lost about it's status
-type machine_simple() :: #{
    ns := mg_core:ns(),
    id := mg_core:id(),
    history := [mg_core_events:event()],
    history_range := mg_core_events:history_range(),
    aux_state := mg_core_events:content(),
    timer := {genlib_time:ts(), mg_core:request_context(), pos_integer(), mg_core_events:history_range()},
    status => machine_status_simple()
}.

%%
%% API
%%

-spec handler(options()) -> mg_woody_utils:woody_handler().
handler(Options) ->
    {"/v1/automaton", {{mg_proto_state_processing_thrift, 'Automaton'}, {?MODULE, Options}}}.

%%
%% woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().
handle_function(Fun, Args, WoodyContext, Options) ->
    {NS, ID} = parse_args(Args),
    NsOpts = maps:get(NS, Options, #{}),
    case NsOpts of
        #{machine := #{engine := progressor}} ->
            ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
            Deadline = get_deadline(NS, WoodyContext, Options),
            {ok, _} =
                Result = mg_woody_utils:handle_error(
                    #{
                        namespace => NS,
                        machine_id => ID,
                        request_context => ReqCtx,
                        deadline => Deadline
                    },
                    fun() -> mg_progressor:handle_function(Fun, Args, ReqCtx) end,
                    pulse(NS, Options)
                ),
            Result;
        _ ->
            handle_function_(Fun, Args, WoodyContext, Options)
    end.

parse_args({NS, IDIn, _Args}) ->
    ID = mg_woody_packer:unpack(id, IDIn),
    {NS, ID};
parse_args({MachineDesc, _Args}) when is_tuple(MachineDesc) ->
    {NS, ID, _Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    {NS, ID};
parse_args({NS, RefIn}) when is_binary(NS) ->
    ID = mg_woody_packer:unpack(ref, RefIn),
    {NS, ID};
parse_args({MachineDesc}) ->
    {NS, ID, _Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    {NS, ID}.

-spec handle_function_(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().
handle_function_('Start', {NS, IDIn, Args}, WoodyContext, Options) ->
    ID = mg_woody_packer:unpack(id, IDIn),
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    Deadline = get_deadline(NS, WoodyContext, Options),
    ok = mg_woody_utils:handle_error(
        #{
            namespace => NS,
            machine_id => ID,
            request_context => ReqCtx,
            deadline => Deadline
        },
        fun() ->
            mg_core_events_machine:start(
                get_machine_options(NS, Options),
                ID,
                mg_woody_packer:unpack(args, Args),
                ReqCtx,
                Deadline
            )
        end,
        pulse(NS, Options)
    ),
    {ok, ok};
handle_function_('Repair', {MachineDesc, Args}, WoodyContext, Options) ->
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    {NS, ID, Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    Deadline = get_deadline(NS, WoodyContext, Options),
    Response =
        mg_woody_utils:handle_error(
            #{namespace => NS, machine_id => ID, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_core_events_machine:repair(
                    get_machine_options(NS, Options),
                    ID,
                    mg_woody_packer:unpack(args, Args),
                    Range,
                    ReqCtx,
                    Deadline
                )
            end,
            pulse(NS, Options)
        ),
    case Response of
        {ok, Reply} ->
            {ok, mg_woody_packer:pack(repair_response, Reply)};
        {error, {failed, Reason}} ->
            woody_error:raise(business, mg_woody_packer:pack(repair_error, Reason))
    end;
handle_function_('SimpleRepair', {NS, RefIn}, WoodyContext, Options) ->
    Deadline = get_deadline(NS, WoodyContext, Options),
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    ID = mg_woody_packer:unpack(ref, RefIn),
    ok = mg_woody_utils:handle_error(
        #{namespace => NS, machine_id => ID, request_context => ReqCtx, deadline => Deadline},
        fun() ->
            mg_core_events_machine:simple_repair(
                get_machine_options(NS, Options),
                ID,
                ReqCtx,
                Deadline
            )
        end,
        pulse(NS, Options)
    ),
    {ok, ok};
handle_function_('Call', {MachineDesc, Args}, WoodyContext, Options) ->
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    {NS, ID, Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    Deadline = get_deadline(NS, WoodyContext, Options),
    Response =
        mg_woody_utils:handle_error(
            #{namespace => NS, machine_id => ID, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_core_events_machine:call(
                    get_machine_options(NS, Options),
                    ID,
                    mg_woody_packer:unpack(args, Args),
                    Range,
                    ReqCtx,
                    Deadline
                )
            end,
            pulse(NS, Options)
        ),
    {ok, mg_woody_packer:pack(call_response, Response)};
handle_function_('GetMachine', {MachineDesc}, WoodyContext, Options) ->
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    {NS, ID, Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    Machine =
        mg_woody_utils:handle_error(
            #{namespace => NS, machine_id => ID, request_context => ReqCtx},
            fun() ->
                mg_core_events_machine:get_machine(
                    get_machine_options(NS, Options),
                    ID,
                    Range
                )
            end,
            pulse(NS, Options)
        ),
    {ok, mg_woody_packer:pack(machine_simple, simplify_core_machine(Machine))};
handle_function_('Remove', {NS, IDIn}, WoodyContext, Options) ->
    ID = mg_woody_packer:unpack(id, IDIn),
    Deadline = get_deadline(NS, WoodyContext, Options),
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    ok = mg_woody_utils:handle_error(
        #{namespace => NS, machine_id => ID, request_context => ReqCtx, deadline => Deadline},
        fun() ->
            mg_core_events_machine:remove(
                get_machine_options(NS, Options),
                ID,
                ReqCtx,
                Deadline
            )
        end,
        pulse(NS, Options)
    ),
    {ok, ok};
handle_function_('Modernize', {MachineDesc}, WoodyContext, Options) ->
    {NS, ID, Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    mg_woody_utils:handle_error(
        #{namespace => NS, machine_id => ID, request_context => ReqCtx},
        fun() ->
            case get_ns_options(NS, Options) of
                #{modernizer := ModernizerOptions, machine := MachineOptions} ->
                    {ok,
                        mg_core_events_modernizer:modernize_machine(
                            ModernizerOptions,
                            MachineOptions,
                            WoodyContext,
                            ID,
                            Range
                        )};
                #{} ->
                    % TODO
                    % Тут нужно отдельное исключение конечно.
                    erlang:throw({logic, namespace_not_found})
            end
        end,
        pulse(NS, Options)
    );
handle_function_('Notify', {MachineDesc, Args}, WoodyContext, Options) ->
    ReqCtx = to_request_context(otel_ctx:get_current(), WoodyContext),
    {NS, ID, Range} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    NotificationID =
        mg_woody_utils:handle_error(
            #{namespace => NS, machine_id => ID, request_context => ReqCtx},
            fun() ->
                mg_core_events_machine:notify(
                    get_machine_options(NS, Options),
                    ID,
                    mg_woody_packer:unpack(args, Args),
                    Range,
                    ReqCtx
                )
            end,
            pulse(NS, Options)
        ),
    {ok, mg_woody_packer:pack(notify_response, NotificationID)}.

%%
%% local
%%
-spec get_machine_options(mg_core:ns(), options()) -> mg_core_events_machine:options().
get_machine_options(Namespace, Options) ->
    maps:get(machine, get_ns_options(Namespace, Options)).

-spec get_ns_options(mg_core:ns(), options()) -> ns_options().
get_ns_options(Namespace, Options) ->
    try
        maps:get(Namespace, Options)
    catch
        error:{badkey, Namespace} ->
            throw({logic, namespace_not_found})
    end.

-spec pulse(mg_core:ns(), options()) -> mpulse:handler().
pulse(Namespace, Options) ->
    try get_machine_options(Namespace, Options) of
        #{machines := #{pulse := Pulse}} ->
            Pulse
    catch
        throw:{logic, namespace_not_found} ->
            undefined
    end.

-spec get_deadline(mg_core:ns(), woody_context:ctx(), options()) -> mg_core_deadline:deadline().
get_deadline(Namespace, WoodyContext, Options) ->
    DefaultTimeout = default_processing_timeout(Namespace, Options),
    mg_woody_utils:get_deadline(WoodyContext, mg_core_deadline:from_timeout(DefaultTimeout)).

-spec default_processing_timeout(mg_core:ns(), options()) -> timeout().
default_processing_timeout(Namespace, Options) ->
    try get_machine_options(Namespace, Options) of
        #{default_processing_timeout := V} -> V
    catch
        throw:{logic, namespace_not_found} -> 0
    end.

-spec simplify_core_machine(mg_core_events_machine:machine()) ->
    machine_simple().
simplify_core_machine(#{status := Status} = Machine) ->
    Machine#{status => simplify_machine_status(Status)}.

-spec exception_to_string(mg_utils:exception()) -> binary().
exception_to_string(Exception) ->
    iolist_to_binary(genlib_format:format_exception(Exception)).

-spec simplify_machine_status(mg_core_machine:machine_status()) -> machine_status_simple().
simplify_machine_status({error, Reason, _}) ->
    {failed, exception_to_string(Reason)};
simplify_machine_status(_) ->
    working.

-spec to_request_context(otel_ctx:t(), woody_context:ctx()) -> mg_core:request_context().
to_request_context(OtelContext, WoodyContext) ->
    #{
        <<"otel">> => mg_core_otel:pack_otel_stub(OtelContext),
        <<"woody">> => mg_woody_utils:woody_context_to_opaque(WoodyContext)
    }.
