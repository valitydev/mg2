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

-module(mg_woody_packer).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_msgpack_thrift.hrl").

%% API
-export([pack/2]).
-export([unpack/2]).

%%
%% API
%%
-spec pack(_, _) -> _.

%% system
pack(_, undefined) ->
    undefined;
pack(binary, Binary) when is_binary(Binary) ->
    Binary;
pack(opaque, Opaque) ->
    pack_opaque(Opaque);
pack(integer, Integer) when is_integer(Integer) ->
    % TODO check size
    Integer;
pack(timestamp_s, Timestamp) when is_integer(Timestamp) ->
    genlib_rfc3339:format(Timestamp, second);
pack(timestamp_ns, Timestamp) when is_integer(Timestamp) ->
    %% Actually rfc3339:format/2 force conversion of nanoseconds
    %% to microseconds, while system formatter does not,
    %% so force microseconds here for backward compatibility
    Micros = erlang:convert_time_unit(Timestamp, nanosecond, microsecond),
    genlib_rfc3339:format_relaxed(Micros, microsecond);
pack(datetime, Datetime) when is_tuple(Datetime) ->
    Seconds = genlib_time:daytime_to_unixtime(Datetime),
    genlib_rfc3339:format_relaxed(Seconds, second);
pack({list, T}, Values) ->
    [pack(T, Value) || Value <- Values];
%% mg base
pack(ns, NS) ->
    pack(binary, NS);
pack(id, ID) ->
    pack(binary, ID);
pack(args, Args) ->
    pack(opaque, Args);
pack(timeout, Timeout) ->
    pack(integer, Timeout);
pack(timer, {deadline, Deadline}) ->
    {deadline, pack(datetime, Deadline)};
pack(timer, {timeout, Timeout}) ->
    {timeout, pack(timeout, Timeout)};
pack(ref, ID) ->
    {id, pack(id, ID)};
pack(direction, Direction) ->
    Direction;
pack(content, {Metadata, Data}) ->
    #mg_stateproc_Content{
        format_version = pack(integer, maps:get(format_version, Metadata, undefined)),
        data = pack(opaque, Data)
    };
%% events and history
pack(aux_state, AuxState) ->
    pack(content, AuxState);
pack(event_id, ID) ->
    pack(integer, ID);
pack(event_body, Body) ->
    pack(content, Body);
pack(event, #{id := ID, created_at := CreatedAt, body := Body}) ->
    #mg_stateproc_Content{
        format_version = FormatVersion,
        data = Data
    } = pack(event_body, Body),
    #mg_stateproc_Event{
        id = pack(event_id, ID),
        created_at = pack(timestamp_ns, CreatedAt),
        format_version = FormatVersion,
        data = Data
    };
pack(history, History) ->
    pack({list, event}, History);
pack(machine_simple, Machine) ->
    #{
        ns := NS,
        id := ID,
        history_range := HRange,
        history := History,
        aux_state := AuxState,
        timer := Timer
    } = Machine,
    #mg_stateproc_Machine{
        ns = pack(ns, NS),
        id = pack(id, ID),
        history = pack(history, History),
        history_range = pack(history_range, HRange),
        aux_state = pack(aux_state, AuxState),
        timer = pack(int_timer, Timer),
        status = pack(machine_status, maps:get(status, Machine, undefined))
    };
pack(machine_status, working) ->
    {working, #mg_stateproc_MachineStatusWorking{}};
pack(machine_status, {failed, Reason}) ->
    {failed, #mg_stateproc_MachineStatusFailed{reason = Reason}};
pack(machine_event, #{ns := NS, id := ID, event := Event}) ->
    #mg_stateproc_MachineEvent{
        ns = pack(ns, NS),
        id = pack(id, ID),
        event = pack(event, Event)
    };
pack(int_timer, {Timestamp, _, _, _}) ->
    % TODO сделать нормально
    pack(timestamp_s, Timestamp);
%% actions
pack(complex_action, ComplexAction) ->
    #mg_stateproc_ComplexAction{
        timer = pack(timer_action, maps:get(timer, ComplexAction, undefined)),
        remove = pack(remove_action, maps:get(remove, ComplexAction, undefined))
    };
pack(timer_action, {set_timer, Timer, HRange, HandlingTimeout}) ->
    {set_timer, #mg_stateproc_SetTimerAction{
        timer = pack(timer, Timer),
        range = pack(history_range, HRange),
        timeout = pack(integer, HandlingTimeout)
    }};
pack(timer_action, unset_timer) ->
    {unset_timer, #mg_stateproc_UnsetTimerAction{}};
pack(remove_action, remove) ->
    #mg_stateproc_RemoveAction{};
%% calls, signals, get_gistory
pack(state_change, {AuxState, EventBodies}) ->
    #mg_stateproc_MachineStateChange{
        aux_state = pack(aux_state, AuxState),
        events = pack({list, event_body}, EventBodies)
    };
pack(signal, timeout) ->
    {timeout, #mg_stateproc_TimeoutSignal{}};
pack(signal, {init, Args}) ->
    {init, #mg_stateproc_InitSignal{arg = pack(args, Args)}};
pack(signal, {notification, Args}) ->
    {notification, #mg_stateproc_NotificationSignal{arg = pack(args, Args)}};
pack(call_response, CallResponse) ->
    pack(opaque, CallResponse);
pack(repair_response, RepairResponse) ->
    pack(opaque, RepairResponse);
pack(notify_response, NotificationID) ->
    #mg_stateproc_NotifyResponse{
        id = NotificationID
    };
pack(repair_error, #{reason := Reason}) ->
    #mg_stateproc_RepairFailed{
        reason = pack(opaque, Reason)
    };
pack(signal_args, {Signal, Machine}) ->
    #mg_stateproc_SignalArgs{
        signal = pack(signal, Signal),
        machine = pack(machine_simple, Machine)
    };
pack(call_args, {Args, Machine}) ->
    #mg_stateproc_CallArgs{
        arg = pack(args, Args),
        machine = pack(machine_simple, Machine)
    };
pack(repair_args, {Args, Machine}) ->
    #mg_stateproc_RepairArgs{
        arg = pack(args, Args),
        machine = pack(machine_simple, Machine)
    };
pack(signal_result, {StateChange, ComplexAction}) ->
    #mg_stateproc_SignalResult{
        change = pack(state_change, StateChange),
        action = pack(complex_action, ComplexAction)
    };
pack(call_result, {Response, StateChange, ComplexAction}) ->
    #mg_stateproc_CallResult{
        response = pack(call_response, Response),
        change = pack(state_change, StateChange),
        action = pack(complex_action, ComplexAction)
    };
pack(modernize_result, EventBody) ->
    #mg_stateproc_ModernizeEventResult{
        event_payload = pack(event_body, EventBody)
    };
pack(repair_result, {Response, StateChange, ComplexAction}) ->
    #mg_stateproc_RepairResult{
        response = pack(repair_response, Response),
        change = pack(state_change, StateChange),
        action = pack(complex_action, ComplexAction)
    };
pack(history_range, {After, Limit, Direction}) ->
    #mg_stateproc_HistoryRange{
        'after' = pack(event_id, After),
        limit = pack(integer, Limit),
        direction = pack(direction, Direction)
    };
pack(machine_descriptor, {NS, Ref, Range}) ->
    #mg_stateproc_MachineDescriptor{
        ns = pack(ns, NS),
        ref = pack(ref, Ref),
        range = pack(history_range, Range)
    };
pack(Type, Value) ->
    erlang:error(badarg, [Type, Value]).

%%

-spec unpack(_, _) -> _.
%% system
unpack(_, undefined) ->
    undefined;
unpack(binary, Binary) when is_binary(Binary) ->
    Binary;
unpack(opaque, Opaque) ->
    unpack_opaque(Opaque);
unpack(integer, Integer) when is_integer(Integer) ->
    % TODO check size
    Integer;
unpack(timestamp_s, Timestamp) when is_binary(Timestamp) ->
    genlib_rfc3339:parse(Timestamp, second);
unpack(timestamp_ns, Timestamp) when is_binary(Timestamp) ->
    genlib_rfc3339:parse(Timestamp, nanosecond);
unpack(datetime, Datetime) when is_binary(Datetime) ->
    parse_datetime(Datetime);
unpack({list, T}, Values) ->
    [unpack(T, Value) || Value <- Values];
%% mg base
unpack(ns, NS) ->
    unpack(binary, NS);
unpack(id, ID) ->
    unpack(binary, ID);
unpack(args, Args) ->
    unpack(opaque, Args);
unpack(timeout, Timeout) ->
    unpack(integer, Timeout);
unpack(timer, {deadline, Deadline}) ->
    {deadline, unpack(datetime, Deadline)};
unpack(timer, {timeout, Timeout}) ->
    {timeout, unpack(timeout, Timeout)};
unpack(ref, {id, ID}) ->
    unpack(id, ID);
unpack(direction, Direction) ->
    Direction;
unpack(content, #mg_stateproc_Content{format_version = FormatVersion, data = Data}) ->
    {
        genlib_map:compact(#{
            format_version => unpack(integer, FormatVersion)
        }),
        unpack(opaque, Data)
    };
%% events and history
unpack(aux_state, AuxState) ->
    unpack(content, AuxState);
unpack(event_id, ID) ->
    unpack(integer, ID);
unpack(event_body, Body) ->
    unpack(content, Body);
unpack(event_body_legacy, BodyLegacy) ->
    unpack(content, #mg_stateproc_Content{data = BodyLegacy});
unpack(event, Event) ->
    #mg_stateproc_Event{
        id = ID,
        created_at = CreatedAt,
        format_version = FormatVersion,
        data = Data
    } = Event,
    Body = #mg_stateproc_Content{
        format_version = FormatVersion,
        data = Data
    },
    #{
        id => unpack(event_id, ID),
        created_at => unpack(timestamp_ns, CreatedAt),
        body => unpack(event_body, Body)
    };
unpack(history, History) ->
    unpack({list, event}, History);
unpack(machine_simple, #mg_stateproc_Machine{} = Machine) ->
    #mg_stateproc_Machine{
        ns = NS,
        id = ID,
        history_range = HRange,
        history = History,
        aux_state = AuxState,
        timer = Timer,
        status = Status
    } = Machine,
    #{
        ns => unpack(ns, NS),
        id => unpack(id, ID),
        history_range => unpack(history_range, HRange),
        history => unpack(history, History),
        aux_state => unpack(aux_state, AuxState),
        timer => unpack(int_timer, Timer),
        status => unpack(machine_status, Status)
    };
unpack(machine_status, {working, #mg_stateproc_MachineStatusWorking{}}) ->
    working;
unpack(machine_status, {failed, #mg_stateproc_MachineStatusFailed{reason = Reason}}) ->
    {failed, Reason};
unpack(machine_event, #mg_stateproc_MachineEvent{ns = NS, id = ID, event = Event}) ->
    #{
        ns => unpack(ns, NS),
        id => unpack(id, ID),
        event => unpack(event, Event)
    };
unpack(int_timer, Timestamp) ->
    % TODO сделать нормально
    {unpack(timestamp_s, Timestamp), undefined, undefined, undefined};
%% actions
unpack(complex_action, ComplexAction) ->
    #mg_stateproc_ComplexAction{
        timer = TimerAction,
        remove = RemoveAction
    } = ComplexAction,
    #{
        timer => unpack(timer_action, TimerAction),
        remove => unpack(remove_action, RemoveAction)
    };
unpack(timer_action, {set_timer, SetTimerAction}) ->
    #mg_stateproc_SetTimerAction{timer = Timer, range = HRange, timeout = HandlingTimeout} =
        SetTimerAction,
    {set_timer, unpack(timer, Timer), unpack(history_range, HRange), unpack(integer, HandlingTimeout)};
unpack(timer_action, {unset_timer, #mg_stateproc_UnsetTimerAction{}}) ->
    unset_timer;
unpack(remove_action, #mg_stateproc_RemoveAction{}) ->
    remove;
%% calls, signals, get_history
unpack(state_change, MachineStateChange) ->
    #mg_stateproc_MachineStateChange{
        aux_state = AuxState,
        events = EventBodies
    } = MachineStateChange,
    {
        unpack(aux_state, AuxState),
        unpack({list, event_body}, mg_utils:take_defined([EventBodies, []]))
    };
unpack(signal, {timeout, #mg_stateproc_TimeoutSignal{}}) ->
    timeout;
unpack(signal, {init, #mg_stateproc_InitSignal{arg = Args}}) ->
    {init, unpack(args, Args)};
unpack(signal, {notification, #mg_stateproc_NotificationSignal{arg = Args}}) ->
    {notification, unpack(args, Args)};
unpack(call_response, CallResponse) ->
    unpack(opaque, CallResponse);
unpack(repair_response, RepairResponse) ->
    unpack(opaque, RepairResponse);
unpack(notify_response, #mg_stateproc_NotifyResponse{id = NotificationID}) ->
    NotificationID;
unpack(repair_error, #mg_stateproc_RepairFailed{reason = Reason}) ->
    #{reason => unpack(opaque, Reason)};
unpack(signal_args, #mg_stateproc_SignalArgs{signal = Signal, machine = Machine}) ->
    {unpack(signal, Signal), unpack(machine_simple, Machine)};
unpack(call_args, #mg_stateproc_CallArgs{arg = Args, machine = Machine}) ->
    {unpack(args, Args), unpack(machine_simple, Machine)};
unpack(repair_args, #mg_stateproc_RepairArgs{arg = Args, machine = Machine}) ->
    {unpack(args, Args), unpack(machine_simple, Machine)};
unpack(signal_result, #mg_stateproc_SignalResult{change = StateChange, action = ComplexAction}) ->
    {
        unpack(state_change, StateChange),
        unpack(complex_action, ComplexAction)
    };
unpack(call_result, #mg_stateproc_CallResult{
    response = Response,
    change = StateChange,
    action = ComplexAction
}) ->
    {
        unpack(call_response, Response),
        unpack(state_change, StateChange),
        unpack(complex_action, ComplexAction)
    };
unpack(repair_result, #mg_stateproc_RepairResult{
    response = Response,
    change = StateChange,
    action = ComplexAction
}) ->
    {
        unpack(repair_response, Response),
        unpack(state_change, StateChange),
        unpack(complex_action, ComplexAction)
    };
unpack(modernize_result, #mg_stateproc_ModernizeEventResult{event_payload = EventBody}) ->
    unpack(event_body, EventBody);
unpack(history_range, #mg_stateproc_HistoryRange{
    'after' = After,
    limit = Limit,
    direction = Direction
}) ->
    {unpack(event_id, After), unpack(integer, Limit), unpack(direction, Direction)};
unpack(machine_descriptor, #mg_stateproc_MachineDescriptor{ns = NS, ref = Ref, range = Range}) ->
    {unpack(ns, NS), unpack(ref, Ref), unpack(history_range, Range)};
unpack(Type, Value) ->
    erlang:error(badarg, [Type, Value]).

%%

-spec pack_opaque(mg_core_storage:opaque()) -> mg_proto_msgpack_thrift:'Value'().
pack_opaque(null) ->
    {nl, #mg_msgpack_Nil{}};
pack_opaque(Boolean) when is_boolean(Boolean) ->
    {b, Boolean};
pack_opaque(Integer) when is_integer(Integer) ->
    {i, Integer};
pack_opaque(Float) when is_float(Float) ->
    {flt, Float};
pack_opaque({string, String}) ->
    {str, unicode:characters_to_binary(String, unicode)};
pack_opaque(Binary) when is_binary(Binary) ->
    {bin, Binary};
pack_opaque(Object) when is_map(Object) ->
    {obj,
        maps:fold(
            fun(K, V, Acc) -> maps:put(pack_opaque(K), pack_opaque(V), Acc) end,
            #{},
            Object
        )};
pack_opaque(Array) when is_list(Array) ->
    {arr, lists:map(fun pack_opaque/1, Array)};
pack_opaque(Arg) ->
    erlang:error(badarg, [Arg]).

-spec unpack_opaque(mg_proto_msgpack_thrift:'Value'()) -> mg_core_storage:opaque().
unpack_opaque({nl, #mg_msgpack_Nil{}}) ->
    null;
unpack_opaque({b, Boolean}) ->
    Boolean;
unpack_opaque({i, Integer}) ->
    Integer;
unpack_opaque({flt, Float}) ->
    Float;
unpack_opaque({str, BString}) ->
    {string, unicode:characters_to_list(BString, unicode)};
unpack_opaque({bin, Binary}) ->
    Binary;
unpack_opaque({obj, Object}) ->
    maps:fold(fun(K, V, Acc) -> maps:put(unpack_opaque(K), unpack_opaque(V), Acc) end, #{}, Object);
unpack_opaque({arr, Array}) ->
    lists:map(fun unpack_opaque/1, Array);
unpack_opaque(Arg) ->
    erlang:error(badarg, [Arg]).

-spec parse_datetime(binary()) -> calendar:datetime().
parse_datetime(Datetime) ->
    true = genlib_rfc3339:is_utc(Datetime),
    Seconds = genlib_rfc3339:parse(Datetime, second),
    genlib_time:unixtime_to_daytime(Seconds).
