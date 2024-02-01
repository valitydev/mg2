%%%
%%% Copyright 2022 Valitydev
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

-module(mg_woody_api_life_sink).

-include_lib("machinegun_core/include/pulse.hrl").
-include_lib("mg_proto/include/mg_proto_lifecycle_sink_thrift.hrl").

%% API types

%% This type should actually belong to machinegun_core app
%% Since the plan is to merge this with event sink mechanics at some point its fine here for now
-type event() ::
    machine_lifecycle_created_event()
    | machine_lifecycle_failed_event()
    | machine_lifecycle_repaired_event()
    | machine_lifecycle_removed_event().

-export([serialize/3]).
-export_type([event/0]).

%% Internal types

-type machine_lifecycle_created_event() ::
    event(machine_lifecycle_created, #{
        occurred_at := timestamp_ns()
    }).
-type machine_lifecycle_failed_event() ::
    event(machine_lifecycle_failed, #{
        occurred_at := timestamp_ns(),
        exception := mg_core_utils:exception()
    }).
-type machine_lifecycle_repaired_event() ::
    event(machine_lifecycle_repaired, #{
        occurred_at := timestamp_ns()
    }).
-type machine_lifecycle_removed_event() ::
    event(machine_lifecycle_removed, #{
        occurred_at := timestamp_ns()
    }).

-type event(T, D) :: {T, D}.

-type timestamp_ns() :: integer().

%%
%% API
%%

%% Trying to future-proof when we would want to merge this and mg_woody_api_event_sink:serialize
-spec serialize(mg_core:ns(), mg_core:id(), event()) -> iodata().
serialize(SourceNS, SourceID, Event) ->
    Codec = thrift_strict_binary_codec:new(),
    Data = serialize_event(SourceNS, SourceID, Event),
    Type = {struct, struct, {mg_proto_lifecycle_sink_thrift, 'LifecycleEvent'}},
    case thrift_strict_binary_codec:write(Codec, Type, Data) of
        {ok, NewCodec} ->
            thrift_strict_binary_codec:close(NewCodec);
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.

%%
%% Internals
%%

-spec serialize_event(mg_core:ns(), mg_core:id(), event()) -> mg_proto_lifecycle_sink_thrift:'LifecycleEvent'().
serialize_event(SourceNS, SourceID, {_, #{occurred_at := Timestamp}} = Event) ->
    #mg_lifesink_LifecycleEvent{
        machine_ns = SourceNS,
        machine_id = SourceID,
        created_at = serialize_timesamp(Timestamp),
        data = serialize_data(Event)
    }.

-spec serialize_data(event()) -> mg_proto_lifecycle_sink_thrift:'LifecycleEventData'().
serialize_data({machine_lifecycle_created, _}) ->
    {machine, {created, #mg_lifesink_MachineLifecycleCreatedEvent{}}};
serialize_data({machine_lifecycle_failed, #{exception := Exception}}) ->
    {machine,
        {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
            new_status =
                {failed, #mg_stateproc_MachineStatusFailed{
                    reason = exception_to_string(Exception)
                }}
        }}};
serialize_data({machine_lifecycle_repaired, _}) ->
    {machine,
        {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
            new_status = {working, #mg_stateproc_MachineStatusWorking{}}
        }}};
serialize_data({machine_lifecycle_removed, _}) ->
    {machine, {removed, #mg_lifesink_MachineLifecycleRemovedEvent{}}}.

-spec exception_to_string(mg_core_utils:exception()) -> binary().
exception_to_string(Exception) ->
    iolist_to_binary(genlib_format:format_exception(Exception)).

-spec serialize_timesamp(timestamp_ns()) -> mg_proto_base_thrift:'Timestamp'().
serialize_timesamp(Timestamp) ->
    mg_woody_api_packer:pack(timestamp_ns, Timestamp).

%%
%% Unit tests
%%

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").
-spec test() -> _.

-spec serialize_machine_lifecycle_created_test() -> _.
serialize_machine_lifecycle_created_test() ->
    Timestamp = 1000,
    ?assertEqual(
        target_event({machine, {created, #mg_lifesink_MachineLifecycleCreatedEvent{}}}, Timestamp),
        test_event({machine_lifecycle_created, #{occurred_at => Timestamp}})
    ).

-spec serialize_machine_lifecycle_failed_test() -> _.
serialize_machine_lifecycle_failed_test() ->
    Timestamp = 1000,
    Exception = {throw, throw, []},
    ?assertEqual(
        target_event(
            {machine,
                {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
                    new_status =
                        {failed, #mg_stateproc_MachineStatusFailed{
                            reason = <<"throw:throw ">>
                        }}
                }}},
            Timestamp
        ),
        test_event({machine_lifecycle_failed, #{occurred_at => Timestamp, exception => Exception}})
    ).

-spec serialize_machine_lifecycle_repaired_test() -> _.
serialize_machine_lifecycle_repaired_test() ->
    Timestamp = 1000,
    ?assertEqual(
        target_event(
            {machine,
                {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
                    new_status = {working, #mg_stateproc_MachineStatusWorking{}}
                }}},
            Timestamp
        ),
        test_event({machine_lifecycle_repaired, #{occurred_at => Timestamp}})
    ).

-spec serialize_machine_lifecycle_removed_test() -> _.
serialize_machine_lifecycle_removed_test() ->
    Timestamp = 1000,
    ?assertEqual(
        target_event({machine, {removed, #mg_lifesink_MachineLifecycleRemovedEvent{}}}, Timestamp),
        test_event({machine_lifecycle_removed, #{occurred_at => Timestamp}})
    ).

-define(NS, <<"MyNS">>).
-define(ID, <<"1">>).

-spec test_event(event()) -> mg_proto_lifecycle_sink_thrift:'LifecycleEvent'().
test_event(BeatEvent) ->
    deserialize(serialize(?NS, ?ID, BeatEvent)).

-spec target_event(mg_proto_lifecycle_sink_thrift:'LifecycleEventData'(), integer()) ->
    mg_proto_lifecycle_sink_thrift:'LifecycleEvent'().
target_event(EventData, Timestamp) ->
    #mg_lifesink_LifecycleEvent{
        machine_ns = ?NS,
        machine_id = ?ID,
        created_at = mg_woody_api_packer:pack(timestamp_ns, Timestamp),
        data = EventData
    }.

-spec deserialize(binary()) -> mg_proto_lifecycle_sink_thrift:'LifecycleEvent'().
deserialize(Data) ->
    Codec = thrift_strict_binary_codec:new(Data),
    Type = {struct, struct, {mg_proto_lifecycle_sink_thrift, 'LifecycleEvent'}},
    {ok, Thrift, _} = thrift_strict_binary_codec:read(Codec, Type),
    Thrift.

-endif.
