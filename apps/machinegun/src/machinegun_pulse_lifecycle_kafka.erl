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

-module(machinegun_pulse_lifecycle_kafka).

-include_lib("machinegun_core/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-type options() :: #{
    topic := brod:topic(),
    client := brod:client(),
    encoder := encoder()
}.

-export([handle_beat/2]).
-export_type([options/0]).

%% internal types
-type beat() :: machinegun_pulse:beat().
-type encoder() :: fun((mg_core:ns(), mg_core:id(), beat_event()) -> iodata()).

%% FIXME: This should reside at machinegun_core level, but since
%% lifecycle kafka over pulse is a *temporary* solution it is probably fine here.
-type beat_event() :: any().

%%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.
handle_beat(Options, Beat) when
    %% We only support a subset of beats
    is_record(Beat, mg_core_machine_lifecycle_created) orelse
        is_record(Beat, mg_core_machine_lifecycle_failed) orelse
        is_record(Beat, mg_core_machine_lifecycle_repaired) orelse
        is_record(Beat, mg_core_machine_lifecycle_removed)
->
    #{client := Client, topic := Topic, encoder := Encoder} = Options,
    {SourceNS, SourceID, Event} = get_beat_data(Beat),
    Batch = encode(Encoder, SourceNS, SourceID, Event),
    ok = produce(Client, Topic, event_key(SourceNS, SourceID), Batch),
    ok;
handle_beat(_, _) ->
    ok.

%% Internals

-spec get_beat_data(beat()) -> {mg_core:ns(), mg_core:id(), beat_event()}.
get_beat_data(#mg_core_machine_lifecycle_created{namespace = NS, machine_id = ID}) ->
    {NS, ID, {machine_lifecycle_created, #{occurred_at => ts_now()}}};
get_beat_data(#mg_core_machine_lifecycle_failed{namespace = NS, machine_id = ID, exception = Exception}) ->
    {NS, ID, {machine_lifecycle_failed, #{occurred_at => ts_now(), exception => Exception}}};
get_beat_data(#mg_core_machine_lifecycle_repaired{namespace = NS, machine_id = ID}) ->
    {NS, ID, {machine_lifecycle_repaired, #{occurred_at => ts_now()}}};
get_beat_data(#mg_core_machine_lifecycle_removed{namespace = NS, machine_id = ID}) ->
    {NS, ID, {machine_lifecycle_removed, #{occurred_at => ts_now()}}}.

-spec ts_now() -> integer().
ts_now() ->
    os:system_time(nanosecond).

-spec event_key(mg_core:ns(), mg_core:id()) -> term().
event_key(NS, MachineID) ->
    <<NS/binary, " ", MachineID/binary>>.

-spec encode(encoder(), mg_core:ns(), mg_core:id(), beat_event()) -> brod:batch_input().
encode(Encoder, SourceNS, SourceID, Event) ->
    [
        #{
            key => event_key(SourceNS, SourceID),
            value => Encoder(SourceNS, SourceID, Event)
        }
    ].

-spec produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    ok.
produce(Client, Topic, Key, Batch) ->
    case do_produce(Client, Topic, Key, Batch) of
        {ok, _Partition, _Offset} ->
            ok;
        {error, Reason} ->
            _ = logger:warning("Failed to produce kafka lifecycle batch, reason: ~p", [Reason]),
            ok
    end.

-spec do_produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    {ok, brod:partition(), brod:offset()} | {error, Reason :: any()}.
do_produce(Client, Topic, PartitionKey, Batch) ->
    try
        do_produce_unsafe(Client, Topic, PartitionKey, Batch)
    catch
        % Ultra safe
        Error:Reason ->
            {error, {caught, Error, Reason}}
    end.

-spec do_produce_unsafe(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    {ok, brod:partition(), brod:offset()} | {error, Reason :: any()}.
do_produce_unsafe(Client, Topic, PartitionKey, Batch) ->
    case brod:get_partitions_count(Client, Topic) of
        {ok, PartitionsCount} ->
            Partition = partition(PartitionsCount, PartitionKey),
            case brod:produce_sync_offset(Client, Topic, Partition, PartitionKey, Batch) of
                {ok, Offset} ->
                    {ok, Partition, Offset};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

-spec partition(non_neg_integer(), brod:key()) -> brod:partition().
partition(PartitionsCount, Key) ->
    erlang:phash2(Key) rem PartitionsCount.
