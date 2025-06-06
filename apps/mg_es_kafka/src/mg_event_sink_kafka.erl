%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_event_sink_kafka).

-include_lib("mg_es_kafka/include/pulse.hrl").

%% mg_core_event_sink handler
-behaviour(mg_core_event_sink).
-export([add_events/6]).

%% Types

-type options() :: #{
    name := atom(),
    topic := brod:topic(),
    client := brod:client(),
    pulse := mpulse:handler(),
    encoder := encoder()
}.

-type encoder() :: fun((mg_core:ns(), mg_core:id(), event()) -> iodata()).

-export_type([options/0]).
-export_type([encoder/0]).

%% Internal types

-type event() :: mg_core_events:event().
-type req_ctx() :: mg_core:request_context().
-type deadline() :: mg_core_deadline:deadline().

%% API

-spec add_events(options(), mg_core:ns(), mg_core:id(), [event()], req_ctx(), deadline()) -> ok.
add_events(Options, NS, MachineID, Events, ReqCtx, Deadline) ->
    #{pulse := Pulse, client := Client, topic := Topic, encoder := Encoder, name := Name} = Options,
    StartTimestamp = erlang:monotonic_time(),
    Batch = encode(Encoder, NS, MachineID, Events),
    EncodeTimestamp = erlang:monotonic_time(),
    {ok, Partition, Offset} = produce(Client, Topic, event_key(NS, MachineID), Batch),
    FinishTimestamp = erlang:monotonic_time(),
    ok = mpulse:handle_beat(Pulse, #mg_event_sink_kafka_sent{
        name = Name,
        namespace = NS,
        machine_id = MachineID,
        request_context = ReqCtx,
        deadline = Deadline,
        encode_duration = EncodeTimestamp - StartTimestamp,
        send_duration = FinishTimestamp - EncodeTimestamp,
        data_size = batch_size(Batch),
        partition = Partition,
        offset = Offset
    }).

%% Internals

-spec event_key(mg_core:ns(), mg_core:id()) -> term().
event_key(NS, MachineID) ->
    <<NS/binary, " ", MachineID/binary>>.

-spec encode(encoder(), mg_core:ns(), mg_core:id(), [event()]) -> brod:batch_input().
encode(Encoder, NS, MachineID, Events) ->
    [
        #{
            key => event_key(NS, MachineID),
            value => Encoder(NS, MachineID, Event)
        }
     || Event <- Events
    ].

-spec produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    {ok, brod:partition(), brod:offset()}.
produce(Client, Topic, Key, Batch) ->
    case do_produce(Client, Topic, Key, Batch) of
        {ok, _Partition, _Offset} = Result ->
            Result;
        {error, Reason} ->
            handle_produce_error(Reason)
    end.

-spec do_produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    {ok, brod:partition(), brod:offset()} | {error, Reason :: any()}.
do_produce(Client, Topic, PartitionKey, Batch) ->
    try brod:get_partitions_count(Client, Topic) of
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
    catch
        exit:Reason ->
            {error, {exit, Reason}}
    end.

-spec handle_produce_error(atom()) -> no_return().
handle_produce_error(timeout) ->
    erlang:throw({transient, timeout});
handle_produce_error({exit, {Reasons = [_ | _], _}}) ->
    case lists:any(fun is_connectivity_reason/1, Reasons) of
        true ->
            erlang:throw({transient, {event_sink_unavailable, {connect_failed, Reasons}}});
        false ->
            erlang:error({?MODULE, {unexpected, Reasons}})
    end;
handle_produce_error({producer_down, Reason}) ->
    erlang:throw({transient, {event_sink_unavailable, {producer_down, Reason}}});
handle_produce_error(Reason) ->
    KnownErrors = #{
        % See https://kafka.apache.org/protocol.html#protocol_error_codes for kafka error details
        client_down => transient,
        unknown_server_error => unknown,
        corrupt_message => transient,
        unknown_topic_or_partition => transient,
        leader_not_available => transient,
        not_leader_for_partition => transient,
        request_timed_out => transient,
        broker_not_available => transient,
        replica_not_available => transient,
        message_too_large => misconfiguration,
        stale_controller_epoch => transient,
        network_exception => transient,
        invalid_topic_exception => logic,
        record_list_too_large => misconfiguration,
        not_enough_replicas => transient,
        not_enough_replicas_after_append => transient,
        invalid_required_acks => transient,
        topic_authorization_failed => misconfiguration,
        cluster_authorization_failed => misconfiguration,
        invalid_timestamp => logic,
        unsupported_sasl_mechanism => misconfiguration,
        illegal_sasl_state => logic,
        unsupported_version => misconfiguration,
        reassignment_in_progress => transient
    },
    case maps:find(Reason, KnownErrors) of
        {ok, transient} ->
            erlang:throw({transient, {event_sink_unavailable, Reason}});
        {ok, misconfiguration} ->
            erlang:throw({transient, {event_sink_misconfiguration, Reason}});
        {ok, Other} ->
            erlang:error({?MODULE, {Other, Reason}});
        error ->
            erlang:error({?MODULE, {unexpected, Reason}})
    end.

-spec is_connectivity_reason(
    {inet:hostname(), {inet:posix() | {failed_to_upgrade_to_ssl, _SSLError}, _ST}}
) ->
    boolean().
is_connectivity_reason({_, {timeout, _ST}}) ->
    true;
is_connectivity_reason({_, {econnrefused, _ST}}) ->
    true;
is_connectivity_reason({_, {ehostunreach, _ST}}) ->
    true;
is_connectivity_reason({_, {enetunreach, _ST}}) ->
    true;
is_connectivity_reason({_, {nxdomain, _ST}}) ->
    true;
is_connectivity_reason({_, {{failed_to_upgrade_to_ssl, _SSLError}, _ST}}) ->
    true;
is_connectivity_reason({_, {{_, closed}, _ST}}) ->
    true;
is_connectivity_reason({_, {{_, timeout}, _ST}}) ->
    true;
is_connectivity_reason(_Reason) ->
    false.

-spec batch_size(brod:batch_input()) -> non_neg_integer().
batch_size(Batch) ->
    lists:foldl(
        fun(#{value := Value}, Acc) ->
            Acc + erlang:iolist_size(Value)
        end,
        0,
        Batch
    ).

-spec partition(non_neg_integer(), brod:key()) -> brod:partition().
partition(PartitionsCount, Key) ->
    erlang:phash2(Key) rem PartitionsCount.
