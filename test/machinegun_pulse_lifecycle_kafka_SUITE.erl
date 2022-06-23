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

-module(machinegun_pulse_lifecycle_kafka_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("kafka_protocol/include/kpro_public.hrl").

-include_lib("machinegun_core/include/pulse.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([handle_known_beats_ok_test/1]).
-export([handle_unknown_beats_ok_test/1]).

-define(TOPIC, <<"test-life-sink">>).
-define(SOURCE_NS, <<"source-ns">>).
-define(SOURCE_ID, <<"source-id">>).
-define(CLIENT, mg_kafka_client).
-define(BROKERS, [{"kafka1", 9092}]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        handle_known_beats_ok_test,
        handle_unknown_beats_ok_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    AppSpecs = [
        {brod, [
            {clients, [
                {?CLIENT, [
                    {endpoints, ?BROKERS},
                    {auto_start_producers, true}
                ]}
            ]}
        ]}
    ],
    Apps = lists:flatten([
        genlib_app:start_application_with(App, AppConf)
     || {App, AppConf} <- AppSpecs
    ]),
    %% Need to pre-create the topic
    PartitionsCount = 1,
    TopicConfig = [
        #{
            configs => [],
            num_partitions => PartitionsCount,
            assignments => [],
            replication_factor => 1,
            name => ?TOPIC
        }
    ],
    ok = brod:create_topics(?BROKERS, TopicConfig, #{timeout => 5000}),
    {ok, PartitionsCount} = brod:get_partitions_count(?CLIENT, ?TOPIC),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    machinegun_ct_helper:stop_applications(?config(apps, C)).

%%
%% tests
%%

-spec handle_known_beats_ok_test(config()) -> _.
handle_known_beats_ok_test(_C) ->
    OldBeats = read_all_beats(),
    Exception = {exit, out, []},
    ?assertEqual(
        ok,
        handle_beats([
            #mg_core_machine_lifecycle_created{
                namespace = ?SOURCE_NS,
                machine_id = ?SOURCE_ID,
                request_context = null
            },
            #mg_core_machine_lifecycle_failed{
                namespace = ?SOURCE_NS,
                machine_id = ?SOURCE_ID,
                request_context = null,
                exception = Exception,
                deadline = mg_core_deadline:default()
            },
            #mg_core_machine_lifecycle_repaired{
                namespace = ?SOURCE_NS,
                machine_id = ?SOURCE_ID,
                request_context = null,
                deadline = mg_core_deadline:default()
            },
            #mg_core_machine_lifecycle_removed{
                namespace = ?SOURCE_NS,
                machine_id = ?SOURCE_ID,
                request_context = null
            }
        ])
    ),
    NewBeats = read_all_beats() -- OldBeats,
    ?assertMatch(
        [
            {?SOURCE_NS, ?SOURCE_ID, {machine_lifecycle_created, #{occurred_at := _}}},
            {?SOURCE_NS, ?SOURCE_ID, {machine_lifecycle_failed, #{occurred_at := _, exception := Exception}}},
            {?SOURCE_NS, ?SOURCE_ID, {machine_lifecycle_repaired, #{occurred_at := _}}},
            {?SOURCE_NS, ?SOURCE_ID, {machine_lifecycle_removed, #{occurred_at := _}}}
        ],
        NewBeats
    ).

-spec handle_unknown_beats_ok_test(config()) -> _.
handle_unknown_beats_ok_test(_C) ->
    OldBeats = read_all_beats(),
    ?assertEqual(
        ok,
        handle_beats([
            #mg_core_timer_lifecycle_created{
                namespace = ?SOURCE_NS,
                machine_id = ?SOURCE_ID,
                request_context = null,
                target_timestamp = genlib_time:now()
            }
        ])
    ),
    ?assertEqual(OldBeats, read_all_beats()).

%%
%% utils
%%

-spec handle_beats([machinegun_pulse:beat()]) -> ok.
handle_beats([]) ->
    ok;
handle_beats([Beat | Rest]) ->
    ok = handle_beat(Beat),
    handle_beats(Rest).

-spec handle_beat(machinegun_pulse:beat()) -> ok.
handle_beat(Beat) ->
    machinegun_pulse_lifecycle_kafka:handle_beat(pulse_options(), Beat).

-spec pulse_options() -> machinegun_pulse_lifecycle_kafka:options().
pulse_options() ->
    #{
        client => ?CLIENT,
        topic => ?TOPIC,
        encoder => fun(NS, ID, Event) ->
            erlang:term_to_binary({NS, ID, Event})
        end
    }.

-spec read_all_beats() -> [term()].
read_all_beats() ->
    {ok, PartitionsCount} = brod:get_partitions_count(?CLIENT, ?TOPIC),
    do_read_all(?BROKERS, ?TOPIC, PartitionsCount - 1, 0, []).

-spec do_read_all(
    [brod:endpoint()],
    brod:topic(),
    brod:partition(),
    brod:offset(),
    [term()]
) -> [term()].
do_read_all(_Hosts, _Topic, Partition, _Offset, Result) when Partition < 0 ->
    lists:reverse(Result);
do_read_all(Hosts, Topic, Partition, Offset, Result) ->
    case brod:fetch(Hosts, Topic, Partition, Offset) of
        {ok, {Offset, []}} ->
            do_read_all(Hosts, Topic, Partition - 1, Offset, Result);
        {ok, {NewOffset, Records}} when NewOffset =/= Offset ->
            NewRecords = lists:reverse([
                erlang:binary_to_term(Value)
             || #kafka_message{value = Value} <- Records
            ]),
            do_read_all(Hosts, Topic, Partition, NewOffset, NewRecords ++ Result)
    end.
