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

-module(mg_core_events_sink_kafka_errors_SUITE).
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% tests
-export([add_events_connect_failed_test/1]).
-export([add_events_ssl_failed_test/1]).
-export([add_events_timeout_test/1]).
-export([add_events_second_timeout_test/1]).
-export([add_events_econnrefused_test/1]).
-export([add_events_ehostunreach_test/1]).
-export([add_events_enetunreach_test/1]).
-export([add_events_nxdomain_test/1]).

%% Pulse
-export([handle_beat/2]).

-define(TOPIC, <<"test_event_sink">>).
-define(SOURCE_NS, <<"source_ns">>).
-define(SOURCE_ID, <<"source_id">>).
-define(CLIENT, mg_core_kafka_client).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, main}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {main, [], [
            add_events_connect_failed_test,
            add_events_ssl_failed_test,
            add_events_timeout_test,
            add_events_second_timeout_test,
            add_events_econnrefused_test,
            add_events_ehostunreach_test,
            add_events_enetunreach_test,
            add_events_nxdomain_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_events_sink_kafka, '_', '_'}, x),
    {Events, _} = mg_core_events:generate_events_with_range(
        [{#{}, Body} || Body <- [1, 2, 3]],
        undefined
    ),
    Apps = genlib_app:start_application_with(ranch, []),
    [{apps, Apps}, {events, Events} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    _ = mg_cth:stop_applications(?config(apps, C)),
    ok.

-spec init_per_testcase(test_name(), config()) -> config().
init_per_testcase(Name, C) ->
    [{testcase, Name} | C].

-spec end_per_testcase(test_name(), config()) -> ok.
end_per_testcase(_Name, _C) ->
    ok.

%%
%% tests
%%

-spec add_events_connect_failed_test(config()) -> _.
add_events_connect_failed_test(C) ->
    {ok, Proxy = #{endpoint := {Host, Port}}} = ct_proxy:start_link({"kafka1", 9092}),
    Apps = genlib_app:start_application_with(brod, [
        {clients, [
            {?CLIENT, [
                {endpoints, [{Host, Port}]},
                {auto_start_producers, true}
            ]}
        ]}
    ]),
    try
        ok = change_proxy_mode(pass, stop, Proxy, C),
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {{_, closed}, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps),
        _ = (catch ct_proxy:stop(Proxy))
    end.

-spec add_events_ssl_failed_test(config()) -> _.
add_events_ssl_failed_test(C) ->
    {ok, Proxy = #{endpoint := {Host, Port}}} = ct_proxy:start_link({"kafka1", 9092}),
    Apps = genlib_app:start_application_with(brod, [
        {clients, [
            {?CLIENT, [
                {endpoints, [{Host, Port}]},
                {ssl, true},
                {auto_start_producers, true}
            ]}
        ]}
    ]),
    try
        ok = change_proxy_mode(pass, ignore, Proxy, C),
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {{failed_to_upgrade_to_ssl, _}, _ST}}]}}},
            mg_core_events_sink_kafka:add_events(
                event_sink_options(),
                ?SOURCE_NS,
                ?SOURCE_ID,
                ?config(events, C),
                null,
                mg_core_deadline:default()
            )
        )
    after
        _ = mg_cth:stop_applications(Apps),
        _ = (catch ct_proxy:stop(Proxy))
    end.

-spec add_events_timeout_test(config()) -> _.
add_events_timeout_test(C) ->
    {ok, LSock} = gen_tcp:listen(9092, [
        binary,
        {packet, 0},
        {active, false}
    ]),
    Apps =
        genlib_app:start_application_with(brod, [
            {clients, [
                {?CLIENT, [
                    {endpoints, [{"localhost", 9092}]},
                    {auto_start_producers, true}
                ]}
            ]}
        ]),
    try
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {{_, timeout}, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps),
        _ = (catch gen_tcp:close(LSock))
    end.

-spec add_events_second_timeout_test(config()) -> _.
add_events_second_timeout_test(C) ->
    Apps = genlib_app:start_application_with(brod, [
        {clients, [
            {?CLIENT, [
                {endpoints, [{{192, 168, 254, 254}, 9092}]},
                {auto_start_producers, true}
            ]}
        ]}
    ]),
    try
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {timeout, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps)
    end.

-spec add_events_econnrefused_test(config()) -> _.
add_events_econnrefused_test(C) ->
    Apps = genlib_app:start_application_with(brod, [
        {clients, [
            {?CLIENT, [
                {endpoints, [{"kafka1", 0}]},
                {auto_start_producers, true}
            ]}
        ]}
    ]),
    try
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {econnrefused, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps)
    end.

-spec add_events_ehostunreach_test(config()) -> _.
add_events_ehostunreach_test(C) ->
    Addr = unreachable_ip(get_ip_addr()),
    Apps =
        genlib_app:start_application_with(brod, [
            {clients, [
                {?CLIENT, [
                    {endpoints, [{Addr, 9092}]},
                    {auto_start_producers, true}
                ]}
            ]}
        ]),
    try
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {ehostunreach, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps)
    end.

-spec add_events_enetunreach_test(config()) -> _.
add_events_enetunreach_test(C) ->
    Apps =
        genlib_app:start_application_with(brod, [
            {clients, [
                {?CLIENT, [
                    {endpoints, [{os:getenv("NETUNREACH_ADDRESS"), 9092}]},
                    {auto_start_producers, true}
                ]}
            ]}
        ]),
    try
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {enetunreach, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps)
    end.

-spec add_events_nxdomain_test(config()) -> _.
add_events_nxdomain_test(C) ->
    Apps = genlib_app:start_application_with(brod, [
        {clients, [
            {?CLIENT, [
                {endpoints, [{"56:92:3e:57:d9:8f", 9092}]},
                {auto_start_producers, true}
            ]}
        ]}
    ]),
    try
        _ = ?assertException(
            throw,
            {transient, {event_sink_unavailable, {connect_failed, [{_, {nxdomain, _ST}}]}}},
            add_events(C)
        )
    after
        _ = mg_cth:stop_applications(Apps)
    end.

-spec event_sink_options() -> mg_core_events_sink_kafka:options().
event_sink_options() ->
    #{
        name => kafka,
        client => ?CLIENT,
        topic => ?TOPIC,
        pulse => ?MODULE,
        encoder => fun(NS, ID, Event) ->
            erlang:term_to_binary({NS, ID, Event})
        end
    }.

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

-spec change_proxy_mode(atom(), atom(), term(), config()) -> ok.
change_proxy_mode(ModeWas, Mode, Proxy, C) ->
    _ = ct:pal(
        debug,
        "[~p] setting proxy from '~p' to '~p'",
        [?config(testcase, C), ModeWas, Mode]
    ),
    _ = ?assertEqual({ok, ModeWas}, ct_proxy:mode(Proxy, Mode)),
    ok.

-spec add_events(config()) -> ok.
add_events(C) ->
    mg_core_events_sink_kafka:add_events(
        event_sink_options(),
        ?SOURCE_NS,
        ?SOURCE_ID,
        ?config(events, C),
        null,
        mg_core_deadline:default()
    ).

-spec unreachable_ip(tuple()) -> tuple().
unreachable_ip(Addr) ->
    erlang:setelement(4, Addr, 254).

-spec get_ip_addr() -> tuple().
get_ip_addr() ->
    {ok, Interfaces} = inet:getifaddrs(),
    genlib_list:foldl_while(
        fun({_Name, Opts}, Acc) ->
            {flags, Flags} = lists:keyfind(flags, 1, Opts),
            Addr = get_opts_addr(Opts),
            Check = {
                lists:member(loopback, Flags),
                lists:member(up, Flags),
                lists:member(running, Flags),
                erlang:tuple_size(Addr) =:= 4
            },
            case Check of
                {false, true, true, true} ->
                    {halt, Addr};
                _ ->
                    {cont, Acc}
            end
        end,
        {0, 0, 0, 0},
        Interfaces
    ).

-spec get_opts_addr(list()) -> tuple().
get_opts_addr(Opts) ->
    case lists:keyfind(addr, 1, Opts) of
        {addr, Addr} ->
            Addr;
        false ->
            {}
    end.
