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

-module(mg_core_notification_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([notification_put_ok_test/1]).
-export([notification_update_test/1]).
-export([notification_get_ok_test/1]).
-export([notification_search_ok_test/1]).
-export([notification_delete_ok_test/1]).
-export([notification_get_not_found_test/1]).
-export([notification_search_not_found_test/1]).
-export([notification_double_delete_test/1]).

%% pulse

-export([handle_beat/2]).

%%

-define(ID, <<"my_notification">>).
-define(TS, 100).
-define(MACHINE_NS, <<"my_ns">>).
-define(MACHINE_ID, <<"my_machine">>).
-define(STORAGE_NAME, {?MACHINE_ID, mg_core_notification, notifications}).

%%

-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, default}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {default, [sequence], [
            notification_put_ok_test,
            notification_update_test,
            notification_get_ok_test,
            notification_search_ok_test,
            notification_delete_ok_test,
            notification_get_not_found_test,
            notification_search_not_found_test,
            notification_double_delete_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = mg_cth:start_applications([mg_core]),
    {ok, StoragePid} = mg_core_storage_memory:start_link(#{
        name => ?STORAGE_NAME,
        pulse => ?MODULE
    }),
    true = erlang:unlink(StoragePid),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_cth:stop_applications(?config(apps, C)).

%%

-spec notification_put_ok_test(config()) -> _.
notification_put_ok_test(C) ->
    Timestamp = 200,
    Data = #{
        machine_id => ?MACHINE_ID,
        args => [<<"a">>, <<"b">>, 0]
    },
    Context = mg_core_notification:put(notification_options(C), ?ID, Data, Timestamp, undefined),
    save_cfg([{context, Context}]).

-spec notification_update_test(config()) -> _.
notification_update_test(C) ->
    Context = get_saved_cfg(context, C),
    Timestamp = ?TS,
    Data = #{
        machine_id => ?MACHINE_ID,
        args => [<<"a">>, <<"b">>, <<"c">>]
    },
    NewContext = mg_core_notification:put(notification_options(C), ?ID, Data, Timestamp, Context),
    save_cfg([{context, NewContext}]).

-spec notification_get_ok_test(config()) -> _.
notification_get_ok_test(C) ->
    ?assertMatch(
        {ok, _, #{
            machine_id := ?MACHINE_ID,
            args := [<<"a">>, <<"b">>, <<"c">>]
        }},
        mg_core_notification:get(notification_options(C), ?ID)
    ),
    pass_saved_cfg(C).

-spec notification_search_ok_test(config()) -> _.
notification_search_ok_test(C) ->
    ?assertMatch(
        [{_, ?ID}],
        mg_core_notification:search(notification_options(C), ?TS - 10, ?TS + 10, inf)
    ),
    pass_saved_cfg(C).

-spec notification_delete_ok_test(config()) -> _.
notification_delete_ok_test(C) ->
    Context = get_saved_cfg(context, C),
    ?assertEqual(ok, mg_core_notification:delete(notification_options(C), ?ID, Context)),
    pass_saved_cfg(C).

-spec notification_get_not_found_test(config()) -> _.
notification_get_not_found_test(C) ->
    ?assertMatch(
        {error, not_found},
        mg_core_notification:get(notification_options(C), ?ID)
    ),
    pass_saved_cfg(C).

-spec notification_search_not_found_test(config()) -> _.
notification_search_not_found_test(C) ->
    ?assertMatch(
        [],
        mg_core_notification:search(notification_options(C), ?TS - 10, ?TS + 10, inf)
    ),
    pass_saved_cfg(C).

-spec notification_double_delete_test(config()) -> _.
notification_double_delete_test(C) ->
    Context = get_saved_cfg(context, C),
    ?assertEqual(
        ok,
        mg_core_notification:delete(notification_options(C), ?ID, Context)
    ).

%%

-spec notification_options(config()) -> mg_core_notification:options().
notification_options(_C) ->
    #{
        namespace => ?MACHINE_NS,
        pulse => ?MODULE,
        storage =>
            {mg_core_storage_memory, #{
                pulse => ?MODULE,
                existing_storage_name => ?STORAGE_NAME
            }}
    }.

%%

-spec get_saved_cfg(Key :: _, config()) -> _.
get_saved_cfg(Key, C) ->
    {_, CI} = get_cfg(saved_config, C),
    get_cfg(Key, CI).

-spec save_cfg(Value) -> {save_config, Value}.
save_cfg(Value) ->
    {save_config, Value}.

-spec pass_saved_cfg(config()) -> {save_config, _}.
pass_saved_cfg(C) ->
    {_, CI} = get_cfg(saved_config, C),
    save_cfg(CI).

-spec get_cfg(Key :: _, config()) -> _.
get_cfg(Key, C) ->
    test_server:lookup_config(Key, C).

-spec handle_beat(_, mpulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
