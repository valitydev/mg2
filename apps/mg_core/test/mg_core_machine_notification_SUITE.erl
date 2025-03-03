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

-module(mg_core_machine_notification_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% tests
-export([no_notification_options_test/1]).
-export([simple_test/1]).
-export([invalid_machine_id_test/1]).
-export([retry_after_fail_test/1]).
-export([timeout_test/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([pool_child_spec/2, process_machine/7]).

-export([start/0]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, no_notification_opts},
        {group, base}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {no_notification_opts, [], [
            no_notification_options_test
        ]},
        {base, [], [
            simple_test,
            invalid_machine_id_test,
            retry_after_fail_test,
            timeout_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_machine, '_', '_'}, x),
    Apps = mg_cth:start_applications([mg_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_cth:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(no_notification_opts, C) ->
    C;
init_per_group(base, C) ->
    Options = automaton_options(C),
    Pid = start_automaton(Options),
    _ = unlink(Pid),
    [{options, Options}, {automaton, Pid} | C].

-spec end_per_group(group_name(), config()) -> _.
end_per_group(no_notification_opts, _C) ->
    ok;
end_per_group(base, C) ->
    ok = stop_automaton(?config(automaton, C)),
    ok.

-define(REQ_CTX, <<"req_ctx">>).

-spec init_per_testcase(test_name(), config()) -> config().
init_per_testcase(no_notification_options_test, C) ->
    C;
init_per_testcase(_, C) ->
    ID = genlib:unique(),
    ok = mg_core_machine:start(?config(options, C), ID, 0, ?REQ_CTX, mg_core_deadline:default()),
    [{id, ID} | C].

-spec end_per_testcase(test_name(), config()) -> _.
end_per_testcase(_, _C) ->
    ok.

%%
%% tests
%%

-spec no_notification_options_test(config()) -> _.
no_notification_options_test(C) ->
    RawOptions = automaton_options(C),
    Options0 = maps:without([notification], RawOptions),
    Options = Options0#{schedulers => maps:without([notification], maps:get(schedulers, Options0))},
    Pid = start_automaton(Options),
    _ = unlink(Pid),
    ID = genlib:unique(),
    ok = mg_core_machine:start(Options, ID, 0, ?REQ_CTX, mg_core_deadline:default()),
    ?assertError(function_clause, mg_core_machine:notify(Options, ID, 42, ?REQ_CTX)),
    ?assertExit(_, mg_core_machine:notify(RawOptions, ID, 42, ?REQ_CTX)),
    _ = stop_automaton(Pid).

-spec simple_test(config()) -> _.
simple_test(C) ->
    Options = ?config(options, C),
    ID = ?config(id, C),
    % simple notification
    _NotificationID = mg_core_machine:notify(Options, ID, 42, ?REQ_CTX),
    {ok, _} = mg_cth:poll_for_value(
        fun() ->
            mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())
        end,
        42,
        1000
    ).

-spec invalid_machine_id_test(config()) -> _.
invalid_machine_id_test(C) ->
    Options = ?config(options, C),
    {logic, machine_not_found} =
        (catch mg_core_machine:notify(Options, <<"dum">>, 42, ?REQ_CTX)).

-spec retry_after_fail_test(config()) -> _.
retry_after_fail_test(C) ->
    Options = ?config(options, C),
    ID = ?config(id, C),
    % fail with notification, repair, retry notification
    _NotificationID = mg_core_machine:notify(Options, ID, [<<"fail_when">>, 0], ?REQ_CTX),
    %% wait for notification to kill the machine
    {ok, _} = mg_cth:poll_for_exception(
        fun() ->
            mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())
        end,
        {logic, machine_failed},
        1000
    ),
    %% test notification failing to retry
    _ = timer:sleep(2000),
    repaired = mg_core_machine:repair(Options, ID, repair_arg, ?REQ_CTX, mg_core_deadline:default()),
    %% machine is repaired but notification has not retried yet
    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    %% wait for notification to kill the machine a second time (it re_tried)
    {ok, _} = mg_cth:poll_for_exception(
        fun() ->
            mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())
        end,
        {logic, machine_failed},
        5000
    ),
    repaired = mg_core_machine:repair(Options, ID, repair_arg, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(Options, ID, increment, ?REQ_CTX, mg_core_deadline:default()),
    ok = await_notification_deleted(ID, 3000),
    1 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()).

-spec timeout_test(config()) -> _.
timeout_test(C) ->
    Options = ?config(options, C),
    ID = ?config(id, C),
    % fail with notification, repair, retry notification
    NotificationID = mg_core_machine:notify(Options, ID, [<<"timeout_when">>, 0], ?REQ_CTX),
    %% test notification timing out
    _ = timer:sleep(1000),
    [{_, NotificationID}] = search_notifications_for_machine(ID),
    %% test notification no longer timing out
    ok = mg_core_machine:call(Options, ID, increment, ?REQ_CTX, mg_core_deadline:default()),
    ok = await_notification_deleted(ID, 3000),
    1 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()).

%%
%% processor
%%
-spec pool_child_spec(_Options, atom()) -> supervisor:child_spec().
pool_child_spec(_Options, Name) ->
    #{
        id => Name,
        start => {?MODULE, start, []}
    }.

-spec process_machine(
    _Options,
    mg_core:id(),
    mg_core_machine:processor_impact(),
    _,
    _,
    _,
    mg_core_machine:machine_state()
) -> mg_core_machine:processor_result() | no_return().
process_machine(_, _, {init, TestValue}, _, ?REQ_CTX, _, null) ->
    {{reply, ok}, sleep, TestValue};
process_machine(_, _, {call, get}, _, ?REQ_CTX, _, TestValue) ->
    {{reply, TestValue}, sleep, TestValue};
process_machine(_, _, {call, increment}, _, ?REQ_CTX, _, TestValue) ->
    {{reply, ok}, sleep, TestValue + 1};
process_machine(_, _, {notification, _, [<<"timeout_when">>, Arg]}, _, ?REQ_CTX, _, State) ->
    _ =
        case State of
            Arg ->
                _ = timer:sleep(1500);
            _ ->
                ok
        end,
    {{reply, ok}, sleep, State};
process_machine(_, _, {notification, _, [<<"fail_when">>, Arg]}, _, ?REQ_CTX, _, State) ->
    case State of
        Arg ->
            _ = exit(1);
        _ ->
            {{reply, ok}, sleep, State}
    end;
process_machine(_, _, {notification, _, Arg}, _, ?REQ_CTX, _, TestValue) when is_integer(Arg) ->
    {{reply, ok}, sleep, TestValue + Arg};
process_machine(_, _, {repair, repair_arg}, _, ?REQ_CTX, _, TestValue) ->
    {{reply, repaired}, sleep, TestValue}.

%%
%% utils
%%

-spec search_notifications_for_machine(binary()) -> list().
search_notifications_for_machine(MachineID) ->
    Options = notification_options(),
    Found = mg_core_notification:search(Options, 1, genlib_time:unow(), inf),
    lists:filter(
        fun({_, NID}) ->
            %% FIXME Investigate race condition with '{error,
            %% not_found}' return. Looks like we must treat notfound
            %% notification as deleted/consumed.
            {ok, _, #{machine_id := FoundMachineID}} = mg_core_notification:get(Options, NID),
            MachineID =:= FoundMachineID
        end,
        Found
    ).

-spec await_notification_deleted(binary(), integer()) -> ok | {error, timeout}.
await_notification_deleted(_, Timeout) when Timeout =< 0 ->
    {error, timeout};
await_notification_deleted(ID, Timeout) ->
    case search_notifications_for_machine(ID) of
        [] ->
            ok;
        _ ->
            _ = timer:sleep(100),
            await_notification_deleted(ID, Timeout - 100)
    end.

-spec start() -> ignore.
start() ->
    ignore.

-spec start_automaton(mg_core_machine:options()) -> pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(mg_core_machine:start_link(Options)).

-spec stop_automaton(pid()) -> ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-define(NS, <<"test">>).

-spec automaton_options(config()) -> mg_core_machine:options().
automaton_options(_C) ->
    Scheduler = #{},
    #{
        namespace => ?NS,
        processor => ?MODULE,
        storage => mg_core_storage_memory,
        worker => #{
            registry => mg_procreg_global
        },
        notification => notification_options(),
        notification_processing_timeout => 500,
        pulse => ?MODULE,
        schedulers => #{
            timers => Scheduler,
            timers_retries => Scheduler,
            overseer => Scheduler,
            notification => #{
                scan_handicap => 1,
                reschedule_time => 2
            }
        }
    }.

-spec notification_options() -> mg_core_notification:options().
notification_options() ->
    #{
        namespace => ?NS,
        pulse => ?MODULE,
        storage => mg_core_storage_memory
    }.

-spec handle_beat(_, mpulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
