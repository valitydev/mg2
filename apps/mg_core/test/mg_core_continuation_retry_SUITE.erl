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

-module(mg_core_continuation_retry_SUITE).
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([continuation_delayed_retries_test/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([process_machine/7]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name()].
all() ->
    [
        continuation_delayed_retries_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_events_machine, '_', '_'}, x),
    Apps = mg_cth:start_applications([mg_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_cth:stop_applications(?config(apps, C)).

%%
%% tests
%%
-define(TEST_SLEEP, 500).
-define(TEST_INTERVALS, [100, 10000]).
-define(REQ_CTX, <<"req_ctx">>).
-define(MH_ID, <<"42">>).
-define(MH_NS, <<"42_ns">>).
-define(ETS_NS, ?MODULE).

-spec continuation_delayed_retries_test(config()) -> _.
continuation_delayed_retries_test(_C) ->
    Options = automaton_options(),
    Pid = start_automaton(Options),
    ID = ?MH_ID,
    ok = mg_core_machine:start(Options, ID, #{}, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(Options, ID, test, ?REQ_CTX, mg_core_deadline:default()),
    ok = timer:sleep(?TEST_SLEEP),
    2 = get_fail_count(),
    _ = stop_automaton(Pid).

%%
%% processor
%%

-spec process_machine(
    _Options,
    mg_core:id(),
    mg_core_machine:processor_impact(),
    _,
    _,
    _,
    mg_core_machine:machine_state()
) -> mg_core_machine:processor_result() | no_return().
process_machine(_, _, {init, InitState}, _, ?REQ_CTX, _, null) ->
    _ = ets:new(?ETS_NS, [set, named_table, public]),
    {{reply, ok}, sleep, InitState};
process_machine(_, _, {call, test}, _, ?REQ_CTX, _, State) ->
    true = ets:insert(?ETS_NS, {fail_count, 0}),
    {{reply, ok}, {continue, #{}}, State};
process_machine(_, _, continuation, _, ?REQ_CTX, _, _State) ->
    ok = increment_fail_count(),
    throw({transient, not_yet}).

%%
%% utils
%%

-spec get_fail_count() -> non_neg_integer().
get_fail_count() ->
    [{fail_count, FailCount}] = ets:lookup(?ETS_NS, fail_count),
    FailCount.

-spec increment_fail_count() -> ok.
increment_fail_count() ->
    _ = ets:update_counter(?ETS_NS, fail_count, 1),
    ok.

-spec start_automaton(mg_core_machine:options()) -> pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(mg_core_machine:start_link(Options)).

-spec stop_automaton(pid()) -> ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec automaton_options() -> mg_core_machine:options().
automaton_options() ->
    #{
        namespace => ?MH_NS,
        processor => ?MODULE,
        storage => mg_core_storage_memory,
        worker => #{registry => mg_procreg_global},
        pulse => ?MODULE,
        notification => #{
            namespace => ?MH_NS,
            pulse => ?MODULE,
            storage => mg_core_storage_memory
        },
        retries => #{
            continuation => {intervals, ?TEST_INTERVALS}
        }
    }.

-spec handle_beat(_, mpulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
