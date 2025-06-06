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

-module(mg_core_instant_timer_task_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([instant_start_test/1]).
-export([without_shedulers_test/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([pool_child_spec/2]).
-export([process_machine/7]).

-export([start/0]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name()] | {group, atom()}.
all() ->
    [
        instant_start_test,
        without_shedulers_test
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

%%
%% tests
%%
-define(REQ_CTX, <<"req_ctx">>).

-spec instant_start_test(config()) -> _.
instant_start_test(_C) ->
    NS = <<"test">>,
    ID = genlib:to_binary(?FUNCTION_NAME),
    Options = automaton_options(NS),
    Pid = start_automaton(Options),

    ok = mg_core_machine:start(Options, ID, 0, ?REQ_CTX, mg_core_deadline:default()),
    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(Options, ID, force_timeout, ?REQ_CTX, mg_core_deadline:default()),
    F = fun() ->
        mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())
    end,
    mg_cth:assert_wait_expected(
        1,
        F,
        genlib_retry:new_strategy({linear, _Retries = 10, _Timeout = 100})
    ),

    ok = stop_automaton(Pid).

-spec without_shedulers_test(config()) -> _.
without_shedulers_test(_C) ->
    NS = <<"test">>,
    ID = genlib:to_binary(?FUNCTION_NAME),
    Options = automaton_options_wo_shedulers(NS),
    Pid = start_automaton(Options),

    ok = mg_core_machine:start(Options, ID, 0, ?REQ_CTX, mg_core_deadline:default()),
    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(Options, ID, force_timeout, ?REQ_CTX, mg_core_deadline:default()),
    % machine is still alive
    _ = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),

    ok = stop_automaton(Pid).

%%
%% processor
%%
-record(machine_state, {
    counter = 0 :: integer(),
    timer = undefined :: {genlib_time:ts(), mg_core:request_context()} | undefined
}).
-type machine_state() :: #machine_state{}.
-type processor_result() ::
    {
        mg_core_machine:processor_reply_action(),
        mg_core_machine:processor_flow_action(),
        machine_state()
    }.

-spec pool_child_spec(_Options, atom()) -> supervisor:child_spec().
pool_child_spec(_Options, Name) ->
    #{
        id => Name,
        start => {?MODULE, start, []}
    }.

-spec process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg_core:id(),
    Impact :: mg_core_machine:processor_impact(),
    PCtx :: mg_core_machine:processing_context(),
    ReqCtx :: mg_core:request_context(),
    Deadline :: mg_core_deadline:deadline(),
    MachineState :: mg_core_machine:machine_state(),
    Result :: mg_core_machine:processor_result().
process_machine(_, _, Impact, _, ReqCtx, _, EncodedState) ->
    State = decode_state(EncodedState),
    {Reply, Action, NewState} = do_process_machine(Impact, ReqCtx, State),
    {Reply, try_set_timer(NewState, Action), encode_state(NewState)}.

-spec do_process_machine(
    mg_core_machine:processor_impact(),
    mg_core:request_context(),
    machine_state()
) -> processor_result().
do_process_machine({init, Counter}, ?REQ_CTX, State) ->
    {{reply, ok}, sleep, State#machine_state{counter = Counter}};
do_process_machine({call, get}, ?REQ_CTX, #machine_state{counter = Counter} = State) ->
    ct:pal("Counter is ~p", [Counter]),
    {{reply, Counter}, sleep, State};
do_process_machine({call, force_timeout}, ?REQ_CTX = ReqCtx, State) ->
    TimerTarget = genlib_time:unow(),
    {{reply, ok}, sleep, State#machine_state{timer = {TimerTarget, ReqCtx}}};
do_process_machine(timeout, ?REQ_CTX, #machine_state{counter = Counter} = State) ->
    ct:pal("Counter updated to ~p", [Counter + 1]),
    {{reply, ok}, sleep, State#machine_state{counter = Counter + 1, timer = undefined}}.

-spec encode_state(machine_state()) -> mg_core_machine:machine_state().
encode_state(#machine_state{counter = Counter, timer = {TimerTarget, ReqCtx}}) ->
    [Counter, TimerTarget, ReqCtx];
encode_state(#machine_state{counter = Counter, timer = undefined}) ->
    [Counter].

-spec decode_state(mg_core_machine:machine_state()) -> machine_state().
decode_state(null) ->
    #machine_state{};
decode_state([Counter, TimerTarget, ReqCtx]) ->
    #machine_state{counter = Counter, timer = {TimerTarget, ReqCtx}};
decode_state([Counter]) ->
    #machine_state{counter = Counter}.

-spec try_set_timer(machine_state(), mg_core_machine:processor_flow_action()) ->
    mg_core_machine:processor_flow_action().
try_set_timer(#machine_state{timer = {TimerTarget, ReqCtx}}, sleep) ->
    {wait, TimerTarget, ReqCtx, 5000};
try_set_timer(#machine_state{timer = undefined}, Action) ->
    Action.

%%
%% utils
%%
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

-spec automaton_options(mg_core:ns()) -> mg_core_machine:options().
automaton_options(NS) ->
    Scheduler = #{
        min_scan_delay => timer:hours(1)
    },
    #{
        namespace => NS,
        processor => ?MODULE,
        storage => mg_cth:build_storage(NS, mg_core_storage_memory),
        worker => #{
            registry => mg_procreg_global
        },
        notification => #{
            namespace => NS,
            pulse => ?MODULE,
            storage => mg_core_storage_memory
        },
        pulse => ?MODULE,
        schedulers => #{
            timers => Scheduler,
            timers_retries => Scheduler,
            overseer => Scheduler
        }
    }.

-spec automaton_options_wo_shedulers(mg_core:ns()) -> mg_core_machine:options().
automaton_options_wo_shedulers(NS) ->
    #{
        namespace => NS,
        processor => ?MODULE,
        storage => mg_cth:build_storage(NS, mg_core_storage_memory),
        worker => #{
            registry => mg_procreg_global
        },
        notification => #{
            namespace => NS,
            pulse => ?MODULE,
            storage => mg_core_storage_memory
        },
        pulse => ?MODULE,
        schedulers => #{
            % none
        }
    }.

-spec handle_beat(_, mpulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
