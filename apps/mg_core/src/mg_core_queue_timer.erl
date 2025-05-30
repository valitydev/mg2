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

-module(mg_core_queue_timer).

-export([build_task/2]).

-behaviour(mg_skd_scanner).
-export([init/1]).
-export([search_tasks/3]).

-behaviour(mg_skd_worker).
-export([execute_task/2]).

%% Types

-type seconds() :: non_neg_integer().
-type milliseconds() :: non_neg_integer().
-type options() :: #{
    pulse := mpulse:handler(),
    machine := mg_core_machine:options(),
    timer_queue := waiting | retrying,
    lookahead => seconds(),
    min_scan_delay => milliseconds(),
    processing_timeout => timeout()
}.

-record(state, {}).

-opaque state() :: #state{}.

-export_type([state/0]).
-export_type([options/0]).

%% Internal types

-type task_id() :: mg_core:id().
-type task_payload() :: #{}.
-type target_time() :: mg_skd_task:target_time().
-type task() :: mg_skd_task:task(task_id(), task_payload()).
-type scan_delay() :: mg_skd_scanner:scan_delay().
-type scan_limit() :: mg_skd_scanner:scan_limit().

% 1 minute
-define(DEFAULT_PROCESSING_TIMEOUT, 60000).

%%
%% API
%%

-spec init(options()) -> {ok, state()}.
init(_Options) ->
    {ok, #state{}}.

-spec build_task(mg_core:id(), target_time()) -> task().
build_task(ID, Timestamp) ->
    #{
        id => ID,
        target_time => Timestamp,
        machine_id => ID
    }.

-spec search_tasks(options(), scan_limit(), state()) -> {{scan_delay(), [task()]}, state()}.
search_tasks(#{timer_queue := TimerQueue} = Options, Limit, #state{} = State) ->
    CurrentTs = mg_skd_task:current_time(),
    Lookahead = maps:get(lookahead, Options, 0),
    Query = {TimerQueue, 1, CurrentTs + Lookahead},
    {Timers, Continuation} = mg_core_machine:search(machine_options(Options), Query, Limit),
    {Tasks, LastTs} = lists:mapfoldl(
        fun({Ts, ID}, _LastWas) -> {build_task(ID, Ts), Ts} end,
        CurrentTs,
        Timers
    ),
    MinDelay = maps:get(min_scan_delay, Options, 1000),
    OptimalDelay =
        case Continuation of
            undefined -> seconds_to_delay(Lookahead);
            _Other -> seconds_to_delay(LastTs - CurrentTs)
        end,
    Delay = erlang:max(OptimalDelay, MinDelay),
    {{Delay, Tasks}, State}.

-spec seconds_to_delay(_Seconds :: integer()) -> scan_delay().
seconds_to_delay(Seconds) ->
    erlang:convert_time_unit(Seconds, second, millisecond).

-spec execute_task(options(), task()) -> ok.
execute_task(Options, #{id := MachineID, target_time := Timestamp}) ->
    %% NOTE
    %% Machine identified by `MachineID` may in fact already have processed timeout signal so that
    % the task we're in is already stale, and we could shed it by reading the machine status. But we
    % expect that most tasks are not stale yet and most of the time machine is online, therefore
    % it's very likely that reading machine status is just unnecessary.
    Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
    Deadline = mg_core_deadline:from_timeout(Timeout),
    ok = mg_core_machine:send_timeout(machine_options(Options), MachineID, Timestamp, Deadline).

-spec machine_options(options()) -> mg_core_machine:options().
machine_options(#{machine := MachineOptions}) ->
    MachineOptions.
