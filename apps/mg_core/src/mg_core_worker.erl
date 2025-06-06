%%%
%%% Copyright 2017 RBKmoney
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

-module(mg_core_worker).

-include_lib("mg_core/include/pulse.hrl").

%% API
-export_type([options/0]).
-export_type([call_context/0]).
-export_type([call_payload/0]).
-export_type([req_ctx/0]).

-export([child_spec/2]).
-export([start_link/4]).
-export([call/7]).
-export([brutal_kill/3]).
-export([reply/2]).
-export([get_call_queue/3]).
-export([is_alive/3]).
-export([list/2]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-callback handle_load(_ID, _Args, req_ctx()) -> {ok, _State} | {error, _Error}.

-callback handle_unload(_State) -> ok.

-callback handle_call(call_payload(), call_context(), req_ctx(), mg_core_deadline:deadline(), _State) ->
    {{reply, _Reply} | noreply, _State}.

-type options() :: #{
    worker => mg_utils:mod_opts(),
    registry => mg_procreg:options(),
    hibernate_timeout => pos_integer(),
    unload_timeout => pos_integer(),
    shutdown_timeout => timeout()
}.
% в OTP он не описан, а нужно бы :(
-type call_context() :: _.
%% TODO Describe call payload and ctx
-type call_payload() :: any().
-type req_ctx() :: any().

%% Internal types

-type call_msg() :: {call, mg_core_deadline:deadline(), call_payload(), req_ctx()}.

-type pulse() :: mpulse:handler().

-define(WRAP_ID(NS, ID), {?MODULE, {NS, ID}}).
-define(DEFAULT_SHUTDOWN, brutal_kill).

-spec child_spec(atom(), options()) -> supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options]},
        restart => temporary,
        shutdown => shutdown_timeout(Options, ?DEFAULT_SHUTDOWN)
    }.

-spec start_link(options(), mg_core:ns(), mg_core:id(), req_ctx()) -> mg_utils:gen_start_ret().
start_link(Options, NS, ID, ReqCtx) ->
    mg_procreg:start_link(
        procreg_options(Options),
        ?WRAP_ID(NS, ID),
        ?MODULE,
        {ID, Options, ReqCtx},
        []
    ).

-spec call(
    options(),
    mg_core:ns(),
    mg_core:id(),
    call_payload(),
    req_ctx(),
    mg_core_deadline:deadline(),
    pulse()
) -> _Result | {error, _}.
call(Options, NS, ID, Call, ReqCtx, Deadline, Pulse) ->
    ok = mpulse:handle_beat(Pulse, #mg_core_worker_call_attempt{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline
    }),
    mg_procreg:call(
        procreg_options(Options),
        ?WRAP_ID(NS, ID),
        {call, Deadline, Call, ReqCtx},
        mg_core_deadline:to_timeout(Deadline)
    ).

%% for testing
-spec brutal_kill(options(), mg_core:ns(), mg_core:id()) -> ok.
brutal_kill(Options, NS, ID) ->
    case mg_utils:gen_reg_name_to_pid(self_ref(Options, NS, ID)) of
        undefined ->
            ok;
        Pid ->
            true = erlang:exit(Pid, kill),
            ok
    end.

%% Internal API
-spec reply(call_context(), _Reply) -> _.
reply(CallCtx, Reply) ->
    _ = gen_server:reply(CallCtx, Reply),
    ok.

-spec get_call_queue(options(), mg_core:ns(), mg_core:id()) -> [_Call].
get_call_queue(Options, NS, ID) ->
    Pid = mg_utils:exit_if_undefined(
        mg_utils:gen_reg_name_to_pid(self_ref(Options, NS, ID)),
        noproc
    ),
    [Call || {'$gen_call', _, {call, _Deadline, Call, _ReqCtx}} <- get_call_messages(Pid)].

-spec get_call_messages(pid()) -> [{'$gen_call', _From, call_msg()}].
get_call_messages(Pid) ->
    {messages, Messages} = erlang:process_info(Pid, messages),
    Messages.

-spec is_alive(options(), mg_core:ns(), mg_core:id()) -> boolean().
is_alive(Options, NS, ID) ->
    Pid = mg_utils:gen_reg_name_to_pid(self_ref(Options, NS, ID)),
    Pid =/= undefined andalso erlang:is_process_alive(Pid).

% TODO nonuniform interface
-spec list(mg_procreg:options(), mg_core:ns()) -> [{mg_core:ns(), mg_core:id(), pid()}].
list(Procreg, NS) ->
    [
        {NS, ID, Pid}
     || {?WRAP_ID(_, ID), Pid} <- mg_procreg:select(Procreg, ?WRAP_ID(NS, '$1'))
    ].

%%
%% gen_server callbacks
%%
-type state() ::
    #{
        id => _ID,
        mod => module(),
        status => {loading, _Args, req_ctx()} | {working, _State},
        unload_tref => reference() | undefined,
        hibernate_timeout => timeout(),
        unload_timeout => timeout()
    }.

-spec init(_) -> mg_utils:gen_server_init_ret(state()).
init({ID, Options = #{worker := WorkerModOpts}, ReqCtx}) ->
    _ = process_flag(trap_exit, true),
    HibernateTimeout = maps:get(hibernate_timeout, Options, 5 * 1000),
    UnloadTimeout = maps:get(unload_timeout, Options, 60 * 1000),
    {Mod, Args} = mg_utils:separate_mod_opts(WorkerModOpts),
    State = #{
        id => ID,
        mod => Mod,
        status => {loading, Args, ReqCtx},
        unload_tref => undefined,
        hibernate_timeout => HibernateTimeout,
        unload_timeout => UnloadTimeout
    },
    {ok, schedule_unload_timer(State)}.

-spec handle_call(call_msg(), mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).

% загрузка делается отдельно и лениво, чтобы не блокировать этим супервизор,
% т.к. у него легко может начать расти очередь
handle_call(
    {call, _, _, _} = Call,
    From,
    #{id := ID, mod := Mod, status := {loading, Args, ReqCtx}} = State
) ->
    case Mod:handle_load(ID, Args, ReqCtx) of
        {ok, ModState} ->
            handle_call(Call, From, State#{status := {working, ModState}});
        Error = {error, _} ->
            {stop, normal, Error, State}
    end;
handle_call(
    {call, Deadline, Call, ReqCtx},
    From,
    #{mod := Mod, status := {working, ModState}} = State
) ->
    case mg_core_deadline:is_reached(Deadline) of
        false ->
            {ReplyAction, NewModState} = Mod:handle_call(Call, From, ReqCtx, Deadline, ModState),
            NewState = State#{status := {working, NewModState}},
            case ReplyAction of
                {reply, Reply} ->
                    {reply, Reply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
                noreply ->
                    {noreply, schedule_unload_timer(NewState), hibernate_timeout(NewState)}
            end;
        true ->
            ok = logger:warning(
                "rancid worker call received: ~p from: ~p deadline: ~s reqctx: ~p",
                [Call, From, mg_core_deadline:format(Deadline), ReqCtx]
            ),
            {noreply, schedule_unload_timer(State), hibernate_timeout(State)}
    end;
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State, hibernate_timeout(State)}.

-spec handle_cast(_Cast, state()) -> mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State, hibernate_timeout(State)}.

-spec handle_info(_Info, state()) -> mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    {noreply, State, hibernate};
handle_info(
    {timeout, TRef, unload},
    #{mod := Mod, unload_tref := TRef, status := Status} = State
) ->
    case Status of
        {working, ModState} ->
            _ = Mod:handle_unload(ModState);
        {loading, _, _} ->
            ok
    end,
    {stop, normal, State};
handle_info({timeout, _, unload}, #{} = State) ->
    % А кто-то опаздал!
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)};
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info ~p", [Info]),
    {noreply, State, hibernate_timeout(State)}.

-spec code_change(_, state(), _) -> mg_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) -> ok.
terminate(_, _) ->
    ok.

%%
%% local
%%
-spec hibernate_timeout(state()) -> timeout().
hibernate_timeout(#{hibernate_timeout := Timeout}) ->
    Timeout.

-spec unload_timeout(state()) -> timeout().
unload_timeout(#{unload_timeout := Timeout}) ->
    Timeout.

%% It's 2022 and half the useful types in OTP are still not exported.
-type supervisor_shutdown() :: brutal_kill | timeout().

-spec shutdown_timeout(options(), supervisor_shutdown()) -> supervisor_shutdown() | no_return().
shutdown_timeout(#{shutdown_timeout := Timeout}, _Default) ->
    timeout_to_shutdown(Timeout);
shutdown_timeout(_, Default) ->
    Default.

-spec timeout_to_shutdown(timeout()) -> supervisor_shutdown().
timeout_to_shutdown(0) ->
    brutal_kill;
timeout_to_shutdown(infinity) ->
    infinity;
timeout_to_shutdown(Timeout) when is_integer(Timeout) andalso Timeout >= 0 ->
    Timeout.

-spec schedule_unload_timer(state()) -> state().
schedule_unload_timer(#{unload_tref := UnloadTRef} = State) ->
    _ =
        case UnloadTRef of
            undefined -> ok;
            TRef -> erlang:cancel_timer(TRef)
        end,
    State#{unload_tref := start_timer(State)}.

-spec start_timer(state()) -> reference().
start_timer(State) ->
    erlang:start_timer(unload_timeout(State), erlang:self(), unload).

-spec self_ref(options(), mg_core:ns(), mg_core:id()) -> mg_procreg:ref().
self_ref(Options, NS, ID) ->
    mg_procreg:ref(procreg_options(Options), ?WRAP_ID(NS, ID)).

-spec procreg_options(options()) -> mg_procreg:options().
procreg_options(#{registry := ProcregOptions}) ->
    ProcregOptions.
