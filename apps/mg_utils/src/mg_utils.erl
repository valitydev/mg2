%%%
%%% Copyright 2024 Valitydev
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

%%%
%%% То, чего не хватает в OTP.
%%% TODO перенести в genlib
%%%
-module(mg_utils).

%% API
%% OTP
-export_type([reason/0]).
-export_type([gen_timeout/0]).
-export_type([gen_start_ret/0]).
-export_type([gen_ref/0]).
-export_type([gen_reg_name/0]).
-export_type([gen_server_from/0]).
-export_type([gen_server_init_ret/1]).
-export_type([gen_server_handle_call_ret/1]).
-export_type([gen_server_handle_cast_ret/1]).
-export_type([gen_server_handle_info_ret/1]).
-export_type([gen_server_code_change_ret/1]).
-export_type([supervisor_ret/0]).
-export([gen_reg_name_to_ref/1]).
-export([gen_reg_name_to_pid/1]).
-export([gen_ref_to_pid/1]).
-export([msg_queue_len/1]).
-export([check_overload/2]).

-export_type([supervisor_old_spec/0]).
-export_type([supervisor_old_flags/0]).
-export_type([supervisor_old_child_spec/0]).
-export([supervisor_old_spec/1]).
-export([supervisor_old_flags/1]).
-export([supervisor_old_child_spec/1]).

%% Other
-export_type([mod_opts/0]).
-export_type([mod_opts/1]).
-export([apply_mod_opts/2]).
-export([apply_mod_opts/3]).
-export([apply_mod_opts_if_defined/3]).
-export([apply_mod_opts_if_defined/4]).
-export([apply_mod_opts_with_fallback/4]).
-export([separate_mod_opts/1]).
-export([separate_mod_opts/2]).

-export([throw_if_error/1]).
-export([throw_if_error/2]).
-export([throw_if_undefined/2]).
-export([exit_if_undefined/2]).

-export_type([exception/0]).
-export([raise/1]).
-export([format_exception/1]).

-export([join/2]).
-export([partition/2]).

-export([lists_compact/1]).

-export([concatenate_namespaces/2]).

-export([take_defined/1]).

%% FIXME Minor crutch for `concatenate_namespaces/2' and scheduler beats
-type ns() :: binary().
-export_type([ns/0]).

-type id() :: binary().
-export_type([id/0]).

-export_type([opaque/0]).
-type opaque() :: null | true | false | number() | binary() | [opaque()] | #{opaque() => opaque()}.

-export_type([request_context/0]).
-type request_context() :: opaque().

%%
%% API
%% OTP
%%
-type reason() ::
    normal
    | shutdown
    | {shutdown, _}
    | _.
-type gen_timeout() ::
    'hibernate'
    | timeout().

-type gen_start_ret() ::
    {ok, pid()}
    | ignore
    | {error, _}.

-type gen_ref() ::
    atom()
    | {atom(), node()}
    | {global, atom()}
    | {via, atom(), term()}
    | pid().
-type gen_reg_name() ::
    {local, atom()}
    | {global, term()}
    | {via, module(), term()}.

-type gen_server_from() :: {pid(), _}.

-type gen_server_init_ret(State) ::
    ignore
    | {ok, State}
    | {stop, reason()}
    | {ok, State, gen_timeout()}.

-type gen_server_handle_call_ret(State) ::
    {noreply, State}
    | {noreply, State, gen_timeout()}
    | {reply, _Reply, State}
    | {stop, reason(), State}
    | {reply, _Reply, State, gen_timeout()}
    | {stop, reason(), _Reply, State}.

-type gen_server_handle_cast_ret(State) ::
    {noreply, State}
    | {noreply, State, gen_timeout()}
    | {stop, reason(), State}.

-type gen_server_handle_info_ret(State) ::
    {noreply, State}
    | {noreply, State, gen_timeout()}
    | {stop, reason(), State}.

-type gen_server_code_change_ret(State) ::
    {ok, State}
    | {error, _}.

-type supervisor_ret() ::
    ignore
    | {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

-spec gen_reg_name_to_ref(gen_reg_name()) -> gen_ref().
gen_reg_name_to_ref({local, Name}) -> Name;
gen_reg_name_to_ref({global, _} = V) -> V;
% Is this correct?
gen_reg_name_to_ref({via, _, _} = V) -> V.

-spec gen_reg_name_to_pid(gen_reg_name()) -> pid() | undefined.
gen_reg_name_to_pid({global, Name}) ->
    global:whereis_name(Name);
gen_reg_name_to_pid({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_reg_name_to_pid({local, Name}) ->
    erlang:whereis(Name).

-spec gen_ref_to_pid(gen_ref()) -> pid() | undefined.
gen_ref_to_pid(Name) when is_atom(Name) ->
    erlang:whereis(Name);
gen_ref_to_pid({Name, Node}) when is_atom(Name) andalso is_atom(Node) ->
    erlang:exit(not_implemented);
gen_ref_to_pid({global, Name}) ->
    global:whereis_name(Name);
gen_ref_to_pid({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_ref_to_pid(Pid) when is_pid(Pid) ->
    Pid.

-spec msg_queue_len(gen_ref()) -> non_neg_integer() | undefined.
msg_queue_len(Ref) ->
    Pid = exit_if_undefined(gen_ref_to_pid(Ref), noproc),
    {message_queue_len, Len} = exit_if_undefined(
        erlang:process_info(Pid, message_queue_len),
        noproc
    ),
    Len.

-spec check_overload(gen_ref(), pos_integer()) -> ok | no_return().
check_overload(Ref, Limit) ->
    case msg_queue_len(Ref) < Limit of
        true -> ok;
        false -> exit(overload)
    end.

-type supervisor_old_spec() :: {supervisor_old_flags(), supervisor_old_child_spec()}.
-type supervisor_old_flags() :: _TODO.
-type supervisor_old_child_spec() :: _TODO.

-spec supervisor_old_spec({supervisor:sup_flags(), [supervisor:child_spec()]}) ->
    supervisor_old_spec().
supervisor_old_spec({Flags, ChildSpecs}) ->
    {supervisor_old_flags(Flags), lists:map(fun supervisor_old_child_spec/1, ChildSpecs)}.

-spec supervisor_old_flags(supervisor:sup_flags()) -> supervisor_old_flags().
supervisor_old_flags(#{strategy := Strategy} = Flags) ->
    {Strategy, maps:get(intensity, Flags, 1), maps:get(period, Flags, 5)}.

-spec supervisor_old_child_spec(supervisor:child_spec()) -> supervisor_old_child_spec().
supervisor_old_child_spec(#{id := ChildID, start := Start = {M, _, _}} = ChildSpec) ->
    {
        ChildID,
        Start,
        maps:get(restart, ChildSpec, permanent),
        maps:get(shutdown, ChildSpec, 5000),
        maps:get(type, ChildSpec, worker),
        maps:get(modules, ChildSpec, [M])
    }.

%%
%% Other
%%
-type mod_opts() :: mod_opts(term()).
-type mod_opts(Options) :: {module(), Options} | module().

-spec apply_mod_opts(mod_opts(), atom()) -> _Result.
apply_mod_opts(ModOpts, Function) ->
    apply_mod_opts(ModOpts, Function, []).

-spec apply_mod_opts(mod_opts(), atom(), list(_Arg)) -> _Result.
apply_mod_opts(ModOpts, Function, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    erlang:apply(Mod, Function, [Arg | Args]).

-spec apply_mod_opts_if_defined(mod_opts(), atom(), _Default) -> _Result.
apply_mod_opts_if_defined(ModOpts, Function, Default) ->
    apply_mod_opts_if_defined(ModOpts, Function, Default, []).

-spec apply_mod_opts_if_defined(mod_opts(), atom(), _Default, list(_Arg)) -> _Result.
apply_mod_opts_if_defined(ModOpts, Function, Default, Args) ->
    case prepare_applicable_mod_opts(ModOpts, Function, Args) of
        {ok, {Mod, Function, FunctionArgs}} ->
            erlang:apply(Mod, Function, FunctionArgs);
        {error, {undefined, _FunctionArgs}} ->
            Default
    end.

-spec apply_mod_opts_with_fallback(mod_opts(), atom(), Fallback :: fun(), list(_Arg)) -> _Result.
apply_mod_opts_with_fallback(ModOpts, Function, Fallback, Args) ->
    case prepare_applicable_mod_opts(ModOpts, Function, Args) of
        {ok, {Mod, Function, FunctionArgs}} ->
            erlang:apply(Mod, Function, FunctionArgs);
        {error, {undefined, FunctionArgs}} ->
            erlang:apply(Fallback, FunctionArgs)
    end.

-spec prepare_applicable_mod_opts(mod_opts(), atom(), list(_Arg)) ->
    {ok, MFArgs :: {module(), atom(), list(_Arg)}} | {error, {undefined, list(_Arg)}}.
prepare_applicable_mod_opts(ModOpts, Function, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    FunctionArgs = [Arg | Args],
    ok = maybe_load_module(Mod),
    case erlang:function_exported(Mod, Function, length(FunctionArgs)) of
        true ->
            {ok, {Mod, Function, FunctionArgs}};
        false ->
            {error, {undefined, FunctionArgs}}
    end.

-spec maybe_load_module(module()) -> ok.
maybe_load_module(Mod) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            ok;
        {error, Reason} ->
            logger:warning("An error occured, while loading module ~p, reason: ~p", [Mod, Reason])
    end.

-spec separate_mod_opts(mod_opts()) -> {module(), _Arg}.
separate_mod_opts(ModOpts) ->
    separate_mod_opts(ModOpts, undefined).

-spec separate_mod_opts(mod_opts(Defaults), Defaults) -> {module(), Defaults}.
separate_mod_opts({_, _} = ModOpts, _) ->
    ModOpts;
separate_mod_opts(Mod, Default) ->
    {Mod, Default}.

-spec throw_if_error
    (ok) -> ok;
    ({ok, Result}) -> Result;
    ({error, _Error}) -> no_return().
throw_if_error(ok) ->
    ok;
throw_if_error({ok, R}) ->
    R;
throw_if_error({error, Error}) ->
    erlang:throw(Error).

-spec throw_if_error
    (ok, _ExceptionTag) -> ok;
    ({ok, Result}, _ExceptionTag) -> Result;
    ({error, _Error}, _ExceptionTag) -> no_return().
throw_if_error(ok, _) ->
    ok;
throw_if_error({ok, R}, _) ->
    R;
throw_if_error(error, Exception) ->
    erlang:throw(Exception);
throw_if_error({error, Error}, Exception) ->
    erlang:throw({Exception, Error}).

-spec throw_if_undefined(Result, _Reason) -> Result | no_return().
throw_if_undefined(undefined, Reason) ->
    erlang:throw(Reason);
throw_if_undefined(Value, _) ->
    Value.

-spec exit_if_undefined(Result, _Reason) -> Result.
exit_if_undefined(undefined, Reason) ->
    erlang:exit(Reason);
exit_if_undefined(Value, _) ->
    Value.

-type exception() :: {exit | error | throw, term(), list()}.

-spec raise(exception()) -> no_return().
raise({Class, Reason, Stacktrace}) ->
    erlang:raise(Class, Reason, Stacktrace).

-spec format_exception(exception()) -> iodata().
format_exception({Class, Reason, Stacktrace}) ->
    io_lib:format("~s:~p ~s", [
        Class,
        Reason,
        genlib_format:format_stacktrace(Stacktrace, [newlines])
    ]).

-spec join(D, list(E)) -> list(D | E).
join(_, []) -> [];
join(_, [H]) -> H;
join(Delim, [H | T]) -> [H, Delim, join(Delim, T)].

-spec partition([T], [{Owner, Weight}, ...]) -> #{Owner => [T]} when Weight :: non_neg_integer().
partition(L, [_ | _] = Owners) ->
    WeightSum = lists:foldl(fun({_, W}, Acc) -> Acc + W end, 0, Owners),
    partition(L, Owners, erlang:max(WeightSum, 1), #{}).

-spec partition([T], [{Owner, Weight}, ...], pos_integer(), Acc) -> Acc when
    Acc :: #{Owner => [T]}, Weight :: non_neg_integer().
partition([V | Vs], Owners, WeightSum, Acc) ->
    Owner = pick(rand:uniform(WeightSum), Owners),
    Acc1 = maps:update_with(Owner, fun(Share) -> [V | Share] end, [V], Acc),
    partition(Vs, Owners, WeightSum, Acc1);
partition([], _, _, Acc) ->
    Acc.

-spec pick(pos_integer(), [{Owner, non_neg_integer()}, ...]) -> Owner.
pick(Pick, [{Owner, Weight} | _]) when Pick - Weight =< 0 ->
    Owner;
pick(_, [{Owner, _}]) ->
    % pick at least last one
    Owner;
pick(Pick, [{_, Weight} | T]) ->
    pick(Pick - Weight, T).

-spec lists_compact(list(T)) -> list(T).
lists_compact(List) ->
    lists:filter(
        fun
            (undefined) -> false;
            (_) -> true
        end,
        List
    ).

-spec concatenate_namespaces(ns(), ns()) -> ns().
concatenate_namespaces(NamespaceA, NamespaceB) ->
    <<NamespaceA/binary, "_", NamespaceB/binary>>.

-spec take_defined([T | undefined]) -> T | undefined.
take_defined([]) ->
    undefined;
take_defined([undefined | Rest]) ->
    take_defined(Rest);
take_defined([V | _]) ->
    V.
