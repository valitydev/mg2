-module(mg_skd_utils).

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

%%

-export([separate_mod_opts/1]).
-export([lists_compact/1]).

%%

-type opaque() :: null | true | false | number() | binary() | [opaque()] | #{opaque() => opaque()}.
-type ns() :: binary().
-type id() :: binary().
-type request_context() :: opaque().

-export_type([opaque/0]).
-export_type([ns/0]).
-export_type([id/0]).
-export_type([request_context/0]).

-type exception() :: {exit | error | throw, term(), list()}.

-export_type([exception/0]).

-type mod_opts() :: mod_opts(term()).
-type mod_opts(Options) :: {module(), Options} | module().

-export_type([mod_opts/0]).
-export_type([mod_opts/1]).

%% OTP

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

%%

-spec separate_mod_opts(mod_opts()) -> {module(), _Arg}.
separate_mod_opts(ModOpts) ->
    separate_mod_opts(ModOpts, undefined).

-spec separate_mod_opts(mod_opts(Defaults), Defaults) -> {module(), Defaults}.
separate_mod_opts(ModOpts = {_, _}, _) ->
    ModOpts;
separate_mod_opts(Mod, Default) ->
    {Mod, Default}.

-spec lists_compact(list(T)) -> list(T).
lists_compact(List) ->
    lists:filter(
        fun
            (undefined) -> false;
            (_) -> true
        end,
        List
    ).
