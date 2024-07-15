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

-module(mg_procreg).

% Any term sans ephemeral ones, like `reference()`s / `pid()`s / `fun()`s.
-type name() :: term().
-type name_pattern() :: ets:match_pattern().

-type ref() :: mg_utils:gen_ref().
-type reg_name() :: mg_utils:gen_reg_name().

-type procreg_options() :: term().
-type options() :: mg_utils:mod_opts(procreg_options()).

-export_type([name/0]).
-export_type([name_pattern/0]).
-export_type([ref/0]).
-export_type([reg_name/0]).
-export_type([options/0]).

-export_type([start_link_ret/0]).

-export([ref/2]).
-export([reg_name/2]).
-export([select/2]).

-export([start_link/5]).
-export([call/3]).
-export([call/4]).

%% Names and references

-callback ref(procreg_options(), name()) -> ref().

-callback reg_name(procreg_options(), name()) -> reg_name().

-callback select(procreg_options(), name_pattern()) -> [{name(), pid()}].

-callback call(procreg_options(), ref(), _Call, timeout()) -> _Reply.

-type start_link_ret() ::
    {ok, pid()} | {error, term()}.

-callback start_link(procreg_options(), reg_name(), module(), _Args, list()) -> start_link_ret().

-optional_callbacks([
    call/4,
    start_link/5
]).

%%

-spec ref(options(), name()) -> ref().
ref(Options, Name) ->
    mg_utils:apply_mod_opts(Options, ref, [Name]).

-spec reg_name(options(), name()) -> reg_name().
reg_name(Options, Name) ->
    mg_utils:apply_mod_opts(Options, reg_name, [Name]).

%% TODO Review usage of this function
-spec select(options(), name_pattern()) -> [{name(), pid()}].
select(Options, NamePattern) ->
    mg_utils:apply_mod_opts(Options, select, [NamePattern]).

%% @doc Functions `call/3', `call/4' and `start_link/5' wrap according
%% `gen_server:call/4' and `gen_server:start_link/4' calls to a
%% worker's gen_server implementation.
%%
%% Direct interaction with a process registry implementation is
%% expected to be performed by separate module with an actual process
%% registry "behaviour".
%% In general this behaviour requires `ref/2'
%% (for referencing pid) and `reg_name/2' (for registering new
%% reference for pid) callbacks that return via-tuple:
%%
%%     {via, RegMod :: module(), ViaName :: term()}
%%
%%     Register the gen_server process with the registry represented
%%     by RegMod. The RegMod callback is to export the functions
%%     register_name/2, unregister_name/1, whereis_name/1, and send/2,
%%     which are to behave like the corresponding functions in
%%     global. Thus, {via,global,GlobalName} is a valid reference
%%     equivalent to {global,GlobalName}.
%%
%% @end
-spec call(options(), name(), _Call) -> _Reply.
call(Options, Name, Call) ->
    call(Options, Name, Call, 5000).

-spec call(options(), name(), _Call, timeout()) -> _Reply.
call(Options, Name, Call, Timeout) ->
    CallArgs = [ref(Options, Name), Call, Timeout],
    mg_utils:apply_mod_opts_with_fallback(Options, call, fun gen_call/4, CallArgs).

-spec start_link(options(), name(), module(), _Args, list()) -> start_link_ret().
start_link(Options, Name, Module, Args, Opts) ->
    StartArgs = [reg_name(Options, Name), Module, Args, Opts],
    mg_utils:apply_mod_opts_with_fallback(Options, start_link, fun gen_start_link/5, StartArgs).

%% Internal

-spec gen_start_link(options(), mg_procreg:reg_name(), module(), _Args, list()) ->
    mg_procreg:start_link_ret().
gen_start_link(_Options, RegName, Module, Args, Opts) ->
    gen_server:start_link(RegName, Module, Args, Opts).

-spec gen_call(options(), mg_procreg:ref(), _Call, timeout()) -> _Reply.
gen_call(_Options, Ref, Call, Timeout) ->
    gen_server:call(Ref, Call, Timeout).
