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

-module(gen_squad_pulse).

-callback handle_beat(_Options, beat()) -> _.

%% TODO remove weak circular deps
-type beat() ::
    {rank, {changed, gen_squad:rank()}}
    | {
        {member, pid()},
        added
        | {refreshed, gen_squad:member()}
        | {removed, gen_squad:member(), _Reason :: lost | {down, _}}
    }
    | {
        {broadcast, gen_squad_heart:payload()},
        {sent, [pid()], _Ctx}
        | received
    }
    | {
        {timer, reference()},
        {started, _Timeout :: non_neg_integer(), _Msg}
        | cancelled
        | {fired, _Msg}
    }
    | {
        {monitor, reference()},
        {started, pid()}
        | cancelled
        | {fired, pid(), _Reason}
    }
    | {unexpected, {{call, _From} | cast | info, _Payload}}.

-type mod_opts() :: mod_opts(term()).
-type mod_opts(Options) :: {module(), Options} | module().

-type handler() :: mod_opts().

-export_type([beat/0]).
-export_type([handler/0]).

-export([handle_beat/2]).

%%

-spec handle_beat(handler(), any()) -> _.
handle_beat(Handler, Beat) ->
    {Mod, Options} = separate_mod_opts(Handler),
    Mod:handle_beat(Options, Beat).

%%

-spec separate_mod_opts(mod_opts()) -> {module(), _Arg}.
separate_mod_opts(ModOpts) ->
    separate_mod_opts(ModOpts, undefined).

-spec separate_mod_opts(mod_opts(Defaults), Defaults) -> {module(), Defaults}.
separate_mod_opts({_, _} = ModOpts, _) ->
    ModOpts;
separate_mod_opts(Mod, Default) ->
    {Mod, Default}.
