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
-module(mpulse).

%% API
-export_type([beat/0]).
-export_type([handler/0]).
-export([handle_beat/2]).

-callback handle_beat(Options :: any(), beat()) -> ok.

%%
%% API
%%
-type beat() :: tuple() | atom() | any().

-type handler() :: mg_utils:mod_opts() | undefined.

-spec handle_beat(handler(), any()) -> ok.
handle_beat(undefined, _Beat) ->
    ok;
handle_beat(Handler, Beat) ->
    {Mod, Options} = mg_utils:separate_mod_opts(Handler),
    try
        ok = Mod:handle_beat(Options, Beat)
    catch
        Class:Reason:ST ->
            Stacktrace = genlib_format:format_stacktrace(ST),
            Msg = "Pulse handler ~p failed at beat ~p: ~p:~p ~s",
            ok = logger:error(Msg, [{Mod, Options}, Beat, Class, Reason, Stacktrace])
    end.
