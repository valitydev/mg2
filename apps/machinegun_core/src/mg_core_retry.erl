%%%
%%% Copyright 2018 RBKmoney
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
%%% Retry helpers.
%%% TODO: Move to genlib_retry
%%%
-module(mg_core_retry).

-export([constrain/2]).
-export([do/2]).

-type strategy() :: genlib_retry:strategy().

%% API

-spec constrain(strategy(), mg_core_deadline:deadline()) -> strategy().
constrain(Strategy, undefined) ->
    Strategy;
constrain(Strategy, Deadline) ->
    Timeout = mg_core_deadline:to_timeout(Deadline),
    genlib_retry:timecap(Timeout, Strategy).

-type throws(_Reason) :: no_return().

-spec do(strategy(), fun(() -> R | throws({transient, _}))) -> R | throws(_).
do(Strategy, Fun) ->
    try
        Fun()
    catch
        throw:(Reason = {transient, _}):ST ->
            NextStep = genlib_retry:next_step(Strategy),
            case NextStep of
                {wait, Timeout, NewStrategy} ->
                    ok = timer:sleep(Timeout),
                    do(NewStrategy, Fun);
                finish ->
                    erlang:raise(throw, Reason, ST)
            end
    end.
