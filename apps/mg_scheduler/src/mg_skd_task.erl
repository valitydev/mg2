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

-module(mg_skd_task).

-type id() :: any().
-type payload() :: any().
% unix timestamp in seconds
-type target_time() :: genlib_time:ts().

-type task(TaskID, TaskPayload) :: #{
    id := TaskID,
    target_time := target_time(),
    machine_id := mg_utils:id(),
    payload => TaskPayload
}.

-type task() :: task(id(), payload()).

-export_type([id/0]).
-export_type([target_time/0]).
-export_type([task/2]).
-export_type([task/0]).

-export([current_time/0]).

%%

-spec current_time() -> target_time().
current_time() ->
    genlib_time:unow().
