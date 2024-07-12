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

-module(mg_core_events_modernizer).

-export_type([options/0]).
-export_type([machine_event/0]).
-export_type([modernized_event_body/0]).

-export([modernize_machine/4]).

%%

-type options() :: #{
    current_format_version := mg_core_events:format_version(),
    handler := mg_core_utils:mod_opts(handler_opts())
}.

% handler specific
-type handler_opts() :: term().
% handler specific
-type request_context() :: term().

-type machine_event() :: #{
    ns => mg_core:ns(),
    id => mg_core:id(),
    event => mg_core_events:event()
}.

-type modernized_event_body() :: mg_core_events:body().

-callback modernize_event(handler_opts(), request_context(), machine_event()) ->
    modernized_event_body().

%%

-type id() :: mg_core_events_machine:id().
-type history_range() :: mg_core_events:history_range().

-spec modernize_machine(
    mg_core_events_machine:options(),
    request_context(),
    id(),
    history_range()
) -> ok.
modernize_machine(EventsMachineOptions, _ReqCtx, ID, HRange) ->
    #{ns := _NS, id := ID, history := History} =
        mg_core_events_machine:get_machine(EventsMachineOptions, ID, HRange),
    lists:foreach(
        fun(Event) ->
            store_event(EventsMachineOptions, ID, Event)
        end,
        History
    ).

-spec store_event(mg_core_events_machine:options(), mg_core:id(), mg_core_events:event()) -> ok.
store_event(Options, ID, Event) ->
    mg_core_events_storage:store_event(Options, ID, Event).
