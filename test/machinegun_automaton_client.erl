%%%
%%% Copyright 2020 RBKmoney
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

-module(machinegun_automaton_client).

%% API
-export_type([options/0]).

-export([start/3]).
-export([start/4]).
-export([repair/3]).
-export([repair/4]).
-export([simple_repair/2]).
-export([simple_repair/3]).
-export([remove/2]).
-export([remove/3]).
-export([call/3]).
-export([call/4]).
-export([get_machine/3]).
-export([get_machine/4]).
-export([modernize/3]).

%% уменьшаем писанину
-import(mg_woody_api_packer, [pack/2, unpack/2]).

%%
%% API
%%
-type options() :: #{
    url := URL :: string(),
    ns := mg_core:ns(),
    retry_strategy => genlib_retry:strategy(),
    transport_opts => woody_client_thrift_http_transport:transport_options()
}.

-spec start(options(), mg_core:id(), mg_core_storage:opaque()) -> ok.
start(Options, ID, Args) ->
    start(Options, ID, Args, undefined).

-spec start(options(), mg_core:id(), mg_core_storage:opaque(), mg_core_deadline:deadline()) -> ok.
start(#{ns := NS} = Options, ID, Args, Deadline) ->
    ok = call_service(Options, 'Start', {pack(ns, NS), pack(id, ID), pack(args, Args)}, Deadline).

-spec repair(options(), mg_core_events_machine:id(), mg_core_storage:opaque()) -> ok.
repair(Options, ID, Args) ->
    repair(Options, ID, Args, undefined).

-spec repair(options(), mg_core_events_machine:id(), mg_core_storage:opaque(), mg_core_deadline:deadline()) -> ok.
repair(#{ns := NS} = Options, ID, Args, Deadline) ->
    Response = call_service(Options, 'Repair', {machine_desc(NS, ID), pack(args, Args)}, Deadline),
    unpack(repair_response, Response).

-spec simple_repair(options(), mg_core_events_machine:id()) -> ok.
simple_repair(Options, ID) ->
    simple_repair(Options, ID, undefined).

-spec simple_repair(options(), mg_core_events_machine:id(), mg_core_deadline:deadline()) -> ok.
simple_repair(#{ns := NS} = Options, ID, Deadline) ->
    ok = call_service(Options, 'SimpleRepair', {pack(ns, NS), pack(ref, ID)}, Deadline).

-spec remove(options(), mg_core:id()) -> ok.
remove(Options, ID) ->
    remove(Options, ID, undefined).

-spec remove(options(), mg_core:id(), mg_core_deadline:deadline()) -> ok.
remove(#{ns := NS} = Options, ID, Deadline) ->
    ok = call_service(Options, 'Remove', {pack(ns, NS), pack(id, ID)}, Deadline).

-spec call(options(), mg_core_events_machine:id(), mg_core_storage:opaque()) -> mg_core_storage:opaque().
call(Options, ID, Args) ->
    call(Options, ID, Args, undefined).

-spec call(options(), mg_core_events_machine:id(), mg_core_storage:opaque(), mg_core_deadline:deadline()) ->
    mg_core_storage:opaque().
call(#{ns := NS} = Options, ID, Args, Deadline) ->
    unpack(
        call_response,
        call_service(Options, 'Call', {machine_desc(NS, ID), pack(args, Args)}, Deadline)
    ).

-spec get_machine(options(), mg_core_events_machine:id(), mg_core_events:history_range()) ->
    mg_core_events_machine:machine().
get_machine(Options, ID, Range) ->
    get_machine(Options, ID, Range, undefined).

-spec get_machine(
    options(),
    mg_core_events_machine:id(),
    mg_core_events:history_range(),
    mg_core_deadline:deadline()
) ->
    mg_core_events_machine:machine().
get_machine(#{ns := NS} = Options, ID, Range, Deadline) ->
    unpack(
        machine,
        call_service(Options, 'GetMachine', {machine_desc(NS, ID, Range)}, Deadline)
    ).

-spec modernize(options(), mg_core_events_machine:id(), mg_core_events:history_range()) -> ok.
modernize(#{ns := NS} = Options, ID, Range) ->
    ok = call_service(Options, 'Modernize', {machine_desc(NS, ID, Range)}, undefined).

%%
%% local
%%
-spec machine_desc(mg_core:ns(), mg_core_events_machine:id()) -> _.
machine_desc(NS, ID) ->
    machine_desc(NS, ID, {undefined, undefined, forward}).

-spec machine_desc(mg_core:ns(), mg_core_events_machine:id(), mg_core_events:history_range()) -> _.
machine_desc(NS, ID, HRange) ->
    pack(machine_descriptor, {NS, ID, HRange}).

-spec call_service(options(), atom(), tuple(), mg_core_deadline:deadline()) -> any().
call_service(#{retry_strategy := Strategy} = Options, Function, Args, Deadline) ->
    try woody_call(Options, Function, Args, Deadline) of
        {ok, R} ->
            R;
        {exception, Exception} ->
            erlang:throw(Exception)
    catch
        error:{woody_error, {_Source, Class, _Details}} = Error when
            Class =:= resource_unavailable orelse Class =:= result_unknown
        ->
            case genlib_retry:next_step(Strategy) of
                {wait, Timeout, NewStrategy} ->
                    ok = timer:sleep(Timeout),
                    call_service(Options#{retry_strategy := NewStrategy}, Function, Args, Deadline);
                finish ->
                    erlang:error(Error)
            end
    end;
call_service(Options, Function, Args, Deadline) ->
    call_service(Options#{retry_strategy => finish}, Function, Args, Deadline).

-spec woody_call(options(), atom(), tuple(), mg_core_deadline:deadline()) -> any().
woody_call(#{url := BaseURL} = Options, Function, Args, Deadline) ->
    TransportOptions = maps:get(transport_opts, Options, #{}),
    Context = mg_woody_api_utils:set_deadline(Deadline, woody_context:new()),
    woody_client:call(
        {{mg_proto_state_processing_thrift, 'Automaton'}, Function, Args},
        #{
            url => BaseURL ++ "/v1/automaton",
            event_handler => {mg_woody_api_event_handler, {machinegun_pulse, #{}}},
            transport_opts => TransportOptions
        },
        Context
    ).
