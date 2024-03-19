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

-module(mg_cth_processor).

-export([start/3]).
-export([start/4]).
-export([start_link/4]).
-export([default_result/2]).

%% processor handlers
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

-export_type([handler_info/0]).
-export_type([processor_functions/0]).
-export_type([modernizer_function/0]).

-type processor_signal_function() ::
    fun((mg_core_events_machine:signal_args()) -> mg_core_events_machine:signal_result()).

-type processor_call_function() ::
    fun((mg_core_events_machine:call_args()) -> mg_core_events_machine:call_result()).

-type processor_repair_function() ::
    fun((mg_core_events_machine:repair_args()) -> mg_core_events_machine:repair_result()).

-type processor_functions() :: #{
    signal => processor_signal_function(),
    call => processor_call_function(),
    repair => processor_repair_function()
}.

-type modernizer_function() ::
    fun((mg_core_events_modernizer:machine_event()) -> mg_core_events_modernizer:modernized_event_body()).

-type modernizer_functions() :: #{
    modernize => modernizer_function()
}.

-type options() :: #{
    processor => {string(), processor_functions()},
    modernizer => {string(), modernizer_functions()}
}.

-type functions() :: processor_functions() | modernizer_functions().

-type endpoint() :: {inet:ip_address(), inet:port_number()}.
-type handler_info() :: #{endpoint := endpoint()}.

%%
%% API
%%

-spec start(_ID, endpoint(), options()) -> {ok, pid(), handler_info()} | {error, _}.
start(ID, Endpoint, Options) ->
    start(ID, Endpoint, Options, undefined).

-spec start(_ID, endpoint(), options(), any()) -> {ok, pid(), handler_info()} | {error, _}.
start(ID, Endpoint, Options, MgConfig) ->
    case start_link(ID, Endpoint, Options, MgConfig) of
        {ok, ProcessorPid, HandlerInfo} ->
            true = erlang:unlink(ProcessorPid),
            {ok, ProcessorPid, HandlerInfo};
        ErrorOrIgnore ->
            ErrorOrIgnore
    end.

-spec start_link(_ID, endpoint(), options(), any()) -> {ok, pid(), handler_info()} | {error, _}.
start_link(ID, {Host, Port}, Options, MgConfig) ->
    Flags = #{strategy => one_for_all},
    ChildsSpecs = [
        woody_server:child_spec(
            ID,
            #{
                ip => Host,
                port => Port,
                event_handler => {mg_woody_event_handler, mg_cth_pulse},
                handlers => maps:values(
                    maps:map(
                        fun
                            (processor, {Path, Functions}) ->
                                {Path, {{mg_proto_state_processing_thrift, 'Processor'}, {?MODULE, Functions}}};
                            (modernizer, {Path, Functions}) ->
                                {Path, {{mg_proto_state_processing_thrift, 'Modernizer'}, {?MODULE, Functions}}}
                        end,
                        Options
                    )
                )
            }
        )
        | mg_cth_configurator:construct_child_specs(MgConfig)
    ],
    case genlib_adhoc_supervisor:start_link(Flags, ChildsSpecs) of
        {ok, SupPid} ->
            Endpoint = woody_server:get_addr(ID, Options),
            {ok, SupPid, #{endpoint => Endpoint}};
        Error ->
            Error
    end.

%%
%% processor handlers
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), functions()) ->
    {ok, _Result} | no_return().
handle_function('ProcessSignal', {Args}, _WoodyContext, Functions) ->
    UnpackedArgs = mg_woody_packer:unpack(signal_args, Args),
    Result = invoke_function(signal, Functions, UnpackedArgs),
    {ok, mg_woody_packer:pack(signal_result, Result)};
handle_function('ProcessCall', {Args}, _WoodyContext, Functions) ->
    UnpackedArgs = mg_woody_packer:unpack(call_args, Args),
    Result = invoke_function(call, Functions, UnpackedArgs),
    {ok, mg_woody_packer:pack(call_result, Result)};
handle_function('ProcessRepair', {Args}, _WoodyContext, Functions) ->
    UnpackedArgs = mg_woody_packer:unpack(repair_args, Args),
    {ok, Result} = invoke_function(repair, Functions, UnpackedArgs),
    {ok, mg_woody_packer:pack(repair_result, Result)};
handle_function('ModernizeEvent', {Args}, _WoodyContext, Functions) ->
    MachineEvent = mg_woody_packer:unpack(machine_event, Args),
    Result = invoke_function(modernize, Functions, MachineEvent),
    {ok, mg_woody_packer:pack(modernize_result, Result)}.

%%
%% helpers
%%
-spec invoke_function
    (signal, functions(), term()) -> mg_core_events_machine:signal_result();
    (call, functions(), term()) -> mg_core_events_machine:call_result();
    (repair, functions(), term()) -> mg_core_events_machine:repair_result();
    (modernize, functions(), term()) -> mg_core_events_modernizer:modernized_event_body().
invoke_function(Type, Functions, Args) ->
    case maps:find(Type, Functions) of
        {ok, Fun} ->
            Fun(Args);
        error ->
            default_result(Type, Args)
    end.

-spec default_result
    (signal, term()) -> mg_core_events_machine:signal_result();
    (call, term()) -> mg_core_events_machine:call_result();
    (repair, term()) -> mg_core_events_machine:repair_result();
    (modernize, term()) -> mg_core_events_modernizer:modernized_event_body().
default_result(signal, _Args) ->
    {{default_content(), []}, #{timer => undefined}};
default_result(call, _Args) ->
    {<<>>, {default_content(), []}, #{timer => undefined}};
default_result(repair, _Args) ->
    {ok, {<<>>, {default_content(), []}, #{timer => undefined}}};
default_result(modernize, #{event := #{body := Body}}) ->
    Body.

-spec default_content() -> mg_core_events:content().
default_content() ->
    {#{}, <<>>}.
