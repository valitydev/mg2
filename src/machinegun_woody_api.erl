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

%%%
%%% Главный модуль приложения.
%%% Тут из конфига строится дерево супервизоров и генерируются структуры с настройками.
%%%
-module(machinegun_woody_api).

%% API
-export([child_spec/2]).

%%
%% API
%%

-export_type([woody_server/0]).

-type woody_server() :: #{
    ip             := tuple(),
    port           := inet:port_number(),
    transport_opts => woody_server_thrift_http_handler:transport_opts(),
    protocol_opts  => woody_server_thrift_http_handler:protocol_opts(),
    limits         => woody_server_thrift_http_handler:handler_limits()
}.

-type automaton() :: mg_woody_api_automaton:options().

-type event_sink() :: mg_woody_api_event_sink:options().

-type options() :: #{
    woody_server := woody_server(),
    health_check := erl_health:check(),
    automaton    := automaton(),
    event_sink   := event_sink(),
    pulse        := module()
}.

-spec child_spec(term(), options()) ->
    supervisor:child_spec().
child_spec(ID, Options) ->
    #{
        woody_server := WoodyConfig,
        health_check := HealthCheck,
        automaton    := Automaton,
        event_sink   := EventSink,
        pulse        := PulseHandler
    } = Options,
    HealthRoute = erl_health_handle:get_route(enable_health_logging(HealthCheck)),
    woody_server:child_spec(
        ID,
        #{
            protocol       => thrift,
            transport      => http,
            ip             => maps:get(ip             , WoodyConfig),
            port           => maps:get(port           , WoodyConfig),
            transport_opts => maps:get(transport_opts , WoodyConfig, #{}),
            protocol_opts  => maps:get(protocol_opts  , WoodyConfig, #{}),
            event_handler  => {mg_woody_api_event_handler, PulseHandler},
            handler_limits => maps:get(limits         , WoodyConfig, #{}),
            handlers       => [
                mg_woody_api_automaton :handler(Automaton),
                mg_woody_api_event_sink:handler(EventSink)
            ],
            additional_routes => [HealthRoute]
        }
    ).

-spec enable_health_logging(erl_health:check()) ->
    erl_health:check().
enable_health_logging(Check) ->
    EvHandler = {erl_health_event_handler, []},
    maps:map(fun (_, V = {_, _, _}) -> #{runner => V, event_handler => EvHandler} end, Check).
