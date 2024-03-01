%%%
%%% Copyright 2020 Valitydev
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
-module(mg_woody).

%% API
-export([child_spec/2]).

%%
%% API
%%

-export_type([woody_server/0]).

-type woody_server() :: #{
    ip := tuple(),
    port := inet:port_number(),
    transport_opts => woody_server_thrift_http_handler:transport_opts(),
    protocol_opts => woody_server_thrift_http_handler:protocol_opts(),
    limits => woody_server_thrift_http_handler:handler_limits(),
    shutdown_timeout => timeout()
}.

-type automaton() :: mg_woody_automaton:options().

-type options() :: #{
    pulse := module(),
    automaton := automaton(),
    woody_server := woody_server(),
    additional_routes => [woody_server_thrift_http_handler:route(any())]
}.

-spec child_spec(term(), options()) -> supervisor:child_spec().
child_spec(ID, Options) ->
    #{
        woody_server := WoodyConfig,
        automaton := Automaton,
        pulse := PulseHandler
    } = Options,
    WoodyOptions = maps:merge(
        #{
            protocol => thrift,
            transport => http,
            ip => maps:get(ip, WoodyConfig),
            port => maps:get(port, WoodyConfig),
            event_handler => {mg_woody_event_handler, PulseHandler},
            handlers => [
                mg_woody_automaton:handler(Automaton)
            ]
        },
        genlib_map:compact(#{
            transport_opts => maps:get(transport_opts, WoodyConfig, undefined),
            protocol_opts => maps:get(protocol_opts, WoodyConfig, undefined),
            handler_limits => maps:get(limits, WoodyConfig, undefined),
            additional_routes => maps:get(additional_routes, Options, undefined),
            shutdown_timeout => maps:get(shutdown_timeout, WoodyConfig, undefined)
        })
    ),
    woody_server:child_spec(ID, WoodyOptions).
