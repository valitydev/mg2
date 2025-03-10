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

-module(mg_stress_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([stress_test/1]).

-define(NS, <<"NS">>).

-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name()].
all() ->
    [
        stress_test
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Config = mg_woody_config(C),
    Apps = mg_cth:start_applications([
        brod,
        {hackney, [{use_default_pool, false}]},
        mg_woody,
        opentelemetry_exporter,
        opentelemetry
    ]),
    CallFunc = fun({Args, _Machine}) ->
        case Args of
            <<"event">> ->
                {Args, {{#{}, <<>>}, [{#{}, <<"event_body">>}]}, #{
                    timer => undefined
                }};
            _ ->
                {Args, {{#{}, <<>>}, []}, #{timer => undefined}}
        end
    end,

    SignalFunc = fun({Args, _Machine}) ->
        _ = mg_cth_processor:default_result(signal, Args)
    end,

    {ok, ProcessorPid, _HandlerInfo} = mg_cth_processor:start(
        ?MODULE,
        {{0, 0, 0, 0}, 8023},
        #{
            processor =>
                {"/processor", #{
                    signal => SignalFunc,
                    call => CallFunc
                }}
        },
        Config
    ),

    [
        {apps, Apps},
        {automaton_options, #{
            url => "http://localhost:8022",
            ns => ?NS,
            retry_strategy => genlib_retry:new_strategy({exponential, 5, 2, 1000})
        }},
        {processor_pid, ProcessorPid}
        | C
    ].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    ok = proc_lib:stop(?config(processor_pid, C)),
    mg_cth:stop_applications(?config(apps, C)).

-spec init_per_testcase(atom(), config()) -> config().
init_per_testcase(Name, C) ->
    mg_cth:trace_testcase(?MODULE, Name, C).

-spec end_per_testcase(atom(), config()) -> _.
end_per_testcase(_Name, C) ->
    ok = mg_cth:maybe_end_testcase_trace(C).

-spec mg_woody_config(config()) -> map().
mg_woody_config(_C) ->
    #{
        woody_server => #{
            ip => {0, 0, 0, 0, 0, 0, 0, 0},
            port => 8022,
            limits => #{},
            transport_opts => #{num_acceptors => 100}
        },
        namespaces => #{
            ?NS => #{
                storage => mg_core_storage_memory,
                processor => #{
                    url => <<"http://localhost:8023/processor">>,
                    transport_opts => #{pool => ns, max_connections => 100}
                },
                default_processing_timeout => 5000,
                schedulers => #{
                    timers => #{}
                },
                retries => #{},
                event_sinks => [
                    {mg_event_sink_kafka, #{
                        name => kafka,
                        topic => <<"mg_core_event_sink">>,
                        client => mg_cth:config(kafka_client_name)
                    }}
                ],
                event_stash_size => 10,
                worker => #{
                    registry => mg_procreg_global,
                    sidecar => mg_cth_worker
                }
            }
        }
    }.

-spec stress_test(config()) -> _.
stress_test(C) ->
    TestTimeout = 5 * 1000,
    N = 10,

    Processes = [stress_test_start_processes(C, integer_to_binary(ID)) || ID <- lists:seq(1, N)],

    ok = timer:sleep(TestTimeout),
    ok = mg_cth:stop_wait_all(Processes, shutdown, 2000).

-spec stress_test_start_processes(term(), mg_core:id()) -> _.
stress_test_start_processes(C, ID) ->
    OtelCtx = otel_ctx:get_current(),
    Pid =
        erlang:spawn_link(
            fun() ->
                _ = otel_ctx:attach(OtelCtx),
                start_machine(C, ID),
                create(C, ID)
            end
        ),
    timer:sleep(1000),
    Pid.

%%
%% utils
%%
-spec start_machine(config(), mg_core:id()) -> _.
start_machine(C, ID) ->
    mg_cth_automaton_client:start(automaton_options(C), ID, ID).

-spec create(config(), mg_core:id()) -> _.
create(C, ID) ->
    _ = create_event(<<"event">>, C, ID),
    timer:sleep(1000),
    create(C, ID).

-spec create_event(binary(), config(), mg_core:id()) -> _.
create_event(Event, C, ID) ->
    Event = mg_cth_automaton_client:call(automaton_options(C), ID, Event).

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).
