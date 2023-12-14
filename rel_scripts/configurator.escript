#!/usr/bin/env escript
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

-mode(compile).
-define(C, machinegun_configuration_utils).

%%
%% main
%%
main([YamlConfigFilename, ConfigsPath]) ->
    ok = logger:set_primary_config(level, error),
    {ok, [[Home]]} = init:get_argument(home),
    Preprocessors = [fun interpolate_envs/1],
    YamlConfig = lists:foldl(
        fun(Proc, Config) -> Proc(Config) end,
        ?C:parse_yaml_config(YamlConfigFilename),
        Preprocessors
    ),
    InetrcFilename = filename:join(ConfigsPath, "erl_inetrc"),
    ErlangCookieFilename = filename:join(Home, ".erlang.cookie"),
    ok = ?C:write_file(filename:join(ConfigsPath, "sys.config"), ?C:print_sys_config(sys_config(YamlConfig))),
    ok = ?C:write_file(filename:join(ConfigsPath, "vm.args"), ?C:print_vm_args(vm_args(YamlConfig, InetrcFilename))),
    ok = ?C:write_file(InetrcFilename, ?C:print_erl_inetrc(erl_inetrc(YamlConfig))),
    % TODO
    % Writing distribution cookie to the file which BEAM looks for when setting up the
    % distribution under *nix, as a fallback mechanism when missing `-setcookie` arg from
    % command line.
    % It's the only method not to expose cookie contents in the BEAM command line in a way which
    % doesn't break various start script functions (e.g. remsh or ping), however it may still
    % appear there for a brief amount of time while running them.
    % One must take care to run service under its own UID because `~/.erlang.cookie` is supposed
    % to be shared between every BEAM instance run by some user.
    ok = ?C:write_file(ErlangCookieFilename, cookie(YamlConfig), 8#00400).

interpolate_envs(YamlConfig) ->
    ?C:traverse(
        fun
            (value, Str) when is_binary(Str) ->
                {replace, ?C:interpolate(fun ?C:env/1, Str)};
            (_, _) ->
                proceed
        end,
        YamlConfig
    ).

%%
%% sys.config
%%
sys_config(YamlConfig) ->
    [
        {os_mon, os_mon(YamlConfig)},
        {kernel, [
            {logger_level, logger_level(YamlConfig)},
            {logger, logger(YamlConfig)}
        ]},
        {consuela, consuela(YamlConfig)},
        {prometheus, prometheus(YamlConfig)},
        {snowflake, snowflake(YamlConfig)},
        {brod, brod(YamlConfig)},
        {hackney, hackney(YamlConfig)},
        {machinegun, machinegun(YamlConfig)},
        {opentelemetry, opentelemetry(YamlConfig)},
        {opentelemetry_exporter, opentelemetry_exporter(YamlConfig)}
    ].

os_mon(_YamlConfig) ->
    [
        % for better compatibility with busybox coreutils
        {disksup_posix_only, true}
    ].

logger_level(YamlConfig) ->
    ?C:log_level(?C:conf([logging, level], YamlConfig, <<"info">>)).

logger(YamlConfig) ->
    Root = ?C:conf([logging, root], YamlConfig, <<"/var/log/machinegun">>),
    LogfileName = ?C:conf([logging, json_log], YamlConfig, <<"log.json">>),
    FullLogname = filename:join(Root, LogfileName),
    OutType = ?C:atom(?C:conf([logging, out_type], YamlConfig, <<"file">>)),
    Out =
        case OutType of
            file -> #{type => file, file => ?C:string(FullLogname)};
            stdout -> #{type => standard_io}
        end,
    [
        {handler, default, logger_std_h, #{
            level => debug,
            config => maps:merge(Out, #{
                burst_limit_enable => ?C:conf([logging, burst_limit_enable], YamlConfig, true),
                sync_mode_qlen => ?C:conf([logging, sync_mode_qlen], YamlConfig, 100),
                drop_mode_qlen => ?C:conf([logging, drop_mode_qlen], YamlConfig, 1000),
                flush_qlen => ?C:conf([logging, flush_qlen], YamlConfig, 2000)
            }),
            formatter =>
                {logger_logstash_formatter, #{
                    chars_limit => ?C:conf([logging, formatter, max_length], YamlConfig, 1000),
                    log_level_map =>
                        conf_with([logging, formatter, level_map], YamlConfig, #{}, fun(LevelMap) ->
                            maps:from_list(lists:map(fun log_level_tuple_to_atom/1, LevelMap))
                        end)
                }}
        }}
    ].

consuela(YamlConfig) ->
    lists:append([
        conf_with([consuela, presence], YamlConfig, [], fun(PresenceConfig) ->
            [
                {presence, #{
                    name => service_presence_name(YamlConfig),
                    consul => consul_client(mg_consuela_presence, YamlConfig),
                    shutdown => ?C:milliseconds(?C:conf([shutdown_timeout], PresenceConfig, <<"5s">>)),
                    service_tags => tags([tags], PresenceConfig, []),
                    session_opts => #{
                        interval => ?C:seconds(?C:conf([check_interval], PresenceConfig, <<"5s">>)),
                        pulse => mg_core_consuela_pulse_adapter:pulse(presence_session, pulse(YamlConfig))
                    }
                }}
            ]
        end),
        conf_with([consuela, registry], YamlConfig, [], fun(RegConfig) ->
            [
                {registry, #{
                    nodename => ?C:string(?C:conf([nodename], RegConfig, ?C:hostname())),
                    namespace => ?C:conf([namespace], RegConfig, <<"mg">>),
                    session => maps:merge(
                        #{
                            ttl => ?C:seconds(?C:conf([session_ttl], RegConfig, <<"30s">>)),
                            lock_delay => ?C:seconds(?C:conf([session_lock_delay], RegConfig, <<"10s">>))
                        },
                        conf_with([consuela, presence], YamlConfig, #{}, fun(_) ->
                            #{
                                presence => service_presence_name(YamlConfig)
                            }
                        end)
                    ),
                    consul => consul_client(mg_consuela_registry, YamlConfig),
                    shutdown => ?C:milliseconds(?C:conf([shutdown_timeout], RegConfig, <<"5s">>)),
                    keeper => maps:merge(
                        #{
                            pulse => mg_core_consuela_pulse_adapter:pulse(session_keeper, pulse(YamlConfig))
                        },
                        conf_with([session_renewal_interval], RegConfig, #{}, fun(V) ->
                            #{
                                interval => ?C:seconds(V)
                            }
                        end)
                    ),
                    reaper => #{
                        pulse => mg_core_consuela_pulse_adapter:pulse(zombie_reaper, pulse(YamlConfig))
                    },
                    registry => #{
                        pulse => mg_core_consuela_pulse_adapter:pulse(registry_server, pulse(YamlConfig))
                    }
                }}
            ]
        end),
        conf_with([consuela, discovery], YamlConfig, [], fun(DiscoveryConfig) ->
            [
                {discovery, #{
                    name => service_presence_name(YamlConfig),
                    tags => tags([tags], DiscoveryConfig, tags([consuela, presence, tags], YamlConfig, [])),
                    consul => consul_client(mg_consuela_discovery, YamlConfig),
                    opts => #{
                        interval => #{
                            init => ?C:milliseconds(?C:conf([interval, init], DiscoveryConfig, <<"5s">>)),
                            idle => ?C:milliseconds(?C:conf([interval, idle], DiscoveryConfig, <<"10m">>))
                        },
                        pulse => mg_core_consuela_pulse_adapter:pulse(discovery_server, pulse(YamlConfig))
                    }
                }}
            ]
        end)
    ]).

tags(Path, Config, Defaults) ->
    [T || T <- ?C:conf(Path, Config, Defaults)].

service_presence_name(YamlConfig) ->
    iolist_to_binary([service_name(YamlConfig), "-consuela"]).

consul_client(Name, YamlConfig) ->
    ACLToken = conf_with(
        [consul, acl_token_file],
        YamlConfig,
        undefined,
        fun(V) -> {file, ?C:file(V, 8#600)} end
    ),
    #{
        url => ?C:conf([consul, url], YamlConfig),
        opts => genlib_map:compact(#{
            datacenter => ?C:conf([consul, datacenter], YamlConfig, undefined),
            acl => ACLToken,
            transport_opts => genlib_map:compact(#{
                pool =>
                    ?C:conf([consul, pool], YamlConfig, Name),
                max_connections =>
                    ?C:conf([consul, max_connections], YamlConfig, undefined),
                max_response_size =>
                    ?C:conf([consul, max_response_size], YamlConfig, undefined),
                connect_timeout =>
                    ?C:maybe(fun ?C:milliseconds/1, ?C:conf([consul, connect_timeout], YamlConfig, undefined)),
                recv_timeout =>
                    ?C:maybe(fun ?C:milliseconds/1, ?C:conf([consul, recv_timeout], YamlConfig, undefined)),
                ssl_options =>
                    ?C:maybe(fun ?C:proplist/1, ?C:conf([consul, ssl_options], YamlConfig, undefined))
            }),
            pulse => mg_core_consuela_pulse_adapter:pulse(client, pulse(YamlConfig))
        })
    }.

prometheus(_YamlConfig) ->
    [
        {collectors, [default]}
    ].

snowflake(YamlConfig) ->
    [
        {machine_id, ?C:conf([snowflake_machine_id], YamlConfig, hostname_hash)},
        {max_backward_clock_moving, 1000}
    ].

pulse(YamlConfig) ->
    MaxLength = ?C:conf([logging, formatter, max_length], YamlConfig, 1000),
    MaxPrintable = ?C:conf([logging, formatter, max_printable_string_length], YamlConfig, 1000),
    LifecycleKafkaOptions = conf_with([lifecycle_pulse], YamlConfig, undefined, fun(LifecyclePulseConfig) ->
        #{
            topic => ?C:conf([topic], LifecyclePulseConfig),
            client => ?C:atom(?C:conf([client], LifecyclePulseConfig)),
            encoder => fun mg_woody_api_life_sink:serialize/3
        }
    end),
    {machinegun_pulse,
        genlib_map:compact(#{
            woody_event_handler_options => #{
                formatter_opts => #{
                    max_length => MaxLength,
                    max_printable_string_length => MaxPrintable
                }
            },
            lifecycle_kafka_options => LifecycleKafkaOptions
        })}.

brod(YamlConfig) ->
    Clients = ?C:conf([kafka], YamlConfig, []),
    [
        {clients, [
            {?C:atom(Name), brod_client(ClientConfig)}
         || {Name, ClientConfig} <- Clients
        ]}
    ].

brod_client(ClientConfig) ->
    ProducerConfig = ?C:conf([producer], ClientConfig, []),
    [
        {endpoints, [
            {?C:conf([host], Endpoint), ?C:conf([port], Endpoint)}
         || Endpoint <- ?C:conf([endpoints], ClientConfig)
        ]},
        {restart_delay_seconds, 10},
        {auto_start_producers, true},
        {default_producer_config, [
            {topic_restart_delay_seconds, 10},
            {partition_restart_delay_seconds, 2},
            {partition_buffer_limit, ?C:conf([partition_buffer_limit], ProducerConfig, 256)},
            {partition_onwire_limit, ?C:conf([partition_onwire_limit], ProducerConfig, 1)},
            {max_batch_size, ?C:mem_bytes(?C:conf([max_batch_size], ProducerConfig, <<"1M">>))},
            {max_retries, ?C:conf([max_retries], ProducerConfig, 3)},
            {retry_backoff_ms, ?C:milliseconds(?C:conf([retry_backoff], ProducerConfig, <<"500ms">>))},
            {required_acks, ?C:atom(?C:conf([required_acks], ProducerConfig, <<"all_isr">>))},
            {ack_timeout, ?C:milliseconds(?C:conf([ack_timeout], ProducerConfig, <<"10s">>))},
            {compression, ?C:atom(?C:conf([compression], ProducerConfig, "no_compression"))},
            {max_linger_ms, ?C:milliseconds(?C:conf([max_linger], ProducerConfig, <<"0ms">>))},
            {max_linger_count, ?C:conf([max_linger_count], ProducerConfig, 0)}
        ]},
        {ssl, brod_client_ssl(?C:conf([ssl], ClientConfig, false))},
        {sasl, brod_client_sasl(?C:conf([sasl], ClientConfig, undefined))}
    ].

brod_client_ssl(false) ->
    false;
brod_client_ssl(SslConfig) ->
    Opts = [
        {certfile, ?C:maybe(fun ?C:string/1, ?C:conf([certfile], SslConfig, undefined))},
        {keyfile, ?C:maybe(fun ?C:string/1, ?C:conf([keyfile], SslConfig, undefined))},
        {cacertfile, ?C:maybe(fun ?C:string/1, ?C:conf([cacertfile], SslConfig, undefined))}
    ],
    [Opt || Opt = {_Key, Value} <- Opts, Value =/= undefined].

brod_client_sasl(undefined) ->
    undefined;
brod_client_sasl(SaslConfig) ->
    Mechanism = ?C:atom(?C:conf([mechanism], SaslConfig, <<"scram_sha_512">>)),
    File = ?C:maybe(fun ?C:string/1, ?C:conf([file], SaslConfig, undefined)),
    case File of
        undefined ->
            Username = ?C:string(?C:conf([username], SaslConfig)),
            Password = ?C:string(?C:conf([password], SaslConfig)),
            {Mechanism, Username, Password};
        _ ->
            {Mechanism, File}
    end.

hackney(_YamlConfig) ->
    [].

machinegun(YamlConfig) ->
    [
        {woody_server, woody_server(YamlConfig)},
        {health_check, health_check(YamlConfig)},
        {quotas, quotas(YamlConfig)},
        {namespaces, namespaces(YamlConfig)},
        {event_sink_ns, event_sink_ns(YamlConfig)},
        {pulse, pulse(YamlConfig)},
        {cluster, cluster(YamlConfig)}
    ].

woody_server(YamlConfig) ->
    #{
        ip => ?C:ip(?C:conf([woody_server, ip], YamlConfig, <<"::">>)),
        port => ?C:conf([woody_server, port], YamlConfig, 8022),
        transport_opts => #{
            % same as ranch defaults
            max_connections => ?C:conf([woody_server, max_concurrent_connections], YamlConfig, 1024)
        },
        protocol_opts => #{
            request_timeout => ?C:milliseconds(?C:conf([woody_server, http_keep_alive_timeout], YamlConfig, <<"5s">>)),
            % idle_timeout must be greater then any possible deadline
            idle_timeout => ?C:milliseconds(?C:conf([woody_server, idle_timeout], YamlConfig, <<"infinity">>)),
            logger => logger
        },
        limits => genlib_map:compact(#{
            max_heap_size => ?C:maybe(fun ?C:mem_words/1, ?C:conf([limits, process_heap], YamlConfig, undefined))
        }),
        shutdown_timeout => ?C:milliseconds(?C:conf([woody_server, shutdown_timeout], YamlConfig, <<"5s">>))
    }.

health_check(YamlConfig) ->
    lists:foldl(
        fun maps:merge/2,
        #{},
        [
            conf_with([limits, disk], YamlConfig, #{}, fun(DiskConfig) ->
                DiskPath = ?C:string(?C:conf([path], DiskConfig, <<"/">>)),
                #{disk => {erl_health, disk, [DiskPath, percent(?C:conf([value], DiskConfig))]}}
            end),
            relative_memory_limit(YamlConfig, #{}, fun(TypeStr, Limit) ->
                Type =
                    case TypeStr of
                        <<"total">> -> total;
                        <<"cgroups">> -> cg_memory
                    end,
                #{memory => {erl_health, Type, [Limit]}}
            end),
            #{service => {erl_health, service, [service_name(YamlConfig)]}},
            conf_with(
                [consuela],
                YamlConfig,
                #{},
                #{consuela => {machinegun_health_check, consuela, []}}
            ),
            conf_with(
                [process_registry],
                YamlConfig,
                #{},
                #{procreg => {machinegun_health_check, health_check_fun(YamlConfig), []}}
            )
        ]
    ).

opentelemetry(YamlConfig) ->
    case opentelemetry_conf(YamlConfig) of
        undefined ->
            [{sampler, always_off}];
        <<"disabled">> ->
            [{sampler, always_off}];
        OtelYamlConfig ->
            ServiceConf =
                case ?C:conf([service_name], OtelYamlConfig, undefined) of
                    undefined ->
                        [];
                    (Name) when is_binary(Name) andalso Name =/= <<"">> ->
                        [
                            {resource, [
                                {service, #{name => Name}}
                            ]}
                        ]
                end,
            [
                {span_processor, batch},
                {traces_exporter, otlp},
                {sampler,
                    %% For sampling see reference
                    %%   https://opentelemetry.io/docs/instrumentation/erlang/sampling/
                    %% and `otel_configuration` module.
                    %%
                    %% Sample always if remote or local parent did so as well
                    {parent_based, #{
                        root => always_off,
                        remote_parent_sampled => always_on,
                        remote_parent_not_sampled => always_off,
                        local_parent_sampled => always_on,
                        local_parent_not_sampled => always_off
                    }}}
                | ServiceConf
            ]
    end.

opentelemetry_exporter(YamlConfig) ->
    case opentelemetry_conf(YamlConfig) of
        undefined ->
            [];
        <<"disabled">> ->
            [];
        OtelYamlConfig ->
            conf_with([exporter], OtelYamlConfig, [], fun(YConf) ->
                [
                    {otlp_protocol,
                        case ?C:conf([protocol], YConf, undefined) of
                            <<"grpc">> -> grpc;
                            <<"http/protobuf">> -> http_protobuf;
                            _Other -> undefined
                        end},
                    {otlp_endpoint,
                        case ?C:conf([endpoint], YConf, undefined) of
                            undefined -> undefined;
                            Endpoint when is_binary(Endpoint) -> binary_to_list(Endpoint)
                        end}
                ]
            end)
    end.

opentelemetry_conf(YamlConfig) ->
    ?C:conf([opentelemetry], YamlConfig, undefined).

health_check_fun(YamlConfig) ->
    case ?C:conf([process_registry, module], YamlConfig, <<"mg_core_procreg_consuela">>) of
        <<"mg_core_procreg_consuela">> -> consuela;
        <<"mg_core_procreg_global">> -> global
    end.

cluster(YamlConfig) ->
    case ?C:conf([consuela], YamlConfig, undefined) of
        undefined ->
            case ?C:conf([cluster, discovery, type], YamlConfig, undefined) of
                undefined ->
                    #{};
                <<"dns">> ->
                    DiscoveryOptsList = ?C:conf([cluster, discovery, options], YamlConfig),
                    ReconnectTimeout = ?C:conf([cluster, reconnect_timeout], YamlConfig, 5000),
                    #{
                        discovery => #{
                            module => mg_core_union,
                            options => maps:from_list(DiscoveryOptsList)
                        },
                        reconnect_timeout => ReconnectTimeout
                    };
                _ ->
                    #{}
            end;
        _ ->
            #{}
    end.

quotas(YamlConfig) ->
    SchedulerLimit = ?C:conf([limits, scheduler_tasks], YamlConfig, 5000),
    [
        #{
            name => <<"scheduler_tasks_total">>,
            limit => #{value => SchedulerLimit},
            update_interval => 1000
        }
    ].

percent(Value) ->
    try
        {NumStr, <<"%">>} = string:take(string:trim(Value), lists:seq($0, $9)),
        binary_to_integer(NumStr)
    catch
        error:_ ->
            erlang:throw({'bad percent value', Value})
    end.

relative_memory_limit(YamlConfig, Default, Fun) ->
    conf_with([limits, memory], YamlConfig, Default, fun(MemoryConfig) ->
        Fun(?C:conf([type], MemoryConfig, <<"total">>), percent(?C:conf([value], MemoryConfig)))
    end).

storage(NS, YamlConfig) ->
    case ?C:conf([storage, type], YamlConfig) of
        <<"memory">> ->
            mg_core_storage_memory;
        <<"riak">> ->
            PoolSize = ?C:conf([storage, pool, size], YamlConfig, 100),
            {mg_core_storage_riak, #{
                host => ?C:conf([storage, host], YamlConfig),
                port => ?C:conf([storage, port], YamlConfig),
                bucket => NS,
                connect_timeout => ?C:milliseconds(?C:conf([storage, connect_timeout], YamlConfig, <<"5s">>)),
                request_timeout => ?C:milliseconds(?C:conf([storage, request_timeout], YamlConfig, <<"10s">>)),
                index_query_timeout => ?C:milliseconds(?C:conf([storage, index_query_timeout], YamlConfig, <<"10s">>)),
                %% r_options => decode_rwd_options(?C:conf([storage, r_options], YamlConfig, undefined)),
                %% w_options => decode_rwd_options(?C:conf([storage, w_options], YamlConfig, undefined)),
                %% d_options => decode_rwd_options(?C:conf([storage, d_options], YamlConfig, undefined)),
                pool_options => #{
                    % If `init_count` is greater than zero, then the service will not start
                    % if the riak is unavailable. The `pooler` synchronously creates `init_count`
                    % connections at the start.
                    init_count => 0,
                    max_count => PoolSize,
                    idle_timeout => timer:seconds(60),
                    cull_interval => timer:seconds(10),
                    queue_max => ?C:conf([storage, pool, queue_max], YamlConfig, 1000)
                },
                batching => #{
                    concurrency_limit => ?C:conf([storage, batch_concurrency_limit], YamlConfig, PoolSize)
                },
                sidecar => {machinegun_riak_prometheus, #{}}
            }}
    end.

decode_rwd_options(List) ->
    lists:map(
        fun(Item) ->
            case Item of
                {Key, Value} when is_binary(Key) andalso is_binary(Value) ->
                    {?C:atom(Key), ?C:atom(Value)};
                {Key, Value} when is_binary(Key) ->
                    {?C:atom(Key), Value};
                Value when is_binary(Value) ->
                    ?C:atom(Value)
            end
        end,
        List
    ).

namespaces(YamlConfig) ->
    lists:foldl(
        fun(NSConfig, Acc) ->
            {Name, NS} = namespace(NSConfig, YamlConfig),
            Acc#{Name => NS}
        end,
        #{},
        ?C:conf([namespaces], YamlConfig)
    ).

-define(NS_TIMEOUT(TimeoutName, Default),
    timeout(TimeoutName, NSYamlConfig, Default, ms)
).
-define(NS_RETRY_SPEC(Key, Name, NSYamlConfig, DefaultConfig),
    ?C:to_retry_policy([namespaces, binary_to_atom(Name), retries, Key], NSYamlConfig, DefaultConfig)
).

namespace({Name, NSYamlConfig}, YamlConfig) ->
    {Name,
        maps:merge(
            #{
                storage => storage(Name, YamlConfig),
                processor => #{
                    url => ?C:conf([processor, url], NSYamlConfig),
                    transport_opts => #{
                        pool => ?C:atom(Name),
                        timeout => ?C:milliseconds(
                            ?C:conf([processor, http_keep_alive_timeout], NSYamlConfig, <<"4s">>)
                        ),
                        max_connections => ?C:conf([processor, pool_size], NSYamlConfig, 50)
                    },
                    resolver_opts => #{
                        ip_picker => random
                    }
                },
                worker => genlib_map:compact(#{
                    registry => procreg(YamlConfig),
                    message_queue_len_limit => ?C:conf([worker, message_queue_len_limit], YamlConfig, 500),
                    worker_options => #{
                        hibernate_timeout => ?NS_TIMEOUT(hibernate_timeout, <<"5s">>),
                        unload_timeout => ?NS_TIMEOUT(unload_timeout, <<"60s">>),
                        shutdown_timeout => ?NS_TIMEOUT(shutdown_timeout, <<"5s">>)
                    }
                }),
                default_processing_timeout => ?NS_TIMEOUT(default_processing_timeout, <<"30s">>),
                timer_processing_timeout => ?NS_TIMEOUT(timer_processing_timeout, <<"60s">>),
                reschedule_timeout => ?NS_TIMEOUT(reschedule_timeout, <<"60s">>),
                retries => #{
                    storage => ?NS_RETRY_SPEC(storage, Name, NSYamlConfig, #{
                        type => <<"exponential">>,
                        max_retries => <<"infinity">>,
                        factor => 2,
                        timeout => <<"10ms">>,
                        max_timeout => <<"60s">>
                    }),
                    %% max_total_timeout not supported for timers yet, see mg_retry:new_strategy/2 comments
                    %% actual timers sheduling resolution is one second
                    timers => ?NS_RETRY_SPEC(timers, Name, NSYamlConfig, #{
                        type => <<"exponential">>,
                        max_retries => 100,
                        factor => 2,
                        timeout => <<"1s">>,
                        max_timeout => <<"30m">>
                    }),
                    processor => ?NS_RETRY_SPEC(processor, Name, NSYamlConfig, #{
                        type => <<"exponential">>,
                        max_retries => #{
                            max_total_timeout => <<"1d">>
                        },
                        factor => 2,
                        timeout => <<"10ms">>,
                        max_timeout => <<"60s">>
                    }),
                    continuation => ?NS_RETRY_SPEC(continuation, Name, NSYamlConfig, #{
                        type => <<"exponential">>,
                        max_retries => <<"infinity">>,
                        factor => 2,
                        timeout => <<"10ms">>,
                        max_timeout => <<"60s">>
                    })
                },
                schedulers => namespace_schedulers(NSYamlConfig),
                event_sinks => [event_sink(ES) || ES <- ?C:conf([event_sinks], NSYamlConfig, [])],
                suicide_probability => ?C:probability(?C:conf([suicide_probability], NSYamlConfig, 0)),
                event_stash_size => ?C:conf([event_stash_size], NSYamlConfig, 0)
            },
            conf_with([modernizer], NSYamlConfig, #{}, fun(ModernizerYamlConfig) ->
                #{
                    modernizer => modernizer(Name, ModernizerYamlConfig)
                }
            end)
        )}.

namespace_schedulers(NSYamlConfig) ->
    Schedulers = [
        case ?C:conf([timers], NSYamlConfig, []) of
            <<"disabled">> ->
                #{};
            TimersConfig ->
                #{
                    timers => timer_scheduler(2, TimersConfig),
                    timers_retries => timer_scheduler(1, TimersConfig)
                }
        end,
        case ?C:conf([overseer], NSYamlConfig, []) of
            <<"disabled">> ->
                #{};
            OverseerConfig ->
                #{
                    overseer => overseer_scheduler(0, OverseerConfig)
                }
        end,
        case ?C:conf([notification], NSYamlConfig, []) of
            <<"disabled">> ->
                #{};
            NotificationConfig ->
                #{
                    notification => notification_scheduler(1, NotificationConfig)
                }
        end
    ],
    lists:foldl(fun maps:merge/2, #{}, Schedulers).

modernizer(Name, ModernizerYamlConfig) ->
    #{
        current_format_version => ?C:conf([current_format_version], ModernizerYamlConfig),
        handler => #{
            url => ?C:conf([handler, url], ModernizerYamlConfig),
            transport_opts => #{
                pool => ?C:atom(Name),
                timeout => ?C:milliseconds(?C:conf([handler, http_keep_alive_timeout], ModernizerYamlConfig, <<"4s">>)),
                max_connections => ?C:conf([handler, pool_size], ModernizerYamlConfig, 50)
            },
            resolver_opts => #{
                ip_picker => random
            }
        }
    }.

-spec scheduler(mg_core_quota:share(), ?C:yaml_config()) -> mg_core_machine:scheduler_opt().
scheduler(Share, Config) ->
    #{
        max_scan_limit => ?C:conf([scan_limit], Config, 5000),
        task_quota => <<"scheduler_tasks_total">>,
        task_share => Share
    }.

timer_scheduler(Share, Config) ->
    (scheduler(Share, Config))#{
        capacity => ?C:conf([capacity], Config, 1000),
        min_scan_delay => timeout(min_scan_delay, Config, <<"1s">>, ms),
        target_cutoff => timeout(scan_interval, Config, <<"60s">>, sec)
    }.

overseer_scheduler(Share, Config) ->
    (scheduler(Share, Config))#{
        capacity => ?C:conf([capacity], Config, 1000),
        min_scan_delay => timeout(min_scan_delay, Config, <<"1s">>, ms),
        rescan_delay => timeout(scan_interval, Config, <<"10m">>, ms)
    }.

notification_scheduler(Share, Config) ->
    (scheduler(Share, Config))#{
        capacity => ?C:conf([capacity], Config, 1000),
        min_scan_delay => timeout(min_scan_delay, Config, <<"1s">>, ms),
        rescan_delay => timeout(scan_interval, Config, <<"1m">>, ms),
        scan_handicap => timeout(scan_handicap, Config, <<"10s">>, ms),
        scan_cutoff => timeout(scan_cutoff, Config, <<"4W">>, ms),
        reschedule_time => timeout(reschedule_time, Config, <<"5s">>, ms)
    }.

timeout(Name, Config, Default, Unit) ->
    ?C:time_interval(?C:conf([Name], Config, Default), Unit).

event_sink_ns(YamlConfig) ->
    #{
        registry => procreg(YamlConfig),
        storage => storage(<<"_event_sinks">>, YamlConfig),
        worker => #{registry => procreg(YamlConfig)},
        duplicate_search_batch => 1000,
        default_processing_timeout => ?C:milliseconds(<<"30s">>)
    }.

event_sink({Name, ESYamlConfig}) ->
    event_sink(?C:atom(?C:conf([type], ESYamlConfig)), Name, ESYamlConfig).

event_sink(machine, Name, ESYamlConfig) ->
    {mg_core_events_sink_machine, #{
        name => ?C:atom(Name),
        machine_id => ?C:conf([machine_id], ESYamlConfig)
    }};
event_sink(kafka, Name, ESYamlConfig) ->
    {mg_core_events_sink_kafka, #{
        name => ?C:atom(Name),
        client => ?C:atom(?C:conf([client], ESYamlConfig)),
        topic => ?C:conf([topic], ESYamlConfig)
    }}.

procreg(YamlConfig) ->
    % Use process_registry if it's set up or consuela if it's set up, gproc otherwise
    Default = conf_with(
        [consuela],
        YamlConfig,
        mg_core_procreg_gproc,
        {mg_core_procreg_consuela, #{pulse => pulse(YamlConfig)}}
    ),
    conf_with(
        [process_registry],
        YamlConfig,
        Default,
        fun(ProcRegYamlConfig) -> ?C:atom(?C:conf([module], ProcRegYamlConfig)) end
    ).

%%
%% vm.args
%%
vm_args(YamlConfig, ERLInetrcFilename) ->
    Flags = [
        node_name(YamlConfig),
        {'-kernel', 'inetrc', ["'\"", ERLInetrcFilename, "\"'"]},
        {'+c', true},
        {'+C', single_time_warp},
        %% Do not burn CPU circles, go sleep
        %% early if no pending work in queue.
        {'+sbwt', 'none'},
        {'+sbwtdcpu', 'none'},
        {'+sbwtdio', 'none'},
        %% Wake up early in case of any new job appeared
        {'+swt', 'very_low'},
        {'+swtdcpu', 'very_low'},
        {'+swtdio', 'very_low'}
    ],
    ProtoFlags = conf_if([erlang, ipv6], YamlConfig, [
        {'-proto_dist', inet6_tcp}
    ]),
    DistFlags = conf_with([dist_port], YamlConfig, [], fun dist_flags/1),
    Flags ++ ProtoFlags ++ DistFlags.

cookie(YamlConfig) ->
    ?C:contents(?C:conf([erlang, secret_cookie_file], YamlConfig)).

service_name(YamlConfig) ->
    ?C:conf([service_name], YamlConfig, <<"machinegun">>).

node_name(YamlConfig) ->
    Name =
        case ?C:conf([dist_node_name], YamlConfig, default_node_name(YamlConfig)) of
            C = [{_, _} | _] ->
                make_node_name(C, YamlConfig);
            S when is_binary(S) ->
                S
        end,
    {node_name_type(Name), Name}.

make_node_name(C, YamlConfig) ->
    NamePart = ?C:conf([namepart], C, service_name(YamlConfig)),
    HostPart =
        case ?C:conf([hostpart], C) of
            <<"hostname">> -> ?C:hostname();
            <<"fqdn">> -> ?C:fqdn();
            <<"ip">> -> guess_host_addr(YamlConfig)
        end,
    iolist_to_binary([NamePart, "@", HostPart]).

node_name_type(Name) ->
    case string:split(Name, "@") of
        [_, Hostname] -> host_name_type(Hostname);
        [_] -> '-sname'
    end.

host_name_type(Name) ->
    case inet:parse_address(?C:string(Name)) of
        {ok, _} ->
            '-name';
        {error, einval} ->
            case string:find(Name, ".") of
                nomatch -> '-sname';
                _ -> '-name'
            end
    end.

default_node_name(YamlConfig) ->
    iolist_to_binary([service_name(YamlConfig), "@", ?C:hostname()]).

guess_host_addr(YamlConfig) ->
    inet:ntoa(?C:guess_host_address(address_family_preference(YamlConfig))).

address_family_preference(YamlConfig) ->
    conf_with([erlang, ipv6], YamlConfig, inet, fun
        (true) -> inet6;
        (false) -> inet
    end).

dist_flags(Config) ->
    case ?C:conf([mode], Config, <<"epmd">>) of
        <<"static">> ->
            Port = ?C:conf([port], Config),
            [
                {'-start_epmd', false},
                {'-erl_epmd_port', Port}
            ];
        <<"epmd">> ->
            conf_with([range], Config, [], fun(Range) ->
                {Min, Max} = port_range(Range),
                [
                    {'-kernel', 'inet_dist_listen_min', Min},
                    {'-kernel', 'inet_dist_listen_max', Max}
                ]
            end)
    end.

port_range(Config) ->
    case lists:sort(Config) of
        [Min, Max] when
            is_integer(Min),
            Min > 0,
            Min < 65536,
            is_integer(Max),
            Max > 0,
            Max < 65536
        ->
            {Min, Max};
        _ ->
            erlang:throw({'bad port range', Config})
    end.

%%
%% erl_inetrc
%%
erl_inetrc(YamlConfig) ->
    conf_if([erlang, ipv6], YamlConfig, [{inet6, true}, {tcp, inet6_tcp}]) ++
        conf_if([erlang, disable_dns_cache], YamlConfig, [{cache_size, 0}]).

conf_if(YamlConfigPath, YamlConfig, Value) ->
    case ?C:conf(YamlConfigPath, YamlConfig, false) of
        true -> Value;
        false -> []
    end.

conf_with(YamlConfigPath, YamlConfig, Default, FunOrVal) ->
    case ?C:conf(YamlConfigPath, YamlConfig, undefined) of
        undefined -> Default;
        Value when is_function(FunOrVal) -> FunOrVal(Value);
        _Value -> FunOrVal
    end.

log_level_tuple_to_atom({<<"emergency">>, NewLevel}) when is_binary(NewLevel) ->
    {emergency, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"alert">>, NewLevel}) when is_binary(NewLevel) ->
    {alert, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"critical">>, NewLevel}) when is_binary(NewLevel) ->
    {critical, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"error">>, NewLevel}) when is_binary(NewLevel) ->
    {error, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"warning">>, NewLevel}) when is_binary(NewLevel) ->
    {warning, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"notice">>, NewLevel}) when is_binary(NewLevel) ->
    {notice, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"info">>, NewLevel}) when is_binary(NewLevel) ->
    {info, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({<<"debug">>, NewLevel}) when is_binary(NewLevel) ->
    {debug, binary_to_atom(NewLevel)};
log_level_tuple_to_atom({Level, _NewLevel}) ->
    throw("Not supported logger level '" ++ binary_to_list(Level) ++ "'").
