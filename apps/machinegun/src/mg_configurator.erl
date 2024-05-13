-module(mg_configurator).

-export([construct_child_specs/1]).

-type modernizer() :: #{
    current_format_version := mg_core_events:format_version(),
    handler := mg_woody_modernizer:options()
}.

-type events_machines() :: #{
    processor := processor(),
    modernizer => modernizer(),
    % all but `worker_options.worker` option
    worker => mg_core_workers_manager:options(),
    machines_storage := mg_core_machine_storage:options(),
    events_storage := mg_core_events_storage:options(),
    notifications_storage := mg_core_notification_storage:options(),
    event_sinks => [mg_core_events_sink:handler()],
    retries := mg_core_machine:retry_opt(),
    schedulers := mg_core_machine:schedulers_opt(),
    default_processing_timeout := timeout(),
    suicide_probability => mg_core_machine:suicide_probability(),
    event_stash_size := non_neg_integer()
}.

-type namespaces() :: #{mg_core:ns() => events_machines()}.

-type config() :: #{
    woody_server := mg_woody:woody_server(),
    namespaces := namespaces(),
    pulse := pulse(),
    quotas => [mg_core_quota_worker:options()],
    health_check => erl_health:check()
}.

-type processor() :: mg_woody_processor:options().

-type pulse() :: mg_core_pulse:handler().

-spec construct_child_specs(config()) -> [supervisor:child_spec()].
construct_child_specs(
    #{
        woody_server := WoodyServer,
        namespaces := Namespaces,
        pulse := Pulse
    } = Config
) ->
    Quotas = maps:get(quotas, Config, []),
    HealthChecks = maps:get(health_check, Config, #{}),
    ClusterOpts = maps:get(cluster, Config, #{}),

    QuotasChildSpec = quotas_child_specs(Quotas, quota),
    EventMachinesChildSpec = events_machines_child_specs(Namespaces, Pulse),
    WoodyServerChildSpec = mg_woody:child_spec(
        woody_server,
        #{
            pulse => Pulse,
            automaton => api_automaton_options(Namespaces, Pulse),
            woody_server => WoodyServer,
            additional_routes => [
                get_startup_route(),
                get_health_route(HealthChecks),
                get_prometheus_route()
            ]
        }
    ),
    ClusterSpec = mg_core_union:child_spec(ClusterOpts),

    lists:flatten([
        QuotasChildSpec,
        EventMachinesChildSpec,
        ClusterSpec,
        WoodyServerChildSpec
    ]).

%%

-spec get_startup_route() -> {iodata(), module(), _Opts :: any()}.
get_startup_route() ->
    EvHandler = {erl_health_event_handler, []},
    Check = #{
        startup => #{
            runner => {mg_health_check, startup, []},
            event_handler => EvHandler
        }
    },
    erl_health_handle:get_startup_route(Check).

-spec get_health_route(erl_health:check()) -> {iodata(), module(), _Opts :: any()}.
get_health_route(Check0) ->
    EvHandler = {erl_health_event_handler, []},
    Check = maps:map(fun(_, V = {_, _, _}) -> #{runner => V, event_handler => EvHandler} end, Check0),
    erl_health_handle:get_route(Check).

-spec get_prometheus_route() -> {iodata(), module(), _Opts :: any()}.
get_prometheus_route() ->
    {"/metrics/[:registry]", prometheus_cowboy2_handler, []}.

-spec quotas_child_specs([mg_core_quota_worker:options()], atom()) -> [supervisor:child_spec()].
quotas_child_specs(Quotas, ChildID) ->
    [
        mg_core_quota_worker:child_spec(Options, {ChildID, maps:get(name, Options)})
     || Options <- Quotas
    ].

-spec events_machines_child_specs(namespaces(), pulse()) -> supervisor:child_spec().
events_machines_child_specs(NSs, Pulse) ->
    NsOptions = [events_machine_options(NS, NSs, Pulse) || NS <- maps:keys(NSs)],
    mg_namespace_sup:child_spec(NsOptions, namespaces_sup).

-spec events_machine_options(mg_core:ns(), namespaces(), pulse()) -> mg_core_events_machine:options().
events_machine_options(NS, NSs, Pulse) ->
    NSConfigs = maps:get(NS, NSs),
    #{processor := ProcessorConfig, events_storage := EventsStorage0} = NSConfigs,
    EventSinks = [event_sink_options(SinkConfig, Pulse) || SinkConfig <- maps:get(event_sinks, NSConfigs, [])],
    EventsStorage1 = sub_storage_options(<<"events">>, EventsStorage0),
    #{
        namespace => NS,
        processor => processor(ProcessorConfig, Pulse),
        machines => machine_options(NS, NSConfigs, Pulse),
        events_storage => EventsStorage1,
        event_sinks => EventSinks,
        pulse => Pulse,
        default_processing_timeout => maps:get(default_processing_timeout, NSConfigs),
        event_stash_size => maps:get(event_stash_size, NSConfigs, 0)
    }.

-spec machine_options(mg_core:ns(), events_machines(), pulse()) -> mg_core_machine:options().
machine_options(NS, Config, Pulse) ->
    #{machines_storage := MachinesStorage0, notifications_storage := NotificationsStorage0} = Config,
    Options = maps:with(
        [
            retries,
            timer_processing_timeout
        ],
        Config
    ),
    MachinesStorage1 = sub_storage_options(<<"machines">>, MachinesStorage0),
    NotificationsStorage1 = sub_storage_options(<<"notifications">>, NotificationsStorage0),
    Options#{
        namespace => NS,
        storage => MachinesStorage1,
        notification => NotificationsStorage1,
        worker => worker_manager_options(Config),
        schedulers => maps:get(schedulers, Config, #{}),
        pulse => Pulse,
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined)
    }.

-spec api_automaton_options(namespaces(), pulse()) -> mg_woody_automaton:options().
api_automaton_options(NSs, Pulse) ->
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{
                NS => maps:merge(
                    #{
                        machine => events_machine_options(NS, NSs, Pulse)
                    },
                    modernizer_options(maps:get(modernizer, ConfigNS, undefined), Pulse)
                )
            }
        end,
        #{},
        NSs
    ).

-spec event_sink_options(mg_core_events_sink:handler(), pulse()) -> mg_core_events_sink:handler().
event_sink_options({mg_core_events_sink_kafka, EventSinkConfig}, Pulse) ->
    {mg_core_events_sink_kafka, EventSinkConfig#{
        pulse => Pulse,
        encoder => fun mg_woody_event_sink:serialize/3
    }}.

-spec worker_manager_options(map()) -> mg_core_workers_manager:ns_options().
worker_manager_options(Config) ->
    maps:merge(
        #{
            registry => mg_core_procreg_gproc
        },
        maps:get(worker, Config, #{})
    ).

-spec processor(processor(), pulse()) -> mg_core_utils:mod_opts().
processor(Processor, Pulse) ->
    {mg_woody_processor, Processor#{event_handler => {mg_woody_event_handler, Pulse}}}.

-spec sub_storage_options(mg_core:ns(), mg_core_machine:storage_options()) -> mg_core_machine:storage_options().
sub_storage_options(SubNS, Storage0) ->
    Storage1 = mg_core_utils:separate_mod_opts(Storage0, #{}),
    Storage2 = add_bucket_postfix(SubNS, Storage1),
    Storage2.

-spec add_bucket_postfix(mg_core:ns(), mg_core_storage:options()) -> mg_core_storage:options().
add_bucket_postfix(SubNS, {KVSMod, #{kvs := {mg_core_storage_riak, #{bucket := Bucket} = Options}} = KVSOpts}) ->
    {KVSMod, KVSOpts#{
        kvs := {mg_core_storage_riak, Options#{bucket := mg_core_utils:concatenate_namespaces(Bucket, SubNS)}}
    }};
add_bucket_postfix(_, Storage) ->
    Storage.

-spec modernizer_options(modernizer() | undefined, pulse()) -> #{modernizer => mg_core_events_modernizer:options()}.
modernizer_options(#{current_format_version := CurrentFormatVersion, handler := WoodyClient}, Pulse) ->
    #{
        modernizer => #{
            current_format_version => CurrentFormatVersion,
            handler => {mg_woody_modernizer, WoodyClient#{event_handler => {mg_woody_event_handler, Pulse}}}
        }
    };
modernizer_options(undefined, _Pulse) ->
    #{}.
