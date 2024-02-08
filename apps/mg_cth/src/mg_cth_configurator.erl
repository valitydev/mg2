-module(mg_cth_configurator).

-export([construct_child_specs/1]).

-type modernizer() :: #{
    current_format_version := mg_core_events:format_version(),
    handler := mg_woody_modernizer:options()
}.

-type events_machines() :: #{
    processor := processor(),
    modernizer => modernizer(),
    % all but `worker_options.worker` option
    worker => mg_core_workers_manager:ns_options(),
    storage := mg_core_machine:storage_options(),
    event_sinks => [mg_core_events_sink:handler()],
    retries := mg_core_machine:retry_opt(),
    schedulers := mg_core_machine:schedulers_opt(),
    default_processing_timeout := timeout(),
    suicide_probability => mg_core_machine:suicide_probability(),
    event_stash_size := non_neg_integer()
}.

-type event_sink_ns() :: #{
    default_processing_timeout := timeout(),
    storage => mg_core_storage:options(),
    worker => mg_core_worker:options()
}.

-type config() :: #{
    woody_server := mg_woody:woody_server(),
    event_sink_ns := event_sink_ns(),
    namespaces := #{mg_core:ns() => events_machines()},
    quotas => [mg_core_quota_worker:options()]
}.

-type processor() :: mg_woody_processor:options().

-spec construct_child_specs(config() | undefined) -> _.
construct_child_specs(undefined) ->
    [];
construct_child_specs(
    #{
        woody_server := WoodyServer,
        event_sink_ns := EventSinkNS,
        namespaces := Namespaces
    } = Config
) ->
    Quotas = maps:get(quotas, Config, []),

    QuotasChSpec = quotas_child_specs(Quotas, quota),
    EventSinkChSpec = event_sink_ns_child_spec(EventSinkNS, event_sink),
    EventMachinesChSpec = events_machines_child_specs(Namespaces, EventSinkNS),
    WoodyServerChSpec = mg_woody:child_spec(
        woody_server,
        #{
            woody_server => WoodyServer,
            automaton => api_automaton_options(Namespaces, EventSinkNS),
            event_sink => api_event_sink_options(Namespaces, EventSinkNS),
            pulse => mg_woody_test_pulse
        }
    ),

    lists:flatten([
        EventSinkChSpec,
        WoodyServerChSpec,
        QuotasChSpec,
        EventMachinesChSpec
    ]).

%%

-spec quotas_child_specs(_, atom()) -> [supervisor:child_spec()].
quotas_child_specs(Quotas, ChildID) ->
    [
        mg_core_quota_worker:child_spec(Options, {ChildID, maps:get(name, Options)})
     || Options <- Quotas
    ].

-spec events_machines_child_specs(_, _) -> [supervisor:child_spec()].
events_machines_child_specs(NSs, EventSinkNS) ->
    [
        mg_core_events_machine:child_spec(
            events_machine_options(NS, NSs, EventSinkNS),
            binary_to_atom(NS, utf8)
        )
     || NS <- maps:keys(NSs)
    ].

-spec events_machine_options(mg_core:ns(), _, event_sink_ns()) -> mg_core_events_machine:options().
events_machine_options(NS, NSs, EventSinkNS) ->
    NSConfigs = maps:get(NS, NSs),
    #{processor := ProcessorConfig, storage := Storage} = NSConfigs,
    EventSinks = [
        event_sink_options(SinkConfig, EventSinkNS)
     || SinkConfig <- maps:get(event_sinks, NSConfigs, [])
    ],
    EventsStorage = sub_storage_options(<<"events">>, Storage),
    #{
        namespace => NS,
        processor => processor(ProcessorConfig),
        machines => machine_options(NS, NSConfigs),
        events_storage => EventsStorage,
        event_sinks => EventSinks,
        pulse => pulse(),
        default_processing_timeout => maps:get(default_processing_timeout, NSConfigs),
        event_stash_size => maps:get(event_stash_size, NSConfigs, 0)
    }.

-spec machine_options(mg_core:ns(), events_machines()) -> mg_core_machine:options().
machine_options(NS, Config) ->
    #{storage := Storage} = Config,
    Options = maps:with(
        [
            retries,
            timer_processing_timeout
        ],
        Config
    ),
    MachinesStorage = sub_storage_options(<<"machines">>, Storage),
    NotificationsStorage = sub_storage_options(<<"notifications">>, Storage),
    Options#{
        namespace => NS,
        storage => MachinesStorage,
        worker => worker_manager_options(Config),
        schedulers => maps:get(schedulers, Config, #{}),
        pulse => pulse(),
        notification => #{
            namespace => NS,
            pulse => pulse(),
            storage => NotificationsStorage
        },
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined)
    }.

-spec api_automaton_options(_, event_sink_ns()) -> mg_woody_automaton:options().
api_automaton_options(NSs, EventSinkNS) ->
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{
                NS => maps:merge(
                    #{
                        machine => events_machine_options(NS, NSs, EventSinkNS)
                    },
                    modernizer_options(maps:get(modernizer, ConfigNS, undefined))
                )
            }
        end,
        #{},
        NSs
    ).

-spec event_sink_options(mg_core_events_sink:handler(), _) -> mg_core_events_sink:handler().
event_sink_options({mg_core_events_sink_machine, EventSinkConfig}, EvSinks) ->
    EventSinkNS = event_sink_namespace_options(EvSinks),
    {mg_core_events_sink_machine, maps:merge(EventSinkNS, EventSinkConfig)};
event_sink_options({mg_core_events_sink_kafka, EventSinkConfig}, _Config) ->
    {mg_core_events_sink_kafka, EventSinkConfig#{
        pulse => pulse(),
        encoder => fun mg_woody_event_sink:serialize/3
    }}.

-spec event_sink_ns_child_spec(_, atom()) -> supervisor:child_spec().
event_sink_ns_child_spec(EventSinkNS, ChildID) ->
    mg_core_events_sink_machine:child_spec(event_sink_namespace_options(EventSinkNS), ChildID).

-spec api_event_sink_options(_, _) -> mg_woody_event_sink:options().
api_event_sink_options(NSs, EventSinkNS) ->
    EventSinkMachines = collect_event_sink_machines(NSs),
    {EventSinkMachines, event_sink_namespace_options(EventSinkNS)}.

-spec collect_event_sink_machines(_) -> [mg_core:id()].
collect_event_sink_machines(NSs) ->
    NSConfigs = maps:values(NSs),
    EventSinks = ordsets:from_list([
        maps:get(machine_id, SinkConfig)
     || NSConfig <- NSConfigs,
        {mg_core_events_sink_machine, SinkConfig} <- maps:get(event_sinks, NSConfig, [])
    ]),
    ordsets:to_list(EventSinks).

-spec event_sink_namespace_options(_) -> mg_core_events_sink_machine:ns_options().
event_sink_namespace_options(#{storage := Storage} = EventSinkNS) ->
    NS = <<"_event_sinks">>,
    MachinesStorage = sub_storage_options(<<"machines">>, Storage),
    EventsStorage = sub_storage_options(<<"events">>, Storage),
    EventSinkNS#{
        namespace => NS,
        pulse => pulse(),
        storage => MachinesStorage,
        events_storage => EventsStorage,
        worker => worker_manager_options(EventSinkNS)
    }.

-spec worker_manager_options(map()) -> mg_core_workers_manager:ns_options().
worker_manager_options(Config) ->
    maps:merge(
        #{
            registry => mg_core_procreg_gproc,
            sidecar => mg_woody_test_worker
        },
        maps:get(worker, Config, #{})
    ).

-spec processor(processor()) -> mg_core_utils:mod_opts().
processor(Processor) ->
    {mg_woody_processor, Processor#{event_handler => {mg_woody_event_handler, pulse()}}}.

-spec sub_storage_options(mg_core:ns(), mg_core_machine:storage_options()) ->
    mg_core_machine:storage_options().
sub_storage_options(SubNS, Storage0) ->
    Storage1 = mg_core_utils:separate_mod_opts(Storage0, #{}),
    Storage2 = add_bucket_postfix(SubNS, Storage1),
    Storage2.

-spec add_bucket_postfix(mg_core:ns(), mg_core_storage:options()) -> mg_core_storage:options().
add_bucket_postfix(_, {mg_core_storage_memory, _} = Storage) ->
    Storage;
add_bucket_postfix(SubNS, {mg_core_storage_riak, #{bucket := Bucket} = Options}) ->
    {mg_core_storage_riak, Options#{bucket := mg_core_utils:concatenate_namespaces(Bucket, SubNS)}}.

-spec pulse() -> mg_core_pulse:handler().
pulse() ->
    mg_woody_test_pulse.

-spec modernizer_options(modernizer() | undefined) ->
    #{modernizer => mg_core_events_modernizer:options()}.
modernizer_options(#{current_format_version := CurrentFormatVersion, handler := WoodyClient}) ->
    #{
        modernizer => #{
            current_format_version => CurrentFormatVersion,
            handler =>
                {mg_woody_modernizer, WoodyClient#{
                    event_handler => {mg_woody_event_handler, pulse()}
                }}
        }
    };
modernizer_options(undefined) ->
    #{}.
