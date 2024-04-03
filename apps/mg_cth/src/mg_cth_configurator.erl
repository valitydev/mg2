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
    quotas => [mg_core_quota_worker:options()]
}.

-type processor() :: mg_woody_processor:options().

-spec construct_child_specs(config() | undefined) -> _.
construct_child_specs(undefined) ->
    [];
construct_child_specs(#{woody_server := WoodyServer, namespaces := Namespaces} = Config) ->
    Quotas = maps:get(quotas, Config, []),

    QuotasChSpec = quotas_child_specs(Quotas, quota),
    EventMachinesChSpec = events_machines_child_specs(Namespaces),
    WoodyServerChSpec = mg_woody:child_spec(
        woody_server,
        #{
            woody_server => WoodyServer,
            automaton => api_automaton_options(Namespaces),
            pulse => mg_cth_pulse
        }
    ),

    lists:flatten([
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

-spec events_machines_child_specs(namespaces()) -> [supervisor:child_spec()].
events_machines_child_specs(NSs) ->
    [
        mg_core_events_machine:child_spec(events_machine_options(NS, NSs), binary_to_atom(NS, utf8))
     || NS <- maps:keys(NSs)
    ].

-spec events_machine_options(mg_core:ns(), namespaces()) -> mg_core_events_machine:options().
events_machine_options(NS, NSs) ->
    NSConfigs = maps:get(NS, NSs),
    #{processor := ProcessorConfig, events_storage := EventsStorage0} = NSConfigs,
    EventSinks = [event_sink_options(SinkConfig) || SinkConfig <- maps:get(event_sinks, NSConfigs, [])],
    EventsStorage1 = sub_storage_options(<<"events">>, EventsStorage0),
    #{
        namespace => NS,
        processor => processor(ProcessorConfig),
        machines => machine_options(NS, NSConfigs),
        events_storage => EventsStorage1,
        event_sinks => EventSinks,
        pulse => pulse(),
        default_processing_timeout => maps:get(default_processing_timeout, NSConfigs),
        event_stash_size => maps:get(event_stash_size, NSConfigs, 0)
    }.

-spec machine_options(mg_core:ns(), events_machines()) -> mg_core_machine:options().
machine_options(NS, Config) ->
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
        worker => worker_manager_options(Config),
        schedulers => maps:get(schedulers, Config, #{}),
        pulse => pulse(),
        notification => NotificationsStorage1,
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined)
    }.

-spec api_automaton_options(_) -> mg_woody_automaton:options().
api_automaton_options(NSs) ->
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{
                NS => maps:merge(
                    #{
                        machine => events_machine_options(NS, NSs)
                    },
                    modernizer_options(maps:get(modernizer, ConfigNS, undefined))
                )
            }
        end,
        #{},
        NSs
    ).

-spec event_sink_options(mg_core_events_sink:handler()) -> mg_core_events_sink:handler().
event_sink_options({mg_core_events_sink_kafka, EventSinkConfig}) ->
    {mg_core_events_sink_kafka, EventSinkConfig#{
        pulse => pulse(),
        encoder => fun mg_woody_event_sink:serialize/3
    }}.

-spec worker_manager_options(map()) -> mg_core_workers_manager:ns_options().
worker_manager_options(Config) ->
    maps:merge(
        #{
            %% Use 'global' process registry
            registry => mg_core_procreg_global,
            sidecar => mg_cth_worker
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
add_bucket_postfix(SubNS, {KVSMod, #{kvs := {mg_core_storage_riak, #{bucket := Bucket} = Options}} = KVSOpts}) ->
    {KVSMod, KVSOpts#{
        kvs := {mg_core_storage_riak, Options#{bucket := mg_core_utils:concatenate_namespaces(Bucket, SubNS)}}
    }};
add_bucket_postfix(_, Storage) ->
    Storage.

-spec pulse() -> mg_core_pulse:handler().
pulse() ->
    mg_cth_pulse.

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
