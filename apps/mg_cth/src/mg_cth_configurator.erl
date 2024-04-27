-module(mg_cth_configurator).

-export([construct_child_specs/1]).

-type config() :: #{
    woody_server := mg_woody:woody_server(),
    event_sink_ns := mg_conf:event_sink_ns(),
    namespaces := mg_conf:namespaces(),
    quotas => [mg_skd_quota_worker:options()]
}.

-spec construct_child_specs(config() | undefined) -> _.
construct_child_specs(undefined) ->
    [];
construct_child_specs(Config) ->
    mg_conf:construct_child_specs(Config#{pulse => mg_cth_pulse}, []).
