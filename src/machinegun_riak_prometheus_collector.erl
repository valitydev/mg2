-module(machinegun_riak_prometheus_collector).

-behaviour(prometheus_collector).
-export([deregister_cleanup/1]).
-export([collect_mf/2]).

-type storage() :: machinegun_riak_prometheus:storage().
-type pooler_metrics() :: [{atom(), number()}].

-type label_name() :: term().
-type label_value() :: term().
-type label() :: {label_name(), label_value()}.

%%

-spec deregister_cleanup(prometheus_registry:registry()) -> ok.
deregister_cleanup(_Registry) ->
    ok.

-spec collect_mf(prometheus_registry:registry(), prometheus_collector:collect_mf_callback()) ->
    ok.
collect_mf(_Registry, Callback) ->
    lists:foreach(
        fun(Storage) -> collect_storage_metrics(Storage, Callback) end,
        machinegun_riak_prometheus:collect_storages()
    ).

-spec collect_storage_metrics(storage(), prometheus_collector:collect_mf_callback()) ->
    ok.
collect_storage_metrics(#{name := {NS, _Module, Type}} = Storage, Callback) ->
    Metrics = gather_metrics(Storage),
    Labels = [{namespace, NS}, {name, Type}],
    % NOTE
    % See https://github.com/seth/pooler/blob/9c28fb47/src/pooler.erl#L946 for metric details.
    lists:foreach(
        fun
            ({max_count, Value}) ->
                dispatch_mf(
                    Callback,
                    mg_riak_pool_connections_limit,
                    "Upper limit on number of Machinegun riak connections managed by this pool.",
                    gauge,
                    {Labels, Value}
                );
            ({in_use_count, Value}) ->
                dispatch_mf(
                    Callback,
                    mg_riak_pool_connections_in_use,
                    "Number of Machinegun riak connections managed by this pool currently in use.",
                    gauge,
                    {Labels, Value}
                );
            ({free_count, Value}) ->
                dispatch_mf(
                    Callback,
                    mg_riak_pool_connections_free,
                    "The number of Machinegun riak connections managed by this pool currently unused.",
                    gauge,
                    {Labels, Value}
                );
            ({queue_max, Value}) ->
                dispatch_mf(
                    Callback,
                    mg_riak_pool_queued_requests_limit,
                    "Upper limit on number of queued requests in this Machinegun riak connection pool.",
                    gauge,
                    {Labels, Value}
                );
            ({queued_count, Value}) ->
                dispatch_mf(
                    Callback,
                    mg_riak_pool_queued_requests,
                    "The number of queued requests in this Machingun riak connection pool.",
                    gauge,
                    {Labels, Value}
                );
            (_Other) ->
                ok
        end,
        Metrics
    ).

-spec gather_metrics(storage()) -> pooler_metrics().
gather_metrics(#{name := Name} = Storage) ->
    case mg_core_storage_riak:pool_utilization(Storage) of
        {ok, Metrics} ->
            Metrics;
        {error, Reason} ->
            logger:warning("Can not gather ~p riak pool utilization: ~p", [Name, Reason]),
            []
    end.

-spec dispatch_mf(
    prometheus_collector:collect_mf_callback(),
    prometheus_metric:name(),
    prometheus_metric:help(),
    atom(),
    {[label()], number()}
) -> ok.
dispatch_mf(Callback, Name, Help, Type, Metrics) ->
    Callback(prometheus_model_helpers:create_mf(Name, Help, Type, Metrics)).
