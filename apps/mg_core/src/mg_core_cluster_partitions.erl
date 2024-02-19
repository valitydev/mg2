-module(mg_core_cluster_partitions).

-type discovery_options() :: #{
    %% #{<<"domain_name">> => <<"machinegun-ha-headless">>,<<"sname">> => <<"machinegun">>}
    binary() => binary()
}.
-type balancing_key() :: term().
-type hash_range() :: {non_neg_integer(), non_neg_integer()}.
-type partition() :: non_neg_integer().
-type partitions_options() :: #{
    capacity => non_neg_integer(),
    max_hash => non_neg_integer()
}.
-type balancing_table() :: #{
    hash_range() => partition()
}.
-type partitions_table() :: #{
    partition() => node()
}.
%% local and remote tables contains single pair: self partition and self node
-type local_partition_table() :: partitions_table().
-type remote_partition_table() :: partitions_table().

-export_type([discovery_options/0]).
-export_type([partitions_options/0]).
-export_type([balancing_key/0]).
-export_type([partition/0]).
-export_type([balancing_table/0]).
-export_type([partitions_table/0]).
-export_type([local_partition_table/0]).
-export_type([remote_partition_table/0]).

%% API
-export([discovery/1]).
-export([make_local_table/1]).
-export([make_balancing_table/2]).
-export([add_partitions/2]).
-export([del_partition/2]).
-export([empty_partitions/0]).
-export([get_node/4]).

-spec discovery(discovery_options()) -> {ok, [node()]}.
discovery(#{<<"domain_name">> := DomainName, <<"sname">> := Sname}) ->
    case get_addrs(unicode:characters_to_list(DomainName)) of
        {ok, ListAddrs} ->
            logger:info("mg_cluster. resolve ~p with result: ~p", [DomainName, ListAddrs]),
            {ok, addrs_to_nodes(lists:uniq(ListAddrs), Sname)};
        Error ->
            error({resolve_error, Error})
    end.

-spec make_local_table(mg_core_cluster:scaling_type()) -> local_partition_table().
make_local_table(global_based) ->
    #{};
make_local_table(partition_based) ->
    {ok, Hostname} = inet:gethostname(),
    {ok, HostIndex} = host_to_index(Hostname),
    #{HostIndex => node()}.

-spec make_balancing_table(partitions_table(), partitions_options() | undefined) -> balancing_table().
make_balancing_table(_PartitionsTable, undefined) ->
    #{};
make_balancing_table(PartitionsTable, #{capacity := Capacity, max_hash := MaxHash}) ->
    ListPartitions = maps:keys(PartitionsTable),
    mg_core_dirange:get_ranges(MaxHash, Capacity, ListPartitions).

-spec get_node(balancing_key(), partitions_table(), balancing_table(), partitions_options()) -> {ok, node()}.
get_node(BalancingKey, PartitionsTable, BalancingTable, #{max_hash := MaxHash}) ->
    Index = mg_core_dirange:find(erlang:phash2(BalancingKey, MaxHash), BalancingTable),
    Node = maps:get(Index, PartitionsTable),
    {ok, Node}.

-spec add_partitions(partitions_table(), partitions_table()) -> partitions_table().
add_partitions(KnownPartitions, NewPartitions) ->
    maps:merge(KnownPartitions, NewPartitions).

-spec del_partition(node(), partitions_table()) -> partitions_table().
del_partition(Node, PartitionsTable) ->
    maps:filter(fun(_Partition, NodeName) -> NodeName =/= Node end, PartitionsTable).

-spec empty_partitions() -> partitions_table().
empty_partitions() ->
    #{}.

% Internal functions

-spec get_addrs(inet:hostname()) -> {ok, [inet:ip_address()]} | {error, _}.
get_addrs(DomainName) ->
    case inet:getaddrs(DomainName, inet) of
        {ok, _} = Ok -> Ok;
        _ -> inet:getaddrs(DomainName, inet6)
    end.

-spec addrs_to_nodes([inet:ip_address()], binary()) -> [node()].
addrs_to_nodes(ListAddrs, Sname) ->
    NodeName = unicode:characters_to_list(Sname),
    lists:foldl(
        fun(Addr, Acc) ->
            [erlang:list_to_atom(NodeName ++ "@" ++ inet:ntoa(Addr)) | Acc]
        end,
        [],
        ListAddrs
    ).

-spec host_to_index(string()) -> {ok, non_neg_integer()} | error.
-ifdef(TEST).
host_to_index(_MaybeFqdn) ->
    {ok, 0}.
-else.
host_to_index(MaybeFqdn) ->
    [Host | _] = string:split(MaybeFqdn, ".", all),
    try
        [_, IndexStr] = string:split(Host, "-", trailing),
        {ok, erlang:list_to_integer(IndexStr)}
    catch
        _:_ ->
            error
    end.
-endif.
