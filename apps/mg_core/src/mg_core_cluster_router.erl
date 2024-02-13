-module(mg_core_cluster_router).

-type dns_discovery_options() :: #{
    %% #{<<"domain_name">> => <<"machinegun-ha-headless">>,<<"sname">> => <<"machinegun">>}
    binary() => binary()
}.

-type routing_key() :: term().

%% API
-export([discovery/1]).
-export([make_routing_opts/1]).
-export([get_route/2]).

-spec discovery(dns_discovery_options()) -> {ok, [node()]}.
discovery(#{<<"domain_name">> := DomainName, <<"sname">> := Sname}) ->
    case get_addrs(unicode:characters_to_list(DomainName)) of
        {ok, ListAddrs} ->
            logger:info("mg_cluster. resolve ~p with result: ~p", [DomainName, ListAddrs]),
            {ok, addrs_to_nodes(lists:uniq(ListAddrs), Sname)};
        Error ->
            error({resolve_error, Error})
    end.

-spec make_routing_opts(mg_core_cluster:routing_type() | undefined) -> mg_core_cluster:routing_opts().
make_routing_opts(undefined) ->
    #{};
make_routing_opts(host_index_based) ->
    {ok, Hostname} = inet:gethostname(),
    {ok, HostIndex} = host_to_index(Hostname),
    #{
        routing_type => host_index_based,
        address => HostIndex,
        node => node()
    }.

-spec get_route(routing_key(), mg_core_cluster:state()) -> {ok, node()}.
get_route(RoutingKey, #{capacity := Capacity, max_hash := MaxHash, routing_table := RoutingTable} = _State) ->
    ListAddrs = maps:keys(RoutingTable),
    RangeMap = ranger:get_ranges(MaxHash, Capacity, ListAddrs),
    Index = ranger:find(erlang:phash2(RoutingKey, MaxHash), RangeMap),
    Node = maps:get(Index, RoutingTable),
    {ok, Node}.

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
