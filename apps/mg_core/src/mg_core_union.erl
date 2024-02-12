-module(mg_core_union).

-behaviour(gen_server).

-export([start_link/1]).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

% cluster functions
-export([child_spec/1]).
-export([cluster_size/0]).
-export([connecting/1]).
-export([get_route/1]).
% discovery functions
-export([discovery/1]).
-export([make_routing_opts/1]).
% router functions
-export([routing/2]).

-ifdef(TEST).
-export([set_state/1]).
-endif.

-define(SERVER, ?MODULE).
-define(RECONNECT_TIMEOUT, 5000).

-type discovery_options() :: #{
    module := module(),
    %% options is module specific structure
    options := term()
}.
-type dns_discovery_options() :: #{
    %% #{<<"domain_name">> => <<"machinegun-ha-headless">>,<<"sname">> => <<"machinegun">>}
    binary() => binary()
}.
-type cluster_options() :: #{
    discovery => discovery_options(),
    reconnect_timeout => non_neg_integer(),
    routing => host_index_based | undefined
}.
-type address() :: term().
-type routing_type() :: host_index_based.
-type routing_opts() :: #{
    routing_type := routing_type(),
    address := address(),
    node := node()
}.
-type routing_table() :: #{address() => node()}.
-type state() :: #{
    known_nodes => [node()],
    discovery => discovery_options(),
    reconnect_timeout => non_neg_integer(),
    routing => routing_type(),
    routing_opts => routing_opts(),
    routing_table => routing_table()
}.

%% discovery API
-spec discovery(dns_discovery_options()) -> {ok, [node()]}.
discovery(#{<<"domain_name">> := DomainName, <<"sname">> := Sname}) ->
    case get_addrs(unicode:characters_to_list(DomainName)) of
        {ok, ListAddrs} ->
            logger:info("union. resolve ~p with result: ~p", [DomainName, ListAddrs]),
            {ok, addrs_to_nodes(lists:uniq(ListAddrs), Sname)};
        Error ->
            error({resolve_error, Error})
    end.

-spec make_routing_opts(routing_type() | undefined) -> routing_opts().
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

%% router API
-spec routing(term(), state()) -> {ok, node()}.
routing(RoutingKey, #{capacity := Capacity, max_hash := MaxHash, routing_table := RoutingTable} = _State) ->
    ListAddrs = maps:keys(RoutingTable),
    RangeMap = ranger:get_ranges(MaxHash, Capacity, ListAddrs),
    Index = ranger:find(erlang:phash2(RoutingKey, MaxHash), RangeMap),
    Node = maps:get(Index, RoutingTable),
    {ok, Node}.

%% cluster API
-spec child_spec(cluster_options()) -> [supervisor:child_spec()].
child_spec(#{discovery := _} = ClusterOpts) ->
    [
        #{
            id => ?MODULE,
            start => {?MODULE, start_link, [ClusterOpts]}
        }
    ];
child_spec(_) ->
    % cluster not configured, skip
    [].

-ifdef(TEST).
-spec set_state(state()) -> ok.
set_state(NewState) ->
    gen_server:call(?MODULE, {set_state, NewState}).
-endif.

-spec cluster_size() -> non_neg_integer().
cluster_size() ->
    case whereis(?MODULE) of
        undefined ->
            %% for backward compatibility with consul
            ReplicaCount = os:getenv("REPLICA_COUNT", "1"),
            erlang:list_to_integer(ReplicaCount);
        Pid when is_pid(Pid) ->
            gen_server:call(Pid, get_cluster_size)
    end.

-spec connecting(routing_opts()) -> routing_table().
connecting(RoutingOpts) ->
    gen_server:call(?MODULE, {connecting, RoutingOpts}).

-spec get_route(term()) -> {ok, node()}.
get_route(RoutingKey) ->
    gen_server:call(?MODULE, {get_route, RoutingKey}).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
-spec start_link(cluster_options()) -> {ok, pid()} | {error, term()}.
start_link(ClusterOpts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, ClusterOpts, []).

-spec init(cluster_options()) -> {ok, state(), {continue, {full_init, cluster_options()}}}.
init(ClusterOpts) ->
    logger:info("union. init with options: ~p", [ClusterOpts]),
    {ok, #{}, {continue, {full_init, ClusterOpts}}}.

-spec handle_continue({full_init, cluster_options()}, state()) -> {noreply, state()}.
handle_continue(
    {
        full_init,
        #{discovery := #{module := Mod, options := Opts}, routing := RoutingType} = ClusterOpts
    },
    _State
) ->
    _ = net_kernel:monitor_nodes(true),
    {ok, ListNodes} = do_discovery(Mod, Opts),
    RoutingOpts = do_make_routing_opts(Mod, RoutingType),
    RoutingTable = try_connect_all(ListNodes, maps:get(reconnect_timeout, ClusterOpts), RoutingOpts),
    {noreply, ClusterOpts#{known_nodes => ListNodes, routing_opts => RoutingOpts, routing_table => RoutingTable}}.

-spec handle_call(term(), {pid(), _}, state()) -> {reply, any(), state()}.
handle_call({set_state, NewState}, _From, _State) ->
    {reply, ok, NewState};
handle_call({get_route, RoutingKey}, _From, #{discovery := #{module := Mod}} = State) ->
    Node = do_routing(Mod, RoutingKey, State),
    {reply, {ok, Node}, State};
handle_call(get_cluster_size, _From, #{known_nodes := ListNodes} = State) ->
    {reply, erlang:length(ListNodes), State};
handle_call(
    {connecting, RemoteRoutingOpts},
    _From,
    #{routing_opts := RoutingOpts, routing_table := RoutingTable} = State
) ->
    #{
        routing_type := RemoteRoutingType,
        address := RemoteAddress,
        node := RemoteNode
    } = RemoteRoutingOpts,
    #{
        routing_type := LocalRoutingType,
        address := LocalAddress,
        node := LocalNode
    } = RoutingOpts,
    case RemoteRoutingType =:= LocalRoutingType of
        true ->
            Route = #{RemoteAddress => RemoteNode},
            {
                reply,
                {ok, #{LocalAddress => LocalNode}},
                State#{routing_table => maps:merge(RoutingTable, Route)}
            };
        false ->
            {reply, {error, unsupported_routing}, State}
    end.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({timeout, _TRef, {reconnect, Node}}, State) ->
    {ListNodes, RoutingTable} = maybe_connect(Node, State),
    {noreply, State#{known_nodes => ListNodes, routing_table => RoutingTable}};
handle_info({nodeup, RemoteNode}, #{known_nodes := ListNodes} = State) ->
    logger:info("union. ~p receive nodeup ~p", [node(), RemoteNode]),
    NewState =
        case lists:member(RemoteNode, ListNodes) of
            true ->
                %% well known node connected, do nothing
                State;
            false ->
                %% new node connected, need update list nodes
                #{
                    discovery := #{module := Mod, options := Opts},
                    routing_opts := RoutingOpts,
                    reconnect_timeout := Timeout
                } = State,
                {ok, NewListNodes} = do_discovery(Mod, Opts),
                RoutingTable = try_connect_all(NewListNodes, Timeout, RoutingOpts),
                State#{known_nodes => NewListNodes, routing_table => RoutingTable}
        end,
    {noreply, NewState};
handle_info({nodedown, RemoteNode}, #{reconnect_timeout := Timeout} = State) ->
    logger:warning("union. ~p receive nodedown ~p", [node(), RemoteNode]),
    _ = erlang:start_timer(Timeout, self(), {reconnect, RemoteNode}),
    {noreply, State}.

-spec terminate(_Reason, state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_OldVsn, state(), _Extra) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_discovery(module(), term()) -> {ok, [node()]}.
do_discovery(mg_core_union, Opts) ->
    mg_core_union:discovery(Opts).

%% cluster functions
-spec connect(node(), non_neg_integer(), map()) -> Route :: map().
connect(Node, ReconnectTimeout, #{address := Address} = RoutingOpts) ->
    case Node =:= node() of
        true ->
            {ok, #{Address => Node}};
        false ->
            case net_adm:ping(Node) of
                pong ->
                    erpc:call(Node, ?MODULE, connecting, [RoutingOpts]);
                _ ->
                    _ = erlang:start_timer(ReconnectTimeout, self(), {reconnect, Node}),
                    {ok, #{}}
            end
    end.

-spec try_connect_all([node()], non_neg_integer(), map()) -> map().
try_connect_all(ListNodes, ReconnectTimeout, RoutingOpts) ->
    lists:foldl(fun(Node, Acc) ->
        case connect(Node, ReconnectTimeout, RoutingOpts) of
            {ok, Route} -> maps:merge(Acc, Route);
            _ -> Acc
        end
    end, #{}, ListNodes).

-spec maybe_connect(node(), state()) -> {[node()], RoutingTable :: map()}.
maybe_connect(
    Node,
    #{
        discovery := #{module := Mod, options := Opts},
        routing_opts := RoutingOpts,
        routing_table := RoutingTable,
        reconnect_timeout := Timeout
    }
) ->
    {ok, ListNodes} = do_discovery(Mod, Opts),
    NewRoutingTable = case lists:member(Node, ListNodes) of
        false ->
            %% node deleted from cluster, do nothing
            RoutingTable;
        true ->
            {ok, Route} = connect(Node, Timeout, RoutingOpts),
            maps:merge(RoutingTable, Route)
    end,
    {ListNodes, NewRoutingTable}.

%% discovery functions
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

-spec host_to_index(string()) -> non_neg_integer() | error.
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

-spec do_make_routing_opts(module(), routing_type() | undefined) -> routing_opts().
do_make_routing_opts(mg_core_union, RoutingType) ->
    mg_core_union:make_routing_opts(RoutingType).

-spec do_routing(module(), term(), state()) -> {ok, node()}.
do_routing(mg_core_union, RoutingKey, State) ->
    mg_core_union:routing(RoutingKey, State).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_OPTS, #{
    discovery => #{
        module => mg_core_union,
        options => #{
            <<"domain_name">> => <<"localhost">>,
            <<"sname">> => <<"test_node">>
        }
    },
    routing => host_index_based,
    capacity => 3,
    max_hash => 4095,
    reconnect_timeout => ?RECONNECT_TIMEOUT
}).

-spec test() -> _.

-spec connect_error_test() -> _.
connect_error_test() ->
    ?assertEqual(
        error,
        connect(
            'foo@127.0.0.1',
            3000,
            #{
                routing_type => host_index_based,
                address => 0,
                node => node()
            }
        )
    ).

-spec child_spec_test() -> _.
child_spec_test() ->
    EmptyChildSpec = mg_core_union:child_spec(#{}),
    ?assertEqual([], EmptyChildSpec),
    ExpectedSpec = [
        #{
            id => mg_core_union,
            start => {
                mg_core_union,
                start_link,
                [?CLUSTER_OPTS]
            }
        }
    ],
    ChildSpec = mg_core_union:child_spec(?CLUSTER_OPTS),
    ?assertEqual(ExpectedSpec, ChildSpec).

-spec for_full_cover_test() -> _.
for_full_cover_test() ->
    ?assertEqual({noreply, #{}}, handle_cast([], #{})),
    ?assertEqual(ok, terminate(term, #{})),
    ?assertEqual({ok, #{}}, code_change(old, #{}, extra)).

-endif.
