-module(mg_core_cluster).

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

-export([child_spec/1]).
-export([cluster_size/0]).
-export([connecting/1]).
-export([get_node/1]).
-export([get_partitions_info/0]).

-define(SERVER, ?MODULE).
-define(RECONNECT_TIMEOUT, 5000).

-type discovery_options() :: mg_core_cluster_partitions:discovery_options().

-type scaling_type() :: global_based | partition_based.

-type cluster_options() :: #{
    discovering => discovery_options(),
    reconnect_timeout => non_neg_integer(),
    scaling => scaling_type(),
    %% partitioning required if scaling = partition_based
    partitioning => mg_core_cluster_partitions:partitions_options()
}.

-type partitions_info() :: #{
    partitioning => mg_core_cluster_partitions:partitions_options(),
    local_table => mg_core_cluster_partitions:local_partition_table(),
    balancing_table => mg_core_cluster_partitions:balancing_table(),
    partitions_table => mg_core_cluster_partitions:partitions_table()
}.

-type state() :: #{
    %% cluster static options
    discovering => discovery_options(),
    reconnect_timeout => non_neg_integer(),
    scaling => scaling_type(),
    partitioning => mg_core_cluster_partitions:partitions_options(),
    %% dynamic
    known_nodes => [node()],
    local_table => mg_core_cluster_partitions:local_partition_table(),
    balancing_table => mg_core_cluster_partitions:balancing_table(),
    partitions_table => mg_core_cluster_partitions:partitions_table()
}.

-export_type([scaling_type/0]).
-export_type([partitions_info/0]).
-export_type([cluster_options/0]).

-spec child_spec(cluster_options()) -> [supervisor:child_spec()].
child_spec(#{discovering := _} = ClusterOpts) ->
    [
        #{
            id => ?MODULE,
            start => {?MODULE, start_link, [ClusterOpts]}
        }
    ];
child_spec(_) ->
    % cluster not configured, skip
    [].

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

-spec connecting({mg_core_cluster_partitions:partitions_table(), node()}) ->
    {ok, mg_core_cluster_partitions:local_partition_table()}.
connecting(RemoteData) ->
    gen_server:call(?MODULE, {connecting, RemoteData}).

-spec get_node(mg_core_cluster_partitions:balancing_key()) -> {ok, node()}.
get_node(BalancingKey) ->
    gen_server:call(?MODULE, {get_node, BalancingKey}).

-spec get_partitions_info() -> partitions_info().
get_partitions_info() ->
    gen_server:call(?MODULE, get_partitions_info).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
-spec start_link(cluster_options()) -> {ok, pid()} | {error, term()}.
start_link(ClusterOpts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, ClusterOpts, []).

-spec init(cluster_options()) -> {ok, state(), {continue, {full_init, cluster_options()}}}.
init(ClusterOpts) ->
    logger:info("mg_cluster. init with options: ~p", [ClusterOpts]),
    {ok, #{}, {continue, {full_init, ClusterOpts}}}.

-spec handle_continue({full_init, cluster_options()}, state()) -> {noreply, state()}.
handle_continue(
    {
        full_init,
        #{
            discovering := DiscoveryOpts,
            scaling := ScalingType
        } = ClusterOpts
    },
    _State
) ->
    _ = net_kernel:monitor_nodes(true),
    {ok, ListNodes} = mg_core_cluster_partitions:discovery(DiscoveryOpts),
    LocalTable = mg_core_cluster_partitions:make_local_table(ScalingType),
    PartitionsTable = try_connect_all(ListNodes, maps:get(reconnect_timeout, ClusterOpts), LocalTable),
    BalancingTable = mg_core_cluster_partitions:make_balancing_table(
        ScalingType,
        PartitionsTable,
        maps:get(partitioning, ClusterOpts, undefined)
    ),
    {
        noreply,
        ClusterOpts#{
            known_nodes => ListNodes,
            local_table => LocalTable,
            partitions_table => PartitionsTable,
            balancing_table => BalancingTable
        }
    }.

-spec handle_call(term(), {pid(), _}, state()) -> {reply, any(), state()}.
handle_call({get_node, BalancingKey}, _From, State) ->
    Response = mg_core_cluster_partitions:get_node(BalancingKey, partitions_info(State)),
    {reply, Response, State};
handle_call(get_cluster_size, _From, #{known_nodes := ListNodes} = State) ->
    {reply, erlang:length(ListNodes), State};
handle_call(get_partitions_info, _From, State) ->
    {reply, partitions_info(State), State};
handle_call(
    {connecting, {RemoteTable, _RemoteNode}},
    _From,
    #{
        scaling := partition_based,
        partitioning := PartitionsOpts,
        local_table := LocalTable,
        partitions_table := PartitionsTable
    } = State
) ->
    NewPartitionsTable = mg_core_cluster_partitions:add_partitions(PartitionsTable, RemoteTable),
    NewBalancingTable = mg_core_cluster_partitions:make_balancing_table(
        partition_based,
        NewPartitionsTable,
        PartitionsOpts
    ),
    {
        reply,
        {ok, LocalTable},
        State#{
            partitions_table => NewPartitionsTable,
            balancing_table => NewBalancingTable
        }
    };
%% Not partition based cluster, only list nodes updating
handle_call({connecting, {_RemoteTable, _RemoteNode}}, _From, State) ->
    {
        reply,
        {ok, mg_core_cluster_partitions:empty_partitions()},
        State
    }.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({timeout, _TRef, {reconnect, Node}}, State) ->
    NewState = maybe_connect(Node, State),
    {noreply, NewState};
handle_info({nodeup, RemoteNode}, #{discovering := DiscoveryOpts} = State) ->
    %% do nothing because rebalance partitions in connecting call
    logger:info("mg_cluster. ~p receive nodeup ~p", [node(), RemoteNode]),
    {ok, ListNodes} = mg_core_cluster_partitions:discovery(DiscoveryOpts),
    {noreply, State#{known_nodes => ListNodes}};
handle_info(
    {nodedown, RemoteNode},
    #{discovering := DiscoveryOpts, reconnect_timeout := Timeout, partitions_table := PartitionsTable} = State
) ->
    %% rebalance without node
    logger:warning("mg_cluster. ~p receive nodedown ~p", [node(), RemoteNode]),
    {ok, ListNodes} = mg_core_cluster_partitions:discovery(DiscoveryOpts),
    NewPartitionsTable = mg_core_cluster_partitions:del_partition(RemoteNode, PartitionsTable),
    NewState = maybe_rebalance(#{}, State#{known_nodes => ListNodes, partitions_table => NewPartitionsTable}),
    _ = erlang:start_timer(Timeout, self(), {reconnect, RemoteNode}),
    {noreply, NewState}.

-spec terminate(_Reason, state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_OldVsn, state(), _Extra) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% cluster functions
-spec connect(node(), non_neg_integer(), mg_core_cluster_partitions:local_partition_table()) ->
    {ok, mg_core_cluster_partitions:partitions_table()} | {error, term()}.
connect(Node, ReconnectTimeout, LocalTable) when Node =/= node() ->
    case net_adm:ping(Node) of
        pong ->
            try
                erpc:call(Node, ?MODULE, connecting, [{LocalTable, node()}])
            catch
                _:_ ->
                    _ = erlang:start_timer(ReconnectTimeout, self(), {reconnect, Node}),
                    {error, not_connected}
            end;
        pang ->
            _ = erlang:start_timer(ReconnectTimeout, self(), {reconnect, Node}),
            {error, not_connected}
    end;
connect(_Node, _ReconnectTimeout, LocalTable) ->
    {ok, LocalTable}.

-spec try_connect_all([node()], non_neg_integer(), mg_core_cluster_partitions:local_partition_table()) ->
    mg_core_cluster_partitions:partitions_table().
try_connect_all(ListNodes, ReconnectTimeout, LocalTable) ->
    lists:foldl(
        fun(Node, Acc) ->
            case connect(Node, ReconnectTimeout, LocalTable) of
                {ok, RemoteTable} ->
                    mg_core_cluster_partitions:add_partitions(Acc, RemoteTable);
                _ ->
                    Acc
            end
        end,
        mg_core_cluster_partitions:empty_partitions(),
        ListNodes
    ).

-spec maybe_connect(node(), state()) -> state().
maybe_connect(
    Node,
    #{
        discovering := Opts,
        local_table := LocalTable,
        reconnect_timeout := ReconnectTimeout
    } = State
) ->
    {ok, ListNodes} = mg_core_cluster_partitions:discovery(Opts),
    case lists:member(Node, ListNodes) of
        false ->
            %% node delete from cluster, do nothing (rebalance was when node down detected)
            State#{known_nodes => ListNodes};
        true ->
            case connect(Node, ReconnectTimeout, LocalTable) of
                {ok, RemoteTable} ->
                    %% node connected after temporary split or new node added, rebalance with node
                    maybe_rebalance(RemoteTable, State#{known_nodes => ListNodes});
                _ ->
                    State#{known_nodes => ListNodes}
            end
    end.

-spec maybe_rebalance(mg_core_cluster_partitions:partitions_table(), state()) -> state().
maybe_rebalance(
    RemoteTable,
    #{
        scaling := partition_based,
        partitioning := PartitionsOpts,
        partitions_table := PartitionsTable
    } = State
) ->
    NewPartitionsTable = mg_core_cluster_partitions:add_partitions(PartitionsTable, RemoteTable),
    NewBalancingTable = mg_core_cluster_partitions:make_balancing_table(
        partition_based,
        NewPartitionsTable,
        PartitionsOpts
    ),
    State#{partitions_table => NewPartitionsTable, balancing_table => NewBalancingTable};
maybe_rebalance(_, State) ->
    State.

-spec partitions_info(state()) -> partitions_info().
partitions_info(State) ->
    maps:with([partitioning, partitions_table, balancing_table, local_table], State).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_OPTS, #{
    discovering => #{
        <<"domain_name">> => <<"localhost">>,
        <<"sname">> => <<"test_node">>
    },
    scaling => partition_based,
    partitioning => #{
        capacity => 3,
        max_hash => 4095
    },
    reconnect_timeout => ?RECONNECT_TIMEOUT
}).

-spec test() -> _.

-spec child_spec_test() -> _.
child_spec_test() ->
    EmptyChildSpec = mg_core_cluster:child_spec(#{}),
    ?assertEqual([], EmptyChildSpec),
    ExpectedSpec = [
        #{
            id => mg_core_cluster,
            start => {
                mg_core_cluster,
                start_link,
                [?CLUSTER_OPTS]
            }
        }
    ],
    ChildSpec = mg_core_cluster:child_spec(?CLUSTER_OPTS),
    ?assertEqual(ExpectedSpec, ChildSpec).

-endif.
