-module(mg_core_cluster_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    all/0,
    groups/0
]).

-define(RECONNECT_TIMEOUT, 2000).
-define(CLUSTER_OPTS, #{
    discovering => #{
        <<"domain_name">> => <<"localhost">>,
        <<"sname">> => <<"test_node">>
    },
    scaling => partition_based,
    partitioning => #{
        capacity => 5,
        max_hash => 4095
    },
    reconnect_timeout => ?RECONNECT_TIMEOUT
}).
-define(NEIGHBOUR, 'peer@127.0.0.1').

-export([peer_test/1]).
-export([base_test/1]).
-export([reconnect_rebalance_test/1]).
-export([connecting_rebalance_test/1]).
-export([double_connecting_test/1]).
-export([deleted_node_down_test/1]).

-type config() :: [{atom(), term()}].
-type test_case_name() :: atom().
-type group_name() :: atom().
-type test_result() :: any() | no_return().

-define(PARTITIONS_INFO_WITH_PEER, #{
    partitioning => #{
        capacity => 5,
        max_hash => 4095
    },
    balancing_table => #{
        {0, 818} => 0,
        {819, 1637} => 1,
        {1638, 2046} => 0,
        {2047, 2456} => 1,
        {2457, 2865} => 0,
        {2866, 3275} => 1,
        {3276, 3684} => 0,
        {3685, 4095} => 1
    },
    local_table => #{
        0 => 'test_node@127.0.0.1'
    },
    partitions_table => #{
        0 => 'test_node@127.0.0.1',
        1 => 'peer@127.0.0.1'
    }
}).
-define(PARTITIONS_INFO_WO_PEER, #{
    partitioning => #{
        capacity => 5,
        max_hash => 4095
    },
    balancing_table => #{
        {0, 818} => 0,
        {819, 1637} => 0,
        {1638, 2456} => 0,
        {2457, 3275} => 0,
        {3276, 4095} => 0
    },
    local_table => #{
        0 => 'test_node@127.0.0.1'
    },
    partitions_table => #{
        0 => 'test_node@127.0.0.1'
    }
}).
%% erlang:phash2({<<"Namespace">>, <<"ID">>}, 4095) = 1075
-define(KEY, {<<"Namespace">>, <<"ID">>}).

-spec init_per_suite(_) -> _.
init_per_suite(Config) ->
    WorkDir = os:getenv("WORK_DIR"),
    EbinDir = WorkDir ++ "/_build/default/lib/mg_cth/ebin",
    ok = start_peer(),
    true = erpc:call(?NEIGHBOUR, code, add_path, [EbinDir]),
    {ok, _Pid} = erpc:call(?NEIGHBOUR, mg_cth_neighbour, start, []),
    Config.

-spec end_per_suite(_) -> _.
end_per_suite(_Config) ->
    ok.

-spec test() -> _.

-spec all() -> [{group, test_case_name()}].
all() ->
    [{group, basic_operations}].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {basic_operations, [], [
            peer_test,
            base_test,
            reconnect_rebalance_test,
            connecting_rebalance_test,
            double_connecting_test,
            deleted_node_down_test
        ]}
    ].

-spec peer_test(config()) -> test_result().
peer_test(_Config) ->
    {ok, 'ECHO'} = erpc:call(?NEIGHBOUR, mg_cth_neighbour, echo, ['ECHO']),
    {ok, #{1 := 'peer@127.0.0.1'}} = erpc:call(?NEIGHBOUR, mg_cth_neighbour, connecting, [{#{}, node()}]).

-spec base_test(config()) -> test_result().
base_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    ?assertEqual(2, mg_core_cluster:cluster_size()),
    ?assertEqual({ok, ?NEIGHBOUR}, mg_core_cluster:get_node(?KEY)),
    ?assertEqual(false, mg_core_cluster_partitions:is_local_partition(?KEY, ?PARTITIONS_INFO_WITH_PEER)),
    ?assertEqual(true, mg_core_cluster_partitions:is_local_partition(?KEY, ?PARTITIONS_INFO_WO_PEER)),
    exit(Pid, normal).

-spec reconnect_rebalance_test(config()) -> test_result().
reconnect_rebalance_test(_Config) ->
    %% node_down - rebalance - reconnect by timer - rebalance
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    Pid ! {nodedown, ?NEIGHBOUR},
    ?assertEqual(?PARTITIONS_INFO_WO_PEER, mg_core_cluster:get_partitions_info()),
    ?assertEqual({ok, node()}, mg_core_cluster:get_node(?KEY)),
    ?assertEqual(true, mg_core_cluster_partitions:is_local_partition(?KEY, ?PARTITIONS_INFO_WO_PEER)),

    %% wait reconnecting
    timer:sleep(?RECONNECT_TIMEOUT + 10),
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    ?assertEqual({ok, ?NEIGHBOUR}, mg_core_cluster:get_node(?KEY)),
    ?assertEqual(false, mg_core_cluster_partitions:is_local_partition(?KEY, ?PARTITIONS_INFO_WITH_PEER)),
    exit(Pid, normal).

-spec connecting_rebalance_test(config()) -> test_result().
connecting_rebalance_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    Pid ! {nodedown, ?NEIGHBOUR},
    ?assertEqual(?PARTITIONS_INFO_WO_PEER, mg_core_cluster:get_partitions_info()),
    ?assertEqual({ok, node()}, mg_core_cluster:get_node(?KEY)),

    %% force connecting
    ?assertEqual({ok, #{0 => node()}}, mg_core_cluster:connecting({#{1 => ?NEIGHBOUR}, ?NEIGHBOUR})),
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    ?assertEqual({ok, ?NEIGHBOUR}, mg_core_cluster:get_node(?KEY)),
    exit(Pid, normal).

-spec double_connecting_test(config()) -> test_result().
double_connecting_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    %% double connect
    ?assertEqual({ok, #{0 => node()}}, mg_core_cluster:connecting({#{1 => ?NEIGHBOUR}, ?NEIGHBOUR})),
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    exit(Pid, normal).

-spec deleted_node_down_test(config()) -> test_result().
deleted_node_down_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    Pid ! {nodedown, 'foo@127.0.0.1'},
    %% wait reconnect timeout
    ?assertEqual(?PARTITIONS_INFO_WITH_PEER, mg_core_cluster:get_partitions_info()),
    exit(Pid, normal).

%% Internal functions

-spec start_peer() -> _.
start_peer() ->
    {ok, _Pid, _Node} = peer:start(#{
        name => peer,
        longnames => true,
        host => "127.0.0.1"
    }),
    pong = net_adm:ping(?NEIGHBOUR),
    ok.
