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
        <<"domain_name">> => <<"machinegun-ha-headless">>,
        <<"sname">> => <<"mg">>
    },
    scaling => partition_based,
    partitioning => #{
        capacity => 5,
        max_hash => 4095
    },
    reconnect_timeout => ?RECONNECT_TIMEOUT
}).

-export([base_test/1]).
-export([reconnect_rebalance_test/1]).
-export([connecting_rebalance_test/1]).
-export([double_connecting_test/1]).
-export([deleted_node_down_test/1]).

-type config() :: [{atom(), term()}].
-type test_case_name() :: atom().
-type group_name() :: atom().
-type test_result() :: any() | no_return().

-define(PARTITIONS_INFO_WITH_PEER(Local, Remote), #{
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
        0 => Local
    },
    partitions_table => #{
        0 => Local,
        1 => Remote
    }
}).
-define(PARTITIONS_INFO_WO_PEER(Local), #{
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
        0 => Local
    },
    partitions_table => #{
        0 => Local
    }
}).
%% erlang:phash2({<<"Namespace">>, <<"ID">>}, 4095) = 1075
-define(KEY, {<<"Namespace">>, <<"ID">>}).

-define(ERLANG_TEST_HOSTS, [
    "mg-0",
    "mg-1",
    "mg-2",
    "mg-3",
    "mg-4"
]).

-define(HOSTS_TEMPLATE, <<
    "127.0.0.1  localhost\n",
    "::1        localhost ip6-localhost ip6-loopback\n",
    "fe00::0    ip6-localnet\n",
    "ff00::0    ip6-mcastprefix\n",
    "ff02::1    ip6-allnodes\n",
    "ff02::2    ip6-allrouters\n"
>>).

-define(LOCAL_NODE(Config), begin
    {local_node, LocalNode} = lists:keyfind(local_node, 1, Config),
    LocalNode
end).

-define(REMOTE_NODE(Config), begin
    {remote_node, RemoteNode} = lists:keyfind(remote_node, 1, Config),
    RemoteNode
end).

-spec init_per_suite(_) -> _.
init_per_suite(Config) ->
    HostsTable = lists:foldl(
        fun(Host, Acc) ->
            {ok, Addr} = inet:getaddr(Host, inet),
            Acc#{unicode:characters_to_binary(Host) => unicode:characters_to_binary(inet:ntoa(Addr))}
        end,
        #{},
        ?ERLANG_TEST_HOSTS
    ),
    _ = prepare_cluster(HostsTable, [<<"mg-0">>, <<"mg-1">>]),
    _ = instance_up(<<"mg-1">>),
    LocalAddr = maps:get(<<"mg-0">>, HostsTable),
    LocalNode = erlang:binary_to_atom(<<"mg@", LocalAddr/binary>>),
    RemoteAddr = maps:get(<<"mg-1">>, HostsTable),
    RemoteNode = erlang:binary_to_atom(<<"mg@", RemoteAddr/binary>>),
    _ = await_peer(RemoteNode, 5),
    [
        {local_node, LocalNode},
        {remote_node, RemoteNode},
        {hosts_table, HostsTable}
        | Config
    ].

-spec end_per_suite(_) -> _.
end_per_suite(_Config) ->
    _ = instance_down(<<"mg-1">>),
    ok.

-spec test() -> _.

-spec all() -> [{group, test_case_name()}].
all() ->
    [{group, basic_operations}].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {basic_operations, [], [
            base_test,
            reconnect_rebalance_test,
            connecting_rebalance_test,
            double_connecting_test,
            deleted_node_down_test
        ]}
    ].

-spec base_test(config()) -> test_result().
base_test(Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    ?assertEqual(2, mg_core_cluster:cluster_size()),
    ?assertEqual({ok, ?REMOTE_NODE(Config)}, mg_core_cluster:get_node(?KEY)),
    ?assertEqual(
        false,
        mg_core_cluster_partitions:is_local_partition(
            ?KEY,
            ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config))
        )
    ),
    ?assertEqual(
        true,
        mg_core_cluster_partitions:is_local_partition(
            ?KEY,
            ?PARTITIONS_INFO_WO_PEER(?LOCAL_NODE(Config))
        )
    ),
    exit(Pid, normal).

-spec reconnect_rebalance_test(config()) -> test_result().
reconnect_rebalance_test(Config) ->
    %% node_down - rebalance - reconnect by timer - rebalance
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    Pid ! {nodedown, ?REMOTE_NODE(Config)},
    ?assertEqual(?PARTITIONS_INFO_WO_PEER(?LOCAL_NODE(Config)), mg_core_cluster:get_partitions_info()),
    ?assertEqual({ok, node()}, mg_core_cluster:get_node(?KEY)),
    ?assertEqual(
        true,
        mg_core_cluster_partitions:is_local_partition(?KEY, ?PARTITIONS_INFO_WO_PEER(?LOCAL_NODE(Config)))
    ),

    %% wait reconnecting
    timer:sleep(?RECONNECT_TIMEOUT + 10),
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    ?assertEqual({ok, ?REMOTE_NODE(Config)}, mg_core_cluster:get_node(?KEY)),
    ?assertEqual(
        false,
        mg_core_cluster_partitions:is_local_partition(
            ?KEY,
            ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config))
        )
    ),
    exit(Pid, normal).

-spec connecting_rebalance_test(config()) -> test_result().
connecting_rebalance_test(Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    Pid ! {nodedown, ?REMOTE_NODE(Config)},
    ?assertEqual(?PARTITIONS_INFO_WO_PEER(?LOCAL_NODE(Config)), mg_core_cluster:get_partitions_info()),
    ?assertEqual({ok, node()}, mg_core_cluster:get_node(?KEY)),

    %% force connecting
    ?assertEqual(
        {ok, #{0 => node()}},
        mg_core_cluster:connecting({#{1 => ?REMOTE_NODE(Config)}, ?REMOTE_NODE(Config)})
    ),
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    ?assertEqual({ok, ?REMOTE_NODE(Config)}, mg_core_cluster:get_node(?KEY)),
    exit(Pid, normal).

-spec double_connecting_test(config()) -> test_result().
double_connecting_test(Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    %% double connect
    ?assertEqual(
        {ok, #{0 => node()}},
        mg_core_cluster:connecting({#{1 => ?REMOTE_NODE(Config)}, ?REMOTE_NODE(Config)})
    ),
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    exit(Pid, normal).

-spec deleted_node_down_test(config()) -> test_result().
deleted_node_down_test(Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    Pid ! {nodedown, 'foo@127.0.0.1'},
    ?assertEqual(
        ?PARTITIONS_INFO_WITH_PEER(?LOCAL_NODE(Config), ?REMOTE_NODE(Config)),
        mg_core_cluster:get_partitions_info()
    ),
    exit(Pid, normal).

%% Internal functions

-spec prepare_cluster(_, _) -> _.
prepare_cluster(HostsTable, HostsToUp) ->
    %% prepare headless emulation records
    HeadlessRecords = lists:foldl(
        fun(Host, Acc) ->
            Address = maps:get(Host, HostsTable),
            <<Acc/binary, Address/binary, "  machinegun-ha-headless\n">>
        end,
        <<"\n">>,
        HostsToUp
    ),

    %% prepare hosts files for each node
    lists:foreach(
        fun(Host) ->
            Address = maps:get(Host, HostsTable),
            Payload = <<
                ?HOSTS_TEMPLATE/binary,
                Address/binary,
                " ",
                Host/binary,
                "\n",
                HeadlessRecords/binary
            >>,
            Filename = unicode:characters_to_list(Host) ++ "_hosts",
            ok = file:write_file(Filename, Payload)
        end,
        HostsToUp
    ),

    %% distribute hosts files
    lists:foreach(
        fun
            (<<"mg-0">>) ->
                LocalFile = "mg-0_hosts",
                RemoteFile = "/etc/hosts",
                Cp = os:find_executable("cp"),
                CMD = Cp ++ " -f " ++ LocalFile ++ " " ++ RemoteFile,
                os:cmd(CMD);
            (Host) ->
                HostString = unicode:characters_to_list(Host),
                LocalFile = HostString ++ "_hosts",
                RemoteFile = HostString ++ ":/etc/hosts",
                SshPass = os:find_executable("sshpass"),
                Scp = os:find_executable("scp"),
                CMD = SshPass ++ " -p security " ++ Scp ++ " " ++ LocalFile ++ " " ++ RemoteFile,
                os:cmd(CMD)
        end,
        HostsToUp
    ).

-spec instance_up(_) -> _.
instance_up(Host) ->
    Ssh = os:find_executable("ssh"),
    CMD = Ssh ++ " " ++ unicode:characters_to_list(Host) ++ " /opt/mg/bin/entrypoint.sh",
    spawn(fun() -> os:cmd(CMD) end).

-spec instance_down(_) -> _.
instance_down(Host) ->
    Ssh = os:find_executable("ssh"),
    CMD = Ssh ++ " " ++ unicode:characters_to_list(Host) ++ " /opt/mg/bin/mg stop",
    spawn(fun() -> os:cmd(CMD) end).

-spec await_peer(_, _) -> _.
await_peer(_RemoteNode, 0) ->
    error(peer_not_started);
await_peer(RemoteNode, Attempt) ->
    case net_adm:ping(RemoteNode) of
        pong ->
            ok;
        pang ->
            timer:sleep(1000),
            await_peer(RemoteNode, Attempt - 1)
    end.
