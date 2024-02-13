-module(mg_core_cluster_SUITE).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    all/0,
    groups/0
]).

-define(RECONNECT_TIMEOUT, 1000).
-define(CLUSTER_OPTS, #{
    discovery => #{
        module => mg_core_cluster_router,
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

-export([start_ok_test/1]).
-export([unknown_nodedown_test/1]).
-export([exists_nodedown_test/1]).
-export([unknown_nodeup_test/1]).
-export([exists_nodeup_test/1]).
-export([cluster_size_test/1]).

-type config() :: [{atom(), term()}].
-type test_case_name() :: atom().
-type group_name() :: atom().
-type test_result() :: any() | no_return().

-spec init_per_suite(_) -> _.
init_per_suite(Config) ->
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
            start_ok_test,
            unknown_nodedown_test,
            exists_nodedown_test,
            unknown_nodeup_test,
            exists_nodeup_test,
            cluster_size_test
        ]}
    ].

-spec start_ok_test(config()) -> test_result().
start_ok_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    State = await_sys_get_state(Pid),
    #{known_nodes := ListNodes} = State,
    lists:foreach(
        fun(Node) ->
            ?assertEqual(pong, net_adm:ping(Node))
        end,
        ListNodes
    ),
    exit(Pid, normal).

-spec unknown_nodedown_test(config()) -> test_result().
unknown_nodedown_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    nodedown_check(Pid, 'foo@127.0.0.1'),
    exit(Pid, normal).

-spec exists_nodedown_test(config()) -> test_result().
exists_nodedown_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    nodedown_check(Pid, node()),
    exit(Pid, normal).

-spec unknown_nodeup_test(config()) -> test_result().
unknown_nodeup_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    State = await_sys_get_state(Pid),
    mg_core_cluster:set_state(State#{known_nodes => []}),
    Pid ! {nodeup, node()},
    #{known_nodes := List} = await_sys_get_state(Pid),
    ?assertEqual(List, [node()]),
    exit(Pid, normal).

-spec exists_nodeup_test(config()) -> test_result().
exists_nodeup_test(_Config) ->
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    #{known_nodes := List1} = await_sys_get_state(Pid),
    ?assertEqual(List1, [node()]),
    Pid ! {nodeup, node()},
    #{known_nodes := List2} = await_sys_get_state(Pid),
    ?assertEqual(List2, [node()]),
    exit(Pid, normal).

-spec cluster_size_test(config()) -> test_result().
cluster_size_test(_Config) ->
    _ = os:putenv("REPLICA_COUNT", "3"),
    ?assertEqual(3, mg_core_cluster:cluster_size()),
    {ok, Pid} = mg_core_cluster:start_link(?CLUSTER_OPTS),
    ?assertEqual(1, mg_core_cluster:cluster_size()),
    exit(Pid, normal).

%% Internal functions
-spec nodedown_check(pid(), node()) -> _.
nodedown_check(Pid, Node) ->
    #{known_nodes := ListNodes1} = await_sys_get_state(Pid),
    Pid ! {nodedown, Node},
    timer:sleep(?RECONNECT_TIMEOUT + 10),
    #{known_nodes := ListNodes2} = await_sys_get_state(Pid),
    ?assertEqual(ListNodes1, ListNodes2).

-spec await_sys_get_state(pid()) -> any().
await_sys_get_state(Pid) ->
    case sys:get_state(Pid, 100) of
        {error, _} -> await_sys_get_state(Pid);
        State -> State
    end.
