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

-export([child_spec/1]).
-export([discovery/1]).
-export([cluster_size/0]).

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
    reconnect_timeout => non_neg_integer()
}.
-type state() :: #{
    known_nodes => [node()],
    discovery => discovery_options(),
    reconnect_timeout => non_neg_integer()
}.

%% discovery behaviour callback
-callback discovery(dns_discovery_options()) -> {ok, [node()]}.

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

-spec discovery(dns_discovery_options()) -> {ok, [node()]}.
discovery(#{<<"domain_name">> := DomainName, <<"sname">> := Sname}) ->
    case get_addrs(unicode:characters_to_list(DomainName)) of
        {ok, ListAddrs} ->
            logger:info("union. resolve ~p with result: ~p", [DomainName, ListAddrs]),
            {ok, addrs_to_nodes(lists:uniq(ListAddrs), Sname)};
        Error ->
            error({resolve_error, Error})
    end.

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
handle_continue({full_init, #{discovery := #{module := Mod, options := Opts}} = ClusterOpts}, _State) ->
    _ = net_kernel:monitor_nodes(true),
    {ok, ListNodes} = Mod:discovery(Opts),
    _ = try_connect_all(ListNodes, maps:get(reconnect_timeout, ClusterOpts)),
    {noreply, ClusterOpts#{known_nodes => ListNodes}}.

-spec handle_call(term(), {pid(), _}, state()) -> {reply, any(), state()}.
-ifdef(TEST).
handle_call({set_state, NewState}, _From, _State) ->
    {reply, ok, NewState};
handle_call(get_cluster_size, _From, #{known_nodes := ListNodes} = State) ->
    {reply, erlang:length(ListNodes), State}.
-else.
handle_call(get_cluster_size, _From, #{known_nodes := ListNodes} = State) ->
    {reply, erlang:length(ListNodes), State}.
-endif.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({timeout, _TRef, {reconnect, Node}}, State) ->
    ListNodes = maybe_connect(Node, State),
    {noreply, State#{known_nodes => ListNodes}};
handle_info({nodeup, RemoteNode}, #{known_nodes := ListNodes} = State) ->
    logger:info("union. ~p receive nodeup ~p", [node(), RemoteNode]),
    NewState =
        case lists:member(RemoteNode, ListNodes) of
            true ->
                %% well known node connected, do nothing
                State;
            false ->
                %% new node connected, need update list nodes
                #{discovery := #{module := Mod, options := Opts}, reconnect_timeout := Timeout} = State,
                {ok, NewListNodes} = Mod:discovery(Opts),
                _ = try_connect_all(NewListNodes, Timeout),
                State#{known_nodes => NewListNodes}
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

%% cluster functions
-spec connect(node(), non_neg_integer()) -> ok | error.
connect(Node, ReconnectTimeout) ->
    case net_adm:ping(Node) of
        pong ->
            ok;
        _ ->
            _ = erlang:start_timer(ReconnectTimeout, self(), {reconnect, Node}),
            error
    end.

-spec try_connect_all([node()], non_neg_integer()) -> ok.
try_connect_all(ListNodes, ReconnectTimeout) ->
    _ = lists:foreach(fun(Node) -> connect(Node, ReconnectTimeout) end, ListNodes).

-spec maybe_connect(node(), state()) -> [node()].
maybe_connect(Node, #{discovery := #{module := Mod, options := Opts}, reconnect_timeout := Timeout}) ->
    {ok, ListNodes} = Mod:discovery(Opts),
    case lists:member(Node, ListNodes) of
        false ->
            %% node deleted from cluster, do nothing
            skip;
        true ->
            connect(Node, Timeout)
    end,
    ListNodes.

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
    reconnect_timeout => ?RECONNECT_TIMEOUT
}).

-spec test() -> _.

-spec connect_error_test() -> _.
connect_error_test() ->
    ?assertEqual(error, connect('foo@127.0.0.1', 3000)).

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

-spec nxdomain_test() -> _.
nxdomain_test() ->
    ?assertError(
        {resolve_error, {error, nxdomain}},
        mg_core_union:discovery(#{<<"domain_name">> => <<"bad_name">>, <<"sname">> => <<"mg">>})
    ).

-spec for_full_cover_test() -> _.
for_full_cover_test() ->
    ?assertEqual({noreply, #{}}, handle_cast([], #{})),
    ?assertEqual(ok, terminate(term, #{})),
    ?assertEqual({ok, #{}}, code_change(old, #{}, extra)).

-endif.
