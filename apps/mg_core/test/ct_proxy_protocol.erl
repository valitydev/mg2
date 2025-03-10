-module(ct_proxy_protocol).
-behaviour(ranch_protocol).

%%

-define(DEFAULT_SOCKET_OPTS, [{packet, 0}, {active, once}]).
-define(DEFAULT_TIMEOUT, 5000).

-type activity() ::
    stop
    | ignore
    | {buffer, binary()}
    | {remote, {inet:ip_address(), inet:port_number()}}.

-type proxy_fun() :: fun((binary()) -> activity()).

-export_type([activity/0]).

%% Behaviour callbacks
-export([start_link/4, init/4]).

%% Internal state
%% ----------------------------------------------------------
%% Callbacks
%% ----------------------------------------------------------

-type opts() :: #{
    proxy := proxy_fun(),
    timeout => timeout(),
    source_opts => list(gen_tcp:option()),
    remote_opts => list(gen_tcp:option())
}.

-spec start_link(pid(), inet:socket(), module(), opts()) ->
    {ok, pid()}.

start_link(ListenerPid, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Opts]),
    {ok, Pid}.

%%

-record(state, {
    socket :: inet:socket(),
    socket_opts = ?DEFAULT_SOCKET_OPTS :: list(gen_tcp:option()),
    transport :: module(),
    proxy :: proxy_fun(),
    buffer = <<>> :: binary(),
    remote_endpoint :: any(),
    remote_socket :: inet:socket() | undefined,
    remote_transport :: module(),
    remote_socket_opts = ?DEFAULT_SOCKET_OPTS :: list(gen_tcp:option()),
    timeout :: non_neg_integer()
}).

-type st() :: #state{}.

-spec init(pid(), inet:socket(), module(), opts()) ->
    no_return().

init(ListenerPid, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(ListenerPid),
    ProxyFun = maps:get(proxy, Opts),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    SOpts = maps:get(source_opts, Opts, ?DEFAULT_SOCKET_OPTS),
    ROpts = maps:get(remote_opts, Opts, ?DEFAULT_SOCKET_OPTS),
    loop(#state{
        socket = Socket,
        transport = Transport,
        proxy = ProxyFun,
        timeout = Timeout,
        socket_opts = SOpts,
        remote_socket_opts = ROpts
    }).

%% ----------------------------------------------------------
%% Proxy internals
%% ----------------------------------------------------------

-spec loop(st()) -> no_return().
loop(
    #state{
        socket = Socket,
        transport = Transport,
        proxy = ProxyFun,
        buffer = Buffer,
        timeout = Timeout
    } = State
) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            Buffer1 = <<Buffer/binary, Data/binary>>,
            case ProxyFun(Buffer1) of
                stop ->
                    terminate(State);
                ignore ->
                    loop(State);
                {buffer, NewData} ->
                    loop(State#state{buffer = NewData});
                {remote, Remote} ->
                    start_proxy_loop(State#state{
                        buffer = Buffer1,
                        remote_endpoint = Remote
                    })
            end;
        _ ->
            terminate(State)
    end.

-spec start_proxy_loop(st()) -> no_return().
start_proxy_loop(#state{remote_endpoint = Remote, buffer = Buffer} = State) ->
    case remote_connect(Remote) of
        {Transport, {ok, Socket}} ->
            _ = Transport:send(Socket, Buffer),
            proxy_loop(State#state{
                remote_socket = Socket,
                remote_transport = Transport,
                buffer = <<>>
            });
        {_, {error, _Error}} ->
            terminate(State)
    end.

-spec proxy_loop(st()) -> no_return().
proxy_loop(
    #state{
        socket = SSock,
        transport = STrans,
        socket_opts = SOpts,
        remote_socket = RSock,
        remote_transport = RTrans,
        remote_socket_opts = ROpts
    } = State
) ->
    _ = STrans:setopts(SSock, SOpts),
    _ = RTrans:setopts(RSock, ROpts),

    receive
        {_, SSock, Data} ->
            _ = RTrans:send(RSock, Data),
            proxy_loop(State);
        {_, RSock, Data} ->
            _ = STrans:send(SSock, Data),
            proxy_loop(State);
        {tcp_closed, RSock} ->
            terminate(State);
        {tcp_closed, SSock} ->
            terminate_remote(State);
        _ ->
            terminate_all(State)
    end.

-spec remote_connect({inet:socket_address() | inet:hostname(), inet:port_number()}) ->
    {ranch_tcp, _ConnectResult}.
remote_connect({Ip, Port}) ->
    {ranch_tcp, gen_tcp:connect(Ip, Port, [binary, {packet, 0}, {delay_send, true}])}.

-spec terminate(st()) -> ok.
terminate(#state{socket = Socket, transport = Transport}) ->
    Transport:close(Socket).

-spec terminate_remote(st()) -> ok.
terminate_remote(#state{remote_socket = Socket, remote_transport = Transport}) ->
    Transport:close(Socket),
    ok.

-spec terminate_all(st()) -> ok.
terminate_all(State) ->
    terminate_remote(State),
    terminate(State).
