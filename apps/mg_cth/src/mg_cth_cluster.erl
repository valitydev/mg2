-module(mg_cth_cluster).

%% API
-export([
    prepare_cluster/1,
    instance_up/1,
    instance_down/1,
    await_peer/2,
    await_peer/1
]).

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

-define(DEFAULT_ATTEMPTS, 5).

-spec prepare_cluster(_) -> _.
prepare_cluster(HostsToUp) ->
    HostsTable = hosts_table(),
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

-spec await_peer(_) -> _.
await_peer(RemoteNode) ->
    await_peer(RemoteNode, ?DEFAULT_ATTEMPTS).

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

-spec hosts_table() -> _.
hosts_table() ->
    lists:foldl(
        fun(Host, Acc) ->
            {ok, Addr} = inet:getaddr(Host, inet),
            Acc#{unicode:characters_to_binary(Host) => unicode:characters_to_binary(inet:ntoa(Addr))}
        end,
        #{},
        ?ERLANG_TEST_HOSTS
    ).
