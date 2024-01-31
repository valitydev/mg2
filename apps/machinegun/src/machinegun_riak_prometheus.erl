-module(machinegun_riak_prometheus).

%% persistence
-export([setup/0]).
-export([child_spec/3]).
-export([collect_storages/0]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

-type options() :: #{}.
-type storage() :: mg_core_storage_riak:options().

-export_type([storage/0]).

-define(PROPNAME, ?MODULE).

%%

%% Sets all metrics up. Call this when the app starts.
-spec setup() -> ok.
setup() ->
    % Utilization metrics collector
    prometheus_registry:register_collector(registry(), machinegun_riak_prometheus_collector).

%%

-spec child_spec(options(), mg_core_storage:options(), _ChildID) -> supervisor:child_spec().
child_spec(_Options, Storage, ChildID) ->
    #{
        id => ChildID,
        start => {supervisor, start_link, [?MODULE, Storage]},
        restart => permanent,
        type => worker
    }.

-spec init(mg_core_storage:options()) -> genlib_gen:supervisor_ret().
init(Storage) ->
    {mg_core_storage_riak, StorageOptions} = mg_core_utils:separate_mod_opts(Storage),
    true = gproc:add_local_property(?PROPNAME, StorageOptions),
    % NOTE
    % We only care about keeping gproc property live through this supervisor process.
    {ok, {#{}, []}}.

-spec collect_storages() -> [storage()].
collect_storages() ->
    [Storage || {_Pid, Storage} <- gproc:lookup_local_properties(?PROPNAME)].

%%

-spec registry() -> prometheus_registry:registry().
registry() ->
    default.
