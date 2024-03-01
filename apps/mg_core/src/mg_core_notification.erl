-module(mg_core_notification).

%% API

-export([child_spec/2]).

-export([put/5]).
-export([get/2]).
-export([search/4]).
-export([delete/3]).

-type id() :: binary().
-type data() :: #{
    machine_id := mg_core:id(),
    args := mg_core_storage:opaque()
}.
-type context() :: mg_core_storage:context().
-type options() :: #{
    namespace := mg_core:ns(),
    pulse := mg_core_pulse:handler(),
    storage := storage_options(),
    scaling => mg_core_cluster:scaling_type()
}.

-export_type([id/0]).
-export_type([data/0]).
-export_type([context/0]).
-export_type([options/0]).

%% Internal types

% FIXME like mg_core_storage:options() except `name`
-type storage_options() :: mg_core_utils:mod_opts(map()).
-type ts() :: genlib_time:ts().

%%
%% API
%%

-spec child_spec(options(), _ChildID) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start =>
            {mg_core_utils_supervisor_wrapper, start_link, [
                #{strategy => rest_for_one},
                mg_core_utils:lists_compact([
                    mg_core_storage:child_spec(storage_options(Options), storage)
                ])
            ]},
        restart => permanent,
        type => supervisor
    }.

%%

-spec put(options(), id(), data(), ts(), context()) -> context().
put(Options, NotificationID, Data, Timestamp, Context) ->
    mg_core_storage:put(
        storage_options(Options),
        NotificationID,
        Context,
        data_to_opaque(Data),
        create_indexes(Timestamp)
    ).

-spec get(options(), id()) -> {ok, context(), data()} | {error, not_found}.
get(Options, NotificationID) ->
    case mg_core_storage:get(storage_options(Options), NotificationID) of
        undefined ->
            {error, not_found};
        {Context, PackedNotification} ->
            {ok, Context, opaque_to_data(PackedNotification)}
    end.

-spec search(
    options(),
    FromTime :: ts(),
    ToTime :: ts(),
    mg_core_storage:index_limit()
) ->
    mg_core_storage:search_result().
search(Options, FromTime, ToTime, Limit) ->
    mg_core_storage:search(
        storage_options(Options),
        create_index_query(FromTime, ToTime, Limit)
    ).

-spec delete(options(), id(), context()) -> ok.
delete(Options, NotificationID, Context) ->
    mg_core_storage:delete(
        storage_options(Options),
        NotificationID,
        Context
    ).

%%

-define(TIMESTAMP_IDX, {integer, <<"timestamp">>}).

-spec create_indexes(ts()) -> [mg_core_storage:index_update()].
create_indexes(TNow) ->
    [{?TIMESTAMP_IDX, TNow}].

-spec create_index_query(From :: ts(), To :: ts(), mg_core_storage:index_limit()) ->
    mg_core_storage:index_query().
create_index_query(FromTime, ToTime, Limit) ->
    {?TIMESTAMP_IDX, {FromTime, ToTime}, Limit}.

-spec opaque_to_data(mg_core_storage:opaque()) -> data().
opaque_to_data([1, MachineID, Args]) ->
    #{
        machine_id => MachineID,
        args => Args
    }.

-spec data_to_opaque(data()) -> mg_core_storage:opaque().
data_to_opaque(#{
    machine_id := MachineID,
    args := Args
}) ->
    [1, MachineID, Args].

%%

-spec storage_options(options()) -> mg_core_storage:options().
storage_options(#{namespace := NS, storage := StorageOptions, pulse := Handler} = Opts) ->
    Scaling = maps:get(scaling, Opts, global_based),
    {Mod, Options} = mg_core_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, ?MODULE, notifications}, pulse => Handler, scaling => Scaling}}.
