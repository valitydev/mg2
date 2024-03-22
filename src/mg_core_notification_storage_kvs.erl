-module(mg_core_notification_storage_kvs).

-behaviour(mg_core_notification_storage).
-export([child_spec/2]).
-export([put/6]).
-export([get/3]).
-export([delete/4]).
-export([search/5]).

-type ns() :: mg_core:ns().
-type id() :: mg_core:id().
-type ts() :: mg_core_notification_storage:ts().
-type data() :: mg_core_notification_storage:data().
-type context() :: mg_core_storage:context().

-type search_query() :: mg_core_notification_storage:search_query().
-type search_limit() :: mg_core_machine_storage:search_limit().
-type search_page(T) :: mg_core_machine_storage:search_page(T).
-type continuation() :: mg_core_storage:continuation().

-type options() :: #{
    % Base
    name := mg_core_machine_storage:name(),
    pulse := mg_core_pulse:handler(),
    % KV Storage
    kvs := mg_core_storage:options()
}.

%%

-spec child_spec(options(), _ChildID) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_core_storage:child_spec(kvs_options(Options), ChildID).

-spec put(options(), ns(), id(), data(), ts(), context()) -> context().
put(Options, _NS, NotificationID, Data, Target, Context) ->
    mg_core_storage:put(
        kvs_options(Options),
        NotificationID,
        Context,
        data_to_opaque(Data),
        create_indexes(Target)
    ).

-spec get(options(), ns(), id()) ->
    {context(), data()} | undefined.
get(Options, _NS, NotificationID) ->
    case mg_core_storage:get(kvs_options(Options), NotificationID) of
        {Context, PackedNotification} ->
            {Context, opaque_to_data(PackedNotification)};
        undefined ->
            undefined
    end.

-spec search(options(), ns(), search_query(), search_limit(), continuation() | undefined) ->
    search_page({ts(), id()}).
search(Options, _NS, {FromTime, ToTime}, Limit, Continuation) ->
    mg_core_storage:search(
        kvs_options(Options),
        create_index_query(FromTime, ToTime, Limit, Continuation)
    ).

-spec delete(options(), ns(), id(), context()) ->
    ok.
delete(Options, _NS, NotificationID, Context) ->
    mg_core_storage:delete(
        kvs_options(Options),
        NotificationID,
        Context
    ).

%%

-define(TIMESTAMP_IDX, {integer, <<"timestamp">>}).

-spec create_indexes(ts()) -> [mg_core_storage:index_update()].
create_indexes(TNow) ->
    [{?TIMESTAMP_IDX, TNow}].

-spec create_index_query(From :: ts(), To :: ts(), search_limit(), continuation() | undefined) ->
    mg_core_storage:index_query().
create_index_query(FromTime, ToTime, Limit, Continuation) ->
    {?TIMESTAMP_IDX, {FromTime, ToTime}, Limit, Continuation}.

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

-spec kvs_options(options()) -> mg_core_storage:options().
kvs_options(#{name := Name, pulse := Handler, kvs := KVSOptions}) ->
    {Mod, Options} = mg_core_utils:separate_mod_opts(KVSOptions, #{}),
    {Mod, Options#{name => Name, pulse => Handler}}.
