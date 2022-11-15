-module(mg_core_notification_storage).

%% API

-export([child_spec/2]).

-export([put/6]).
-export([get/3]).
-export([delete/4]).
-export([search/4]).
-export([search/5]).

-export_type([ts/0]).
-export_type([data/0]).
-export_type([context/0]).
-export_type([options/0]).

-type name() :: term().
-type ns() :: mg_core:ns().
-type id() :: mg_core:notification_id().
-type ts() :: genlib_time:ts().
-type data() :: #{
    machine_id := mg_core:id(),
    args := mg_core_storage:opaque()
}.

-type options() :: mg_core_utils:mod_opts(storage_options()).
-type storage_options() :: #{
    name := name(),
    pulse := mg_core_pulse:handler(),
    atom() => _
}.

-type context() :: term().
-type search_query() :: {_From :: ts(), _To :: ts()}.
-type search_limit() :: mg_core_machine_storage:search_limit().
-type search_page(T) :: mg_core_machine_storage:search_page(T).
-type continuation() :: mg_core_machine_storage:continuation().

-callback child_spec(storage_options(), atom()) ->
    supervisor:child_spec() | undefined.

-callback put(storage_options(), ns(), id(), data(), ts(), context()) ->
    context().
-callback get(storage_options(), ns(), id()) ->
    {context(), data()} | undefined.
-callback delete(storage_options(), ns(), id(), context()) ->
    ok.
-callback search(storage_options(), ns(), search_query(), search_limit(), continuation() | undefined) ->
    search_page({ts(), id()}).

-optional_callbacks([child_spec/2]).

%%
%% API
%%

-spec child_spec(options(), term()) -> supervisor:child_spec() | undefined.
child_spec(Options, ChildID) ->
    mg_core_utils:apply_mod_opts_if_defined(Options, child_spec, undefined, [ChildID]).

-spec get(options(), ns(), id()) -> {context(), data()} | undefined.
get(Options, NS, ID) ->
    mg_core_utils:apply_mod_opts(Options, get, [NS, ID]).

-spec put(options(), ns(), id(), data(), ts(), context()) -> ok.
put(Options, NS, ID, Data, Target, Context) ->
    mg_core_utils:apply_mod_opts(Options, put, [NS, ID, Data, Target, Context]).

-spec delete(options(), ns(), id(), context()) -> ok.
delete(Options, NS, ID, Context) ->
    mg_core_utils:apply_mod_opts(Options, delete, [NS, ID, Context]).

search(Options, NS, Query, Limit) ->
    mg_core_utils:apply_mod_opts(Options, search, [NS, Query, Limit, undefined]).

search(Options, NS, Query, Limit, Continuation) ->
    mg_core_utils:apply_mod_opts(Options, search, [NS, Query, Limit, Continuation]).
