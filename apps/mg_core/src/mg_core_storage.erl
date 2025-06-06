%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

%%%
%%% Интерфейс работы с хранилищем данных.
%%% Он с виду выглядит абстрактным и не привязанным к конкретной базе,
%%% но по факту он копирует интерфейс риака.
%%% (Хотя положить на него можно и другие базы.)
%%%
%%% TODO:
%%%  - сделать работу с пачками через функтор и контекст
%%%
-module(mg_core_storage).
-include_lib("mg_core/include/pulse.hrl").

%% API
-export_type([name/0]).

-export_type([opaque/0]).
-export_type([key/0]).
-export_type([value/0]).
-export_type([kv/0]).
-export_type([context/0]).
-export_type([continuation/0]).

-export_type([index_name/0]).
-export_type([index_value/0]).
-export_type([index_update/0]).
-export_type([index_query_value/0]).
-export_type([index_limit/0]).
-export_type([index_query/0]).
-export_type([search_result/0]).

-export_type([storage_options/0]).
-export_type([options/0]).

-export_type([request/0]).
-export_type([response/0]).
-export_type([batch/0]).

-export([child_spec/2]).
-export([put/5]).
-export([get/2]).
-export([search/2]).
-export([delete/3]).

-export([new_batch/0]).
-export([add_batch_request/2]).
-export([run_batch/2]).

-export([do_request/2]).

%% Internal API
-export([start_link/1]).

-export([opaque_to_binary/1]).
-export([binary_to_opaque/1]).

-define(KEY_SIZE_LOWER_BOUND, 1).
-define(KEY_SIZE_UPPER_BOUND, 1024).

%%
%% API
%%
-type name() :: term().

-type opaque() :: mg_utils:opaque().
-type key() :: binary().
-type value() :: opaque().
-type kv() :: {key(), value()}.
-type context() :: term().
% undefined означает, что данные кончились
-type continuation() :: term().

%% типизация получилась отвратная, но лучше не вышло :-\
-type index_name() :: {binary | integer, binary()}.
-type index_value() :: binary() | integer().
-type index_update() :: {index_name(), index_value()}.
-type index_query_value() :: index_value() | {index_value(), index_value()}.
-type index_limit() :: non_neg_integer() | inf.
-type index_query() ::
    {index_name(), index_query_value()}
    | {index_name(), index_query_value(), index_limit()}
    | {index_name(), index_query_value(), index_limit(), continuation()}.

-type search_result() ::
    {[{index_value(), key()}], continuation()}
    | {[key()], continuation()}
    | [{index_value(), key()}]
    | [key()].

-type storage_options() :: #{
    name := name(),
    pulse := mpulse:handler(),
    sidecar => mg_utils:mod_opts(),
    batching => batching_options(),
    atom() => any()
}.
-type options() :: mg_utils:mod_opts(storage_options()).

-type batching_options() :: #{
    % How many storage requests may be served concurrently at most?
    % If unset concurrency is unlimited.
    concurrency_limit => pos_integer()
}.

%%

-type request() ::
    {put, key(), context() | undefined, value(), [index_update()]}
    | {get, key()}
    | {search, index_query()}
    | {delete, key(), context()}.

-opaque batch() :: [request()].

-type response() ::
    context()
    | {context(), value()}
    | undefined
    | search_result()
    | ok.

%% Timestamp and duration are in native units
-type duration() :: non_neg_integer().

-callback child_spec(storage_options(), atom()) -> supervisor:child_spec() | undefined.

-callback do_request(storage_options(), request()) -> response().

-optional_callbacks([child_spec/2]).

%%

-spec start_link(options()) -> mg_utils:gen_start_ret().
start_link(Options) ->
    genlib_adhoc_supervisor:start_link(
        #{strategy => rest_for_one},
        mg_utils:lists_compact([
            mg_utils:apply_mod_opts_if_defined(Options, child_spec, undefined, [storage]),
            sidecar_child_spec(Options, sidecar)
        ])
    ).

-spec child_spec(options(), term()) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options]},
        restart => permanent,
        type => supervisor
    }.

-spec put(options(), key(), context() | undefined, value(), [index_update()]) -> context().
put(Options, Key, Context, Value, Indexes) ->
    _ = validate_key(Key),
    do_request(Options, {put, Key, Context, Value, Indexes}).

-spec get(options(), key()) -> {context(), value()} | undefined.
get(Options, Key) ->
    _ = validate_key(Key),
    do_request(Options, {get, Key}).

-spec search(options(), index_query()) -> search_result().
search(Options, Query) ->
    do_request(Options, {search, Query}).

-spec delete(options(), key(), context()) -> ok.
delete(Options, Key, Context) ->
    _ = validate_key(Key),
    do_request(Options, {delete, Key, Context}).

-spec new_batch() -> batch().
new_batch() ->
    [].

-spec add_batch_request(request(), batch()) -> batch().
add_batch_request({get, Key} = Request, Batch) ->
    _ = validate_key(Key),
    [Request | Batch];
add_batch_request({put, Key, _Context, _Value, _Indices} = Request, Batch) ->
    _ = validate_key(Key),
    [Request | Batch];
add_batch_request({delete, Key, _Context} = Request, Batch) ->
    _ = validate_key(Key),
    [Request | Batch];
add_batch_request({search, _} = Request, Batch) ->
    [Request | Batch].

-spec run_batch(options(), batch()) -> [{request(), response()}].
run_batch(Options, Batch) ->
    {_Handler, StorageOptions} = mg_utils:separate_mod_opts(Options, #{}),
    genlib_pmap:map(
        fun(Request) ->
            {Request, do_request(Options, Request)}
        end,
        lists:reverse(Batch),
        construct_pmap_options(StorageOptions)
    ).

-spec construct_pmap_options(storage_options()) -> #{atom() => _}.
construct_pmap_options(Options) ->
    Batching = maps:get(batching, Options, #{}),
    maps:fold(
        fun(concurrency_limit, N, Opts) when is_integer(N), N > 0 ->
            Opts#{proc_limit => N}
        end,
        #{},
        Batching
    ).

-spec do_request(options(), request()) -> response().
do_request(Options, Request) ->
    {_Handler, StorageOptions} = mg_utils:separate_mod_opts(Options, #{}),
    StartTimestamp = erlang:monotonic_time(),
    ok = emit_beat_start(Request, StorageOptions),
    Result = mg_utils:apply_mod_opts(Options, do_request, [Request]),
    FinishTimestamp = erlang:monotonic_time(),
    Duration = FinishTimestamp - StartTimestamp,
    ok = emit_beat_finish(Request, StorageOptions, Duration),
    Result.

-spec validate_key(key()) -> _ | no_return().
validate_key(Key) when byte_size(Key) < ?KEY_SIZE_LOWER_BOUND ->
    throw({logic, {invalid_key, {too_small, Key}}});
validate_key(Key) when byte_size(Key) > ?KEY_SIZE_UPPER_BOUND ->
    throw({logic, {invalid_key, {too_big, Key}}});
validate_key(_Key) ->
    ok.

%%
%% Internal API
%%
-define(MSGPACK_OPTIONS, [
    {spec, new},
    {allow_atom, none},
    {unpack_str, as_tagged_list},
    {validate_string, false},
    {pack_str, from_tagged_list},
    {map_format, map}
]).

-spec opaque_to_binary(opaque()) -> binary().
opaque_to_binary(Opaque) ->
    case msgpack:pack(Opaque, ?MSGPACK_OPTIONS) of
        Data when is_binary(Data) ->
            Data;
        {error, Reason} ->
            erlang:error(msgpack_pack_error, [Opaque, Reason])
    end.

-spec binary_to_opaque(binary()) -> opaque().
binary_to_opaque(Binary) ->
    case msgpack:unpack(Binary, ?MSGPACK_OPTIONS) of
        {ok, Data} ->
            Data;
        {error, Reason} ->
            erlang:error(msgpack_unpack_error, [Binary, Reason])
    end.

%% Internals

-spec sidecar_child_spec(options(), term()) -> supervisor:child_spec() | undefined.
sidecar_child_spec(Options, ChildID) ->
    {_Handler, StorageOptions} = mg_utils:separate_mod_opts(Options, #{}),
    case maps:find(sidecar, StorageOptions) of
        {ok, Sidecar} ->
            mg_utils:apply_mod_opts(Sidecar, child_spec, [Options, ChildID]);
        error ->
            undefined
    end.

%%
%% logging
%%

-spec emit_beat_start(mg_core_storage:request(), storage_options()) -> ok.
emit_beat_start({get, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_get_start{
        name = Name
    });
emit_beat_start({put, _, _, _, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_put_start{
        name = Name
    });
emit_beat_start({search, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_search_start{
        name = Name
    });
emit_beat_start({delete, _, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_delete_start{
        name = Name
    }).

-spec emit_beat_finish(mg_core_storage:request(), storage_options(), duration()) -> ok.
emit_beat_finish({get, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_get_finish{
        name = Name,
        duration = Duration
    });
emit_beat_finish({put, _, _, _, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_put_finish{
        name = Name,
        duration = Duration
    });
emit_beat_finish({search, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_search_finish{
        name = Name,
        duration = Duration
    });
emit_beat_finish({delete, _, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_core_storage_delete_finish{
        name = Name,
        duration = Duration
    }).
