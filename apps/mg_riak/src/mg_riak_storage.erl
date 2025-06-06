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
%%% Riak хранилище для machinegun'а.
%%%
%%% Важный момент, что единовременно не может существовать 2-х процессов записи в БД по одной
%%% машине, это гарантируется самим MG (а точнее mg_core_workers).
%%%
%%%  Всё энкодится в msgpack и версионируется в метадате.
%%%
%%%
%%% Требования:
%%%  - Данные риака лежат локально желательно на SSD
%%%  - N >= 3, при этом мы сможем безболезненно терять minority
%%%    от N машины из кластера размером S (R=W=PR=PW=DW=quorum).
%%%
%%% Для append only бакетов (эвентов) можно подумать о чтении с R=PR=1 и notfound_ok=false
%%%
%%% Ошибка {error, timeout} — это неопределённость и нужно понять, что с этим делать!
%%%  (Мы предполагаем, что тот факт, что мы получили неопределённость от одной из нод
%%%    транслируется в неопределённость на один запрос)
%%% Нужно делать все записи в базу идемпотентными и при любой ошибке неопределённости или
%%% недоступности ретраить.
%%%
%%% Вопросы:
%%%  - Равен ли размера cp кластера MG размеру кластера riak? (нет, это совсем разные кластеры)
%%%  - Что делать с Riak и cross-dc? (пока не думаем)
%%%  - Можно ли при отсутствии after эвента возвращать []? (обсудили — нет)
%%%  - Верна ли гипотеза, что при записи в один поток с R=W=PR=PW=quorum не будет slibing'ов?
%%%  -
%%%
%%% TODO:
%%%  - классификация и обработка ошибок
%%%
-module(mg_riak_storage).
-include_lib("riakc/include/riakc.hrl").
-include_lib("mg_riak/include/pulse.hrl").

%% mg_core_storage callbacks
-behaviour(mg_core_storage).
-export_type([options/0]).
-export([child_spec/2, do_request/2]).

%% API
-export([pool_utilization/1]).

%% internal
-export([start_client/1]).
-export([start_link/1]).

%% pooler callbacks
-export([update_or_create/4]).

% from riakc
% -type bucket() :: binary().
-type options() :: #{
    name := mg_core_storage:name(),
    host := inet:ip_address() | inet:hostname() | binary(),
    port := inet:port_number(),
    bucket := bucket(),
    pool_options := pool_options(),
    pulse := mpulse:handler(),
    resolve_timeout => timeout(),
    connect_timeout => timeout(),
    request_timeout => timeout(),
    index_query_timeout => timeout(),
    r_options => _,
    w_options => _,
    d_options => _
}.

-type context() :: riakc_obj:vclock() | undefined.

-type index_opt() ::
    {timeout, timeout()}
    | {call_timeout, timeout()}
    | {stream, boolean()}
    | {continuation, binary()}
    | {pagination_sort, boolean()}
    | {max_results, non_neg_integer() | all}.
-type range_index_opt() ::
    {return_terms, boolean()}
    | {term_regex, binary()}.
-type range_index_opts() :: [index_opt() | range_index_opt()].
-type client_ref() :: mg_utils:gen_ref().

%% See https://github.com/seth/pooler/blob/master/src/pooler_config.erl for pool option details
-type pool_options() :: #{
    init_count := non_neg_integer(),
    max_count := non_neg_integer(),
    idle_timeout => timeout(),
    cull_interval => timeout(),
    auto_grow_threshold => non_neg_integer(),
    queue_max => non_neg_integer()
}.

-type pool_name() :: atom().
-type pulse_options() :: #{
    name := mg_core_storage:name(),
    pulse := mpulse:handler()
}.

%% Duration is measured in native units
-type duration() :: non_neg_integer().

%% TODO: Replace by deadline
-define(TAKE_CLIENT_TIMEOUT, 30000).

-define(GPROC_POOL_NAME(Name), {n, l, {?MODULE, pool, Name}}).
-define(GPROC_PULSE_OPTIONS(PoolNameString), {n, l, {?MODULE, pulse, PoolNameString}}).

%%
%% API
%%

-spec pool_utilization(options()) ->
    {ok, [{MetricName :: atom(), Value :: integer()}]}
    | {error, {transient, timeout | killed | noproc}}.
pool_utilization(Options) ->
    try
        {ok, pooler:pool_utilization(pool_name(Options))}
    catch
        exit:{Reason, _MFA} when
            Reason =:= timeout orelse
                Reason =:= killed orelse
                Reason =:= noproc
        ->
            {error, {transient, Reason}}
    end.

%%
%% internal API
%%

-spec start_client(options()) -> mg_utils:gen_start_ret().
start_client(#{port := Port} = Options) ->
    IP = get_riak_addr(Options),
    riakc_pb_socket:start_link(IP, Port, [{connect_timeout, get_option(connect_timeout, Options)}]).

-spec start_link(options()) -> mg_utils:gen_start_ret().
start_link(Options) ->
    PoolName = construct_pool_name(Options),
    PoolConfig = pooler_config:list_to_pool(make_pool_config(PoolName, Options)),
    {ok, SupPid} = pooler_pool_sup:start_link(PoolConfig),
    ok = register_pool(Options, SupPid, PoolName),
    {ok, SupPid}.

%%
%% mg_core_storage callbacks
%%

-spec child_spec(options(), atom()) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options]},
        restart => permanent,
        type => supervisor
    }.

-spec do_request(options(), mg_core_storage:request()) -> mg_core_storage:response().
do_request(Options, Request) ->
    PoolName = pool_name(Options),
    ClientRef = take_client(PoolName),
    try
        StartTimestamp = erlang:monotonic_time(),
        ok = emit_beat_start(Request, Options),
        Result = try_do_request(Options, ClientRef, Request),
        Duration = erlang:monotonic_time() - StartTimestamp,
        ok = emit_beat_finish(Request, Options, Duration),
        ok = return_client(PoolName, ClientRef, ok),
        Result
    catch
        Class:Error:StackTrace ->
            ok = return_client(PoolName, ClientRef, fail),
            erlang:raise(Class, Error, StackTrace)
    end.

%%
%% local
%%
%% MG-123
%% При отваливании сокета процесс riakc_pb_socket падает c reason `disconnected`.
%% По его мнению это нестандартная ситуация (и нужно именно падать, а не выходить с normal),
%% но мы думаем иначе.
%%
-define(SAFE(Expr),
    try
        Expr
    catch
        exit:{disconnected, _} ->
            {error, disconnected};
        % в риак клиенте все ошибки при подключении будут выглядеть так
        % https://github.com/rbkmoney/riak-erlang-client/blob/edba3d0f/src/riakc_pb_socket.erl#L1378
        exit:{tcp, _} ->
            {error, disconnected}
    end
).

-spec try_do_request(options(), client_ref(), mg_core_storage:request()) ->
    mg_core_storage:response().
try_do_request(Options, ClientRef, Request) ->
    case Request of
        {put, Key, Context, Value, IndexesUpdates} ->
            put(Options, ClientRef, Key, Context, Value, IndexesUpdates);
        {get, Key} ->
            get(Options, ClientRef, Key);
        {search, Query} ->
            search(Options, ClientRef, Query);
        {delete, Key, Context} ->
            delete(Options, ClientRef, Key, Context)
    end.

-spec put(
    options(),
    client_ref(),
    mg_core_storage:key(),
    context(),
    mg_core_storage:value(),
    IndexesUpdates
) -> context() when
    IndexesUpdates :: [mg_core_storage:index_update()].
put(#{bucket := Bucket} = Options, ClientRef, Key, Context, Value, IndexesUpdates) ->
    Object = to_riak_obj(Bucket, Key, Context, Value, IndexesUpdates),
    Timeout = get_option(request_timeout, Options),
    NewObject =
        handle_riak_response(
            ?SAFE(
                riakc_pb_socket:put(
                    ClientRef,
                    Object,
                    [return_body] ++ get_option(w_options, Options),
                    Timeout
                )
            )
        ),
    riakc_obj:vclock(NewObject).

-spec get(options(), client_ref(), mg_core_storage:key()) ->
    {context(), mg_core_storage:value()} | undefined.
get(#{bucket := Bucket} = Options, ClientRef, Key) ->
    Timeout = get_option(request_timeout, Options),
    case ?SAFE(riakc_pb_socket:get(ClientRef, Bucket, Key, get_option(r_options, Options), Timeout)) of
        {error, notfound} ->
            undefined;
        Result ->
            Object = handle_riak_response_(Result),
            from_riak_obj(Object)
    end.

-spec search(options(), client_ref(), mg_core_storage:index_query()) ->
    mg_core_storage:search_result().
search(#{bucket := Bucket} = Options, ClientRef, Query) ->
    LiftedQuery = lift_query(Query),
    Result = handle_riak_response_(do_get_index(ClientRef, Bucket, LiftedQuery, Options)),
    get_index_response(LiftedQuery, Result).

-spec delete(options(), client_ref(), mg_core_storage:key(), context()) -> ok.
delete(#{bucket := Bucket} = Options, ClientRef, Key, Context) ->
    case
        ?SAFE(
            riakc_pb_socket:delete_vclock(
                ClientRef,
                Bucket,
                Key,
                Context,
                get_option(d_options, Options)
            )
        )
    of
        ok ->
            ok;
        {error, Reason} ->
            erlang:throw({transient, {storage_unavailable, Reason}})
    end.

%%

-spec do_get_index(client_ref(), bucket(), mg_core_storage:index_query(), options()) -> _.
do_get_index(ClientRef, Bucket, {IndexName, {From, To}, IndexLimit, Continuation}, Options) ->
    SearchOptions = index_opts([{return_terms, true}], Options, IndexLimit, Continuation),
    ?SAFE(
        riakc_pb_socket:get_index_range(
            ClientRef,
            Bucket,
            prepare_index_name(IndexName),
            From,
            To,
            SearchOptions
        )
    );
do_get_index(ClientRef, Bucket, {IndexName, Value, IndexLimit, Continuation}, Options) ->
    SearchOptions = index_opts(Options, IndexLimit, Continuation),
    ?SAFE(
        riakc_pb_socket:get_index_eq(
            ClientRef,
            Bucket,
            prepare_index_name(IndexName),
            Value,
            SearchOptions
        )
    ).

-spec get_index_response(mg_core_storage:index_query(), get_index_results()) ->
    mg_core_storage:search_result().
get_index_response({_, Val, Limit, _}, #index_results_v1{keys = [], continuation = Cont}) when
    is_tuple(Val)
->
    % это какой-то пипец, а не код, они там все упоролись что-ли?
    wrap_index_response([], Limit, Cont);
get_index_response({_, Val, Limit, _}, #index_results_v1{terms = Terms, continuation = Cont}) when
    is_tuple(Val)
->
    Res = lists:map(
        fun({IndexValue, Key}) ->
            {erlang:binary_to_integer(IndexValue), Key}
        end,
        Terms
    ),
    wrap_index_response(Res, Limit, Cont);
get_index_response({_, _, Limit, _}, #index_results_v1{keys = Keys, continuation = Cont}) ->
    wrap_index_response(Keys, Limit, Cont).

-spec wrap_index_response(_, mg_core_storage:index_limit(), continuation()) ->
    mg_core_storage:search_result().
wrap_index_response(Res, Limit, Cont) ->
    case Limit of
        inf -> Res;
        _ -> {Res, Cont}
    end.

-spec lift_query(mg_core_storage:index_query()) -> mg_core_storage:index_query().
lift_query({Name, Val}) ->
    {Name, Val, inf, undefined};
lift_query({Name, Val, Limit}) ->
    {Name, Val, Limit, undefined};
lift_query({Name, Val, Limit, Continuation}) ->
    {Name, Val, Limit, Continuation}.

-spec index_opts(options(), mg_core_storage:index_limit(), continuation()) -> range_index_opts().
index_opts(Options, IndexLimit, Continuation) ->
    index_opts([], Options, IndexLimit, Continuation).

-spec index_opts(range_index_opts(), options(), mg_core_storage:index_limit(), continuation()) ->
    range_index_opts().
index_opts(DefaultOpts, Options, IndexLimit, Continuation) ->
    lists:append([
        common_index_opts(Options),
        max_result_opts(IndexLimit),
        continuation_opts(Continuation),
        DefaultOpts
    ]).

-spec continuation_opts(continuation()) -> range_index_opts().
continuation_opts(Continuation) ->
    case Continuation of
        undefined -> [];
        _ -> [{continuation, Continuation}]
    end.

-spec max_result_opts(mg_core_storage:index_limit()) -> range_index_opts().
max_result_opts(IndexLimit) ->
    case IndexLimit of
        inf -> [];
        _ -> [{max_results, IndexLimit}]
    end.

-spec common_index_opts(options()) -> range_index_opts().
common_index_opts(Options) ->
    [{pagination_sort, true}, {timeout, get_option(index_query_timeout, Options)}].

%%
%% packer
%%
%% фи-фи подтекает абстракция вызова mg_core_storage:opaque_to_binary(Value)
-define(MSGPACK_CT, "application/x-msgpack").
-define(SCHEMA_VERSION_MD_KEY, <<"schema-version">>).
-define(SCHEMA_VERSION_1, <<"1">>).

-spec to_riak_obj(
    bucket(),
    mg_core_storage:key(),
    context(),
    mg_core_storage:value(),
    IndexesUpdates
) -> riakc_obj:riakc_obj() when
    IndexesUpdates :: [mg_core_storage:index_update()].
to_riak_obj(Bucket, Key, Context, Value, IndexesUpdates) ->
    Object = riakc_obj:set_vclock(new_riak_object(Bucket, Key, Value), Context),
    riakc_obj:update_content_type(
        riakc_obj:update_metadata(
            Object,
            riakc_obj:set_user_metadata_entry(
                riakc_obj:set_secondary_index(
                    riakc_obj:get_metadata(Object),
                    prepare_indexes_updates(IndexesUpdates)
                ),
                {?SCHEMA_VERSION_MD_KEY, ?SCHEMA_VERSION_1}
            )
        ),
        ?MSGPACK_CT
    ).

-spec new_riak_object(bucket(), mg_core_storage:key(), mg_core_storage:value()) ->
    riakc_obj:riakc_obj().
new_riak_object(Bucket, Key, Value) ->
    case riakc_obj:new(Bucket, Key, mg_core_storage:opaque_to_binary(Value)) of
        {error, Reason} ->
            exit({storage_unexpected_error, Reason});
        Obj ->
            Obj
    end.

-spec from_riak_obj(riakc_obj:riakc_obj()) -> {context(), mg_core_storage:value()}.
from_riak_obj(Object) ->
    Metadata = riakc_obj:get_metadata(Object),
    ?SCHEMA_VERSION_1 = riakc_obj:get_user_metadata_entry(Metadata, ?SCHEMA_VERSION_MD_KEY),
    ?MSGPACK_CT = riakc_obj:get_content_type(Object),
    {riakc_obj:vclock(Object), mg_core_storage:binary_to_opaque(riakc_obj:get_value(Object))}.

-type riak_index_name() :: {integer_index, list()}.
-type riak_index_update() :: {riak_index_name(), [mg_core_storage:index_value()]}.
-type get_index_results() :: #index_results_v1{}.

-spec prepare_indexes_updates([mg_core_storage:index_update()]) -> [riak_index_update()].
prepare_indexes_updates(IndexesUpdates) ->
    [prepare_index_update(IndexUpdate) || IndexUpdate <- IndexesUpdates].

-spec prepare_index_update(mg_core_storage:index_update()) -> riak_index_update().
prepare_index_update({IndexName, IndexValue}) ->
    {prepare_index_name(IndexName), [IndexValue]}.

-spec prepare_index_name(mg_core_storage:index_name()) -> riak_index_name().
prepare_index_name({binary, Name}) ->
    {binary_index, erlang:binary_to_list(Name)};
prepare_index_name({integer, Name}) ->
    {integer_index, erlang:binary_to_list(Name)}.

%%

-spec get_option(atom(), options()) -> _.
get_option(Key, Options) ->
    maps:get(Key, Options, default_option(Key)).

-spec handle_riak_response(ok | {ok, T} | {error, _Reason}) -> T | no_return().
handle_riak_response(ok) ->
    ok;
handle_riak_response(V) ->
    handle_riak_response_(V).

-spec handle_riak_response_({ok, T} | {error, _Reason}) -> T | no_return().
handle_riak_response_({ok, Value}) ->
    Value;
handle_riak_response_({error, Reason}) ->
    % TODO понять какие проблемы временные, а какие постоянные
    erlang:throw({transient, {storage_unavailable, Reason}}).

%%
%% Про опции посмотреть можно тут
%% https://github.com/basho/riak-erlang-client/blob/develop/src/riakc_pb_socket.erl#L1526
%% Почитать про NRW и прочую магию можно тут:
%% http://basho.com/posts/technical/riaks-config-behaviors-part-2/
%%
-spec default_option(atom()) -> _.
default_option(resolve_timeout) -> 5000;
default_option(connect_timeout) -> 5000;
default_option(request_timeout) -> 10000;
default_option(index_query_timeout) -> 10000;
default_option(r_options) -> [{r, quorum}, {pr, quorum}, {sloppy_quorum, false}];
default_option(w_options) -> [{w, quorum}, {pw, quorum}, {dw, quorum}, {sloppy_quorum, false}];
% ?
default_option(d_options) -> [].

-spec get_riak_addr(options()) -> inet:ip_address().
get_riak_addr(#{host := Host} = Options) ->
    lists_random(get_addrs_by_host(Host, get_option(resolve_timeout, Options))).

-spec lists_random(list(T)) -> T.
lists_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

-spec get_addrs_by_host(inet:ip_address() | inet:hostname() | binary(), timeout()) ->
    [inet:ip_address()].
get_addrs_by_host(Host, Timeout) when is_binary(Host) ->
    get_addrs_by_host(unicode:characters_to_list(Host, utf8), Timeout);
get_addrs_by_host(Host, Timeout) ->
    case inet_parse:address(Host) of
        {ok, Addr} ->
            [Addr];
        {error, _} ->
            Timer = inet:start_timer(Timeout),
            R = erlang:apply(inet_db:tcp_module(), getaddrs, [Host, Timer]),
            _ = inet:stop_timer(Timer),
            case R of
                {ok, Addrs} ->
                    Addrs;
                {error, _} ->
                    exit({'invalid host address', Host})
            end
    end.

%% pool helpers

-spec make_pool_config(pool_name(), options()) -> [{atom(), term()}].
make_pool_config(Name, Options) ->
    PoolOptions = maps:get(pool_options, Options),
    StartTimeout = get_option(connect_timeout, Options) + get_option(resolve_timeout, Options),
    DefaultConfig = [
        {name, Name},
        {start_mfa, {?MODULE, start_client, [Options]}},
        {stop_mfa, {riakc_pb_socket, stop, ['$pooler_pid']}},
        {member_start_timeout, {StartTimeout, ms}},
        {metrics_mod, ?MODULE},
        {metrics_api, exometer}
    ],
    Config = maps:fold(
        fun
            (init_count, V, Acc) when is_integer(V) ->
                [{init_count, V} | Acc];
            (max_count, V, Acc) when is_integer(V) ->
                [{max_count, V} | Acc];
            (idle_timeout, V, Acc) when is_integer(V) ->
                [{max_age, {V, ms}} | Acc];
            (cull_interval, V, Acc) when is_integer(V) ->
                [{cull_interval, {V, ms}} | Acc];
            (auto_grow_threshold, V, Acc) when is_integer(V) ->
                [{auto_grow_threshold, V} | Acc];
            (queue_max, V, Acc) when is_integer(V) ->
                [{queue_max, V} | Acc]
        end,
        [],
        PoolOptions
    ),
    DefaultConfig ++ Config.

-spec take_client(pool_name()) -> client_ref().
take_client(PoolName) ->
    Timeout = ?TAKE_CLIENT_TIMEOUT,
    case pooler:take_member(PoolName, {Timeout, ms}) of
        Ref when is_pid(Ref) ->
            Ref;
        error_no_members ->
            erlang:throw({transient, {storage_unavailable, no_pool_members}})
    end.

-spec return_client(pool_name(), client_ref(), ok | fail) -> ok.
return_client(PoolName, ClientRef, Status) ->
    pooler:return_member(PoolName, ClientRef, Status).

-spec construct_pool_name(options()) -> atom().
construct_pool_name(#{name := Name}) ->
    term_to_atom(Name).

-spec term_to_atom(term()) -> atom().
term_to_atom(Term) ->
    erlang:binary_to_atom(base64:encode(erlang:term_to_binary(Term)), latin1).

-spec register_pool(options(), pid(), pool_name()) -> ok.
register_pool(#{name := Name} = Options, Pid, PoolName) ->
    PulseOptions = maps:with([name, pulse], Options),
    true = gproc:reg_other(?GPROC_POOL_NAME(Name), Pid, PoolName),
    true = gproc:reg_other(?GPROC_PULSE_OPTIONS(genlib:to_binary(PoolName)), Pid, PulseOptions),
    ok.

-spec pool_name(options()) -> pool_name().
pool_name(#{name := Name}) ->
    gproc:lookup_value(?GPROC_POOL_NAME(Name)).

-spec pulse_options(binary()) -> pulse_options().
pulse_options(PoolNameString) ->
    gproc:lookup_value(?GPROC_PULSE_OPTIONS(PoolNameString)).

%%
%% pool events
%%

-type pooler_metric_type() :: counter | meter | histogram.

-spec update_or_create([binary()], number(), pooler_metric_type(), []) -> ok.
update_or_create([<<"pooler">>, PoolNameString, <<"error_no_members_count">>], _, _Counter, []) ->
    #{name := Name, pulse := Handler} = pulse_options(PoolNameString),
    mpulse:handle_beat(
        Handler,
        #mg_riak_connection_pool_state_reached{
            name = Name,
            state = no_free_connections
        }
    );
update_or_create([<<"pooler">>, PoolNameString, <<"queue_max_reached">>], _, _Counter, []) ->
    #{name := Name, pulse := Handler} = pulse_options(PoolNameString),
    mpulse:handle_beat(
        Handler,
        #mg_riak_connection_pool_state_reached{
            name = Name,
            state = queue_limit_reached
        }
    );
update_or_create([<<"pooler">>, PoolNameString, <<"starting_member_timeout">>], _, _Counter, []) ->
    #{name := Name, pulse := Handler} = pulse_options(PoolNameString),
    mpulse:handle_beat(
        Handler,
        #mg_riak_connection_pool_error{
            name = Name,
            reason = connect_timeout
        }
    );
update_or_create([<<"pooler">>, PoolNameString, <<"killed_free_count">>], _, _Counter, []) ->
    #{name := Name, pulse := Handler} = pulse_options(PoolNameString),
    mpulse:handle_beat(
        Handler,
        #mg_riak_connection_pool_connection_killed{
            name = Name,
            state = free
        }
    );
update_or_create([<<"pooler">>, PoolNameString, <<"killed_in_use_count">>], _, _Counter, []) ->
    #{name := Name, pulse := Handler} = pulse_options(PoolNameString),
    mpulse:handle_beat(
        Handler,
        #mg_riak_connection_pool_connection_killed{
            name = Name,
            state = in_use
        }
    );
update_or_create(_MetricKey, _Value, _Type, []) ->
    ok.

%%
%% logging
%%

-spec emit_beat_start(mg_core_storage:request(), options()) -> ok.
emit_beat_start({get, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_get_start{
        name = Name
    });
emit_beat_start({put, _, _, _, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_put_start{
        name = Name
    });
emit_beat_start({search, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_search_start{
        name = Name
    });
emit_beat_start({delete, _, _}, #{pulse := Handler, name := Name}) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_delete_start{
        name = Name
    }).

-spec emit_beat_finish(mg_core_storage:request(), options(), duration()) -> ok.
emit_beat_finish({get, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_get_finish{
        name = Name,
        duration = Duration
    });
emit_beat_finish({put, _, _, _, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_put_finish{
        name = Name,
        duration = Duration
    });
emit_beat_finish({search, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_search_finish{
        name = Name,
        duration = Duration
    });
emit_beat_finish({delete, _, _}, #{pulse := Handler, name := Name}, Duration) ->
    ok = mpulse:handle_beat(Handler, #mg_riak_client_delete_finish{
        name = Name,
        duration = Duration
    }).
