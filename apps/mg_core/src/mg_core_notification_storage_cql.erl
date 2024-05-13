-module(mg_core_notification_storage_cql).

-include_lib("cqerl/include/cqerl.hrl").

-behaviour(mg_core_notification_storage).
-export([put/6]).
-export([get/3]).
-export([delete/4]).
-export([search/5]).

-type ns() :: mg_core:ns().
-type id() :: mg_core:id().
-type ts() :: mg_core_notification_storage:ts().
-type data() :: mg_core_notification_storage:data().
-type context() :: mg_core_storage:context().

-type machine_ns() :: mg_core:ns().

-type search_query() :: mg_core_notification_storage:search_query().
-type search_limit() :: mg_core_machine_storage:search_limit().
-type search_page(T) :: mg_core_machine_storage:search_page(T).
-type continuation() :: mg_core_storage:continuation().

%% Bootstrapping
-export([bootstrap/2]).
-export([teardown/2]).

-type options() :: #{
    % Base
    name := mg_core_events_storage:name(),
    pulse := mg_core_pulse:handler(),
    % Network
    node := {inet:ip_address() | string(), Port :: integer()},
    ssl => [ssl:tls_client_option()],
    % Data
    keyspace := atom(),
    consistency => #{
        read => consistency_level(),
        write => consistency_level()
    }
}.

-type column() :: atom().
-type record() :: #{column() => mg_core_storage_cql:cql_datum()}.

%%

-define(COLUMNS, [
    machine_id
    | ?NOTIFICATION_COLUMNS
]).

-define(NOTIFICATION_COLUMNS, [
    notification_id,
    deliver_at,
    args,
    args_format_vsn
]).

%%

-spec put(options(), ns(), id(), data(), ts(), context()) -> context().
put(Options, NS, NotificationID, Data, Target, _Context) ->
    Query = mk_put_query(Options, NS, NotificationID, Data, Target),
    mg_core_storage_cql:execute_query(Options, Query, fun(void) -> [] end).

-spec get(options(), ns(), id()) ->
    {context(), data()} | undefined.
get(Options, NS, NotificationID) ->
    Query = mk_get_query(Options, NS, NotificationID),
    mg_core_storage_cql:execute_query(Options, Query, fun(Result) ->
        case cqerl:head(Result) of
            Record when is_list(Record) ->
                {[], read_notification(Options, maps:from_list(Record))};
            empty_dataset ->
                undefined
        end
    end).

-spec search(options(), ns(), search_query(), search_limit(), continuation() | undefined) ->
    search_page({ts(), id()}).
search(Options, NS, {FromTime, ToTime}, Limit, undefined) ->
    Query = mk_search_query(Options, NS, FromTime, ToTime, Limit),
    mg_core_storage_cql:execute_query(Options, Query, fun mk_search_page/1);
search(Options, _NS, {_FromTime, _ToTime}, _Limit, Continuation) ->
    mg_core_storage_cql:execute_continuation(Options, Continuation, fun mk_search_page/1).

-spec delete(options(), ns(), id(), context()) -> ok.
delete(Options, NS, NotificationID, _Context) ->
    Query = mk_delete_query(Options, NS, NotificationID),
    mg_core_storage_cql:execute_query(Options, Query, fun(void) -> ok end).

%%

-spec mk_put_query(options(), ns(), id(), data(), ts()) -> mg_core_storage_cql:cql_query().
mk_put_query(Options, NS, NotificationID, #{machine_id := MachineID, args := Args}, Target) ->
    Query = #{
        machine_id => MachineID,
        notification_id => NotificationID,
        deliver_at => mg_core_storage_cql:write_timestamp_s(Target),
        args => mg_core_storage_cql:write_opaque(Args),
        args_format_vsn => 1
    },
    mg_core_storage_cql:mk_query(
        Options,
        update,
        mk_statement([
            "UPDATE",
            mk_table_name(NS),
            "SET",
            mg_core_string_utils:join_with(
                [mg_core_string_utils:join(Column, "= ?") || Column <- maps:keys(Query)],
                ","
            ),
            "WHERE id = ?"
        ]),
        Query
    ).

-spec mk_get_query(options(), ns(), id()) -> mg_core_storage_cql:cql_query().
mk_get_query(Options, NS, NotificationID) ->
    mg_core_storage_cql:mk_query(
        Options,
        select,
        mk_statement([
            "SELECT",
            mg_core_string_utils:join_with(?COLUMNS, ","),
            "FROM",
            mk_table_name(NS),
            "WHERE id = ?"
        ]),
        [{id, NotificationID}]
    ).

-spec mk_delete_query(options(), ns(), id()) -> mg_core_storage_cql:cql_query().
mk_delete_query(Options, NS, NotificationID) ->
    mg_core_storage_cql:mk_query(
        Options,
        delete,
        mk_statement(["DELETE FROM", mk_table_name(NS), "WHERE id = ?"]),
        [{id, NotificationID}]
    ).

-spec mk_search_query(options(), ns(), ts(), ts(), search_limit()) -> mg_core_storage_cql:cql_query().
mk_search_query(Options, NS, FromTime, ToTime, Limit) ->
    Query = mg_core_storage_cql:mk_query(
        Options,
        select,
        mk_statement([
            "SELECT deliver_at, notification_id FROM",
            mk_table_name(NS),
            "WHERE deliver_at > :target_from AND deliver_at <= :target_to",
            "ALLOW FILTERING"
        ]),
        [
            {target_from, mg_core_storage_cql:write_timestamp_s(FromTime)},
            {target_to, mg_core_storage_cql:write_timestamp_s(ToTime)}
        ]
    ),
    Query#cql_query{page_size = Limit}.

-spec mk_search_page(mg_core_storage_cql:cql_result()) ->
    mg_core_machine_storage:search_page({ts(), id()}).
mk_search_page(Result) ->
    Page = accumulate_page(Result, []),
    case cqerl:has_more_pages(Result) of
        true -> {Page, Result};
        false -> {Page, undefined}
    end.

-spec accumulate_page(mg_core_storage_cql:cql_result(), [Result]) -> [Result].
accumulate_page(Result, Acc) ->
    case cqerl:head(Result) of
        [{deliver_at, Target}, {notification_id, NotificationID}] ->
            SR = {mg_core_storage_cql:read_timestamp_s(Target), NotificationID},
            accumulate_page(cqerl:tail(Result), [SR | Acc]);
        empty_dataset ->
            Acc
    end.

-spec read_notification(options(), record()) -> mg_core_notification_storage:data().
read_notification(_Options, #{args_format_vsn := 1, machine_id := MachineID, args := Args}) ->
    #{
        machine_id => MachineID,
        args => mg_core_storage_cql:read_opaque(Args)
    }.

%%

-spec bootstrap(options(), machine_ns()) -> ok.
bootstrap(Options, NS) ->
    TableName = mk_table_name(NS),
    ok = mg_core_storage_cql:execute_query(
        Options,
        mk_statement([
            mg_core_string_utils:join(["CREATE TABLE", TableName, "("]),
            % NOTE
            % Keep in sync with `?COLUMNS`.
            "machine_id TEXT,"
            "notification_id INT,"
            "deliver_at TIMESTAMP,"
            "args BLOB,"
            "args_format_vsn SMALLINT,"
            "PRIMARY KEY (notification_id)"
            ")"
        ])
    ),
    ok = mg_core_storage_cql:execute_query(
        Options,
        mk_statement([
            "CREATE INDEX",
            mg_core_string_utils:join_(TableName, "deliver_at"),
            "ON",
            TableName,
            "(deliver_at)"
        ])
    ).

-spec teardown(options(), machine_ns()) -> ok.
teardown(Options, NS) ->
    ok = mg_core_storage_cql:execute_query(
        Options,
        mk_statement([
            "DROP TABLE IF EXISTS", mk_table_name(NS)
        ])
    ).

-spec mk_table_name(machine_ns()) -> iodata().
mk_table_name(NS) ->
    mg_core_string_utils:join_(NS, "notifications").

-spec mk_statement(list()) -> binary().
mk_statement(List) ->
    erlang:iolist_to_binary(mg_core_string_utils:join(List)).
