%%%
%%% Copyright 2020 RBKmoney
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

-module(mg_pulse_log).

-include_lib("mg_core/include/pulse.hrl").
-include_lib("mg_woody/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-export([handle_beat/2]).

%% internal types
-type meta() :: mg_log:meta().
-type beat() :: mg_pulse:beat().
-type log_msg() :: mg_log:log_msg().
-type options() :: woody_event_handler:options().

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.
handle_beat(Options, Beat) ->
    ok = mg_log:log(format_beat(Beat, Options)).

%% Internals

-define(BEAT_TO_META(RecordName, Record), [
    {mg_pulse_event_id, RecordName}
    | lists:foldl(fun add_meta/2, [], [
        extract_meta(FieldName, Value)
     || {FieldName, Value} <- lists:zip(
            record_info(fields, RecordName),
            erlang:tl(erlang:tuple_to_list(Record))
        )
    ])
]).

-spec format_beat(beat(), options()) -> log_msg() | undefined.
format_beat(#woody_request_handle_error{exception = {_, Reason, _}} = Beat, _WoodyOptions) ->
    Context = ?BEAT_TO_META(woody_request_handle_error, Beat),
    LogLevel =
        case Reason of
            {logic, _Details} ->
                % бизнес ошибки это не warning
                info;
            _OtherReason ->
                warning
        end,
    {LogLevel, {"request handling failed ~p", [Reason]}, Context};
format_beat(#woody_event{event = Event, rpc_id = RPCID, event_meta = EventMeta}, WoodyOptions) ->
    Level = woody_event_handler:get_event_severity(Event, EventMeta),
    Msg = woody_event_handler:format_event(Event, EventMeta, RPCID, WoodyOptions),
    WoodyMetaFields = [event, service, function, type, metadata, url, deadline, role, execution_duration_ms],
    WoodyMeta = woody_event_handler:format_meta(Event, EventMeta, WoodyMetaFields),
    Meta = lists:flatten([extract_woody_meta(WoodyMeta), extract_meta(rpc_id, RPCID)]),
    {Level, Msg, Meta};
format_beat(#mg_core_scheduler_task_error{scheduler_name = Name, exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_scheduler_task_error, Beat),
    {warning, {"scheduler task ~p failed ~p", [Name, Reason]}, Context};
format_beat(#mg_core_scheduler_task_add_error{scheduler_name = Name, exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_scheduler_task_add_error, Beat),
    {warning, {"scheduler task ~p add failed ~p", [Name, Reason]}, Context};
format_beat(#mg_core_scheduler_search_error{scheduler_name = Name, exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_scheduler_search_error, Beat),
    {warning, {"scheduler search ~p failed ~p", [Name, Reason]}, Context};
format_beat(#mg_core_machine_process_transient_error{exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_machine_process_transient_error, Beat),
    {warning, {"transient error ~p", [Reason]}, Context};
format_beat(#mg_core_machine_lifecycle_failed{exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_machine_lifecycle_failed, Beat),
    {error, {"machine failed ~p", [Reason]}, Context};
format_beat(#mg_core_machine_lifecycle_loading_error{exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_machine_lifecycle_loading_error, Beat),
    {error, {"loading failed ~p", [Reason]}, Context};
format_beat(#mg_core_machine_lifecycle_committed_suicide{} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_machine_lifecycle_committed_suicide, Beat),
    {info, {"machine has committed suicide", []}, Context};
format_beat(#mg_core_machine_lifecycle_transient_error{context = Ctx, exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_machine_lifecycle_transient_error, Beat),
    case Beat#mg_core_machine_lifecycle_transient_error.retry_action of
        {wait, Timeout, _} ->
            {warning, {"transient error ~p during ~p, retrying in ~p msec", [Ctx, Reason, Timeout]}, Context};
        finish ->
            {warning, {"transient error ~p during ~p, retires exhausted", [Ctx, Reason]}, Context}
    end;
format_beat(#mg_core_machine_notification_delivery_error{exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_machine_notification_delivery_error, Beat),
    {warning, {"machine notification delivery failed ~p", [Reason]}, Context};
format_beat(#mg_core_timer_lifecycle_rescheduled{target_timestamp = TS, attempt = Attempt} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_timer_lifecycle_rescheduled, Beat),
    {info, {"machine rescheduled to ~s, attempt ~p", [format_timestamp(TS), Attempt]}, Context};
format_beat(#mg_core_timer_lifecycle_rescheduling_error{exception = {_, Reason, _}} = Beat, _Options) ->
    Context = ?BEAT_TO_META(mg_core_timer_lifecycle_rescheduling_error, Beat),
    {info, {"machine rescheduling failed ~p", [Reason]}, Context};
format_beat({squad, {Producer, Beat, Extra}}, _Options) ->
    case format_squad_beat(Beat) of
        {Level, Format, Context} ->
            MetaExtra = [extract_meta(Name, Value) || {Name, Value} <- Extra],
            Meta0 = lists:foldl(fun add_meta/2, Context, MetaExtra),
            Meta1 = add_meta({squad_producer, Producer}, Meta0),
            {Level, Format, Meta1};
        undefined ->
            undefined
    end;
format_beat(_Beat, _Options) ->
    undefined.

%% squad
-spec format_squad_beat(mg_core_gen_squad_pulse:beat()) -> log_msg() | undefined.
format_squad_beat({rank, {changed, Rank}}) ->
    {info, {"rank changed to: ~p", [Rank]}, [
        {mg_pulse_event_id, squad_rank_changed},
        {squad_rank, Rank}
    ]};
format_squad_beat({{member, Pid}, Status}) ->
    case Status of
        added ->
            {info, {"member ~p added", [Pid]}, add_event_id(squad_member_added, [])};
        {refreshed, Member} ->
            Meta = extract_meta(squad_member, Member),
            {debug, {"member ~p refreshed", [Pid]}, add_event_id(squad_member_added, Meta)};
        {removed, Member, Reason} ->
            Meta = extract_meta(squad_member, Member),
            {info, {"member ~p removed: ~p", [Pid, Reason]}, add_event_id(squad_member_removed, Meta)}
    end;
format_squad_beat({{broadcast, _}, _}) ->
    undefined;
format_squad_beat({{timer, TRef}, Status}) ->
    case Status of
        {started, Timeout, Msg} ->
            Meta = add_event_id(squad_timer_started, [{timeout, Timeout}]),
            {debug, {"timer ~p armed to fire ~p after ~p ms", [TRef, Msg, Timeout]}, Meta};
        cancelled ->
            {debug, {"timer ~p cancelled", [TRef]}, add_event_id(squad_timer_fired, [])};
        {fired, Msg} ->
            {debug, {"timer ~p fired ~p", [TRef, Msg]}, add_event_id(squad_timer_reset, [])}
    end;
format_squad_beat({{monitor, MRef}, Status}) ->
    case Status of
        {started, Pid} ->
            {debug, {"monitor ~p set on ~p", [MRef, Pid]}, add_event_id(squad_monitor_set, [])};
        cancelled ->
            {debug, {"monitor ~p cancelled", [MRef]}, add_event_id(squad_monitor_cancelled, [])};
        {fired, Pid, Reason} ->
            Meta = add_event_id(squad_monitor_fired, []),
            {debug, {"monitor ~p on ~p fired: ~p", [MRef, Pid, Reason]}, Meta}
    end;
format_squad_beat({unexpected, Unexpected = {Type, _}}) ->
    format_unexpected_beat(
        Unexpected,
        add_event_id(
            case Type of
                {call, _} -> squad_unexpected_call;
                cast -> squad_unexpected_cast;
                info -> squad_unexpected_info
            end,
            []
        )
    );
format_squad_beat(Beat) ->
    {warning, {"unknown or mishandled squad beat: ~p", [Beat]}, []}.

-spec format_unexpected_beat(Beat, meta()) -> log_msg() when Beat :: {{call, _From} | cast | info, _Message}.
format_unexpected_beat({Type, Message}, Meta) ->
    case Type of
        {call, From} ->
            {warning, {"received unexpected call from ~p: ~p", [From, Message]}, Meta};
        cast ->
            {warning, {"received unexpected cast: ~p", [Message]}, Meta};
        info ->
            {warning, {"received unexpected info: ~p", [Message]}, Meta}
    end.

-spec add_event_id(atom(), meta()) -> meta().
add_event_id(EventID, Meta) ->
    add_meta({mg_pulse_event_id, EventID}, Meta).

-spec add_meta(meta() | {atom(), any()}, meta()) -> meta().
add_meta(Meta, MetaAcc) when is_list(Meta), is_list(MetaAcc) ->
    Meta ++ MetaAcc;
add_meta(Meta, MetaAcc) when is_list(MetaAcc) ->
    [Meta | MetaAcc];
add_meta(Meta, MetaAcc) ->
    add_meta(Meta, [MetaAcc]).

-spec extract_meta(atom(), any()) -> [meta()] | meta().
extract_meta(_Name, undefined) ->
    [];
extract_meta(request_context, null) ->
    [];
extract_meta(request_context, ReqCtx) ->
    #{rpc_id := RPCID} = mg_woody_utils:opaque_to_woody_context(ReqCtx),
    extract_meta(rpc_id, RPCID);
extract_meta(rpc_id, RPCID) ->
    maps:to_list(RPCID);
extract_meta(deadline, Deadline) when is_integer(Deadline) ->
    {deadline, mg_core_deadline:format(Deadline)};
extract_meta(target_timestamp, Timestamp) ->
    {target_timestamp, format_timestamp(Timestamp)};
extract_meta(exception, {Class, Reason, Stacktrace}) ->
    [
        {error, [
            {class, genlib:to_binary(Class)},
            {reason, genlib:format(Reason)},
            {stack_trace, genlib_format:format_stacktrace(Stacktrace)}
        ]}
    ];
extract_meta(retry_action, {wait, Timeout, NextStrategy}) ->
    [
        {wait_timeout, Timeout},
        {next_retry_strategy, genlib:format(NextStrategy)}
    ];
extract_meta(retry_action, _Other) ->
    [];
extract_meta(action, {reschedule, Timestamp}) ->
    [
        {action, <<"reschedule">>},
        {reschedule_time, format_timestamp(Timestamp)}
    ];
extract_meta(action, Action) ->
    {action, genlib:to_binary(Action)};
extract_meta(namespace, NS) ->
    {machine_ns, NS};
extract_meta(squad_member, Member) ->
    {squad_member, [
        {age, maps:get(age, Member, 0)},
        {last_contact, maps:get(last_contact, Member, 0)}
    ]};
extract_meta(Name, Value) ->
    {Name, Value}.

-spec extract_woody_meta(woody_event_handler:event_meta()) -> meta().
extract_woody_meta(#{role := server} = Meta) ->
    [{'rpc.server', Meta}];
extract_woody_meta(#{role := client} = Meta) ->
    [{'rpc.client', Meta}];
extract_woody_meta(Meta) ->
    [{rpc, Meta}].

-spec format_timestamp(genlib_time:ts()) -> binary().
format_timestamp(TS) ->
    genlib_rfc3339:format(TS, second).
