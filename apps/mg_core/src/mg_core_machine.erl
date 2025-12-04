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
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% При падении хендлера переводит машину в error состояние.
%%% Оперирует стейтом и набором атрибутов.
%%%
-module(mg_core_machine).

%%
%% Логика работы с ошибками.
%%
%% Возможные ошибки:
%%  - ожиданные -- throw()
%%   - бизнес-логические -- logic
%%    - машина не найдена -- machine_not_found
%%    - машина уже существует -- machine_already_exist
%%    - машина находится в упавшем состоянии -- machine_failed
%%    - некорректная попытка повторно запланировать обработку события -- invalid_reschedule_request
%%    - что-то ещё?
%%   - временные -- transient
%%    - сервис перегружен —- overload
%%    - хранилище недоступно -- {storage_unavailable, Details}
%%    - процессор недоступен -- {processor_unavailable, Details}
%%   - таймауты -- {timeout, Details}
%%   - окончательные -- permanent
%%    - исчерпаны попытки повтора обработки таймера -- timer_retries_exhausted
%%  - неожиданные
%%   - что-то пошло не так -- падение с любой другой ошибкой
%%
%% Например: throw:{logic, machine_not_found}, throw:{transient, {storage_unavailable, ...}},
%% error:badarg
%%
%% Если в процессе обработки внешнего запроса происходит ожидаемая ошибка, она мапится в код
%% ответа, если неожидаемая ошибка, то запрос падает с internal_error, ошибка пишется в лог.
%%
%% Если в процессе обработки запроса машиной происходит ожидаемая ошибка, то она прокидывается
%% вызывающему коду, если неожидаемая, то машина переходит в error состояние и в ответ возникает
%% ошибка machine_failed.
%%
%% Хранилище и процессор кидают либо ошибку о недоступности, либо падают.
%%

-include_lib("mg_core/include/pulse.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% API
-export_type([retry_opt/0]).
-export_type([suicide_probability/0]).
-export_type([scheduler_opt/0]).
-export_type([schedulers_opt/0]).
-export_type([options/0]).
-export_type([storage_options/0]).
-export_type([thrown_error/0]).
-export_type([logic_error/0]).
-export_type([transient_error/0]).
-export_type([throws/0]).
-export_tupe([storage_machine/0]).
-export_type([machine_state/0]).
-export_type([machine_status/0]).
-export_type([processor_impact/0]).
-export_type([processing_context/0]).
-export_type([processor_result/0]).
-export_type([processor_reply_action/0]).
-export_type([processor_flow_action/0]).
-export_type([search_query/0]).
-export_type([machine_regular_status/0]).

-export([child_spec/2]).
-export([start_link/1]).
-export([start_link/2]).

-export([start/5]).
-export([simple_repair/4]).
-export([repair/5]).
-export([call/5]).
-export([notify/4]).
-export([send_timeout/4]).
-export([send_notification/5]).
-export([resume_interrupted/3]).
-export([fail/4]).
-export([fail/5]).
-export([get/2]).
-export([is_exist/2]).
-export([search/2]).
-export([search/3]).
-export([search/4]).
-export([reply/2]).
-export([call_with_lazy_start/6]).

%% Internal API
-export([all_statuses/0]).
-export([manager_options/1]).
-export([get_storage_machine/2]).

%% mg_core_worker
-behaviour(mg_core_worker).
-export([handle_load/3, handle_call/5, handle_unload/1]).

%%
%% API
%%
-type seconds() :: non_neg_integer().
-type scheduler_type() :: overseer | timers | timers_retries | notification.
%% TODO Refactor and simplify scheduler options specs. Maybe use introduce
%%      configurator module to construct options for child specs and make it its
%%      exclusive responsibility as in public API.
-type scheduler_opt() ::
    disable
    | #{
        % actual scheduling start delay (defaults to 1000)
        start_interval => non_neg_integer(),
        % how much tasks in total scheduler is ready to enqueue for processing
        capacity => non_neg_integer(),
        % wait at least this delay before subsequent scanning of persistent store for queued tasks
        min_scan_delay => mg_skd_scanner:scan_delay(),
        % wait at most this delay before subsequent scanning attempts when queue appears to be empty
        rescan_delay => mg_skd_scanner:scan_delay(),
        % how many tasks to fetch at most
        max_scan_limit => mg_skd_scanner:scan_limit(),
        % by how much to adjust limit to account for possibly duplicated tasks
        scan_ahead => mg_skd_scanner:scan_ahead(),
        % how many seconds in future a task can be for it to be sent to the local scheduler
        target_cutoff => seconds(),
        % name of quota limiting number of active tasks
        task_quota => mg_skd_quota_worker:name(),
        % share of quota limit
        task_share => mg_skd_quota:share(),
        % notifications: upper bound for scan ([_; TSNow - scan_handicap])
        scan_handicap => seconds(),
        % notifications: lower bound for scan ([TSNow - scan_handicap - scan_cutoff; _])
        scan_cutoff => seconds(),
        % notifications: amount of time into the future to reschedule a failed notification to
        reschedule_time => seconds()
    }.
-type retry_subj() :: storage | processor | timers | continuation.
-type retry_opt() :: #{
    retry_subj() => genlib_retry:policy()
}.
-type schedulers_opt() :: #{scheduler_type() => scheduler_opt()}.
% [0, 1]
-type suicide_probability() :: float() | integer() | undefined.

%% FIXME: some of these are listed as optional (=>)
%%        whereas later in the code they are rigidly matched (:=)
%%        fixed for namespace and pulse, worker
-type options() :: #{
    namespace := mg_core:ns(),
    pulse := mpulse:handler(),
    storage => storage_options(),
    notification => mg_core_notification:options(),
    processor => mg_utils:mod_opts(),
    worker := mg_core_workers_manager:ns_options(),
    retries => retry_opt(),
    schedulers => schedulers_opt(),
    suicide_probability => suicide_probability(),
    timer_processing_timeout => timeout(),
    notification_processing_timeout => timeout()
}.

% like mg_core_storage:options() except `name`
-type storage_options() :: mg_utils:mod_opts(map()).

-type thrown_error() ::
    {logic, logic_error()} | {transient, transient_error()} | {timeout, _Reason}.
-type logic_error() ::
    machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() ::
    overload | {storage_unavailable, _Reason} | {processor_unavailable, _Reason} | unavailable.

-type throws() :: no_return().

-type storage_machine() :: #{
    status => machine_status(),
    state => machine_state()
}.

-type machine_state() :: mg_core_storage:opaque().

-type machine_regular_status() ::
    sleeping
    | {waiting, genlib_time:ts(), request_context(), HandlingTimeout :: pos_integer()}
    | {retrying, Target :: genlib_time:ts(), Start :: genlib_time:ts(), Attempt :: non_neg_integer(), request_context()}
    | {processing, request_context()}.
-type machine_status() ::
    machine_regular_status() | {error, Reason :: term(), machine_regular_status()}.

%%

-type processing_state() :: term().
% контест обработки, сбрасывается при выгрузке машины
-type processing_context() ::
    #{
        call_context => mg_core_worker:call_context(),
        state => processing_state()
    }
    | undefined.
-type processor_impact() ::
    {init, term()}
    | {repair, term()}
    | {call, term()}
    | {notification, mg_core_notification:id(), term()}
    | timeout
    | continuation.
-type processor_reply_action() :: noreply | {reply, _}.
-type processor_flow_action() ::
    sleep
    | {wait, genlib_time:ts(), request_context(), HandlingTimeout :: pos_integer()}
    | {continue, processing_state()}
    | keep
    | remove.
-type processor_result() :: {processor_reply_action(), processor_flow_action(), machine_state()}.
-type request_context() :: mg_core:request_context().

-type processor_retry() :: genlib_retry:strategy() | undefined.

-type deadline() :: mg_core_deadline:deadline().
-type 'maybe'(T) :: T | undefined.

-callback processor_child_spec(_Options) -> supervisor:child_spec() | undefined.
-callback process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg_core:id(),
    Impact :: processor_impact(),
    PCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    MachineState :: machine_state(),
    Result :: processor_result().
-optional_callbacks([processor_child_spec/1]).

-type search_query() ::
    sleeping
    | waiting
    | {waiting, From :: genlib_time:ts(), To :: genlib_time:ts()}
    | retrying
    | {retrying, From :: genlib_time:ts(), To :: genlib_time:ts()}
    | processing
    | failed.

%%

-spec child_spec(options(), atom()) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options, ChildID]},
        restart => permanent,
        type => supervisor
    }.

-spec start_link(options()) -> mg_utils:gen_start_ret().
start_link(#{namespace := NS} = Options) ->
    start_link(Options, {?MODULE, NS}).

-spec start_link(options(), _ChildID) -> mg_utils:gen_start_ret().
start_link(Options, ChildID) ->
    genlib_adhoc_supervisor:start_link(
        #{strategy => one_for_one},
        [
            machine_sup_child_spec(Options, {ChildID, machine_sup}),
            scheduler_sup_child_spec(Options, {ChildID, scheduler_sup})
        ]
    ).

-spec machine_sup_child_spec(options(), _ChildID) -> supervisor:child_spec().
machine_sup_child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start =>
            {genlib_adhoc_supervisor, start_link, [
                #{strategy => rest_for_one},
                mg_utils:lists_compact([
                    mg_core_storage:child_spec(storage_options(Options), storage),
                    notification_child_spec(Options),
                    processor_child_spec(Options),
                    mg_core_workers_manager:child_spec(manager_options(Options), manager)
                ])
            ]},
        restart => permanent,
        type => supervisor
    }.

-spec scheduler_sup_child_spec(options(), _ChildID) -> supervisor:child_spec().
scheduler_sup_child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start =>
            {genlib_adhoc_supervisor, start_link, [
                #{
                    strategy => one_for_one,
                    intensity => 10,
                    period => 30
                },
                mg_utils:lists_compact([
                    scheduler_child_spec(timers, Options),
                    scheduler_child_spec(timers_retries, Options),
                    scheduler_child_spec(overseer, Options),
                    scheduler_child_spec(notification, Options)
                ])
            ]},
        restart => permanent,
        type => supervisor
    }.

-spec start(options(), mg_core:id(), term(), request_context(), deadline()) -> _Resp | throws().
start(Options, ID, Args, ReqCtx, Deadline) ->
    call_(Options, ID, {start, Args}, ReqCtx, Deadline).

-spec simple_repair(options(), mg_core:id(), request_context(), deadline()) -> _Resp | throws().
simple_repair(Options, ID, ReqCtx, Deadline) ->
    call_(Options, ID, simple_repair, ReqCtx, Deadline).

-spec repair(options(), mg_core:id(), term(), request_context(), deadline()) -> _Resp | throws().
repair(Options, ID, Args, ReqCtx, Deadline) ->
    call_(Options, ID, {repair, Args}, ReqCtx, Deadline).

-spec call(options(), mg_core:id(), term(), request_context(), deadline()) -> _Resp | throws().
call(Options, ID, Call, ReqCtx, Deadline) ->
    call_(Options, ID, {call, Call}, ReqCtx, Deadline).

-spec notify(options(), mg_core:id(), mg_core_storage:opaque(), request_context()) ->
    mg_core_notification:id() | throws().
notify(Options, ID, Args, ReqCtx) ->
    %% Ensure machine exists
    _ = get(Options, ID),
    NotificationID = generate_snowflake_id(),
    Timestamp = genlib_time:unow(),
    OpaqueArgs = notification_args_to_opaque({Args, ReqCtx}),
    Context = mg_core_notification:put(
        notification_options(Options),
        NotificationID,
        #{
            machine_id => ID,
            args => OpaqueArgs
        },
        Timestamp,
        undefined
    ),
    ok = send_notification_task(Options, NotificationID, OpaqueArgs, ID, Context, Timestamp),
    ok = emit_beat(Options, #mg_core_machine_notification_created{
        namespace = maps:get(namespace, Options),
        machine_id = ID,
        notification_id = NotificationID,
        target_timestamp = Timestamp
    }),
    NotificationID.

-spec send_timeout(options(), mg_core:id(), genlib_time:ts(), deadline()) -> _Resp | throws().
send_timeout(Options, ID, Timestamp, Deadline) ->
    call_(Options, ID, {timeout, Timestamp}, undefined, Deadline).

-spec send_notification(options(), mg_core:id(), mg_core_notification:id(), term(), deadline()) -> _Resp | throws().
send_notification(Options, ID, NotificationID, OpaqueArgs, Deadline) ->
    {Args, ReqCtx} = opaque_to_notification_args(OpaqueArgs),
    call_(Options, ID, {notification, NotificationID, Args}, ReqCtx, Deadline).

-spec resume_interrupted(options(), mg_core:id(), deadline()) -> _Resp | throws().
resume_interrupted(Options, ID, Deadline) ->
    call_(Options, ID, resume_interrupted_one, undefined, Deadline).

-spec fail(options(), mg_core:id(), request_context(), deadline()) -> ok.
fail(Options, ID, ReqCtx, Deadline) ->
    fail(Options, ID, {error, explicit_fail, []}, ReqCtx, Deadline).

-spec fail(options(), mg_core:id(), mg_utils:exception(), request_context(), deadline()) -> ok.
fail(Options, ID, Exception, ReqCtx, Deadline) ->
    call_(Options, ID, {fail, Exception}, ReqCtx, Deadline).

-spec get(options(), mg_core:id()) -> storage_machine() | throws().
get(Options, ID) ->
    {_, StorageMachine} =
        mg_utils:throw_if_undefined(
            get_storage_machine(Options, ID),
            {logic, machine_not_found}
        ),
    StorageMachine.

-spec is_exist(options(), mg_core:id()) -> boolean() | throws().
is_exist(Options, ID) ->
    get_storage_machine(Options, ID) =/= undefined.

-spec search(
    options(),
    search_query(),
    mg_core_storage:index_limit(),
    mg_core_storage:continuation()
) -> mg_core_storage:search_result() | throws().
search(Options, Query, Limit, Continuation) ->
    StorageQuery = storage_search_query(Query, Limit, Continuation),
    mg_core_storage:search(storage_options(Options), StorageQuery).

-spec search(options(), search_query(), mg_core_storage:index_limit()) ->
    mg_core_storage:search_result() | throws().
search(Options, Query, Limit) ->
    mg_core_storage:search(storage_options(Options), storage_search_query(Query, Limit)).

-spec search(options(), search_query()) -> mg_core_storage:search_result() | throws().
search(Options, Query) ->
    % TODO deadline
    mg_core_storage:search(storage_options(Options), storage_search_query(Query)).

-spec call_with_lazy_start(
    options(),
    mg_core:id(),
    term(),
    request_context(),
    deadline(),
    term()
) -> _Resp | throws().
call_with_lazy_start(Options, ID, Call, ReqCtx, Deadline, StartArgs) ->
    try
        call(Options, ID, Call, ReqCtx, Deadline)
    catch
        throw:{logic, machine_not_found} ->
            try
                _ = start(Options, ID, StartArgs, ReqCtx, Deadline)
            catch
                throw:{logic, machine_already_exist} ->
                    % вдруг кто-то ещё делает аналогичный процесс
                    ok
            end,
            % если к этому моменту машина не создалась, значит она уже не создастся
            % и исключение будет оправданным
            call(Options, ID, Call, ReqCtx, Deadline)
    end.

-spec reply(processing_context(), _) -> ok.
reply(undefined, _) ->
    % отвечать уже некому
    ok;
reply(#{call_context := CallContext}, Reply) ->
    ok = mg_core_worker:reply(CallContext, Reply).

%%
%% Internal API
%%
-define(DEFAULT_RETRY_POLICY, {exponential, infinity, 2, 10, 60 * 1000}).
-define(DEFAULT_SCHEDULER_CAPACITY, 1000).

-define(CAN_BE_RETRIED(ErrorType), ErrorType =:= transient orelse ErrorType =:= timeout).

%%

-spec all_statuses() -> [atom()].
all_statuses() ->
    [sleeping, waiting, retrying, processing, failed].

-spec call_(options(), mg_core:id(), _, 'maybe'(request_context()), deadline()) -> _ | no_return().
call_(Options, ID, Call, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(
        mg_core_workers_manager:call(manager_options(Options), ID, Call, ReqCtx, Deadline)
    ).

%%
%% mg_core_worker callbacks
%%
-type state() :: #{
    id => mg_core:id(),
    namespace => mg_core:ns(),
    options => options(),
    schedulers => #{scheduler_type() => scheduler_ref()},
    storage_machine => storage_machine() | nonexistent | unknown,
    storage_context => mg_core_storage:context() | undefined,
    notifications_processed => mg_core_circular_buffer:t(mg_core_notification:id())
}.

-type scheduler_ref() ::
    {mg_skd:id(), _TargetCutoff :: seconds()}.

-spec handle_load(mg_core:id(), options(), request_context()) -> {ok, state()}.
handle_load(ID, Options, ReqCtx) ->
    %% TODO Maybe add explicit pulse beat on machine load
    ok = attach_otel_ctx(ReqCtx),
    Namespace = maps:get(namespace, Options),
    State1 = #{
        id => ID,
        namespace => Namespace,
        options => Options,
        schedulers => #{},
        storage_machine => unknown,
        storage_context => undefined,
        %% TODO: configurable?
        notifications_processed => mg_core_circular_buffer:new(50, 10)
    },
    State2 = lists:foldl(
        fun try_acquire_scheduler/2,
        State1,
        [timers, timers_retries]
    ),
    load_storage_machine(ReqCtx, State2).

-define(SPAN_NAME(Call), <<"running machine '", (atom_to_binary(Call))/binary, "'">>).

-spec handle_call(_Call, mg_core_worker:call_context(), 'maybe'(request_context()), deadline(), state()) ->
    {{reply, _Resp} | noreply, state()}.
handle_call(Call, CallContext, ReqCtx, Deadline, S) ->
    %% NOTE Consider adding new pulse beats to wrap 'processing calls' here.
    ok = attach_otel_ctx(ReqCtx),
    ParentSpanId = mg_core_otel:current_span_id(otel_ctx:get_current()),
    ?with_span(?SPAN_NAME(call), #{kind => ?SPAN_KIND_INTERNAL}, fun(SpanCtx) ->
        do_handle_call(Call, CallContext, ReqCtx, Deadline, ParentSpanId, SpanCtx, S)
    end).

-define(NOREPLY(State), {noreply, State}).
-define(REPLY_OK(State), {{reply, ok}, State}).
-define(REPLY_ERROR(Error, State), begin
    _ = ?record_exception(error, Error, [], #{}),
    {{reply, {error, Error}}, State}
end).

-define(with_parent_span_linked(ReqOtelCtx, ParentSpanId, SpanCtx, SpanName, ProcessFun),
    case mg_core_otel:current_span_id(ReqOtelCtx) =:= ParentSpanId of
        %% Update span name and go with it
        true ->
            _ = ?update_name(SpanName),
            ProcessFun(SpanCtx);
        %% Link spans
        false ->
            SpanOpts = #{kind => ?SPAN_KIND_INTERNAL, links => [opentelemetry:link(SpanCtx)]},
            otel_tracer:with_span(ReqOtelCtx, ?current_tracer, SpanName, SpanOpts, ProcessFun)
    end
).

-spec do_handle_call(
    _Call,
    mg_core_worker:call_context(),
    'maybe'(request_context()),
    deadline(),
    opentelemetry:span_id(),
    opentelemetry:span_ctx(),
    state()
) ->
    {{reply, _Resp} | noreply, state()}.
do_handle_call(Call, CallContext, ReqCtx, Deadline, ParentSpanId, SpanCtx, #{storage_machine := StorageMachine} = S) ->
    PCtx = new_processing_context(CallContext),

    % довольно сложное место, тут определяется приоритет реакции на внешние раздражители, нужно
    % быть аккуратнее
    case {Call, StorageMachine} of
        % восстановление после ошибок обращения в хранилище
        {Call, unknown} ->
            {ok, S1} = load_storage_machine(ReqCtx, S),
            handle_call(Call, CallContext, ReqCtx, Deadline, S1);
        %% ====================================================================
        %% 'start'
        {{start, Args}, nonexistent} ->
            _ = ?update_name(?SPAN_NAME(start)),
            ?NOREPLY(process({init, Args}, PCtx, ReqCtx, Deadline, S));
        {_, nonexistent} ->
            %% NOTE Any other call that is not 'start'
            ?REPLY_ERROR({logic, machine_not_found}, S);
        {{start, _}, #{status := _}} ->
            _ = ?update_name(?SPAN_NAME(start)),
            ?REPLY_ERROR({logic, machine_already_exist}, S);
        %% ====================================================================
        %% 'fail'
        %% NOTE Explicit fail used only in tests suites?
        {{fail, Exception}, _} ->
            _ = ?update_name(?SPAN_NAME(fail)),
            ?REPLY_OK(handle_exception(Exception, ReqCtx, Deadline, S));
        %% ====================================================================
        % сюда мы не должны попадать если машина не падала во время обработки запроса
        % (когда мы переходили в стейт processing)
        %% FIXME Review naming ambiguity: 'ProcessingReqCtx' is a
        %%       request context, not a processing context!
        {_, #{status := {processing, ProcessingReqCtx}}} ->
            % обработка машин в стейте processing идёт без дедлайна
            % машина должна либо упасть, либо перейти в другое состояние
            %% NOTE Use _possibly_ different OTEL context for 'continuation' span
            ReqOtelCtx = request_context_to_otel_context(ProcessingReqCtx),
            S1 = ?with_parent_span_linked(ReqOtelCtx, ParentSpanId, SpanCtx, ?SPAN_NAME(continuation), fun(_) ->
                process(continuation, undefined, ProcessingReqCtx, undefined, S)
            end),
            handle_call(Call, CallContext, ReqCtx, Deadline, S1);
        % ничего не просходит, просто убеждаемся, что машина загружена
        {resume_interrupted_one, _} ->
            ?REPLY_OK(S);
        %% ====================================================================
        %% 'call'
        {{call, SubCall}, #{status := sleeping}} ->
            ?NOREPLY(process({call, SubCall}, PCtx, ReqCtx, Deadline, S));
        {{call, SubCall}, #{status := {waiting, _, _, _}}} ->
            ?NOREPLY(process({call, SubCall}, PCtx, ReqCtx, Deadline, S));
        {{call, SubCall}, #{status := {retrying, _, _, _, _}}} ->
            ?NOREPLY(process({call, SubCall}, PCtx, ReqCtx, Deadline, S));
        {{call, _}, #{status := {error, _, _}}} ->
            ?REPLY_ERROR({logic, machine_failed}, S);
        %% ====================================================================
        %% 'repair'
        {{repair, Args}, #{status := {error, _, _}}} ->
            _ = ?update_name(?SPAN_NAME(repair)),
            ?NOREPLY(process({repair, Args}, PCtx, ReqCtx, Deadline, S));
        {{repair, _}, #{status := _}} ->
            _ = ?update_name(?SPAN_NAME(repair)),
            ?REPLY_ERROR({logic, machine_already_working}, S);
        %% ====================================================================
        %% 'simple_repair'
        {simple_repair, #{status := {error, _, _}}} ->
            _ = ?update_name(?SPAN_NAME(simple_repair)),
            ?REPLY_OK(process_simple_repair(ReqCtx, Deadline, S));
        {simple_repair, #{status := _}} ->
            _ = ?update_name(?SPAN_NAME(simple_repair)),
            ?REPLY_ERROR({logic, machine_already_working}, S);
        %% ====================================================================
        %% 'timeout' (from timers and retry_timers queues)
        {{timeout, Ts0}, #{status := {waiting, Ts1, InitialReqCtx, _}}} when Ts0 =:= Ts1 ->
            %% NOTE Use _possibly_ different OTEL context for 'timeout' signal span
            ReqOtelCtx = request_context_to_otel_context(InitialReqCtx),
            ?NOREPLY(
                ?with_parent_span_linked(ReqOtelCtx, ParentSpanId, SpanCtx, ?SPAN_NAME(timeout), fun(_) ->
                    process(timeout, PCtx, InitialReqCtx, Deadline, S)
                end)
            );
        {{timeout, Ts0}, #{status := {retrying, Ts1, _, _, InitialReqCtx}}} when Ts0 =:= Ts1 ->
            %% NOTE Use _possibly_ different OTEL context for retry 'timeout' signal spa
            ReqOtelCtx = request_context_to_otel_context(InitialReqCtx),
            ?NOREPLY(
                ?with_parent_span_linked(ReqOtelCtx, ParentSpanId, SpanCtx, ?SPAN_NAME(timeout), fun(_) ->
                    process(timeout, PCtx, InitialReqCtx, Deadline, S)
                end)
            );
        {{timeout, _}, #{status := _}} ->
            ?REPLY_OK(S);
        %% ====================================================================
        %% 'notification'
        {{notification, _, _}, #{status := {error, _, _}}} ->
            _ = ?update_name(?SPAN_NAME(notification)),
            ?REPLY_ERROR({logic, machine_failed}, S);
        {{notification, NotificationID, Args}, #{status := _}} ->
            _ = ?update_name(?SPAN_NAME(notification)),
            ?NOREPLY(process_notification(NotificationID, Args, PCtx, ReqCtx, Deadline, S))
    end.

-spec handle_unload(state()) -> ok.
handle_unload(State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_lifecycle_unloaded{
        namespace = NS,
        machine_id = ID
    }).

%%
%% processing context
%%
-spec new_processing_context(mg_core_worker:call_context()) -> processing_context().
new_processing_context(CallContext) ->
    #{
        call_context => CallContext,
        state => undefined
    }.

%%
%% storage machine
%%
-spec new_storage_machine() -> storage_machine().
new_storage_machine() ->
    #{
        status => sleeping,
        state => null
    }.

-spec get_storage_machine(options(), mg_core:id()) ->
    {mg_core_storage:context(), storage_machine()} | undefined.
get_storage_machine(Options, ID) ->
    try mg_core_storage:get(storage_options(Options), ID) of
        undefined ->
            undefined;
        {Context, PackedMachine} ->
            {Context, opaque_to_storage_machine(PackedMachine)}
    catch
        throw:{logic, {invalid_key, _StorageDetails} = Details} ->
            throw({logic, {invalid_machine_id, Details}})
    end.

-spec load_storage_machine(request_context(), state()) -> {ok, state()} | {error, Reason :: any()}.
load_storage_machine(ReqCtx, State) ->
    #{options := Options, id := ID, namespace := Namespace} = State,
    try
        {StorageContext, StorageMachine} =
            case get_storage_machine(Options, ID) of
                undefined -> {undefined, nonexistent};
                V -> V
            end,

        NewState = State#{
            storage_machine => StorageMachine,
            storage_context => StorageContext
        },
        ok = emit_machine_load_beat(Options, Namespace, ID, ReqCtx, StorageMachine),
        {ok, NewState}
    catch
        throw:Reason:ST ->
            Exception = {throw, Reason, ST},
            ok = emit_beat(Options, #mg_core_machine_lifecycle_loading_error{
                namespace = Namespace,
                machine_id = ID,
                request_context = ReqCtx,
                exception = Exception
            }),
            {error, Reason}
    end.

%%
%% packer to opaque
%%
-spec storage_machine_to_opaque(storage_machine()) -> mg_core_storage:opaque().
storage_machine_to_opaque(#{status := Status, state := State}) ->
    [1, machine_status_to_opaque(Status), State].

-spec opaque_to_storage_machine(mg_core_storage:opaque()) -> storage_machine().
opaque_to_storage_machine([1, Status, State]) ->
    #{status => opaque_to_machine_status(Status), state => State}.

-spec machine_status_to_opaque(machine_status()) -> mg_core_storage:opaque().
machine_status_to_opaque(Status) ->
    Opaque =
        case Status of
            sleeping ->
                1;
            {waiting, TS, ReqCtx, HdlTo} ->
                [2, TS, ReqCtx, HdlTo];
            {processing, ReqCtx} ->
                [3, ReqCtx];
            % TODO подумать как упаковывать reason
            {error, Reason, OldStatus} ->
                [4, erlang:term_to_binary(Reason), machine_status_to_opaque(OldStatus)];
            {retrying, TS, StartTS, Attempt, ReqCtx} ->
                [5, TS, StartTS, Attempt, ReqCtx]
        end,
    Opaque.

-spec opaque_to_machine_status(mg_core_storage:opaque()) -> machine_status().
opaque_to_machine_status(Opaque) ->
    case Opaque of
        1 ->
            sleeping;
        % совместимость со старой версией
        [2, TS] ->
            {waiting, TS, null, 30000};
        [2, TS, ReqCtx, HdlTo] ->
            {waiting, TS, ReqCtx, HdlTo};
        % совместимость со старой версией
        3 ->
            {processing, null};
        [3, ReqCtx] ->
            {processing, ReqCtx};
        [4, Reason, OldStatus] ->
            {error, erlang:binary_to_term(Reason), opaque_to_machine_status(OldStatus)};
        % устаревшее
        [4, Reason] ->
            {error, erlang:binary_to_term(Reason), sleeping};
        [5, TS, StartTS, Attempt, ReqCtx] ->
            {retrying, TS, StartTS, Attempt, ReqCtx}
    end.

%%
%% indexes
%%
-define(STATUS_IDX, {integer, <<"status">>}).
-define(WAITING_IDX, {integer, <<"waiting_date">>}).
-define(RETRYING_IDX, {integer, <<"retrying_date">>}).

-spec storage_search_query(
    search_query(),
    mg_core_storage:index_limit(),
    mg_core_storage:continuation()
) -> mg_core_storage:index_query().
storage_search_query(Query, Limit, Continuation) ->
    erlang:append_element(storage_search_query(Query, Limit), Continuation).

-spec storage_search_query(search_query(), mg_core_storage:index_limit()) ->
    mg_core_storage:index_query().
storage_search_query(Query, Limit) ->
    erlang:append_element(storage_search_query(Query), Limit).

-spec storage_search_query(search_query()) -> mg_core_storage:index_query().
storage_search_query(sleeping) ->
    {?STATUS_IDX, 1};
storage_search_query(waiting) ->
    {?STATUS_IDX, 2};
storage_search_query({waiting, FromTs, ToTs}) ->
    {?WAITING_IDX, {FromTs, ToTs}};
storage_search_query(processing) ->
    {?STATUS_IDX, 3};
storage_search_query(failed) ->
    {?STATUS_IDX, 4};
storage_search_query(retrying) ->
    {?STATUS_IDX, 5};
storage_search_query({retrying, FromTs, ToTs}) ->
    {?RETRYING_IDX, {FromTs, ToTs}}.

-spec storage_machine_to_indexes(storage_machine()) -> [mg_core_storage:index_update()].
storage_machine_to_indexes(#{status := Status}) ->
    status_index(Status) ++ status_range_index(Status).

-spec status_index(machine_status()) -> [mg_core_storage:index_update()].
status_index(Status) ->
    StatusInt =
        case Status of
            sleeping -> 1;
            {waiting, _, _, _} -> 2;
            {processing, _} -> 3;
            {error, _, _} -> 4;
            {retrying, _, _, _, _} -> 5
        end,
    [{?STATUS_IDX, StatusInt}].

-spec status_range_index(machine_status()) -> [mg_core_storage:index_update()].
status_range_index({waiting, Timestamp, _, _}) ->
    [{?WAITING_IDX, Timestamp}];
status_range_index({retrying, Timestamp, _, _, _}) ->
    [{?RETRYING_IDX, Timestamp}];
status_range_index(_) ->
    [].

%%
%% processing
%%

-spec process_simple_repair(request_context(), deadline(), state()) -> state().
process_simple_repair(ReqCtx, Deadline, State) ->
    #{storage_machine := StorageMachine = #{status := {error, _, OldStatus}}} = State,
    transit_state(
        ReqCtx,
        Deadline,
        StorageMachine#{status => OldStatus},
        State
    ).

-spec emit_repaired_beat(request_context(), deadline(), state()) ->
    ok.
emit_repaired_beat(ReqCtx, Deadline, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_lifecycle_repaired{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline
    }).

-spec process(processor_impact(), processing_context(), request_context(), deadline(), state()) ->
    state().
process(Impact, ProcessingCtx, ReqCtx, Deadline, State) ->
    RetryStrategy = get_impact_retry_strategy(Impact, Deadline, State),
    #{id := ID, namespace := NS} = State,
    SpanOpts = #{kind => ?SPAN_KIND_INTERNAL, attributes => mg_core_otel:machine_tags(NS, ID)},
    SpanName = <<"processing machine '", (mg_core_otel:impact_to_machine_activity(Impact))/binary, "'">>,
    ?with_span(SpanName, SpanOpts, fun(_SpanCtx) ->
        try
            process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, RetryStrategy)
        catch
            Class:Reason:ST ->
                ok = do_reply_action({reply, {error, {logic, machine_failed}}}, ProcessingCtx),
                handle_exception({Class, Reason, ST}, ReqCtx, Deadline, State)
        end
    end).

%% 😠
-spec process_notification(NotificationID, Args, ProcessingCtx, ReqCtx, Deadline, State) -> State when
    NotificationID :: mg_core_notification:id(),
    Args :: term(),
    ProcessingCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    State :: state().
process_notification(NotificationID, Args, PCtx, ReqCtx, Deadline, State) ->
    %% Check if this process already processed a notification with this id
    %% This check does not work if the machine process was reloaded at some point (by design)
    case is_notification_processed(NotificationID, State) of
        false ->
            process({notification, NotificationID, Args}, PCtx, ReqCtx, Deadline, State);
        true ->
            ok = do_reply_action({reply, ok}, PCtx),
            State
    end.

-spec is_notification_processed(mg_core_notification:id(), state()) ->
    boolean().
is_notification_processed(NotificationID, #{notifications_processed := Buffer}) ->
    mg_core_circular_buffer:member(NotificationID, Buffer).

-spec notification_args_to_opaque({mg_core_storage:opaque(), request_context()}) ->
    mg_core_storage:opaque().
notification_args_to_opaque({Args, RequestContext}) ->
    [1, Args, RequestContext].

-spec opaque_to_notification_args(mg_core_storage:opaque()) ->
    {mg_core_storage:opaque(), request_context()}.
opaque_to_notification_args([1, Args, RequestContext]) ->
    {Args, RequestContext}.

-spec send_notification_task(Options, NotificationID, Args, MachineID, Context, TargetTime) -> ok when
    Options :: options(),
    NotificationID :: mg_core_notification:id(),
    Args :: mg_core_storage:opaque(),
    MachineID :: mg_core:id(),
    Context :: mg_core_notification:context(),
    TargetTime :: genlib_time:ts().
send_notification_task(Options, NotificationID, Args, MachineID, Context, TargetTime) ->
    Task = mg_core_queue_notifications:build_task(NotificationID, MachineID, TargetTime, Context, Args),
    mg_skd:send_task(scheduler_id(notification, Options), Task).

-spec process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, Retry) -> State when
    Impact :: processor_impact(),
    ProcessingCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    State :: state(),
    Retry :: processor_retry().
process_with_retry(_, _, _, _, #{storage_machine := unknown} = State, _) ->
    % После попыток обработать вызов пришли в неопредленное состояние.
    % На этом уровне больше ничего не сделать, пусть разбираются выше.
    State;
process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, RetryStrategy) ->
    #{id := ID, namespace := NS, options := Opts} = State,
    try
        process_unsafe(Impact, ProcessingCtx, ReqCtx, Deadline, try_init_state(Impact, State))
    catch
        throw:(Reason = ({ErrorType, _Details})):ST when ?CAN_BE_RETRIED(ErrorType) ->
            ok = emit_beat(Opts, #mg_core_machine_process_transient_error{
                namespace = NS,
                machine_id = ID,
                exception = {throw, Reason, ST},
                request_context = ReqCtx
            }),
            ok = do_reply_action({reply, {error, Reason}}, ProcessingCtx),
            NewState = handle_transient_exception(Reason, State),
            case process_retry_next_step(RetryStrategy) of
                ignore when Impact == timeout ->
                    reschedule(ReqCtx, undefined, NewState);
                ignore ->
                    NewState;
                finish ->
                    erlang:throw({permanent, {retries_exhausted, Reason}});
                {wait, Timeout, NewRetryStrategy} ->
                    ok = timer:sleep(Timeout),
                    process_with_retry(
                        Impact,
                        ProcessingCtx,
                        ReqCtx,
                        Deadline,
                        NewState,
                        NewRetryStrategy
                    )
            end
    end.

-spec process_retry_next_step(processor_retry()) ->
    {wait, timeout(), genlib_retry:strategy()} | finish | ignore.
process_retry_next_step(undefined) ->
    ignore;
process_retry_next_step(RetryStrategy) ->
    genlib_retry:next_step(RetryStrategy).

-spec get_impact_retry_strategy(processor_impact(), deadline(), state()) -> processor_retry().
get_impact_retry_strategy(continuation, Deadline, #{options := Options}) ->
    retry_strategy(continuation, Options, Deadline);
get_impact_retry_strategy(_Impact, _Deadline, _State) ->
    undefined.

-spec try_init_state(processor_impact(), state()) -> state().
try_init_state({init, _}, State) ->
    State#{storage_machine := new_storage_machine()};
try_init_state(_Impact, State) ->
    State.

-spec handle_transient_exception(transient_error(), state()) -> state().
handle_transient_exception({storage_unavailable, _Details}, State) ->
    State#{storage_machine := unknown};
handle_transient_exception(_Reason, State) ->
    State.

-spec handle_exception(Exception, ReqCtx, Deadline, state()) -> state() when
    Exception :: mg_utils:exception(),
    ReqCtx :: request_context(),
    Deadline :: deadline().
handle_exception(Exception, ReqCtx, Deadline, State) ->
    #{options := Options, id := ID, namespace := NS, storage_machine := StorageMachine} = State,
    ok = emit_beat(Options, #mg_core_machine_lifecycle_failed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline,
        exception = Exception
    }),
    case StorageMachine of
        nonexistent ->
            State;
        #{status := {error, _, _}} ->
            State;
        #{status := OldStatus} ->
            NewStorageMachine = StorageMachine#{status => {error, Exception, OldStatus}},
            transit_state(ReqCtx, Deadline, NewStorageMachine, State)
    end.

-spec process_unsafe(
    processor_impact(),
    processing_context(),
    request_context(),
    deadline(),
    state()
) -> state().
process_unsafe(
    Impact,
    ProcessingCtx,
    ReqCtx,
    Deadline,
    #{storage_machine := StorageMachine} = State
) ->
    ok = emit_pre_process_beats(Impact, ReqCtx, Deadline, State),
    ProcessStart = erlang:monotonic_time(),
    {ReplyAction, Action, NewMachineState} =
        call_processor(Impact, ProcessingCtx, ReqCtx, Deadline, State),
    ProcessDuration = erlang:monotonic_time() - ProcessStart,
    ok = emit_post_process_beats(Impact, ReqCtx, Deadline, ProcessDuration, State),
    ok = try_suicide(State, ReqCtx),
    NewStorageMachine0 = StorageMachine#{state := NewMachineState},
    NewState0 =
        case Action of
            {continue, _} ->
                NewStorageMachine = NewStorageMachine0#{status := {processing, ReqCtx}},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            sleep ->
                NewStorageMachine = NewStorageMachine0#{status := sleeping},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            {wait, Timestamp, HdlReqCtx, HdlTo} ->
                Status = {waiting, Timestamp, HdlReqCtx, HdlTo},
                NewStorageMachine = NewStorageMachine0#{status := Status},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            keep ->
                State;
            remove ->
                remove_from_storage(ReqCtx, Deadline, State)
        end,
    ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
    NewState1 =
        case Impact of
            {notification, NotificationID, _} ->
                %% Count notification as processed even if it wants to do a continuation
                handle_notification_processed(NotificationID, NewState0);
            _ ->
                NewState0
        end,
    case Action of
        {continue, NewProcessingSubState} ->
            % продолжение обработки машины делается без дедлайна
            % предполагается, что машина должна рано или поздно завершить свои дела или упасть
            process(
                continuation,
                ProcessingCtx#{state := NewProcessingSubState},
                ReqCtx,
                undefined,
                NewState1
            );
        _ ->
            NewState1
    end.

-spec generate_snowflake_id() -> binary().
generate_snowflake_id() ->
    <<ID:64>> = snowflake:new(),
    genlib_format:format_int_base(ID, 61).

-spec handle_notification_processed(mg_core_notification:id(), state()) ->
    state().
handle_notification_processed(NotificationID, #{notifications_processed := Buffer} = State) ->
    State#{notifications_processed => mg_core_circular_buffer:push(NotificationID, Buffer)}.

-spec call_processor(
    processor_impact(),
    processing_context(),
    request_context(),
    deadline(),
    state()
) -> processor_result().
call_processor(Impact, ProcessingCtx, ReqCtx, Deadline, State) ->
    #{options := Options, id := ID, storage_machine := #{state := MachineState}} = State,
    mg_utils:apply_mod_opts(
        get_options(processor, Options),
        process_machine,
        [ID, Impact, ProcessingCtx, ReqCtx, Deadline, MachineState]
    ).

-spec notification_child_spec(options()) -> supervisor:child_spec() | undefined.
notification_child_spec(#{notification := NotificationOptions}) ->
    mg_core_notification:child_spec(NotificationOptions, notification);
notification_child_spec(#{}) ->
    undefined.

-spec processor_child_spec(options()) -> supervisor:child_spec().
processor_child_spec(Options) ->
    mg_utils:apply_mod_opts_if_defined(
        get_options(processor, Options),
        processor_child_spec,
        undefined
    ).

-spec reschedule(ReqCtx, Deadline, state()) -> state() when
    ReqCtx :: request_context(),
    Deadline :: deadline().
reschedule(_, _, #{storage_machine := unknown} = State) ->
    % После попыток обработать вызов пришли в неопредленное состояние.
    % На этом уровне больше ничего не сделать, пусть разбираются выше.
    State;
reschedule(ReqCtx, Deadline, State) ->
    #{id := ID, options := Options, namespace := NS} = State,
    try
        {ok, NewState, Target, Attempt} = reschedule_unsafe(ReqCtx, Deadline, State),
        ok = emit_beat(Options, #mg_core_timer_lifecycle_rescheduled{
            namespace = NS,
            machine_id = ID,
            request_context = ReqCtx,
            deadline = Deadline,
            target_timestamp = Target,
            attempt = Attempt
        }),
        NewState
    catch
        throw:(Reason = ({ErrorType, _Details})):ST when ?CAN_BE_RETRIED(ErrorType) ->
            Exception = {throw, Reason, ST},
            ok = emit_beat(Options, #mg_core_timer_lifecycle_rescheduling_error{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                deadline = Deadline,
                exception = Exception
            }),
            handle_transient_exception(Reason, State)
    end.

-spec reschedule_unsafe(ReqCtx, Deadline, state()) -> Result when
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    Result :: {ok, state(), genlib_time:ts(), non_neg_integer()}.
reschedule_unsafe(
    ReqCtx,
    Deadline,
    #{
        storage_machine := StorageMachine = #{status := Status},
        options := Options
    } = State
) ->
    {Start, Attempt} =
        case Status of
            {waiting, _, _, _} -> {genlib_time:unow(), 0};
            {retrying, _, S, A, _} -> {S, A + 1}
        end,
    RetryStrategy = retry_strategy(timers, Options, undefined, Start, Attempt),
    case genlib_retry:next_step(RetryStrategy) of
        {wait, Timeout, _NewRetryStrategy} ->
            Target = get_schedule_target(Timeout),
            NewStatus = {retrying, Target, Start, Attempt, ReqCtx},
            NewStorageMachine = StorageMachine#{status => NewStatus},
            {ok, transit_state(ReqCtx, Deadline, NewStorageMachine, State), Target, Attempt};
        finish ->
            throw({permanent, timer_retries_exhausted})
    end.

-spec get_schedule_target(timeout()) -> genlib_time:ts().
get_schedule_target(TimeoutMS) ->
    Now = genlib_time:unow(),
    Now + (TimeoutMS div 1000).

-spec do_reply_action(processor_reply_action(), undefined | processing_context()) -> ok.
do_reply_action(noreply, _) ->
    ok;
do_reply_action({reply, Reply}, ProcessingCtx) ->
    ok = reply(ProcessingCtx, Reply),
    ok.

-spec wrap_reply_action(_, processor_reply_action()) -> processor_reply_action().
wrap_reply_action(_, noreply) ->
    noreply;
wrap_reply_action(Wrapper, {reply, R}) ->
    {reply, {Wrapper, R}}.

-spec transit_state(request_context(), deadline(), storage_machine(), state()) -> state().
transit_state(
    _ReqCtx,
    _Deadline,
    NewStorageMachine,
    #{storage_machine := OldStorageMachine} = State
) when NewStorageMachine =:= OldStorageMachine ->
    State;
transit_state(ReqCtx, Deadline, #{status := Status} = NewStorageMachine, State) ->
    #{
        id := ID,
        options := Options,
        storage_machine := #{status := StatusWas},
        storage_context := StorageContext
    } = State,
    _ =
        case Status of
            StatusWas -> ok;
            _Different -> handle_status_transition(StatusWas, Status, ReqCtx, Deadline, State)
        end,
    F = fun() ->
        mg_core_storage:put(
            storage_options(Options),
            ID,
            StorageContext,
            storage_machine_to_opaque(NewStorageMachine),
            storage_machine_to_indexes(NewStorageMachine)
        )
    end,
    RS = retry_strategy(storage, Options, Deadline),
    NewStorageContext = do_with_retry(Options, ID, F, RS, ReqCtx, transit),
    State#{
        storage_machine := NewStorageMachine,
        storage_context := NewStorageContext
    }.

-spec handle_status_transition(
    From :: machine_status(), To :: machine_status(), request_context(), deadline(), state()
) -> _.
handle_status_transition({error, _Reason, _}, _Any, ReqCtx, Deadline, State) ->
    emit_repaired_beat(ReqCtx, Deadline, State);
handle_status_transition(_Any, {error, _Reason, _}, _, _, _State) ->
    %% Can't put machine_lifetime_failed beat here because
    %% nonexistant storage_machines can also fail on init
    ok;
handle_status_transition(_Any, {waiting, TargetTimestamp, _, _}, _, _, State) ->
    try_send_timer_task(timers, TargetTimestamp, State);
handle_status_transition(_Any, {retrying, TargetTimestamp, _, _, _}, _, _, State) ->
    try_send_timer_task(timers_retries, TargetTimestamp, State);
handle_status_transition(_FromStatus, _ToStatus, _ReqCtx, _Deadline, _State) ->
    ok.

-spec try_acquire_scheduler(scheduler_type(), state()) -> state().
try_acquire_scheduler(SchedulerType, #{schedulers := Schedulers, options := Options} = State) ->
    case get_scheduler_ref(SchedulerType, Options) of
        undefined ->
            State;
        SchedulerRef ->
            State#{schedulers => Schedulers#{SchedulerType => SchedulerRef}}
    end.

-spec get_scheduler_ref(scheduler_type(), options()) -> scheduler_ref() | undefined.
get_scheduler_ref(SchedulerType, Options) ->
    case maps:find(SchedulerType, maps:get(schedulers, Options, #{})) of
        {ok, Config} ->
            SchedulerID = scheduler_id(SchedulerType, Options),
            {SchedulerID, scheduler_cutoff(Config)};
        _Disabled ->
            undefined
    end.

-spec try_send_timer_task(scheduler_type(), mg_skd_task:target_time(), state()) -> ok.
try_send_timer_task(SchedulerType, TargetTime, #{id := ID, schedulers := Schedulers}) ->
    case maps:get(SchedulerType, Schedulers, undefined) of
        {SchedulerID, Cutoff} when is_integer(Cutoff) ->
            % Ok let's send if it's not too far in the future.
            CurrentTime = mg_skd_task:current_time(),
            case TargetTime =< CurrentTime + Cutoff of
                true ->
                    Task = mg_core_queue_timer:build_task(ID, TargetTime),
                    mg_skd:send_task(SchedulerID, Task);
                false ->
                    ok
            end;
        {_SchedulerID, undefined} ->
            % No defined cutoff, can't make decisions.
            ok;
        undefined ->
            % No scheduler to send task to.
            ok
    end.

-spec remove_from_storage(request_context(), deadline(), state()) -> state().
remove_from_storage(ReqCtx, Deadline, State) ->
    #{namespace := NS, id := ID, options := Options, storage_context := StorageContext} = State,
    F = fun() ->
        mg_core_storage:delete(
            storage_options(Options),
            ID,
            StorageContext
        )
    end,
    RS = retry_strategy(storage, Options, Deadline),
    ok = do_with_retry(Options, ID, F, RS, ReqCtx, remove),
    ok = emit_beat(Options, #mg_core_machine_lifecycle_removed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx
    }),
    State#{storage_machine := nonexistent, storage_context := undefined}.

-spec retry_strategy(retry_subj(), options(), deadline()) -> genlib_retry:strategy().
retry_strategy(Subj, Options, Deadline) ->
    retry_strategy(Subj, Options, Deadline, undefined, undefined).

-spec retry_strategy(Subj, Options, Deadline, InitialTs, Attempt) -> genlib_retry:strategy() when
    Subj :: retry_subj(),
    Options :: options(),
    Deadline :: deadline(),
    InitialTs :: genlib_time:ts() | undefined,
    Attempt :: non_neg_integer() | undefined.
retry_strategy(Subj, Options, Deadline, InitialTs, Attempt) ->
    Retries = maps:get(retries, Options, #{}),
    Policy = maps:get(Subj, Retries, ?DEFAULT_RETRY_POLICY),
    Strategy = genlib_retry:new_strategy(Policy, InitialTs, Attempt),
    mg_core_retry:constrain(Strategy, Deadline).

-spec emit_pre_process_beats(processor_impact(), request_context(), deadline(), state()) -> ok.
emit_pre_process_beats(Impact, ReqCtx, Deadline, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_process_started{
        processor_impact = Impact,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline
    }),
    emit_pre_process_timer_beats(Impact, ReqCtx, Deadline, State).

-spec emit_pre_process_timer_beats(processor_impact(), request_context(), deadline(), state()) ->
    ok.
emit_pre_process_timer_beats(timeout, ReqCtx, Deadline, State) ->
    #{
        id := ID,
        options := #{namespace := NS} = Options,
        storage_machine := #{status := Status}
    } = State,
    {ok, QueueName, TargetTimestamp} = extract_timer_queue_info(Status),
    emit_beat(Options, #mg_core_timer_process_started{
        queue = QueueName,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = TargetTimestamp,
        deadline = Deadline
    });
emit_pre_process_timer_beats(_Impact, _ReqCtx, _Deadline, _State) ->
    ok.

-spec emit_post_process_beats(
    processor_impact(),
    request_context(),
    deadline(),
    integer(),
    state()
) -> ok.
emit_post_process_beats(Impact, ReqCtx, Deadline, Duration, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_process_finished{
        processor_impact = Impact,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline,
        duration = Duration
    }),
    emit_post_process_timer_beats(Impact, ReqCtx, Deadline, Duration, State).

-spec emit_post_process_timer_beats(
    processor_impact(),
    request_context(),
    deadline(),
    integer(),
    state()
) -> ok.
emit_post_process_timer_beats(timeout, ReqCtx, Deadline, Duration, State) ->
    #{
        id := ID,
        options := #{namespace := NS} = Options,
        storage_machine := #{status := Status}
    } = State,
    {ok, QueueName, TargetTimestamp} = extract_timer_queue_info(Status),
    emit_beat(Options, #mg_core_timer_process_finished{
        queue = QueueName,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = TargetTimestamp,
        deadline = Deadline,
        duration = Duration
    });
emit_post_process_timer_beats(_Impact, _ReqCtx, _Deadline, _Duration, _State) ->
    ok.

-spec extract_timer_queue_info(machine_status()) ->
    {ok, normal | retries, genlib_time:ts()} | {error, not_timer}.
extract_timer_queue_info({waiting, Timestamp, _, _}) ->
    {ok, normal, Timestamp};
extract_timer_queue_info({retrying, Timestamp, _, _, _}) ->
    {ok, retries, Timestamp};
extract_timer_queue_info(_Other) ->
    {error, not_timer}.

-spec emit_machine_load_beat(
    options(),
    mg_core:ns(),
    mg_core:id(),
    request_context(),
    StorageMachine
) -> ok when
    StorageMachine :: storage_machine() | unknown | nonexistent.
emit_machine_load_beat(Options, Namespace, ID, ReqCtx, nonexistent) ->
    ok = emit_beat(Options, #mg_core_machine_lifecycle_created{
        namespace = Namespace,
        machine_id = ID,
        request_context = ReqCtx
    });
emit_machine_load_beat(Options, Namespace, ID, ReqCtx, _StorageMachine) ->
    ok = emit_beat(Options, #mg_core_machine_lifecycle_loaded{
        namespace = Namespace,
        machine_id = ID,
        request_context = ReqCtx
    }).

%%

-spec manager_options(options()) -> mg_core_workers_manager:options().
manager_options(#{namespace := NS, worker := ManagerOptions, pulse := Pulse} = Options) ->
    ManagerOptions#{
        name => NS,
        pulse => Pulse,
        worker_options => maps:merge(
            maps:get(worker_options, ManagerOptions, #{}),
            #{
                worker => {?MODULE, Options}
            }
        )
    }.

-spec storage_options(options()) -> mg_core_storage:options().
storage_options(#{namespace := NS, storage := StorageOptions, pulse := Handler}) ->
    {Mod, Options} = mg_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, ?MODULE, machines}, pulse => Handler}}.

-spec notification_options(options()) -> mg_core_notification:options().
notification_options(#{notification := NotificationOptions}) ->
    NotificationOptions.

-spec scheduler_child_spec(scheduler_type(), options()) -> supervisor:child_spec() | undefined.
scheduler_child_spec(SchedulerType, Options) ->
    case maps:get(SchedulerType, maps:get(schedulers, Options, #{}), disable) of
        disable ->
            undefined;
        Config ->
            SchedulerID = scheduler_id(SchedulerType, Options),
            SchedulerOptions = scheduler_options(SchedulerType, Options, Config),
            mg_skd_sup:child_spec(SchedulerID, SchedulerOptions, SchedulerType)
    end.

-spec scheduler_id(scheduler_type(), options()) -> mg_skd:id() | undefined.
scheduler_id(SchedulerType, #{namespace := NS}) ->
    {SchedulerType, NS}.

-spec scheduler_options(scheduler_type(), options(), scheduler_opt()) ->
    mg_skd_sup:options().
scheduler_options(SchedulerType, Options, Config) when
    SchedulerType == timers;
    SchedulerType == timers_retries
->
    TimerQueue =
        case SchedulerType of
            timers -> waiting;
            timers_retries -> retrying
        end,
    HandlerOptions = #{
        processing_timeout => maps:get(timer_processing_timeout, Options, undefined),
        timer_queue => TimerQueue,
        min_scan_delay => maps:get(min_scan_delay, Config, undefined),
        lookahead => scheduler_cutoff(Config)
    },
    scheduler_options(mg_core_queue_timer, Options, HandlerOptions, Config);
scheduler_options(overseer, Options, Config) ->
    HandlerOptions = #{
        min_scan_delay => maps:get(min_scan_delay, Config, undefined),
        rescan_delay => maps:get(rescan_delay, Config, undefined)
    },
    scheduler_options(mg_core_queue_interrupted, Options, HandlerOptions, Config);
scheduler_options(notification = SchedulerType, Options, Config) ->
    HandlerOptions = #{
        scheduler_id => scheduler_id(SchedulerType, Options),
        notification => notification_options(Options),
        processing_timeout => maps:get(notification_processing_timeout, Options, undefined),
        min_scan_delay => maps:get(min_scan_delay, Config, undefined),
        rescan_delay => maps:get(rescan_delay, Config, undefined),
        scan_handicap => maps:get(scan_handicap, Config, undefined),
        scan_cutoff => maps:get(scan_cutoff, Config, undefined),
        reschedule_time => maps:get(reschedule_time, Config, undefined)
    },
    scheduler_options(mg_core_queue_notifications, Options, HandlerOptions, Config).

-spec scheduler_options(module(), options(), map(), scheduler_opt()) -> mg_skd_sup:options().
scheduler_options(HandlerMod, Options, HandlerOptions, Config) ->
    #{
        pulse := Pulse
    } = Options,
    FullHandlerOptions = genlib_map:compact(
        maps:merge(
            #{
                pulse => Pulse,
                machine => Options
            },
            HandlerOptions
        )
    ),
    Handler = {HandlerMod, FullHandlerOptions},
    genlib_map:compact(#{
        start_interval => maps:get(start_interval, Config, undefined),
        capacity => maps:get(capacity, Config, ?DEFAULT_SCHEDULER_CAPACITY),
        quota_name => maps:get(task_quota, Config, unlimited),
        quota_share => maps:get(task_share, Config, 1),
        queue_handler => Handler,
        max_scan_limit => maps:get(max_scan_limit, Config, undefined),
        scan_ahead => maps:get(scan_ahead, Config, undefined),
        task_handler => Handler,
        pulse => Pulse
    }).

-spec scheduler_cutoff(scheduler_opt()) -> seconds().
scheduler_cutoff(#{target_cutoff := Cutoff}) ->
    Cutoff;
scheduler_cutoff(#{min_scan_delay := MinScanDelay}) ->
    erlang:convert_time_unit(MinScanDelay, millisecond, second);
scheduler_cutoff(#{}) ->
    undefined.

-spec get_options(atom(), options()) -> _.
get_options(Subj, Options) ->
    maps:get(Subj, Options).

-spec try_suicide(state(), request_context()) -> ok | no_return().
try_suicide(
    #{options := Options = #{suicide_probability := Prob}, id := ID, namespace := NS},
    ReqCtx
) ->
    case (Prob =/= undefined) andalso (rand:uniform() < Prob) of
        true ->
            ok = emit_beat(Options, #mg_core_machine_lifecycle_committed_suicide{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                suicide_probability = Prob
            }),
            erlang:exit(self(), kill);
        false ->
            ok
    end;
try_suicide(#{}, _) ->
    ok.

-spec attach_otel_ctx(mg_core_events_machine:request_context()) -> ok.
attach_otel_ctx(ReqCtx) ->
    mg_core_otel:maybe_attach_otel_ctx(request_context_to_otel_context(ReqCtx)).

-spec request_context_to_otel_context(mg_core_events_machine:request_context()) -> otel_ctx:t().
request_context_to_otel_context(Ctx) ->
    {_WoodyContext, OtelContext} = woody_rpc_helper:decode_rpc_context(Ctx),
    OtelContext.

%%
%% retrying
%%
-spec do_with_retry(
    options(),
    mg_core:id(),
    fun(() -> R),
    genlib_retry:strategy(),
    request_context(),
    atom()
) -> R.
do_with_retry(#{namespace := NS} = Options, ID, Fun, RetryStrategy, ReqCtx, BeatCtx) ->
    try
        Fun()
    catch
        throw:(Reason = {transient, _}):ST ->
            NextStep = genlib_retry:next_step(RetryStrategy),
            ok = emit_beat(Options, #mg_core_machine_lifecycle_transient_error{
                context = BeatCtx,
                namespace = NS,
                machine_id = ID,
                exception = {throw, Reason, ST},
                request_context = ReqCtx,
                retry_strategy = RetryStrategy,
                retry_action = NextStep
            }),
            case NextStep of
                {wait, Timeout, NewRetryStrategy} ->
                    ok = timer:sleep(Timeout),
                    do_with_retry(Options, ID, Fun, NewRetryStrategy, ReqCtx, BeatCtx);
                finish ->
                    throw(Reason)
            end
    end.

%%
%% logging
%%

-spec emit_beat(options(), mpulse:beat()) -> ok.
emit_beat(#{pulse := Handler}, Beat) ->
    ok = mpulse:handle_beat(Handler, Beat).
