-module(machinegun_pulse_otel).

-include_lib("machinegun_core/include/pulse.hrl").
-include_lib("machinegun_woody_api/include/pulse.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-export([handle_beat/2]).

%% TODO Specify available options if any
-type options() :: map().

-export_type([options/0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), machinegun_pulse:beat()) -> ok.

%%
%% Woody API beats
%% ============================================================================
%%
handle_beat(Options, #woody_event{event = Event, rpc_id = RpcID, event_meta = Meta}) ->
    woody_event_handler_otel:handle_event(Event, RpcID, Meta, Options);
%% Woody server's function handling error beat.
handle_beat(_Options, #woody_request_handle_error{namespace = NS, machine_id = ID, exception = Exception}) ->
    record_exception(Exception, machine_tags(NS, ID));
%%
%% Machinegun core beats
%% ============================================================================
%%
%% Timer
%% Event machine action 'set_timer' performed.
handle_beat(_Options, #mg_core_timer_lifecycle_created{machine_id = ID, namespace = NS}) ->
    add_event(<<"timer created">>, machine_tags(NS, ID));
%% In case of transient error during processing 'timeout' machine will try to
%% reschedule its next action according to configured 'timers' retry strategy.
%% Then it transitions to new state with status 'retrying' and emits this beat.
handle_beat(_Options, #mg_core_timer_lifecycle_rescheduled{machine_id = ID, namespace = NS}) ->
    add_event(<<"timer rescheduled">>, machine_tags(NS, ID));
%% Since rescheduling produces new state transition, it can fail transiently.
%% If thrown exception has types 'transient' or 'timeout' then this beat is
%% emitted.
handle_beat(
    _Options,
    #mg_core_timer_lifecycle_rescheduling_error{machine_id = ID, namespace = NS, exception = Exception}
) ->
    record_exception(Exception, machine_tags(NS, ID));
%% Event machine timer removed: action 'unset_timer' performed.
handle_beat(_Options, #mg_core_timer_lifecycle_removed{machine_id = ID, namespace = NS}) ->
    add_event(<<"timer removed">>, machine_tags(NS, ID));
%% Scheduler handling
%% TODO Handle and trace events for 'mg_core_scheduler_*' beats
%% Timer handling
%% Wraps `Module:process_machine/7` when processor impact is 'timeout'.
handle_beat(_Options, #mg_core_timer_process_started{machine_id = ID, namespace = NS, queue = Queue}) ->
    span_start({process, Queue}, mk_machine_span_name(timeout), #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => machine_tags(NS, ID, #{<<"queue">> => atom_to_binary(Queue)})
    });
handle_beat(_Options, #mg_core_timer_process_finished{queue = Queue}) ->
    span_end({process, Queue});
%% Machine process state
%% Machine created and loaded
%% Mind that loading of machine state happens in its worker' process context
%% and not during call to supervisor.
handle_beat(_Options, #mg_core_machine_lifecycle_created{machine_id = ID, namespace = NS}) ->
    add_event(<<"machine created">>, machine_tags(NS, ID));
%% Removal of machine (from storage); signalled by 'remove' action in a new
%% processed state.
handle_beat(_Options, #mg_core_machine_lifecycle_removed{machine_id = ID, namespace = NS}) ->
    add_event(<<"machine removed">>, machine_tags(NS, ID));
%% Existing machine loaded.
handle_beat(_Options, #mg_core_machine_lifecycle_loaded{machine_id = ID, namespace = NS}) ->
    add_event(<<"machine loaded">>, machine_tags(NS, ID));
%% When machine's worker process handles scheduled timeout timer and stops
%% normally.
handle_beat(_Options, #mg_core_machine_lifecycle_unloaded{machine_id = ID, namespace = NS}) ->
    add_event(<<"machine unloaded">>, machine_tags(NS, ID));
%% Machine can be configured with probability of suicide via
%% "erlang:exit(self(), kill)". Each time machine successfully completes
%% `Module:process_machine/7` call and before persisting transition artifacts
%% (including its very own new state snapshot), it attempts a suicide.
handle_beat(_Options, #mg_core_machine_lifecycle_committed_suicide{machine_id = ID, namespace = NS}) ->
    add_event(<<"machine committed suicide">>, machine_tags(NS, ID));
%% When existing machine with nonerroneous state fails to handle processor
%% response it transitions to special 'failed' state.
%% See `mg_core_machine:machine_status/0`:
%% "{error, Reason :: term(), machine_regular_status()}".
%% NOTE Nonexisting machine can also fail on init.
handle_beat(_Options, #mg_core_machine_lifecycle_failed{exception = Exception, machine_id = ID, namespace = NS}) ->
    record_exception(Exception, machine_tags(NS, ID));
%% This event occrurs once existing machine successfully transitions from
%% special 'failed' state, but before it's new state persistence in storage.
handle_beat(_Options, #mg_core_machine_lifecycle_repaired{machine_id = ID, namespace = NS}) ->
    add_event(<<"machine repaired">>, machine_tags(NS, ID));
%% When failed to load machine.
handle_beat(_Options, #mg_core_machine_lifecycle_loading_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    record_exception(Exception, machine_tags(NS, ID));
%% Transient error when removing or persisting machine state transition.
handle_beat(_Options, #mg_core_machine_lifecycle_transient_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    record_exception(Exception, machine_tags(NS, ID));
%% Machine call handling
%% Wraps core machine call `Module:process_machine/7`.
handle_beat(
    _Options,
    #mg_core_machine_process_started{processor_impact = ProcessorImpact, machine_id = ID, namespace = NS}
) ->
    span_start(to_event_machine_activity(ProcessorImpact), mk_machine_span_name(ProcessorImpact), #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => machine_tags(NS, ID)
    });
handle_beat(_Options, #mg_core_machine_process_finished{processor_impact = ProcessorImpact}) ->
    span_end(to_event_machine_activity(ProcessorImpact));
%% Transient error _throw_n from during state processing.
handle_beat(_Options, #mg_core_machine_process_transient_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    record_exception(Exception, machine_tags(NS, ID));
%% Machine notification
handle_beat(_Options, #mg_core_machine_notification_created{machine_id = ID, namespace = NS}) ->
    add_event(<<"notification created">>, machine_tags(NS, ID));
handle_beat(_Options, #mg_core_machine_notification_delivered{machine_id = ID, namespace = NS}) ->
    add_event(<<"notification delivered">>, machine_tags(NS, ID));
handle_beat(_Options, #mg_core_machine_notification_delivery_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    record_exception(Exception, machine_tags(NS, ID));
%% Machine worker handling
%% Happens upon worker's gen_server call.
handle_beat(_Options, #mg_core_worker_call_attempt{machine_id = ID, namespace = NS}) ->
    add_event(<<"worker call attempt">>, machine_tags(NS, ID));
%% Upon worker's gen_server start.
handle_beat(_Options, #mg_core_worker_start_attempt{machine_id = ID, namespace = NS}) ->
    add_event(<<"worker start attempt">>, machine_tags(NS, ID));
%% Storage calls
%% NOTE It is expected that machine process executes storage calls strictly sequentially
%% Get
handle_beat(_Options, #mg_core_storage_get_start{name = Name}) ->
    span_start(Name, mk_storge_span_name(Name, get), #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_get_finish{name = Name}) ->
    span_end(Name);
%% Put
handle_beat(_Options, #mg_core_storage_put_start{name = Name}) ->
    span_start(Name, mk_storge_span_name(Name, put), #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_put_finish{name = Name}) ->
    span_end(Name);
%% Search
handle_beat(_Options, #mg_core_storage_search_start{name = Name}) ->
    span_start(Name, mk_storge_span_name(Name, search), #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_search_finish{name = Name}) ->
    span_end(Name);
%% Delete
handle_beat(_Options, #mg_core_storage_delete_start{name = Name}) ->
    span_start(Name, mk_storge_span_name(Name, delete), #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_delete_finish{name = Name}) ->
    span_end(Name);
%% Disregard any other
handle_beat(_Options, _Beat) ->
    ok.

%% Internal

-define(SPANS_STACK, 'spans_ctx_stack').

-spec span_start(term(), opentelemetry:span_name(), otel_span:start_opts()) -> ok.
span_start(Key, SpanName, Opts) ->
    Tracer = opentelemetry:get_application_tracer(?MODULE),
    Ctx = otel_ctx:get_current(),
    SpanCtx = otel_tracer:start_span(Ctx, Tracer, SpanName, Opts),
    Ctx1 = record_current_span_ctx(Key, SpanCtx, Ctx),
    Ctx2 = otel_tracer:set_current_span(Ctx1, SpanCtx),
    _ = otel_ctx:attach(Ctx2),
    ok.

-spec record_current_span_ctx(term(), opentelemetry:span_ctx(), otel_ctx:t()) -> otel_ctx:t().
record_current_span_ctx(Key, SpanCtx, Ctx) ->
    Stack = otel_ctx:get_value(Ctx, ?SPANS_STACK, []),
    Entry = {Key, SpanCtx, otel_tracer:current_span_ctx(Ctx)},
    otel_ctx:set_value(Ctx, ?SPANS_STACK, [Entry | Stack]).

-spec span_end(term()) -> ok.
span_end(SpanKey) ->
    Ctx = otel_ctx:get_current(),
    Stack = otel_ctx:get_value(Ctx, ?SPANS_STACK, []),
    %% NOTE Only first occurrence is taken
    case lists:keytake(SpanKey, 1, Stack) of
        false ->
            ok;
        {value, {_Key, SpanCtx, ParentSpanCtx}, Stack1} ->
            _ = otel_span:end_span(SpanCtx, undefined),
            Ctx1 = otel_ctx:set_value(Ctx, ?SPANS_STACK, Stack1),
            Ctx2 = otel_tracer:set_current_span(Ctx1, ParentSpanCtx),
            _ = otel_ctx:attach(Ctx2),
            ok
    end.

-spec add_event(opentelemetry:event_name(), opentelemetry:attributes_map()) -> ok.
add_event(Name, Attributes) ->
    _ = otel_span:add_event(otel_tracer:current_span_ctx(), Name, Attributes),
    ok.

-spec record_exception(mg_core_utils:exception(), opentelemetry:attributes_map()) -> ok.
record_exception({Class, Reason, Stacktrace}, Attributes) ->
    _ = otel_span:record_exception(otel_tracer:current_span_ctx(), Class, Reason, Stacktrace, Attributes),
    ok.

-spec mk_machine_span_name(mg_core_machine:processor_impact()) -> binary().
mk_machine_span_name(ProcessorImpact) ->
    Activity = to_event_machine_activity(ProcessorImpact),
    <<"internal EventMachine:", Activity/binary>>.

-spec to_event_machine_activity(mg_core_machine:processor_impact()) -> binary().
to_event_machine_activity(ProcessorImpact) when is_tuple(ProcessorImpact) ->
    atom_to_binary(element(1, ProcessorImpact));
to_event_machine_activity(ProcessorImpact) when is_atom(ProcessorImpact) ->
    atom_to_binary(ProcessorImpact).

-spec machine_tags(mg_core:ns(), mg_core:id()) -> map().
machine_tags(Namespace, ID) ->
    machine_tags(Namespace, ID, #{}).

-spec machine_tags(mg_core:ns(), mg_core:id(), map()) -> map().
machine_tags(Namespace, ID, OtherTags) ->
    maps:merge(OtherTags, #{
        <<"machine.ns">> => Namespace,
        <<"machine.id">> => ID
    }).

-spec mk_storge_span_name(mg_core_storage:name(), atom()) -> binary().
mk_storge_span_name(StorageName, OperationType) ->
    iolist_to_binary(io_lib:format("internal ~p:~s", [StorageName, OperationType])).
