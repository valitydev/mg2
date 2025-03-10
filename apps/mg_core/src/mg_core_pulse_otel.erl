-module(mg_core_pulse_otel).

-include_lib("mg_core/include/pulse.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% mg_pulse handler
-behaviour(mpulse).

-export([handle_beat/2]).

%% TODO Specify available options if any
-type options() :: map().

-type beat() ::
    mg_core:beat()
    | mg_skd:beat()
    | mg_skd_scanner:beat().

-export_type([options/0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.
%%
%% Machinegun core beats
%% ============================================================================
%%
%% Timer
%% Event machine action 'set_timer' performed.
handle_beat(_Options, #mg_core_timer_lifecycle_created{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"timer created">>, mg_core_otel:machine_tags(NS, ID));
%% In case of transient error during processing 'timeout' machine will try to
%% reschedule its next action according to configured 'timers' retry strategy.
%% Then it transitions to new state with status 'retrying' and emits this beat.
handle_beat(_Options, #mg_core_timer_lifecycle_rescheduled{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"timer rescheduled">>, mg_core_otel:machine_tags(NS, ID));
%% Since rescheduling produces new state transition, it can fail transiently.
%% If thrown exception has types 'transient' or 'timeout' then this beat is
%% emitted.
handle_beat(
    _Options,
    #mg_core_timer_lifecycle_rescheduling_error{machine_id = ID, namespace = NS, exception = Exception}
) ->
    mg_core_otel:record_exception(Exception, mg_core_otel:machine_tags(NS, ID));
%% Event machine timer removed: action 'unset_timer' performed.
handle_beat(_Options, #mg_core_timer_lifecycle_removed{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"timer removed">>, mg_core_otel:machine_tags(NS, ID));
%% Scheduler handling
%% TODO Handle and trace events for 'mg_skd_*' beats
%% Timer handling
%% Wraps `Module:process_machine/7` when processor impact is 'timeout'.
handle_beat(_Options, #mg_core_timer_process_started{machine_id = _ID, namespace = _NS, queue = _Queue}) ->
    ok;
handle_beat(_Options, #mg_core_timer_process_finished{queue = _Queue}) ->
    ok;
%% Machine process state
%% Machine created and loaded
%% Mind that loading of machine state happens in its worker' process context
%% and not during call to supervisor.
handle_beat(_Options, #mg_core_machine_lifecycle_created{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"machine created">>, mg_core_otel:machine_tags(NS, ID));
%% Removal of machine (from storage); signalled by 'remove' action in a new
%% processed state.
handle_beat(_Options, #mg_core_machine_lifecycle_removed{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"machine removed">>, mg_core_otel:machine_tags(NS, ID));
%% Existing machine loaded.
handle_beat(_Options, #mg_core_machine_lifecycle_loaded{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"machine loaded">>, mg_core_otel:machine_tags(NS, ID));
%% When machine's worker process handles scheduled timeout timer and stops
%% normally.
handle_beat(_Options, #mg_core_machine_lifecycle_unloaded{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"machine unloaded">>, mg_core_otel:machine_tags(NS, ID));
%% Machine can be configured with probability of suicide via
%% "erlang:exit(self(), kill)". Each time machine successfully completes
%% `Module:process_machine/7` call and before persisting transition artifacts
%% (including its very own new state snapshot), it attempts a suicide.
handle_beat(_Options, #mg_core_machine_lifecycle_committed_suicide{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"machine committed suicide">>, mg_core_otel:machine_tags(NS, ID));
%% When existing machine with nonerroneous state fails to handle processor
%% response it transitions to special 'failed' state.
%% See `mg_core_machine:machine_status/0`:
%% "{error, Reason :: term(), machine_regular_status()}".
%% NOTE Nonexisting machine can also fail on init.
handle_beat(_Options, #mg_core_machine_lifecycle_failed{exception = Exception, machine_id = ID, namespace = NS}) ->
    mg_core_otel:record_exception(Exception, mg_core_otel:machine_tags(NS, ID));
%% This event occrurs once existing machine successfully transitions from
%% special 'failed' state, but before it's new state persistence in storage.
handle_beat(_Options, #mg_core_machine_lifecycle_repaired{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"machine repaired">>, mg_core_otel:machine_tags(NS, ID));
%% When failed to load machine.
handle_beat(_Options, #mg_core_machine_lifecycle_loading_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    mg_core_otel:record_exception(Exception, mg_core_otel:machine_tags(NS, ID));
%% Transient error when removing or persisting machine state transition.
handle_beat(_Options, #mg_core_machine_lifecycle_transient_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    mg_core_otel:record_exception(Exception, mg_core_otel:machine_tags(NS, ID));
%% Machine call handling
%% Wraps core machine call `Module:process_machine/7`.
handle_beat(
    _Options,
    #mg_core_machine_process_started{processor_impact = _ProcessorImpact, machine_id = _ID, namespace = _NS}
) ->
    ok;
handle_beat(_Options, #mg_core_machine_process_finished{processor_impact = _ProcessorImpact}) ->
    ok;
%% Transient error _throw_n from during state processing.
handle_beat(_Options, #mg_core_machine_process_transient_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    mg_core_otel:record_exception(Exception, mg_core_otel:machine_tags(NS, ID));
%% Machine notification
handle_beat(_Options, #mg_core_machine_notification_created{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"notification created">>, mg_core_otel:machine_tags(NS, ID));
handle_beat(_Options, #mg_core_machine_notification_delivered{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"notification delivered">>, mg_core_otel:machine_tags(NS, ID));
handle_beat(_Options, #mg_core_machine_notification_delivery_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    mg_core_otel:record_exception(Exception, mg_core_otel:machine_tags(NS, ID));
%% Machine worker handling
%% Happens upon worker's gen_server call.
handle_beat(_Options, #mg_core_worker_call_attempt{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"worker call attempt">>, mg_core_otel:machine_tags(NS, ID));
%% Upon worker's gen_server start.
handle_beat(_Options, #mg_core_worker_start_attempt{machine_id = ID, namespace = NS}) ->
    mg_core_otel:add_event(<<"worker start attempt">>, mg_core_otel:machine_tags(NS, ID));
%% Storage calls
%% NOTE It is expected that machine process executes storage calls strictly sequentially
%% Get
handle_beat(_Options, #mg_core_storage_get_start{name = Name}) ->
    mg_core_otel:span_start(Name, mk_storge_span_name(Name, get), storage_span_opts(Name));
handle_beat(_Options, #mg_core_storage_get_finish{name = Name}) ->
    mg_core_otel:span_end(Name);
%% Put
handle_beat(_Options, #mg_core_storage_put_start{name = Name}) ->
    mg_core_otel:span_start(Name, mk_storge_span_name(Name, put), storage_span_opts(Name));
handle_beat(_Options, #mg_core_storage_put_finish{name = Name}) ->
    mg_core_otel:span_end(Name);
%% Search
handle_beat(_Options, #mg_core_storage_search_start{name = Name}) ->
    mg_core_otel:span_start(Name, mk_storge_span_name(Name, search), storage_span_opts(Name));
handle_beat(_Options, #mg_core_storage_search_finish{name = Name}) ->
    mg_core_otel:span_end(Name);
%% Delete
handle_beat(_Options, #mg_core_storage_delete_start{name = Name}) ->
    mg_core_otel:span_start(Name, mk_storge_span_name(Name, delete), storage_span_opts(Name));
handle_beat(_Options, #mg_core_storage_delete_finish{name = Name}) ->
    mg_core_otel:span_end(Name);
%% Disregard any other
handle_beat(_Options, _Beat) ->
    ok.

%% Internal

-spec mk_storge_span_name(mg_core_storage:name(), atom()) -> binary().
mk_storge_span_name({_NS, _Mod, machines}, search) ->
    <<"searching machines">>;
mk_storge_span_name({_NS, _Mod, machines}, get) ->
    <<"getting machine">>;
mk_storge_span_name({_NS, _Mod, machines}, put) ->
    <<"updating machine">>;
mk_storge_span_name({_NS, _Mod, machines}, delete) ->
    <<"deleting machine">>;
mk_storge_span_name({_NS, _Mod, events}, search) ->
    <<"searching events">>;
mk_storge_span_name({_NS, _Mod, events}, get) ->
    <<"getting event">>;
mk_storge_span_name({_NS, _Mod, events}, put) ->
    <<"updating event">>;
mk_storge_span_name({_NS, _Mod, events}, delete) ->
    <<"deleting event">>;
mk_storge_span_name({_NS, _Mod, notifications}, search) ->
    <<"searching notifications">>;
mk_storge_span_name({_NS, _Mod, notifications}, get) ->
    <<"getting notification">>;
mk_storge_span_name({_NS, _Mod, notifications}, put) ->
    <<"updating notification">>;
mk_storge_span_name({_NS, _Mod, notifications}, delete) ->
    <<"deleting notification">>;
mk_storge_span_name(StorageName, OperationType) ->
    iolist_to_binary(io_lib:format("~p:~s", [StorageName, OperationType])).

-spec storage_span_opts(Name :: {mg_core:ns(), module(), atom()} | term()) -> otel_span:start_opts().
storage_span_opts({NS, _Mod, _Type}) ->
    #{kind => ?SPAN_KIND_INTERNAL, attributes => mg_core_otel:machine_tags(NS, undefined)};
storage_span_opts(_Name) ->
    #{kind => ?SPAN_KIND_INTERNAL}.
