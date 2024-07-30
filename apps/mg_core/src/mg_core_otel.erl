-module(mg_core_otel).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-export([pack_otel_stub/1]).
-export([restore_otel_stub/2]).
-export([maybe_attach_otel_ctx/1]).

-export([span_start/3]).
-export([span_end/1]).
-export([record_current_span_ctx/3]).
-export([add_event/2]).
-export([record_exception/2]).

-export([current_span_id/1]).

-export([impact_to_machine_activity/1]).
-export([machine_tags/2]).
-export([machine_tags/3]).
-export([event_range_to_attributes/1]).

-type packed_otel_stub() :: [mg_core_storage:opaque()].

-export_type([packed_otel_stub/0]).

-define(SPANS_STACK, 'spans_ctx_stack').

%%

%% @doc Packs OTEL context for storage.
-spec pack_otel_stub(otel_ctx:t()) -> packed_otel_stub().
pack_otel_stub(Ctx) ->
    case otel_tracer:current_span_ctx(Ctx) of
        undefined ->
            [];
        #span_ctx{trace_id = TraceID, span_id = SpanID, trace_flags = TraceFlags} ->
            [trace_id_to_binary(TraceID), span_id_to_binary(SpanID), TraceFlags]
    end.

%% @doc Restores OTEL context with current span. Restored span context
%% status is nor actual nor have according data in OTEL storage
%% backend. Its only purpose is to preserve ability to start new child
%% spans in compliance with OTEL tracer API.
%%
%% Restored otel span can be unfinished if machine is interrupted
%% with node stop, thus span data is lost anyway.
%%
%% We can't get around this issue without implementing our own
%% tracer with distributed storage with write order guarantee.
%%
%% However we can start new span for 'resumption' signal. And set
%% original machine start call as its parent. Same goes for 'timeouts'
%% and 'retries' signals.
-spec restore_otel_stub(otel_ctx:t(), packed_otel_stub()) -> otel_ctx:t().
restore_otel_stub(Ctx, [TraceID, SpanID, TraceFlags]) ->
    SpanCtx = otel_tracer:from_remote_span(binary_to_id(TraceID), binary_to_id(SpanID), TraceFlags),
    %% NOTE Thus restored span context is considered being remote and not recording.
    otel_tracer:set_current_span(Ctx, SpanCtx);
restore_otel_stub(Ctx, _Other) ->
    Ctx.

-spec maybe_attach_otel_ctx(otel_ctx:t()) -> ok.
maybe_attach_otel_ctx(NewCtx) when map_size(NewCtx) =:= 0 ->
    %% Don't attach empty context
    ok;
maybe_attach_otel_ctx(NewCtx) ->
    _ = otel_ctx:attach(choose_viable_otel_ctx(NewCtx, otel_ctx:get_current())),
    ok.

%% lowest bit is if it is sampled
-define(IS_NOT_SAMPLED(SpanCtx), SpanCtx#span_ctx.trace_flags band 2#1 =/= 1).

-spec choose_viable_otel_ctx(T, T) -> T when T :: otel_ctx:t().
choose_viable_otel_ctx(NewCtx, CurrentCtx) ->
    case {otel_tracer:current_span_ctx(NewCtx), otel_tracer:current_span_ctx(CurrentCtx)} of
        %% Don't attach if new context is without sampled span and old
        %% context has span defined
        {SpanCtx = #span_ctx{}, #span_ctx{}} when ?IS_NOT_SAMPLED(SpanCtx) -> CurrentCtx;
        {undefined, #span_ctx{}} -> CurrentCtx;
        {_, _} -> NewCtx
    end.

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

-spec current_span_id(otel_ctx:t()) -> opentelemetry:span_id().
current_span_id(Ctx) ->
    span_id(otel_tracer:current_span_ctx(Ctx)).

-spec impact_to_machine_activity(mg_core_machine:processor_impact()) -> binary().
impact_to_machine_activity(ProcessorImpact) when is_tuple(ProcessorImpact) ->
    atom_to_binary(element(1, ProcessorImpact));
impact_to_machine_activity(ProcessorImpact) when is_atom(ProcessorImpact) ->
    atom_to_binary(ProcessorImpact).

-spec machine_tags(mg_core:ns(), mg_core:id() | undefined) -> map().
machine_tags(Namespace, ID) ->
    machine_tags(Namespace, ID, #{}).

-spec machine_tags(mg_core:ns(), mg_core:id() | undefined, map()) -> map().
machine_tags(Namespace, ID, OtherTags) ->
    genlib_map:compact(
        maps:merge(OtherTags, #{
            <<"mg.machine.ns">> => Namespace,
            <<"mg.machine.id">> => ID
        })
    ).

-spec event_range_to_attributes(mg_core_events:events_range()) -> map().
event_range_to_attributes(undefined) ->
    #{};
event_range_to_attributes({UpperBoundary, LowerBoundary, Direction}) ->
    #{
        <<"mg.machine.event_range.upper_boundary">> => UpperBoundary,
        <<"mg.machine.event_range.lower_boundary">> => LowerBoundary,
        <<"mg.machine.event_range.direction">> =>
            case Direction of
                +1 -> <<"forward">>;
                -1 -> <<"backward">>
            end
    }.

%%

-spec span_id(opentelemetry:span_ctx()) -> opentelemetry:span_id() | undefined.
span_id(#span_ctx{span_id = SpanID}) ->
    SpanID;
span_id(_) ->
    undefined.

-spec trace_id_to_binary(opentelemetry:trace_id()) -> binary().
trace_id_to_binary(TraceID) ->
    {ok, EncodedTraceID} = otel_utils:format_binary_string("~32.16.0b", [TraceID]),
    EncodedTraceID.

-spec span_id_to_binary(opentelemetry:span_id()) -> binary().
span_id_to_binary(SpanID) ->
    {ok, EncodedSpanID} = otel_utils:format_binary_string("~16.16.0b", [SpanID]),
    EncodedSpanID.

-spec binary_to_id(binary()) -> non_neg_integer().
binary_to_id(Opaque) when is_binary(Opaque) ->
    binary_to_integer(Opaque, 16).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-type testgen() :: {_ID, fun(() -> _)}.
-spec test() -> _.

-define(IS_SAMPLED, 1).
-define(NOT_SAMPLED, 0).
-define(OTEL_CTX(IsSampled),
    otel_tracer:set_current_span(
        otel_ctx:new(),
        (otel_tracer_noop:noop_span_ctx())#span_ctx{
            trace_id = otel_id_generator:generate_trace_id(),
            span_id = otel_id_generator:generate_span_id(),
            is_valid = true,
            is_remote = true,
            is_recording = false,
            trace_flags = IsSampled
        }
    )
).

-spec choose_viable_otel_ctx_test_() -> [testgen()].
choose_viable_otel_ctx_test_() ->
    A = ?OTEL_CTX(?IS_SAMPLED),
    B = ?OTEL_CTX(?NOT_SAMPLED),
    [
        ?_assertEqual(A, choose_viable_otel_ctx(A, B)),
        ?_assertEqual(A, choose_viable_otel_ctx(B, A)),
        ?_assertEqual(A, choose_viable_otel_ctx(A, otel_ctx:new())),
        ?_assertEqual(B, choose_viable_otel_ctx(otel_ctx:new(), B)),
        ?_assertEqual(otel_ctx:new(), choose_viable_otel_ctx(otel_ctx:new(), otel_ctx:new()))
    ].

-endif.
