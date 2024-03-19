-module(mg_core_otel).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").
-include_lib("opentelemetry/src/otel_tracer.hrl").

-export([pack_otel_stub/1]).
-export([restore_otel_stub/2]).
-export([maybe_attach_otel_ctx/1]).

-export([span_start/3]).
-export([span_end/1]).
-export([record_current_span_ctx/3]).
-export([add_event/2]).
-export([record_exception/2]).

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
        #span_ctx{trace_id = TraceID, span_id = SpanID, trace_flags = TraceFlags, is_recording = IsRecording} ->
            [TraceID, SpanID, TraceFlags, IsRecording]
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
restore_otel_stub(Ctx, [TraceID, SpanID, TraceFlags, IsRecording]) ->
    %% Use default tracer for stubbing
    {_Mod, #tracer{on_end_processors = OnEndProcessors}} = opentelemetry:get_application_tracer(?MODULE),
    SpanCtx = #span_ctx{
        trace_flags = TraceFlags,
        tracestate = [],
        is_valid = true,
        is_recording = IsRecording,
        trace_id = TraceID,
        span_id = SpanID,
        %% Default tracer uses `otel_span_ets'
        span_sdk = {otel_span_ets, OnEndProcessors}
    },
    otel_tracer:set_current_span(Ctx, SpanCtx);
restore_otel_stub(Ctx, _Other) ->
    Ctx.

-spec maybe_attach_otel_ctx(otel_ctx:t()) -> ok.
maybe_attach_otel_ctx(OtelCtx) when map_size(OtelCtx) =:= 0 ->
    %% Don't attach empty context
    ok;
maybe_attach_otel_ctx(OtelCtx) ->
    _Old = otel_ctx:attach(OtelCtx),
    ok.

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
