-ifndef(__mg_cth__).
-define(__mg_cth__, 42).

-define(EMPTY_ID, <<"">>).
-define(ES_ID, <<"test_event_sink_2">>).

-define(DEADLINE_TIMEOUT, 1000).

-define(CLIENT, mg_cth_kafka_client).
-define(BROKERS_ADVERTIZED, [{"kafka1", 9092}]).
-define(BROKERS, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]).

-define(flushMailbox(__Acc0),
    (fun __Flush(__Acc) ->
        receive
            __M -> __Flush([__M | __Acc])
        after 0 -> __Acc
        end
    end)(
        __Acc0
    )
).

-define(assertReceive(__Expr),
    ?assertReceive(__Expr, 1000)
).

-define(assertReceive(__Expr, __Timeout),
    (fun() ->
        receive
            (__Expr) = __V -> __V
        after __Timeout ->
            erlang:error(
                {assertReceive, [
                    {module, ?MODULE},
                    {line, ?LINE},
                    {expression, (??__Expr)},
                    {mailbox, ?flushMailbox([])}
                ]}
            )
        end
    end)()
).

-define(assertNoReceive(),
    ?assertNoReceive(1000)
).

-define(assertNoReceive(__Timeout),
    (fun() ->
        receive
            __Message ->
                erlang:error(
                    {assertNoReceive, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {mailbox, ?flushMailbox([__Message])}
                    ]}
                )
        after __Timeout -> ok
        end
    end)()
).

-endif.
