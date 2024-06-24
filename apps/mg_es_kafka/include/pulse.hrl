%% Events sink operations

-record(mg_event_sink_kafka_sent, {
    name :: atom(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    % in native units
    encode_duration :: non_neg_integer(),
    % in native units
    send_duration :: non_neg_integer(),
    % in bytes
    data_size :: non_neg_integer(),
    partition :: brod:partition(),
    offset :: brod:offset()
}).
