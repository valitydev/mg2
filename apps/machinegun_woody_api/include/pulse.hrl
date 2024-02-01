-record(woody_request_handle_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core_events_machine:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    exception :: mg_core_utils:exception()
}).

-record(woody_event, {
    event :: woody_event_handler:event(),
    rpc_id :: woody:rpc_id(),
    event_meta :: woody_event_handler:event_meta()
}).
