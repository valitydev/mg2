%% Timer operations

-record(mg_core_timer_lifecycle_created, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    target_timestamp :: genlib_time:ts()
}).

-record(mg_core_timer_lifecycle_removed, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_timer_lifecycle_rescheduled, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    target_timestamp :: genlib_time:ts(),
    attempt :: non_neg_integer()
}).

-record(mg_core_timer_lifecycle_rescheduling_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    exception :: mg_utils:exception()
}).

%% Timer processing

-record(mg_core_timer_process_started, {
    queue :: normal | retries,
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    target_timestamp :: genlib_time:ts(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_timer_process_finished, {
    queue :: normal | retries,
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    target_timestamp :: genlib_time:ts(),
    deadline :: mg_core_deadline:deadline(),
    % in native units
    duration :: non_neg_integer()
}).

%% Machine

-record(mg_core_machine_process_transient_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    exception :: mg_utils:exception(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_process_started, {
    processor_impact :: mg_core_machine:processor_impact(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_machine_process_finished, {
    processor_impact :: mg_core_machine:processor_impact(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    % in native units
    duration :: non_neg_integer()
}).

%% Machines state

-record(mg_core_machine_lifecycle_loaded, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_lifecycle_created, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_lifecycle_removed, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_lifecycle_unloaded, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id()
}).

-record(mg_core_machine_lifecycle_committed_suicide, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    suicide_probability :: mg_core_machine:suicide_probability()
}).

-record(mg_core_machine_lifecycle_failed, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    exception :: mg_utils:exception()
}).

-record(mg_core_machine_lifecycle_repaired, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_machine_lifecycle_loading_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    exception :: mg_utils:exception()
}).

-record(mg_core_machine_lifecycle_transient_error, {
    context :: atom(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    exception :: mg_utils:exception(),
    request_context :: mg_core:request_context(),
    retry_strategy :: genlib_retry:strategy(),
    retry_action :: {wait, timeout(), genlib_retry:strategy()} | finish
}).

%% Machine notification

-record(mg_core_machine_notification_created, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    notification_id :: mg_core:id(),
    target_timestamp :: genlib_time:ts()
}).

-record(mg_core_machine_notification_delivered, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    notification_id :: mg_core:id()
}).

-record(mg_core_machine_notification_delivery_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    notification_id :: mg_core:id(),
    exception :: mg_utils:exception(),
    action :: delete | {reschedule, genlib_time:ts()} | ignore
}).

%% Storage operations
%% Duration is in native units

-record(mg_core_storage_get_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_get_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_core_storage_put_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_put_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_core_storage_search_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_search_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_core_storage_delete_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_delete_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

%% Workers management

-record(mg_core_worker_call_attempt, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_worker_start_attempt, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    msg_queue_len :: non_neg_integer(),
    msg_queue_limit :: mg_core_workers_manager:queue_limit()
}).
