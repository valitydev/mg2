%% Scheduler

-record(mg_skd_search_success, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    delay :: mg_skd_scanner:scan_delay(),
    tasks :: [mg_skd_task:task()],
    limit :: mg_skd_scanner:scan_limit(),
    % in native units
    duration :: non_neg_integer()
}).

-record(mg_skd_search_error, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    exception :: mg_skd_utils:exception()
}).

-record(mg_skd_task_error, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    exception :: mg_skd_utils:exception(),
    machine_id :: mg_skd_utils:id() | undefined
}).

-record(mg_skd_task_add_error, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    exception :: mg_skd_utils:exception(),
    machine_id :: mg_skd_utils:id(),
    request_context :: mg_skd_utils:request_context()
}).

-record(mg_skd_new_tasks, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    new_tasks_count :: non_neg_integer()
}).

-record(mg_skd_task_started, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    machine_id :: mg_skd_utils:id() | undefined,
    task_delay :: timeout()
}).

-record(mg_skd_task_finished, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    machine_id :: mg_skd_utils:id() | undefined,
    task_delay :: timeout(),
    % in native units
    process_duration :: non_neg_integer()
}).

-record(mg_skd_quota_reserved, {
    namespace :: mg_skd_utils:ns(),
    scheduler_name :: mg_skd:name(),
    active_tasks :: non_neg_integer(),
    waiting_tasks :: non_neg_integer(),
    quota_name :: mg_skd_quota_worker:name(),
    quota_reserved :: mg_skd_quota:resource()
}).
