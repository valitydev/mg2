%% Riak client operations
%% Duration is in native units

-record(mg_riak_client_get_start, {
    name :: mg_core_storage:name()
}).

-record(mg_riak_client_get_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_riak_client_put_start, {
    name :: mg_core_storage:name()
}).

-record(mg_riak_client_put_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_riak_client_search_start, {
    name :: mg_core_storage:name()
}).

-record(mg_riak_client_search_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_riak_client_delete_start, {
    name :: mg_core_storage:name()
}).

-record(mg_riak_client_delete_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

%% Riak connection pool events

-record(mg_riak_connection_pool_state_reached, {
    name :: mg_core_storage:name(),
    state :: no_free_connections | queue_limit_reached
}).

-record(mg_riak_connection_pool_connection_killed, {
    name :: mg_core_storage:name(),
    state :: free | in_use
}).

-record(mg_riak_connection_pool_error, {
    name :: mg_core_storage:name(),
    reason :: connect_timeout
}).
