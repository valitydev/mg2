-module(mg_riak_pulse).

-include_lib("mg_riak/include/pulse.hrl").

%% API
-export_type([beat/0]).
-export([handle_beat/2]).

%%
%% API
%%
-type beat() ::
    % Riak client call handling
    #mg_riak_client_get_start{}
    | #mg_riak_client_get_finish{}
    | #mg_riak_client_put_start{}
    | #mg_riak_client_put_finish{}
    | #mg_riak_client_search_start{}
    | #mg_riak_client_search_finish{}
    | #mg_riak_client_delete_start{}
    | #mg_riak_client_delete_finish{}
    % Riak client call handling
    | #mg_riak_connection_pool_state_reached{}
    | #mg_riak_connection_pool_connection_killed{}
    | #mg_riak_connection_pool_error{}.

-spec handle_beat(any(), beat() | _OtherBeat) -> ok.
handle_beat(_Options, _Beat) ->
    ok.
