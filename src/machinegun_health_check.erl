-module(machinegun_health_check).

-export([consuela/0]).
-export([global/0]).
-export([startup/0]).

-spec consuela() -> {erl_health:status(), erl_health:details()}.
consuela() ->
    case consuela:test() of
        ok -> {passing, []};
        {error, Reason} -> {critical, genlib:format(Reason)}
    end.

-spec global() -> {erl_health:status(), erl_health:details()}.
global() ->
    ClusterSize = mg_core_union:cluster_size(),
    ConnectedCount = erlang:length(erlang:nodes()),
    case is_quorum(ClusterSize, ConnectedCount) of
        true ->
            {passing, []};
        false ->
            Reason =
                <<"union. no quorum. cluster size: ", (erlang:integer_to_binary(ClusterSize))/binary, ", online: ",
                    (erlang:integer_to_binary(ConnectedCount + 1))/binary>>,
            {critical, Reason}
    end.

-spec startup() -> {erl_health:status(), erl_health:details()}.
startup() ->
    %% maybe any checks?
    logger:info("union. node ~p started", [node()]),
    {passing, []}.

%% Internal functions

-spec is_quorum(non_neg_integer(), integer()) -> boolean().
is_quorum(1, _) ->
    true;
is_quorum(ClusterSize, ConnectedCount) ->
    ConnectedCount >= ClusterSize div 2.
