-module(mg_woody_api_test_worker).

-export([child_spec/3]).
-export([start_link/1]).
-export([init/1]).

-spec child_spec(_, _, term()) -> supervisor:child_spec().
child_spec(_, _, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [undefined]},
        type => worker
    }.

-spec start_link(_) -> {ok, pid()}.
start_link(_) ->
    gen_server:start_link(?MODULE, [], []).

-spec init(_) -> {ok, map()}.
init(_) ->
    {ok, #{}}.
