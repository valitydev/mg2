%%%-------------------------------------------------------------------
%% @doc mg_cth_neighbour public API
%% @end
%%%-------------------------------------------------------------------

-module(mg_cth_neighbour_app).

-behaviour(application).

-export([start/2, stop/1]).

-spec start(_, _) -> _.
start(_StartType, _StartArgs) ->
    mg_cth_neighbour_sup:start_link().

-spec stop(_) -> _.
stop(_State) ->
    ok.

%% internal functions
