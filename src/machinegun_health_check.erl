-module(machinegun_health_check).

-export([consuela/0]).

-spec consuela() -> {erl_health:status(), erl_health:details()}.
consuela() ->
    case consuela:test() of
        ok -> {passing, []};
        {error, Reason} -> {critical, genlib:format(Reason)}
    end.
