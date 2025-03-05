-module(mg_cth_nbr_processor).

-export([process/3]).

-spec process({_TaskType, _Args, _Process}, _Opts, _Ctx) -> _.
process({init, _InitArgs, _Process}, _Opts, _Ctx) ->
    Result = #{events => [event(1)]},
    {ok, Result};
process(
    %% <<"simple_call">>
    {call, <<131, 109, 0, 0, 0, 11, 115, 105, 109, 112, 108, 101, 95, 99, 97, 108, 108>> = CallArgs, _Process},
    _Opts,
    _Ctx
) ->
    Result = #{
        response => CallArgs,
        events => []
    },
    {ok, Result};
process(
    %% <<"fail_call">>
    {call, <<131, 109, 0, 0, 0, 9, 102, 97, 105, 108, 95, 99, 97, 108, 108>> = _CallArgs, _Process},
    _Opts,
    _Ctx
) ->
    {error, do_not_retry};
process(
    %% simple repair
    {timeout, _Args, _Process},
    _Opts,
    _Ctx
) ->
    Result = #{events => []},
    {ok, Result};
%
process(
    %% <<"repair_ok">>
    {repair, <<131, 109, 0, 0, 0, 9, 114, 101, 112, 97, 105, 114, 95, 111, 107>> = CallArgs, _Process},
    _Opts,
    _Ctx
) ->
    Result = #{events => [], response => CallArgs},
    {ok, Result};
process(
    %% <<"repair_fail">>
    {repair, <<131, 109, 0, 0, 0, 11, 114, 101, 112, 97, 105, 114, 95, 102, 97, 105, 108>> = _CallArgs, _Process},
    _Opts,
    _Ctx
) ->
    {error, unreach}.

%
event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        payload => erlang:term_to_binary({bin, crypto:strong_rand_bytes(8)})
    }.
