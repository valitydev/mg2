-module(mg_progressor).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-export([handle_function/3]).

-spec handle_function(woody:func(), woody:args(), _) ->
    {ok, _Result} | no_return().
handle_function('Start', {NS, IDIn, PackedArgs}, Context) ->
    ID = mg_woody_packer:unpack(id, IDIn),
    Args = mg_woody_packer:unpack(args, PackedArgs),
    Req = genlib_map:compact(#{
        ns => erlang:binary_to_atom(NS),
        id => ID,
        args => maybe_unmarshal(term, Args),
        context => maybe_unmarshal(term, Context)
    }),
    handle_result(progressor:init(Req));
handle_function('Call', {MachineDesc, PackedArgs}, Context) ->
    {NS, ID, {After, Limit, Direction}} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    Args = mg_woody_packer:unpack(args, PackedArgs),
    Range = genlib_map:compact(#{
        limit => Limit,
        offset => After,
        direction => Direction
    }),
    Req = genlib_map:compact(#{
        ns => erlang:binary_to_atom(NS),
        id => ID,
        args => maybe_unmarshal(term, Args),
        context => maybe_unmarshal(term, Context),
        range => Range
    }),
    case progressor:call(Req) of
        {ok, RawResponse} ->
            %% TODO: CHECK THIS!
            {ok, mg_woody_packer:pack(call_response, marshal(term, RawResponse))};
        Error ->
            handle_error(Error)
    end;
handle_function('SimpleRepair', {NS, RefIn}, Context) ->
    ID = mg_woody_packer:unpack(ref, RefIn),
    Req = genlib_map:compact(#{
        ns => erlang:binary_to_atom(NS),
        id => ID,
        context => maybe_unmarshal(term, Context)
    }),
    handle_result(progressor:simple_repair(Req));
handle_function('Repair', {MachineDesc, PackedArgs}, Context) ->
    {NS, ID, _} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    Args = mg_woody_packer:unpack(args, PackedArgs),
    Req = genlib_map:compact(#{
        ns => erlang:binary_to_atom(NS),
        id => ID,
        args => maybe_unmarshal(term, Args),
        context => maybe_unmarshal(term, Context)
    }),
    case progressor:repair(Req) of
        {ok, RawResponse} ->
            %% TODO: CHECK THIS!
            {ok, mg_woody_packer:pack(repair_response, marshal(term, RawResponse))};
        {error, <<"process is running">>} = Error ->
            handle_error(Error);
        {error, <<"process not found">>} = Error ->
            handle_error(Error);
        {error, <<"namespace not found">>} = Error ->
            handle_error(Error);
        _Error ->
            handle_error({error, <<"process is error">>})
    end;
handle_function('GetMachine', {MachineDesc}, Context) ->
    {NS, ID, {After, Limit, Direction}} = mg_woody_packer:unpack(machine_descriptor, MachineDesc),
    Range = genlib_map:compact(#{
        limit => Limit,
        offset => After,
        direction => Direction
    }),
    Req = genlib_map:compact(#{
        ns => erlang:binary_to_atom(NS),
        id => ID,
        context => maybe_unmarshal(term, Context),
        range => Range
    }),
    case progressor:get(Req) of
        {ok, Process} ->
            {ok, marshal(process, Process#{ns => NS, history_range => Range})};
        Error ->
            handle_error(Error)
    end;
handle_function(Func, _Args, _Context) ->
    error({not_implemented, Func}).

%% Internal functions

handle_result({ok, _} = Ok) ->
    Ok;
handle_result({error, _Reason} = Error) ->
    handle_error(Error).

-spec handle_error(any()) -> no_return().
handle_error({error, <<"process already exists">>}) ->
    erlang:throw({logic, machine_already_exist});
handle_error({error, <<"process not found">>}) ->
    erlang:throw({logic, machine_not_found});
handle_error({error, <<"namespace not found">>}) ->
    erlang:throw({logic, namespace_not_found});
handle_error({error, <<"process is running">>}) ->
    erlang:throw({logic, machine_already_working});
handle_error({error, <<"process is error">>}) ->
    erlang:throw({logic, machine_failed});
handle_error({error, {exception, _, _}}) ->
    erlang:throw({logic, machine_failed});
handle_error(UnknownError) ->
    erlang:throw(UnknownError).

maybe_unmarshal(_Type, undefined) ->
    undefined;
maybe_unmarshal(Type, Value) ->
    unmarshal(Type, Value).

unmarshal(term, Value) ->
    erlang:term_to_binary(Value).

maybe_marshal(_Type, undefined) ->
    undefined;
maybe_marshal(Type, Value) ->
    marshal(Type, Value).

marshal(process, Process) ->
    #mg_stateproc_Machine{
        ns = maps:get(ns, Process),
        id = maps:get(process_id, Process),
        history = maybe_marshal(history, maps:get(history, Process)),
        history_range = marshal(history_range, maps:get(history_range, Process)),
        status = marshal(status, {maps:get(status, Process), maps:get(detail, Process, undefined)}),
        aux_state = maybe_marshal(term, maps:get(aux_state, Process, undefined))
    };
marshal(history, History) ->
    lists:map(fun(Ev) -> marshal(event, Ev) end, History);
marshal(event, Event) ->
    #{
        event_id := EventId,
        timestamp := Timestamp,
        payload := Payload
    } = Event,
    Meta = maps:get(metadata, Event, #{}),
    #mg_stateproc_Event{
        id = EventId,
        created_at = marshal(timestamp, Timestamp),
        format_version = format_version(Meta),
        data = marshal(term, Payload)
    };
marshal(timestamp, Timestamp) ->
    unicode:characters_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{offset, "Z"}]));
marshal(term, Term) ->
    binary_to_term(Term);
marshal(history_range, Range) ->
    #mg_stateproc_HistoryRange{
        'after' = maps:get(offset, Range, undefined),
        limit = maps:get(limit, Range, undefined),
        direction = maps:get(direction, Range, undefined)
    };
marshal(status, {<<"running">>, _Detail}) ->
    {'working', #mg_stateproc_MachineStatusWorking{}};
marshal(status, {<<"error">>, Detail}) ->
    {'failed', #mg_stateproc_MachineStatusFailed{reason = Detail}}.

format_version(#{<<"format_version">> := Version}) ->
    Version;
format_version(_) ->
    undefined.
