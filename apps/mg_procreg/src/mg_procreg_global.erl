-module(mg_procreg_global).

%%

-behaviour(mg_procreg).

-export([ref/2]).
-export([reg_name/2]).
-export([select/2]).

-type options() :: undefined.

%%

-spec ref(options(), mg_procreg:name()) -> mg_procreg:ref().
ref(_Options, Name) ->
    {global, Name}.

-spec reg_name(options(), mg_procreg:name()) -> mg_procreg:reg_name().
reg_name(Options, Name) ->
    ref(Options, Name).

-spec select(options(), mg_procreg:name_pattern()) -> [{mg_procreg:name(), pid()}].
select(_Options, NamePattern) ->
    lists:foldl(
        fun(Name, Acc) ->
            case match(Name, NamePattern) of
                true -> [{Name, global:whereis_name(Name)} | Acc];
                false -> Acc
            end
        end,
        [],
        global:registered_names()
    ).

%% Internal functions

-spec match(term(), term()) -> boolean().
%% optimization for frequent cases
match({Mod, {NS, _ID}}, {Mod, {NS, '$1'}}) ->
    true;
match({_Mod, {_NS, _ID}}, {_NeMod, {_NeNS, '$1'}}) ->
    false;
%% general implementation
match(Value, Pattern) when is_atom(Pattern) ->
    case erlang:atom_to_binary(Pattern) of
        <<"$", _T/binary>> -> true;
        <<"_">> -> true;
        _Binary -> Value =:= Pattern
    end;
match(Value, Pattern) when
    is_list(Pattern) andalso
        is_list(Value) andalso
        erlang:length(Value) =:= erlang:length(Pattern)
->
    not lists:any(
        fun({Num, ValueElement}) ->
            not match(ValueElement, lists:nth(Num, Pattern))
        end,
        lists:enumerate(Value)
    );
match(Value, Pattern) when
    is_tuple(Pattern) andalso
        is_tuple(Value) andalso
        erlang:size(Value) =:= erlang:size(Pattern)
->
    match(tuple_to_list(Value), tuple_to_list(Pattern));
match(Value, Pattern) ->
    Value =:= Pattern.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec match_test() -> _.
match_test() ->
    ?assertEqual(
        true,
        match({module, {ns, [{1, 2, 3}, <<"qwe">>]}}, '$_')
    ),
    ?assertEqual(
        false,
        match({module, {ns, [{1, 2, 3}, <<"qwe">>]}}, proc_name)
    ),
    ?assertEqual(
        true,
        match({module, {ns, [{1, 2, 3}, <<"qwe">>]}}, {'_', {ns, [{'$1', 2, '$3'}, <<"qwe">>]}})
    ),
    ?assertEqual(
        false,
        match({module, {ns, [{1, 2, 3}, <<"qwe">>]}}, {'_', {ns, [{'$1', 2, '$3'}, <<"ewq">>]}})
    ),
    ?assertEqual(
        false,
        match({module, {ns, [{1, 2, 3}, <<"qwe">>]}}, {'_', {ns, [{'$1', 222, '$3'}, <<"qwe">>]}})
    ),
    ?assertEqual(
        true,
        match({module, {ns, <<"qwe">>}}, {module, {ns, '$1'}})
    ),
    ?assertEqual(
        false,
        match({module, {ns, <<"qwe">>}}, {module, {<<"ns">>, '$1'}})
    ).

-spec select_test() -> _.
select_test() ->
    Fun = fun() ->
        receive
            _M -> ok
        end
    end,
    Pid1 = spawn(Fun),
    Pid2 = spawn(Fun),
    global:register_name(proc1, Pid1),
    global:register_name(proc2, Pid2),
    ?assertEqual(
        [{proc1, Pid1}],
        select(undefined, proc1)
    ).

-spec ref_reg_test() -> _.
ref_reg_test() ->
    ?assertEqual({global, abc}, ref(undefined, abc)),
    ?assertEqual({global, abc}, reg_name(undefined, abc)).

-endif.
