-module(mg_woody_automaton_balancer).

%% woody handler
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% уменьшаем писанину
-import(mg_woody_packer, [unpack/2]).

%% API types
-type options() :: mg_woody_automaton:options().

-define(AUTOMATON_HANDLER, mg_woody_automaton).

%%
%% woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().

handle_function('Start' = Call, {NS, IDIn, _Args} = Data, WoodyContext, Options) ->
    ID = unpack(id, IDIn),
    erpc:call(
        target_node({NS, ID}),
        ?AUTOMATON_HANDLER,
        handle_function,
        [Call, Data, WoodyContext, Options]
    );
handle_function(Call, {MachineDesc, _Args} = Data, WoodyContext, Options) when
    Call =:= 'Repair';
    Call =:= 'Call';
    Call =:= 'Notify'
->
    {NS, ID, _Range} = unpack(machine_descriptor, MachineDesc),
    erpc:call(
        target_node({NS, ID}),
        ?AUTOMATON_HANDLER,
        handle_function,
        [Call, Data, WoodyContext, Options]
    );
handle_function('SimpleRepair' = Call, {NS, RefIn} = Data, WoodyContext, Options) ->
    ID = unpack(ref, RefIn),
    erpc:call(
        target_node({NS, ID}),
        ?AUTOMATON_HANDLER,
        handle_function,
        [Call, Data, WoodyContext, Options]
    );
handle_function(Call, {MachineDesc} = Data, WoodyContext, Options) when
    Call =:= 'GetMachine';
    Call =:= 'Modernize'
->
    {NS, ID, _Range} = unpack(machine_descriptor, MachineDesc),
    erpc:call(
        target_node({NS, ID}),
        ?AUTOMATON_HANDLER,
        handle_function,
        [Call, Data, WoodyContext, Options]
    );
handle_function('Remove' = Call, {NS, IDIn} = Data, WoodyContext, Options) ->
    ID = unpack(id, IDIn),
    erpc:call(
        target_node({NS, ID}),
        ?AUTOMATON_HANDLER,
        handle_function,
        [Call, Data, WoodyContext, Options]
    ).

%% internal functions

-spec target_node(term()) -> node().
target_node(BalancingKey) ->
    {ok, Node} = mg_core_cluster:get_node(BalancingKey),
    Node.
