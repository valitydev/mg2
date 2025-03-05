-module(mg_woody_prg_engine_tests_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-include_lib("mg_cth/include/mg_cth.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% TESTS
-export([namespace_not_found/1]).
-export([machine_start/1]).
-export([machine_already_exists/1]).
-export([machine_call_by_id/1]).
-export([machine_id_not_found/1]).
-export([failed_machine_call/1]).
-export([failed_machine_simple_repair/1]).
-export([failed_machine_repair/1]).
-export([failed_machine_repair_error/1]).
-export([working_machine_repair/1]).
-export([working_machine_simple_repair/1]).

-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    _ = mg_cth:start_applications([
        epg_connector,
        progressor
    ]),
    C.

-spec end_per_suite(config()) -> ok.
end_per_suite(_C) ->
    _ = mg_cth:stop_applications([
        epg_connector,
        progressor
    ]),
    ok.

-spec init_per_group(group_name(), config()) -> config().
init_per_group(_, C) ->
    Config = mg_woody_config(C),
    Apps = mg_cth:start_applications([
        brod,
        mg_woody
    ]),
    {ok, SupPid} = start_automaton(Config),
    [
        {apps, Apps},
        {automaton_options, #{
            url => "http://localhost:38022",
            ns => ?NS,
            retry_strategy => genlib_retry:linear(3, 1)
        }},
        {sup_pid, SupPid}
        | C
    ].

-spec end_per_group(group_name(), config()) -> ok.
end_per_group(_, C) ->
    ok = proc_lib:stop(?config(sup_pid, C)),
    mg_cth:stop_applications(?config(apps, C)).

-spec init_per_testcase(atom(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(atom(), config()) -> _.
end_per_testcase(_Name, _C) ->
    ok.
%

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, base}
    ].

-spec groups() -> [{group_name(), list(_), [test_name()]}].
groups() ->
    [
        {base, [], [
            namespace_not_found,
            machine_start,
            machine_already_exists,
            machine_call_by_id,
            machine_id_not_found,
            failed_machine_call,
            failed_machine_simple_repair,
            failed_machine_repair,
            failed_machine_repair_error,
            working_machine_repair,
            working_machine_simple_repair
        ]}
    ].

%% TESTS

-spec namespace_not_found(config()) -> _.
namespace_not_found(C) ->
    Opts = maps:update(ns, <<"incorrect_NS">>, automaton_options(C)),
    #mg_stateproc_NamespaceNotFound{} = (catch mg_cth_automaton_client:start(Opts, ?ID, <<>>)).

-spec machine_start(config()) -> _.
machine_start(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>).

-spec machine_already_exists(config()) -> _.
machine_already_exists(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    #mg_stateproc_MachineAlreadyExists{} =
        (catch mg_cth_automaton_client:start(automaton_options(C), ID, <<>>)).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    <<"simple_call">> = mg_cth_automaton_client:call(automaton_options(C), ID, <<"simple_call">>).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(C) ->
    IncorrectID = <<"incorrect_ID">>,
    #mg_stateproc_MachineNotFound{} =
        (catch mg_cth_automaton_client:call(automaton_options(C), IncorrectID, <<"simple_call">>)).

-spec failed_machine_call(config()) -> _.
failed_machine_call(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    _Fail = catch mg_cth_automaton_client:call(automaton_options(C), ID, <<"fail_call">>),
    #mg_stateproc_MachineFailed{} =
        (catch mg_cth_automaton_client:call(automaton_options(C), ID, <<"simple_call">>)).

-spec failed_machine_simple_repair(config()) -> _.
failed_machine_simple_repair(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    _Fail = catch mg_cth_automaton_client:call(automaton_options(C), ID, <<"fail_call">>),
    ok = mg_cth_automaton_client:simple_repair(automaton_options(C), ID),
    %% await repair result
    timer:sleep(1100),
    #{status := working} =
        mg_cth_automaton_client:get_machine(automaton_options(C), ID, {undefined, undefined, forward}).

-spec failed_machine_repair(config()) -> _.
failed_machine_repair(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    _Fail = catch mg_cth_automaton_client:call(automaton_options(C), ID, <<"fail_call">>),
    <<"repair_ok">> = mg_cth_automaton_client:repair(automaton_options(C), ID, <<"repair_ok">>),
    %% await repair result
    timer:sleep(1100),
    #{status := working} =
        mg_cth_automaton_client:get_machine(automaton_options(C), ID, {undefined, undefined, forward}).

-spec failed_machine_repair_error(config()) -> _.
failed_machine_repair_error(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    _Fail = catch mg_cth_automaton_client:call(automaton_options(C), ID, <<"fail_call">>),
    #mg_stateproc_MachineFailed{} =
        (catch mg_cth_automaton_client:repair(automaton_options(C), ID, <<"repair_fail">>)).

-spec working_machine_repair(config()) -> _.
working_machine_repair(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    #mg_stateproc_MachineAlreadyWorking{} =
        (catch mg_cth_automaton_client:repair(automaton_options(C), ID, <<"repair_ok">>)).

-spec working_machine_simple_repair(config()) -> _.
working_machine_simple_repair(C) ->
    ID = gen_id(),
    ok = mg_cth_automaton_client:start(automaton_options(C), ID, <<"init_args">>),
    #mg_stateproc_MachineAlreadyWorking{} =
        (catch mg_cth_automaton_client:simple_repair(automaton_options(C), ID)).

%% Internal functions

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).

-spec mg_woody_config(_) -> _.
mg_woody_config(_C) ->
    #{
        woody_server => #{ip => {0, 0, 0, 0}, port => 38022, limits => #{}},
        quotas => [],
        namespaces => #{
            ?NS => #{
                storage => mg_core_storage_memory,
                processor => #{
                    url => <<"http://null">>
                },
                default_processing_timeout => 5000,
                schedulers => #{},
                retries => #{},
                event_sinks => [],
                worker => #{
                    registry => mg_procreg_global,
                    sidecar => mg_cth_worker
                },
                engine => progressor
            }
        }
    }.

-spec start_automaton(_) -> _.
start_automaton(MgConfig) ->
    Flags = #{strategy => one_for_all},
    ChildsSpecs = mg_cth_conf:construct_child_specs(MgConfig),
    {ok, SupPid} = Res = genlib_adhoc_supervisor:start_link(Flags, ChildsSpecs),
    true = erlang:unlink(SupPid),
    Res.

gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).
