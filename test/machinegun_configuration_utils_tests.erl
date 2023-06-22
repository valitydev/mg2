-module(machinegun_configuration_utils_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-type testgen() :: {_ID, fun(() -> _)}.

-spec test() -> _.

-spec print_vm_args_test_() -> [testgen()].
print_vm_args_test_() ->
    [
        ?_assertEqual(
            <<"">>,
            iolist_to_binary(machinegun_configuration_utils:print_vm_args([]))
        ),
        ?_assertEqual(
            <<
                "+c true \n"
                "+C single_time_warp \n"
                "-kernel inet_dist_listen_min 1337 \n"
                "-kernel inet_dist_listen_max 31337 \n"
            >>,
            iolist_to_binary(
                machinegun_configuration_utils:print_vm_args([
                    {'+c', true},
                    {'+C', "single_time_warp"},
                    {'-kernel', 'inet_dist_listen_min', 1337},
                    {'-kernel', 'inet_dist_listen_max', 31337}
                ])
            )
        )
    ].

-spec time_interval_test_() -> [testgen()].
time_interval_test_() ->
    [
        ?_assertEqual({1, 'ms'}, machinegun_configuration_utils:time_interval(<<"1ms">>)),
        ?_assertEqual({1, 'mu'}, machinegun_configuration_utils:time_interval(<<"1mu">>)),
        ?_assertEqual({1, 'sec'}, machinegun_configuration_utils:time_interval(<<"1s">>)),
        ?_assertEqual(86400, machinegun_configuration_utils:time_interval(<<"1d">>, sec)),
        ?_assertEqual(48, machinegun_configuration_utils:time_interval(<<"2d">>, hour)),
        ?_assertEqual(60000, machinegun_configuration_utils:time_interval(<<"1m">>, ms)),
        ?_assertEqual(3600000, machinegun_configuration_utils:milliseconds(<<"1H">>)),
        ?_assertEqual(14400, machinegun_configuration_utils:seconds(<<"4H">>)),
        ?_assertEqual(0, machinegun_configuration_utils:seconds(<<"0W">>)),
        ?_assertThrow(_, machinegun_configuration_utils:milliseconds(<<>>)),
        ?_assertThrow(_, machinegun_configuration_utils:milliseconds(<<"-1s">>)),
        ?_assertThrow(_, machinegun_configuration_utils:milliseconds(<<"42">>))
    ].

-spec mem_bytes_test_() -> [testgen()].
mem_bytes_test_() ->
    [
        ?_assertEqual(42, machinegun_configuration_utils:mem_bytes(<<"42b">>)),
        ?_assertEqual(1024, machinegun_configuration_utils:mem_bytes(<<"1k">>)),
        ?_assertEqual(2097152, machinegun_configuration_utils:mem_bytes(<<"2M">>)),
        ?_assertEqual(0, machinegun_configuration_utils:mem_bytes(<<"0G">>)),
        ?_assertEqual(96207267430400, machinegun_configuration_utils:mem_words(<<"700T">>)),
        ?_assertEqual(188166021931073536, machinegun_configuration_utils:mem_words(<<"1337P">>)),
        ?_assertThrow(_, machinegun_configuration_utils:mem_bytes(<<"1">>)),
        ?_assertThrow(_, machinegun_configuration_utils:mem_bytes(<<"-7k">>)),
        ?_assertThrow(_, machinegun_configuration_utils:mem_bytes(<<"mlem">>))
    ].

-spec interpolate_test_() -> [testgen()].
interpolate_test_() ->
    [
        ?_assertEqual(
            <<"Nothing to see here">>,
            machinegun_configuration_utils:interpolate(
                fun erlang:throw/1,
                <<"Nothing to see here">>
            )
        ),
        ?_assertThrow(
            <<"OOPS">>,
            machinegun_configuration_utils:interpolate(
                fun erlang:throw/1,
                <<"Nothing to ${OOPS} here">>
            )
        ),
        ?_assertEqual(
            <<"Interpolated">>,
            machinegun_configuration_utils:interpolate(
                fun identity/1,
                <<"Int${e}r${pol}at${e}d">>
            )
        ),
        ?_assertEqual(
            <<"Badly inter${polate}d">>,
            machinegun_configuration_utils:interpolate(
                fun identity/1,
                <<"Badly int${e}r${polat${e}}d">>
            )
        ),
        ?_assertEqual(
            <<"Hi Mr. Side Effects! 3 3 3">>,
            machinegun_configuration_utils:interpolate(
                mk_counter_ifun(1),
                <<"Hi Mr. Side Effects! ${1} ${1} ${1}">>
            )
        )
    ].

-spec identity(T) -> T.
identity(T) ->
    T.

-spec mk_counter_ifun(_Size) -> fun().
mk_counter_ifun(Size) ->
    Ref = counters:new(Size, []),
    fun(IdxStr) ->
        Idx = binary_to_integer(IdxStr),
        ok = counters:add(Ref, Idx, 1),
        integer_to_binary(counters:get(Ref, Idx))
    end.

-spec traverse_test_() -> testgen().
traverse_test_() ->
    Yaml = <<
        "\nblarg:"
        "\n  - token: '/home/${HOME}/.ssh/id_rsa'"
        "\n    read_only: true"
        "\n    mode: 31337"
        "\n  - port: 8088"
        "\n    name: ${PORTNAME}"
        "\n  - port: 8089"
        "\n    name: ${PORTNAME}"
    >>,
    ConfigPre = machinegun_configuration_utils:parse_yaml(Yaml),
    Config = machinegun_configuration_utils:traverse(
        fun
            (value, Str) when is_binary(Str) ->
                {replace, machinegun_configuration_utils:interpolate(fun identity/1, Str)};
            (_, _) ->
                proceed
        end,
        ConfigPre
    ),
    ?_assertEqual(
        [
            [
                {<<"token">>, <<"/home/HOME/.ssh/id_rsa">>},
                {<<"read_only">>, true},
                {<<"mode">>, 31337}
            ],
            [
                {<<"port">>, 8088},
                {<<"name">>, <<"PORTNAME">>}
            ],
            [
                {<<"port">>, 8089},
                {<<"name">>, <<"PORTNAME">>}
            ]
        ],
        machinegun_configuration_utils:conf([blarg], Config)
    ).

-spec retry_policy_test_() -> [testgen()].
retry_policy_test_() ->
    DataSets = [
        {
            <<"\nblah: blah">>,
            #{
                type => <<"exponential">>,
                max_retries => 10,
                factor => 2,
                timeout => <<"10ms">>,
                max_timeout => <<"60s">>
            },
            {exponential, 10, 2, 10, 60 * 1000}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: exponential"
                "\n    max_retries: 100"
                "\n    factor: 3"
                "\n    timeout: 10ms"
                "\n    max_timeout: 60s"
            >>,
            #{
                factor => 2,
                timeout => <<"1d">>,
                max_retries => 10
            },
            {exponential, 100, 3, 10, 60 * 1000}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: exponential"
                "\n    timeout: 10s"
                "\n    max_retries:"
                "\n      max_total_timeout: 1h"
            >>,
            #{
                type => <<"linear">>,
                factor => 2,
                max_retries => 10
            },
            {exponential, {max_total_timeout, 1 * 60 * 60 * 1000}, 2, 10 * 1000}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    timeout: 10s"
            >>,
            #{
                type => <<"linear">>,
                max_retries => 10
            },
            {linear, 10, 10 * 1000}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: intervals"
                "\n    timeouts:"
                "\n      - 100ms"
                "\n      - 50s"
                "\n      - 5h"
                "\n      - 1d"
            >>,
            #{},
            {intervals, [100, 50 * 1000, 5 * 60 * 60 * 1000, 24 * 60 * 60 * 1000]}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: linear"
                "\n    timecap: 10m"
                "\n    max_retries: infinity"
                "\n    factor: 2"
                "\n    timeout: 10ms"
                "\n    max_timeout: 60s"
            >>,
            #{},
            {timecap, 10 * 60 * 1000, {linear, infinity, 10}}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: linear"
                "\n    max_retries: infinity"
                "\n    timeout: 20ms"
                "\n    jitter: 5ms"
            >>,
            #{
                jitter => 10
            },
            {linear, infinity, {jitter, 20, 5}}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: exponential"
                "\n    max_retries: infinity"
                "\n    factor: 2"
                "\n    timeout: 20ms"
                "\n    jitter: 5ms"
            >>,
            #{},
            {exponential, infinity, 2, {jitter, 20, 5}}
        },
        {
            <<
                "\nretries:"
                "\n  storage:"
                "\n    type: intervals"
                "\n    timeouts:"
                "\n      - 100ms"
                "\n      - 50s"
                "\n      - 1d"
                "\n    jitter: 5s"
            >>,
            #{},
            {intervals, [
                {jitter, 100, 5 * 1000}, {jitter, 50 * 1000, 5 * 1000}, {jitter, 24 * 60 * 60 * 1000, 5 * 1000}
            ]}
        }
    ],
    [
        ?_assertEqual(
            Policy,
            machinegun_configuration_utils:to_retry_policy(
                [namespaces, 'test-namespace', retries, storage],
                machinegun_configuration_utils:parse_yaml(Yaml),
                DefaultMap
            )
        )
     || {Yaml, DefaultMap, Policy} <- DataSets
    ].

-spec retry_policy_failure_test_() -> [testgen()].
retry_policy_failure_test_() ->
    DataSets = [
        #{},
        #{
            type => <<"non existing type">>,
            max_retries => <<"infinity">>,
            factor => 2,
            timeout => <<"10ms">>
        },
        #{
            %% required "max_retries" not present
            type => <<"exponential">>,
            factor => 2,
            timeout => <<"10ms">>
        },
        #{
            %% required "factor" not present
            type => <<"exponential">>,
            max_retries => <<"infinity">>,
            timeout => <<"10ms">>
        },
        #{
            %% required "timeout" not present
            type => <<"exponential">>,
            max_retries => <<"infinity">>,
            factor => 2
        },
        #{
            %% required "timeouts" not present
            type => <<"intervals">>
        },
        #{
            %% required "timeout" not present
            type => <<"linear">>,
            max_retries => 10
        }
    ],
    Path = [namespaces, 'test-namespace', retries, storage],
    [
        ?_assertThrow(
            {'bad retry config', Path, Map},
            machinegun_configuration_utils:to_retry_policy(Path, [], Map)
        )
     || Map <- DataSets
    ].

-endif.
