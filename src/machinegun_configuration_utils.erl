%%%
%%% Copyright 2020 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(machinegun_configuration_utils).

-include_lib("kernel/include/file.hrl").

-export([parse_yaml_config/1]).
-export([parse_yaml/1]).
-export([write_file/2]).
-export([write_file/3]).
-export([print_sys_config/1]).
-export([print_vm_args/1]).
-export([print_erl_inetrc/1]).

-export([guess_host_address/1]).
-export([hostname/0]).
-export([fqdn/0]).

-export([file/2]).
-export([env/1]).
-export([log_level/1]).
-export([mem_words/1]).
-export([mem_bytes/1]).
-export([seconds/1]).
-export([milliseconds/1]).
-export([time_interval/1]).
-export([time_interval/2]).
-export([proplist/1]).
-export([string/1]).
-export([ip/1]).
-export([atom/1]).
-export([conf/3]).
-export([conf/2]).
-export([traverse/2]).
-export([probability/1]).
-export([contents/1]).
-export([interpolate/2]).
-export([maybe/2]).
-export([maybe/3]).

%%

% hello to librares without an explicit typing 😡
-type yaml_config() :: _TODO.
-type yaml_config_path() :: [atom()].
-type yaml_string() :: binary().

-type vm_args() :: [{vm_flag_name(), vm_flag_value()} | vm_flag_name()].
-type vm_flag_name() :: atom() | binary().
-type vm_flag_value() :: atom() | binary() | integer().

-type sys_config() :: [{atom, term()}].
-type erl_inetrc() :: [{atom, term()}].

-type filename() :: file:filename_all().
-type mem_words() :: non_neg_integer().
-type mem_bytes() :: non_neg_integer().
-type maybe(T) :: undefined | T.

-type time_interval_unit() :: 'week' | 'day' | 'hour' | 'min' | 'sec' | 'ms' | 'mu'.
-type time_interval() :: {non_neg_integer(), time_interval_unit()}.

-spec parse_yaml_config(filename()) -> yaml_config().
parse_yaml_config(Filename) ->
    {ok, _} = application:ensure_all_started(yamerl),
    [Config] = yamerl_constr:file(Filename, [str_node_as_binary]),
    Config.

-spec parse_yaml(binary()) -> yaml_config().
parse_yaml(String) ->
    {ok, _} = application:ensure_all_started(yamerl),
    [Config] = yamerl_constr:string(String, [str_node_as_binary]),
    Config.

-spec write_file(filename(), iodata()) -> ok.
write_file(Name, Data) ->
    ok = file:write_file(Name, Data).

-spec write_file(filename(), iodata(), _Mode :: non_neg_integer()) -> ok.
write_file(Name, Data, Mode) ->
    % Turn write permission on temporarily
    _ = file:change_mode(Name, Mode bor 8#00200),
    % Truncate it
    ok = file:write_file(Name, <<>>),
    ok = file:change_mode(Name, Mode bor 8#00200),
    % Write contents
    ok = file:write_file(Name, Data),
    % Drop write permission (if `Mode` doesn't specify it)
    ok = file:change_mode(Name, Mode).

-spec print_sys_config(sys_config()) -> iolist().
print_sys_config(SysConfig) ->
    [io_lib:print(SysConfig), $., $\n].

-spec print_vm_args(vm_args()) -> iolist().
print_vm_args(VMArgs) ->
    lists:map(
        fun
            ({Arg, Value}) ->
                [genlib:to_binary(Arg), $\s, genlib:to_binary(Value), $\n];
            (Arg) when not is_tuple(Arg) ->
                [genlib:to_binary(Arg), $\n]
        end,
        VMArgs
    ).

-spec print_erl_inetrc(erl_inetrc()) -> iolist().
print_erl_inetrc(ERLInetrc) ->
    [[io_lib:print(E), $., $\n] || E <- ERLInetrc].

-spec file(string(), _AtMostMode :: non_neg_integer()) -> filename().
file(Filename, AtMostMode) ->
    case file:read_file_info(Filename) of
        {ok, #file_info{type = regular, mode = Mode}} ->
            case (Mode band 8#777) bor AtMostMode of
                AtMostMode ->
                    Filename;
                _ ->
                    erlang:throw({'bad file mode', Filename, io_lib:format("~.8.0B", [Mode])})
            end;
        {ok, #file_info{type = Type}} ->
            erlang:throw({'bad file type', Filename, Type});
        {error, Reason} ->
            erlang:throw({'error accessing file', Filename, Reason})
    end.

-spec env(yaml_string()) -> yaml_string().
env(VarName) ->
    case byte_size(VarName) > 0 andalso os:getenv(string(VarName)) of
        false -> erlang:throw({'undefined environment variable', VarName});
        Value -> unicode:characters_to_binary(Value)
    end.

-spec guess_host_address(inet:address_family()) -> inet:ip_address().
guess_host_address(AddressFamilyPreference) ->
    {ok, Ifaces0} = inet:getifaddrs(),
    Ifaces1 = filter_running_ifaces(Ifaces0),
    IfaceAddrs0 = gather_iface_addrs(Ifaces1, AddressFamilyPreference),
    [{_Name, Addr} | _] = sort_iface_addrs(IfaceAddrs0),
    Addr.

-type iface_name() :: string().
-type iface() :: {iface_name(), proplists:proplist()}.

-spec filter_running_ifaces([iface()]) -> [iface()].
filter_running_ifaces(Ifaces) ->
    lists:filter(
        fun({_, Ps}) -> is_iface_running(proplists:get_value(flags, Ps)) end,
        Ifaces
    ).

-spec is_iface_running([up | running | atom()]) -> boolean().
is_iface_running(Flags) ->
    [] == [up, running] -- Flags.

-spec gather_iface_addrs([iface()], inet:address_family()) -> [{iface_name(), inet:ip_address()}].
gather_iface_addrs(Ifaces, Pref) ->
    lists:filtermap(
        fun({Name, Ps}) -> choose_iface_address(Name, proplists:get_all_values(addr, Ps), Pref) end,
        Ifaces
    ).

-spec choose_iface_address(iface_name(), [inet:ip_address()], inet:address_family()) ->
    false | {true, {iface_name(), inet:ip_address()}}.
choose_iface_address(Name, [Addr = {_, _, _, _} | _], inet) ->
    {true, {Name, Addr}};
choose_iface_address(Name, [Addr = {_, _, _, _, _, _, _, _} | _], inet6) ->
    {true, {Name, Addr}};
choose_iface_address(Name, [_ | Rest], Pref) ->
    choose_iface_address(Name, Rest, Pref);
choose_iface_address(_, [], _) ->
    false.

-spec sort_iface_addrs([{iface_name(), inet:ip_address()}]) -> [{iface_name(), inet:ip_address()}].
sort_iface_addrs(IfaceAddrs) ->
    lists:sort(fun({N1, _}, {N2, _}) -> get_iface_prio(N1) =< get_iface_prio(N2) end, IfaceAddrs).

-spec get_iface_prio(iface_name()) -> integer().
get_iface_prio("eth" ++ _) -> 1;
get_iface_prio("en" ++ _) -> 1;
get_iface_prio("wl" ++ _) -> 2;
get_iface_prio("tun" ++ _) -> 3;
get_iface_prio("lo" ++ _) -> 4;
get_iface_prio(_) -> 100.

-spec hostname() -> inet:hostname().
hostname() ->
    {ok, Name} = inet:gethostname(),
    Name.

-spec fqdn() -> inet:hostname().
fqdn() ->
    Hostname = hostname(),
    case net_adm:localhost() of
        Hostname ->
            error({'can not determine fqdn', Hostname});
        FQDN ->
            FQDN
    end.

-spec log_level(yaml_string()) -> atom().
log_level(<<"critical">>) -> critical;
log_level(<<"error">>) -> error;
log_level(<<"warning">>) -> warning;
log_level(<<"info">>) -> info;
log_level(<<"debug">>) -> debug;
log_level(<<"trace">>) -> trace;
log_level(BadLevel) -> erlang:throw({bad_log_level, BadLevel}).

-spec mem_words(yaml_string()) -> mem_words().
mem_words(MemStr) ->
    mem_bytes(MemStr) div erlang:system_info(wordsize).

-spec mem_bytes(yaml_string()) -> mem_bytes().
mem_bytes(MemStr) ->
    {NumStr, Unit} = string:take(string:trim(MemStr), lists:seq($0, $9)),
    try
        case string:uppercase(Unit) of
            <<"P">> -> pow2x0(5) * binary_to_integer(NumStr);
            <<"T">> -> pow2x0(4) * binary_to_integer(NumStr);
            <<"G">> -> pow2x0(3) * binary_to_integer(NumStr);
            <<"M">> -> pow2x0(2) * binary_to_integer(NumStr);
            <<"K">> -> pow2x0(1) * binary_to_integer(NumStr);
            <<"B">> -> pow2x0(0) * binary_to_integer(NumStr);
            _ -> erlang:throw({'bad memory amount', MemStr})
        end
    catch
        error:badarg ->
            erlang:throw({'bad memory amount', MemStr})
    end.

-spec pow2x0(integer()) -> integer().
pow2x0(X) ->
    1 bsl (X * 10).

-spec seconds(yaml_string()) -> timeout().
seconds(TimeStr) ->
    time_interval(TimeStr, 'sec').

-spec milliseconds(yaml_string()) -> timeout().
milliseconds(TimeStr) ->
    time_interval(TimeStr, 'ms').

-spec time_interval(yaml_string()) -> time_interval().
time_interval(TimeStr) ->
    parse_time_interval(TimeStr).

-spec time_interval(yaml_string(), time_interval_unit()) -> timeout().
time_interval(<<"infinity">>, _) ->
    infinity;
time_interval(TimeStr, Unit) ->
    time_interval_in(parse_time_interval(TimeStr), Unit).

-spec parse_time_interval(yaml_string()) -> time_interval().
parse_time_interval(TimeStr) ->
    {NumStr, Unit} = string:take(string:trim(TimeStr), lists:seq($0, $9)),
    try
        case string:uppercase(Unit) of
            <<"W">> -> {binary_to_integer(NumStr), 'week'};
            <<"D">> -> {binary_to_integer(NumStr), 'day'};
            <<"H">> -> {binary_to_integer(NumStr), 'hour'};
            <<"M">> -> {binary_to_integer(NumStr), 'min'};
            <<"MS">> -> {binary_to_integer(NumStr), 'ms'};
            <<"MU">> -> {binary_to_integer(NumStr), 'mu'};
            <<"S">> -> {binary_to_integer(NumStr), 'sec'};
            _ -> erlang:throw({'bad time interval', TimeStr})
        end
    catch
        error:badarg ->
            erlang:throw({'bad time interval', TimeStr})
    end.

-spec time_interval_in(time_interval(), time_interval_unit()) -> non_neg_integer().
time_interval_in({Amount, UnitFrom}, UnitTo) ->
    time_interval_in_(Amount, time_interval_unit_to_int(UnitFrom), time_interval_unit_to_int(UnitTo)).

-spec time_interval_in_(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> non_neg_integer().
time_interval_in_(Amount, UnitFrom, UnitTo) when UnitFrom =:= UnitTo ->
    Amount;
time_interval_in_(Amount, UnitFrom, UnitTo) when UnitFrom < UnitTo ->
    time_interval_in_(Amount div time_interval_mul(UnitFrom + 1), UnitFrom + 1, UnitTo);
time_interval_in_(Amount, UnitFrom, UnitTo) when UnitFrom > UnitTo ->
    time_interval_in_(Amount * time_interval_mul(UnitFrom), UnitFrom - 1, UnitTo).

-spec time_interval_unit_to_int(time_interval_unit()) -> non_neg_integer().
time_interval_unit_to_int('week') -> 6;
time_interval_unit_to_int('day') -> 5;
time_interval_unit_to_int('hour') -> 4;
time_interval_unit_to_int('min') -> 3;
time_interval_unit_to_int('sec') -> 2;
time_interval_unit_to_int('ms') -> 1;
time_interval_unit_to_int('mu') -> 0.

-spec time_interval_mul(non_neg_integer()) -> non_neg_integer().
time_interval_mul(6) -> 7;
time_interval_mul(5) -> 24;
time_interval_mul(4) -> 60;
time_interval_mul(3) -> 60;
time_interval_mul(2) -> 1000;
time_interval_mul(1) -> 1000.

-spec proplist(yaml_config()) -> proplists:proplist().
proplist(Config) ->
    [{erlang:binary_to_existing_atom(Key), Value} || {Key, Value} <- Config].

-spec ip(yaml_string()) -> inet:ip_address().
ip(Host) ->
    mg_core_utils:throw_if_error(inet:parse_address(string(Host))).

-spec atom(yaml_string()) -> atom().
atom(AtomStr) ->
    erlang:binary_to_atom(AtomStr, utf8).

-spec string(yaml_string()) -> string().
string(BinStr) ->
    unicode:characters_to_list(BinStr).

-spec conf(yaml_config_path(), yaml_config(), _) -> _.
conf(Path, Config, Default) ->
    conf_({default, Default}, Path, Config).

-spec conf(yaml_config_path(), yaml_config()) -> _.
conf(Path, Config) ->
    conf_({throw, Path}, Path, Config).

-spec conf_({throw, yaml_config_path()} | {default, _}, yaml_config_path(), yaml_config()) -> _.
conf_(_, [], Value) ->
    Value;
conf_(Throw, Key, Config) when is_atom(Key) andalso is_list(Config) ->
    case lists:keyfind(erlang:atom_to_binary(Key), 1, Config) of
        false -> conf_maybe_default(Throw);
        {_, Value} -> Value
    end;
conf_(Throw, [Key | Path], Config) when is_list(Path) andalso is_list(Config) ->
    case lists:keyfind(erlang:atom_to_binary(Key), 1, Config) of
        false -> conf_maybe_default(Throw);
        {_, Value} -> conf_(Throw, Path, Value)
    end.

-spec conf_maybe_default({throw, yaml_config_path()} | {default, _}) -> _ | no_return().
conf_maybe_default({throw, Path}) ->
    erlang:throw({'config element not found', Path});
conf_maybe_default({default, Default}) ->
    Default.

-type traverse_fun() :: fun(
    ({property, yaml_string()} | {element, pos_integer()} | value, Value) -> {replace, Value} | proceed
).

-spec traverse(traverse_fun(), yaml_config()) -> yaml_config().
traverse(TFun, Config = [{_, _} | _]) ->
    lists:map(
        fun({Name, Value}) ->
            case TFun({property, Name}, Value) of
                {replace, ValueNext} ->
                    {Name, ValueNext};
                proceed ->
                    {Name, traverse(TFun, Value)}
            end
        end,
        Config
    );
traverse(TFun, Seq) when is_list(Seq) ->
    lists:map(
        fun({Index, Value}) ->
            case TFun({element, Index}, Value) of
                {replace, ValueNext} ->
                    ValueNext;
                proceed ->
                    traverse(TFun, Value)
            end
        end,
        lists:zip(lists:seq(1, length(Seq)), Seq)
    );
traverse(TFun, Value) ->
    case TFun(value, Value) of
        {replace, ValueNext} ->
            ValueNext;
        proceed ->
            Value
    end.

-spec probability(term()) -> float() | integer() | no_return().
probability(Prob) when is_number(Prob) andalso 0 =< Prob andalso Prob =< 1 ->
    Prob;
probability(Prob) ->
    throw({'bad probability', Prob}).

-spec contents(filename()) -> binary().
contents(Filename) ->
    case file:read_file(Filename) of
        {ok, Contents} ->
            Contents;
        {error, Reason} ->
            erlang:throw({'could not read file contents', Filename, Reason})
    end.

-spec maybe(fun((T) -> U), maybe(T)) -> maybe(U).
maybe(_Fun, undefined) ->
    undefined;
maybe(Fun, T) ->
    Fun(T).

-spec maybe(fun((T) -> U), maybe(T), Default) -> U | Default.
maybe(_Fun, undefined, Default) ->
    Default;
maybe(Fun, T, _Default) ->
    Fun(T).

-type interpolate_fun() :: fun((yaml_string()) -> yaml_string()).
-type replacements(T) :: #{T => T}.

-spec interpolate(interpolate_fun(), yaml_string()) -> yaml_string().
interpolate(IFun, Str) ->
    case re:run(Str, "\\$\\{([^{}]+)\\}", [global, {capture, all, binary}]) of
        {match, Matches} ->
            Replacements = replacements(IFun, Matches),
            replace(Replacements, Str);
        nomatch ->
            Str
    end.

-spec replacements(interpolate_fun(), [[yaml_string()]]) -> replacements(yaml_string()).
replacements(IFun, Matches) ->
    lists:foldl(
        fun([Interp, Expr], Acc) ->
            Acc#{Interp => IFun(Expr)}
        end,
        #{},
        Matches
    ).

-spec replace(replacements(yaml_string()), yaml_string()) -> yaml_string().
replace(Replacements, StrIn) ->
    maps:fold(
        fun(Interp, Value, Str) ->
            binary:replace(Str, Interp, Value, [global])
        end,
        StrIn,
        Replacements
    ).
