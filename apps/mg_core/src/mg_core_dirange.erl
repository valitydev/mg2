%%%
%%% Copyright 2017 RBKmoney
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

-module(mg_core_dirange).

-export_type([dirange/1]).

-export([empty/0]).
-export([forward/2]).
-export([backward/2]).

-export([to_opaque/1]).
-export([from_opaque/1]).

-export([align/2]).
-export([reverse/1]).
-export([dissect/2]).
-export([conjoin/2]).
-export([intersect/2]).
-export([unify/2]).
-export([limit/2]).
-export([fold/3]).
-export([enumerate/1]).

-export([direction/1]).
-export([size/1]).
-export([bounds/1]).
-export([from/1]).
-export([to/1]).

-export([
    get_ranges/3,
    find/2
]).

%% Directed range over integers
-type dirange(_T) :: nonempty_dirange(_T) | undefined.
-type direction() :: -1 | +1.
-type nonempty_dirange(_T) ::
    % Non-empty, unambiguously oriented directed range [from..to].
    {_T :: integer(), _T :: integer(), direction()}.

-type max_value() :: non_neg_integer().
-type capacity() :: non_neg_integer().
-type section_number() :: non_neg_integer().
-type alive_sections() :: [section_number()].
-type min_range_value() :: non_neg_integer().
-type max_range_value() :: non_neg_integer().
-type range_map() :: #{{min_range_value(), max_range_value()} => section_number()}.

%%

-spec empty() -> dirange(_).
empty() ->
    undefined.

-spec forward(_T :: integer(), _T :: integer()) -> dirange(_T).
forward(A, B) when A =< B ->
    {A, B, +1};
forward(A, B) when A > B ->
    {B, A, +1}.

-spec backward(_T :: integer(), _T :: integer()) -> dirange(_T).
backward(A, B) when A >= B ->
    {A, B, -1};
backward(A, B) when A < B ->
    {B, A, -1}.

-spec to_opaque(dirange(_)) -> mg_core_storage:opaque().
to_opaque(undefined) ->
    null;
to_opaque({A, B, +1}) ->
    [A, B];
to_opaque({A, B, D = -1}) ->
    [A, B, D].

-spec from_opaque(mg_core_storage:opaque()) -> dirange(_).
from_opaque(null) ->
    undefined;
from_opaque([A, B]) ->
    {A, B, +1};
from_opaque([A, B, D]) ->
    {A, B, D}.

%%

-spec align(dirange(T), _Pivot :: dirange(T)) -> dirange(T).
align(R, Rp) ->
    case direction(R) * direction(Rp) of
        -1 -> reverse(R);
        _S -> R
    end.

-spec reverse(dirange(T)) -> dirange(T).
reverse({A, B, D}) ->
    {B, A, -D};
reverse(undefined) ->
    undefined.

-spec dissect(dirange(T), T) -> {dirange(T), dirange(T)}.
dissect(undefined, _) ->
    {undefined, undefined};
dissect({A, B, +1 = D} = R, C) ->
    if
        C < A -> {undefined, R};
        B =< C -> {R, undefined};
        A =< C, C < B -> {{A, C, D}, {C + 1, B, D}}
    end;
dissect(R, C) ->
    {R1, R2} = dissect(reverse(R), C - 1),
    {reverse(R2), reverse(R1)}.

-spec conjoin(dirange(T), dirange(T)) -> dirange(T).
conjoin(undefined, R) ->
    R;
conjoin(R, undefined) ->
    R;
conjoin({A1, B1, D}, {A2, B2, D}) when A2 == B1 + D ->
    {A1, B2, D};
conjoin(R1, R2) ->
    erlang:error(badarg, [R1, R2]).

-spec intersect(_Range :: dirange(T), _With :: dirange(T)) ->
    {
        % part of `Range` to the «left» of `With`
        _LeftDiff :: dirange(T),
        % intersection between `Range` and `With`
        _Intersection :: dirange(T),
        % part of `Range` to the «right» of `With`
        _RightDiff :: dirange(T)
    }.
intersect(R0, undefined) ->
    erlang:error(badarg, [R0, undefined]);
intersect(R0, With) ->
    D0 = direction(R0),
    {WA, WB} = bounds(align(With, R0)),
    % to NOT include WA itself
    {LeftDiff, R1} = dissect(R0, WA - D0),
    {Intersection, RightDiff} = dissect(R1, WB),
    {LeftDiff, Intersection, RightDiff}.

-spec unify(dirange(T), dirange(T)) -> dirange(T).
unify(R, undefined) ->
    R;
unify(undefined, R) ->
    R;
unify({A1, B1, D}, {A2, B2, D}) ->
    {min(A1 * D, A2 * D) * D, max(B1 * D, B2 * D) * D, D}.

-spec limit(dirange(T), non_neg_integer()) -> dirange(T).
limit(undefined, _) ->
    undefined;
limit(_, 0) ->
    undefined;
limit({A, B, +1}, N) when N > 0 ->
    {A, erlang:min(B, A + N - 1), +1};
limit({B, A, -1}, N) when N > 0 ->
    {B, erlang:max(A, B - N + 1), -1}.

-spec enumerate(dirange(T)) -> [T].
enumerate(undefined) ->
    [];
enumerate({A, B, D}) ->
    lists:seq(A, B, D).

-spec fold(fun((T, Acc) -> Acc), Acc, dirange(T)) -> Acc.
fold(_, Acc, undefined) ->
    Acc;
fold(F, Acc, {A, B, D}) ->
    fold(F, Acc, A, B, D).

-spec fold(fun((T, Acc) -> Acc), Acc, T, T, -1..1) -> Acc.
fold(F, Acc, A, A, _) ->
    F(A, Acc);
fold(F, Acc, A, B, S) ->
    fold(F, F(A, Acc), A + S, B, S).

-spec direction(dirange(_)) -> direction() | 0.
direction({_, _, D}) ->
    D;
direction(_) ->
    0.

-spec size(dirange(_)) -> non_neg_integer().
size(undefined) ->
    0;
size({A, B, D}) ->
    (B - A) * D + 1.

-spec bounds(dirange(_T)) -> {_T, _T} | undefined.
bounds({A, B, _}) ->
    {A, B};
bounds(undefined) ->
    undefined.

-spec from(dirange(_T)) -> _T | undefined.
from(undefined) ->
    undefined;
from({A, _, _}) ->
    A.

-spec to(dirange(_T)) -> _T | undefined.
to(undefined) ->
    undefined;
to({_, B, _}) ->
    B.

-spec get_ranges(max_value(), capacity(), alive_sections()) -> range_map().
get_ranges(MaxValue, Capacity, AliveList) ->
    AliveSize = erlang:length(AliveList),
    AliveListSorted = lists:sort(AliveList),
    FullList = lists:seq(0, Capacity - 1),
    DeadList = lists:filter(fun(E) -> not lists:member(E, AliveList) end, FullList),
    BaseRangeMap = distribute({0, MaxValue}, Capacity, FullList),
    redistribute(BaseRangeMap, AliveSize, AliveListSorted, DeadList).

-spec find(non_neg_integer(), range_map()) -> {ok, section_number()} | none.
find(Value, RangeMap) ->
    Iterator = maps:iterator(RangeMap),
    do_find(maps:next(Iterator), Value).

%% Internal functions

-spec do_find(none | {{min_range_value(), max_range_value()}, section_number(), maps:iterator()}, non_neg_integer()) ->
    {ok, section_number()} | none.
do_find(none, _) ->
    none;
do_find({Range, Num, Iterator}, Value) ->
    case in_range(Value, Range) of
        true -> {ok, Num};
        false -> do_find(maps:next(Iterator), Value)
    end.

-spec in_range(non_neg_integer(), {min_range_value(), max_range_value()}) -> boolean().
in_range(Value, {Min, Max}) when Value >= Min andalso Value =< Max -> true;
in_range(_, _) -> false.

-spec distribute({min_range_value(), max_range_value()}, non_neg_integer(), [section_number()]) ->
    range_map().
distribute(Range, Size, ListSorted) ->
    distribute(Range, Size, ListSorted, #{}).

-spec distribute({min_range_value(), max_range_value()}, non_neg_integer(), [section_number()], range_map()) ->
    range_map().
distribute({Min, Max}, Size, ListSorted, Acc) ->
    Delta = not_zero(((Max - Min) div Size) - 1),
    SizeFromZero = Size - 1,
    {_, Result} = lists:foldl(
        fun
            (Num, {StartPos, Map}) when Num =:= SizeFromZero ->
                %% because lists indexed from 1
                {Max, Map#{{StartPos, Max} => lists:nth(Num + 1, ListSorted)}};
            (Num, {StartPos, Map}) ->
                MaxVal = StartPos + Delta,
                {MaxVal + 1, Map#{{StartPos, MaxVal} => lists:nth(Num + 1, ListSorted)}}
        end,
        {Min, Acc},
        lists:seq(0, SizeFromZero)
    ),
    Result.

-spec redistribute(range_map(), non_neg_integer(), [section_number()], [section_number()]) -> range_map().
redistribute(BaseRangeMap, AliveSize, AliveListSorted, DeadList) ->
    maps:fold(
        fun(Range, RangeNum, Acc) ->
            case lists:member(RangeNum, DeadList) of
                true ->
                    distribute(Range, AliveSize, AliveListSorted, Acc);
                false ->
                    Acc#{Range => RangeNum}
            end
        end,
        #{},
        BaseRangeMap
    ).

-spec not_zero(non_neg_integer()) -> non_neg_integer().
not_zero(0) -> 1;
not_zero(Value) -> Value.

%% Tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec without_dead_test() -> _.
without_dead_test() ->
    ?assertEqual(
        #{{0, 3} => 0, {4, 7} => 1, {8, 11} => 2, {12, 16} => 3},
        get_ranges(16, 4, [0, 1, 2, 3])
    ),
    ?assertEqual(
        #{{0, 3} => 0, {4, 7} => 1, {8, 11} => 2, {12, 17} => 3},
        get_ranges(17, 4, [0, 1, 2, 3])
    ),
    ?assertEqual(
        #{{0, 4} => 0, {5, 9} => 1, {10, 14} => 2, {15, 21} => 3},
        get_ranges(21, 4, [0, 1, 2, 3])
    ).

-spec with_dead_test() -> _.
with_dead_test() ->
    ?assertEqual(
        #{
            {0, 3} => 0,
            {4, 5} => 0,
            {6, 7} => 2,
            {8, 7} => 3,
            {8, 11} => 2,
            {12, 16} => 3
        },
        get_ranges(16, 4, [0, 2, 3])
    ),
    ?assertEqual(
        #{
            {0, 3} => 0,
            {4, 5} => 0,
            {6, 7} => 2,
            {8, 11} => 2,
            {12, 13} => 0,
            {14, 16} => 2
        },
        get_ranges(16, 4, [0, 2])
    ).

-spec find_test() -> _.
find_test() ->
    RangeMap = get_ranges(16, 4, [0, 2]),
    ?assertEqual({ok, 0}, find(5, RangeMap)),
    ?assertEqual({ok, 2}, find(10, RangeMap)),
    ?assertEqual(none, find(100, RangeMap)).

-endif.
