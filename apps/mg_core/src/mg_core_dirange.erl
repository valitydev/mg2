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

%% Directed range over integers
-type dirange(_T) :: nonempty_dirange(_T) | undefined.
-type direction() :: -1 | +1.
-type nonempty_dirange(_T) ::
    % Non-empty, unambiguously oriented directed range [from..to].
    {_T :: integer(), _T :: integer(), direction()}.

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
