%%%
%%% Copyright 2022 Valitydev
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

-module(mg_core_circular_buffer).

%%
%% @moduledoc
%% Circular buffer implementation with an ability to configure
%% how many elements are deleted when the buffer is full
%%

%% API Types

-export([new/1]).
-export([new/2]).
-export([push/2]).
-export([member/2]).

-export_type([t/1]).

-opaque t(T) :: {Size :: non_neg_integer(), bounds(), Buffer :: list(T)}.

%% Internal types

-type bounds() :: {UpperBound :: pos_integer(), LowerBound :: non_neg_integer()}.

%%
%% API Functions
%%

-spec new(UpperBound :: pos_integer()) ->
    t(_) | no_return().
new(UpperBound) ->
    new(UpperBound, UpperBound - 1).

-spec new(UpperBound :: pos_integer(), LowerBound :: non_neg_integer()) ->
    t(_) | no_return().
new(UpperBound, LowerBound) when UpperBound > 0, LowerBound >= 0, LowerBound < UpperBound ->
    {0, {UpperBound, LowerBound}, []};
new(_, _) ->
    error(badarg).

-spec push(T, t(T)) ->
    t(T).
push(Element, {Size, {UpperBound, _} = Bs, Buffer}) when Size < UpperBound ->
    {Size + 1, Bs, [Element | Buffer]};
push(Element, {Size, {UpperBound, LowerBound} = Bs, Buffer}) when Size =:= UpperBound ->
    BufferShrunk = rebound(Bs, Buffer),
    {LowerBound + 1, Bs, [Element | BufferShrunk]}.

-spec member(T, t(T)) ->
    boolean().
member(Element, {_, _, Buffer}) ->
    lists:member(Element, Buffer).

%%
%% Internal Functions
%%

-spec rebound(bounds(), Buffer :: list()) -> Result :: list().
rebound({UpperBound, LowerBound}, Buffer) when LowerBound =:= UpperBound - 1 ->
    % lists:reverse(tl(lists:reverse(Buffer))),
    lists:droplast(Buffer);
rebound({_, LowerBound}, Buffer) ->
    pick_n(LowerBound, Buffer).

-spec pick_n(Amount :: non_neg_integer(), Init :: list()) -> Result :: list().
% Basically what lists:droplast/1 does
pick_n(Amount, Buffer) ->
    pick_n(Amount, Buffer, []).

-spec pick_n(Amount :: non_neg_integer(), Init :: list(), Acc :: list()) -> Result :: list().
pick_n(0, _Buffer, Acc) ->
    lists:reverse(Acc);
pick_n(Amount, [H | T], Acc) ->
    pick_n(Amount - 1, T, [H | Acc]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

-spec test() -> _.

-spec circular_buffer_new_test() -> _.
-spec circular_buffer_push_test() -> _.
-spec circular_buffer_push_overflow_test() -> _.
-spec circular_buffer_member_test() -> _.

circular_buffer_new_test() ->
    ?assertEqual({0, {20, 19}, []}, new(20)),
    ?assertError(badarg, apply(?MODULE, new, [0])),
    ?assertEqual({0, {20, 10}, []}, new(20, 10)),
    ?assertError(badarg, apply(?MODULE, new, [0, 0])),
    ?assertError(badarg, apply(?MODULE, new, [10, 10])),
    ?assertError(badarg, apply(?MODULE, new, [20, 30])).

circular_buffer_push_test() ->
    Buffer = new(3),
    ?assertMatch({1, _, [abc]}, push(abc, Buffer)),
    ?assertMatch({2, _, [bcd, abc]}, lists:foldl(fun push/2, Buffer, [abc, bcd])),
    ?assertMatch({3, _, [cbd, bcd, abc]}, lists:foldl(fun push/2, Buffer, [abc, bcd, cbd])).

circular_buffer_push_overflow_test() ->
    ?assertMatch({2, _, [cbd, bcd]}, lists:foldl(fun push/2, new(2), [abc, bcd, cbd])),
    ?assertMatch({2, _, [xyz, bde]}, lists:foldl(fun push/2, new(4, 1), [abc, bcd, cbd, bde, xyz])).

circular_buffer_member_test() ->
    Buffer = lists:foldl(fun push/2, new(3), [abc, bcd, cbd]),
    ?assertEqual(true, member(bcd, Buffer)),
    ?assertEqual(false, member(xyz, Buffer)).

-endif.
