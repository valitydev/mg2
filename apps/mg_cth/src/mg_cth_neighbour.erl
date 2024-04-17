-module(mg_cth_neighbour).

-behaviour(gen_server).

-export([start/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([connecting/1]).
-export([echo/1]).

-define(SERVER, ?MODULE).

-record(state, {}).

-type state() :: #state{}.

-spec connecting(_) -> _.
connecting(RemoteData) ->
    gen_server:call(?MODULE, {connecting, RemoteData}).

-spec echo(_) -> _.
echo(Msg) ->
    gen_server:call(?MODULE, {echo, Msg}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
-spec start() -> _.
start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

-spec init(_) -> {ok, state()}.
init([]) ->
    {ok, #state{}}.

-spec handle_call(term(), {pid(), _}, state()) -> {reply, any(), state()}.
handle_call({echo, Echo}, _From, State = #state{}) ->
    {reply, {ok, Echo}, State};
handle_call({connecting, _RemoteData}, _From, State = #state{}) ->
    {reply, {ok, #{1 => node()}}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

-spec terminate(_Reason, state()) -> ok.
terminate(_Reason, _State = #state{}) ->
    ok.

-spec code_change(_OldVsn, state(), _Extra) -> {ok, state()}.
code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
