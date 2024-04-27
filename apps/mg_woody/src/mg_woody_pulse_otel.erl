-module(mg_woody_pulse_otel).

-include_lib("mg_woody/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-export([handle_beat/2]).

%% TODO Specify available options if any
-type options() :: map().

-type beat() ::
    #woody_event{}
    | #woody_request_handle_error{}
    | mg_core_pulse:beat()
    | mg_skd_scanner:beat().

-export_type([options/0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.

%%
%% Woody API beats
%% ============================================================================
%%
handle_beat(Options, #woody_event{event = Event, rpc_id = RpcID, event_meta = Meta}) ->
    woody_event_handler_otel:handle_event(Event, RpcID, Meta, Options);
%% Woody server's function handling error beat.
handle_beat(_Options, #woody_request_handle_error{namespace = NS, machine_id = ID, exception = Exception}) ->
    mg_core_otel:record_exception(Exception, machine_tags(NS, ID));
%% Disregard any other
handle_beat(_Options, _Beat) ->
    ok.

%% Internal

-spec machine_tags(mg_core:ns(), mg_core:id()) -> map().
machine_tags(Namespace, ID) ->
    machine_tags(Namespace, ID, #{}).

-spec machine_tags(mg_core:ns(), mg_core:id(), map()) -> map().
machine_tags(Namespace, ID, OtherTags) ->
    maps:merge(OtherTags, #{
        <<"machine.ns">> => Namespace,
        <<"machine.id">> => ID
    }).
