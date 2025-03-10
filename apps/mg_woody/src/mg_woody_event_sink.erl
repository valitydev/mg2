%%%
%%% Copyright 2024 Valitydev
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

-module(mg_woody_event_sink).

-include_lib("mg_proto/include/mg_proto_event_sink_thrift.hrl").

-export([serialize/3]).

%%
%% event_sink events encoder
%%

-spec serialize(mg_core:ns(), mg_core:id(), mg_core_events:event()) -> iodata().

serialize(SourceNS, SourceID, Event) ->
    Codec = thrift_strict_binary_codec:new(),
    #{
        id := EventID,
        created_at := CreatedAt,
        body := {Metadata, Content}
    } = Event,
    Data =
        {event, #mg_evsink_MachineEvent{
            source_ns = SourceNS,
            source_id = SourceID,
            event_id = EventID,
            created_at = mg_woody_packer:pack(timestamp_ns, CreatedAt),
            format_version = maps:get(format_version, Metadata, undefined),
            data = mg_woody_packer:pack(opaque, Content)
        }},
    Type = {struct, union, {mg_proto_event_sink_thrift, 'SinkEvent'}},
    case thrift_strict_binary_codec:write(Codec, Type, Data) of
        {ok, NewCodec} ->
            thrift_strict_binary_codec:close(NewCodec);
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.
