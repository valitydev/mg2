defmodule LoadProcessor.Machine.Utils do
  @moduledoc false
  alias MachinegunProto.MsgPack
  alias MachinegunProto.StateProcessing.Content
  alias MachinegunProto.StateProcessing.Event

  def pack(nil) do
    %MsgPack.Value{nl: %MsgPack.Nil{}}
  end

  def pack(data) do
    %MsgPack.Value{bin: :erlang.term_to_binary(data)}
  end

  def unpack(%MsgPack.Value{nl: %MsgPack.Nil{}}) do
    nil
  end

  def unpack(%MsgPack.Value{bin: bin}) do
    :erlang.binary_to_term(bin)
  end

  @format_version 1

  def marshal(:content, %Content{format_version: @format_version, data: data}), do: unpack(data)
  def marshal(:content, _), do: nil
  def marshal(:aux_state, value), do: marshal(:content, value)
  def marshal(:event, %Event{format_version: @format_version, data: data}), do: unpack(data)
  def marshal(type, _value), do: raise("Marshalling of type #{inspect(type)} is not supported")

  def unmarshal(:content, nil), do: nil
  def unmarshal(:content, value), do: %Content{format_version: @format_version, data: pack(value)}
  def unmarshal(:aux_state, value), do: unmarshal(:content, value)

  def unmarshal(type, _value),
    do: raise("Unmarshalling of type #{inspect(type)} is not supported")
end
