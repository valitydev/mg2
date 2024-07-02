defmodule LoadProcessor.Utils do
  @moduledoc false
  alias MachinegunProto.MsgPack

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
end
