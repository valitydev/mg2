defmodule LoadProcessor.Machinery do
  @moduledoc false
  alias Woody.Generated.MachinegunProto.StateProcessing.Automaton.Client
  alias LoadProcessor.Utils
  alias MachinegunProto.StateProcessing.{MachineDescriptor, Reference, HistoryRange, Direction}

  @enforce_keys [:client]
  defstruct client: nil

  def new(url, opts \\ nil) do
    new(Woody.Context.new(), url, opts)
  end

  def new(ctx, url, opts) do
    %__MODULE__{client: Client.new(ctx, url, List.wrap(opts))}
  end

  def start(%__MODULE__{client: client}, ns, id, args) do
    Client.start(client, ns, id, Utils.pack(args))
  end

  def get(%__MODULE__{client: client}, ns, id) do
    require Direction
    Client.get_machine(client, make_descr(ns, id))
  end

  def call(%__MODULE__{client: client}, ns, id, args) do
    case Client.call(client, make_descr(ns, id), Utils.pack(args)) do
      {:ok, result} -> {:ok, Utils.unpack(result)}
      other_response -> other_response
    end
  end

  def simple_repair(%__MODULE__{client: client}, ns, id) do
    Client.simple_repair(client, ns, make_ref(id))
  end

  ###

  defp make_descr(ns, id) do
    require Direction

    %MachineDescriptor{
      ns: ns,
      ref: make_ref(id),
      range: %HistoryRange{limit: 5, direction: Direction.backward()}
    }
  end

  defp make_ref(id) do
    %Reference{id: id}
  end
end
