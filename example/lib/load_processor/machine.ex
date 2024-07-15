defmodule LoadProcessor.Machine do
  @moduledoc false

  alias Woody.Generated.MachinegunProto.StateProcessing.Automaton.Client
  alias MachinegunProto.StateProcessing.{MachineDescriptor, Reference, HistoryRange, Direction}
  alias LoadProcessor.Machine.{History, Utils}

  require OpenTelemetry.Tracer, as: Tracer
  require Direction

  @default_range %HistoryRange{limit: 10, direction: Direction.backward()}

  @enforce_keys [:client, :ns, :id]
  defstruct client: nil,
            ns: nil,
            id: nil,
            status: nil,
            history: nil,
            aux_state: nil

  @automaton_url Application.compile_env!(:load_processor, [:automaton, :url])
  @automaton_opts List.wrap(Application.compile_env!(:load_processor, [:automaton, :options]))

  def new(ns, id) do
    new(Woody.Context.new(), ns, id)
  end

  def new(ns) do
    new(ns, random_id())
  end

  def new(woody_ctx, ns, id) do
    %__MODULE__{
      client: Client.new(woody_ctx, @automaton_url, @automaton_opts),
      ns: ns,
      id: id
    }
  end

  def loaded?(%__MODULE__{status: nil}), do: false
  def loaded?(%__MODULE__{status: _}), do: true

  def start(%__MODULE__{client: client, ns: ns, id: id} = machine, args) do
    Tracer.with_span "starting machine" do
      _ = Client.start!(client, ns, id, Utils.pack(args))
    end

    get(machine)
  end

  def get(%__MODULE__{client: client, ns: ns, id: id} = machine) do
    Tracer.with_span "getting machine" do
      machine_state = Client.get_machine!(client, make_descr(ns, id, nil))

      %{
        machine
        | history: History.from_machine_state(machine_state),
          status: machine_state.status,
          aux_state: Utils.marshal(:aux_state, machine_state.aux_state)
      }
    end
  end

  def call(%__MODULE__{client: client, ns: ns, id: id}, args) do
    Tracer.with_span "calling machine" do
      client
      |> Client.call!(make_descr(ns, id, nil), Utils.pack(args))
      |> Utils.unpack()
    end
  end

  def notify(%__MODULE__{client: client, ns: ns, id: id}, args) do
    Tracer.with_span "sending notification to machine" do
      client
      |> Client.notify!(make_descr(ns, id, nil), Utils.pack(args))
    end
  end

  def simple_repair(%__MODULE__{client: client, ns: ns, id: id} = machine) do
    Tracer.with_span "simply repairaing machine" do
      _ = Client.simple_repair(client, ns, make_ref(id))
      machine
    end
  end

  defp make_descr(ns, id, range) do
    %MachineDescriptor{ns: ns, ref: make_ref(id), range: range || @default_range}
  end

  defp make_ref(id) do
    %Reference{id: id}
  end

  defp random_id() do
    <<id::64>> = :snowflake.new()
    :genlib_format.format_int_base(id, 62)
  end
end
