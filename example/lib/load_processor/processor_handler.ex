defmodule LoadProcessor.ProcessorHandler do
  @moduledoc false
  alias Woody.Generated.MachinegunProto.StateProcessing.Processor
  @behaviour Processor.Handler

  require Logger

  alias LoadProcessor.Machine.Utils
  alias MachinegunProto.StateProcessing.{SignalArgs, CallArgs, RepairArgs}
  alias MachinegunProto.StateProcessing.{Signal, InitSignal, TimeoutSignal, NotificationSignal}
  alias MachinegunProto.StateProcessing.{SignalResult, CallResult}
  alias MachinegunProto.StateProcessing.{HistoryRange, Direction}
  alias MachinegunProto.StateProcessing.{Machine, MachineStateChange}
  alias MachinegunProto.StateProcessing.{ComplexAction, TimerAction, SetTimerAction}
  alias MachinegunProto.Base.Timer

  def new(http_path, options \\ []) do
    Processor.Handler.new(__MODULE__, http_path, options)
  end

  @impl true
  def process_signal(%SignalArgs{signal: signal, machine: machine}, _ctx, _hdlops) do
    case signal do
      %Signal{init: %InitSignal{arg: args}} ->
        process_init(machine, args)

      %Signal{timeout: %TimeoutSignal{}} ->
        process_timeout(machine)

      %Signal{notification: %NotificationSignal{arg: args}} ->
        process_notification(machine, args)

      _uknown_signal ->
        throw(:not_implemented)
    end
  end

  @impl true
  def process_call(%CallArgs{arg: args, machine: machine}, _ctx, _hdlops) do
    args = Utils.unpack(args)
    Logger.debug("Calling machine #{machine.id} of #{machine.ns} with #{inspect(args)}")

    change =
      %MachineStateChange{}
      |> put_events([{:call_processed, args}])
      |> put_aux_state(get_aux_state(machine))

    action =
      %ComplexAction{}
      |> set_timer(0)

    {:ok, %CallResult{response: Utils.pack("result"), change: change, action: action}}
  end

  @impl true
  def process_repair(%RepairArgs{arg: _arg, machine: _machine}, _ctx, _hdlops) do
    throw(:not_implemented)
  end

  defp process_init(%Machine{id: id, ns: ns} = _machine, args) do
    Logger.debug("Starting '#{id}' of '#{ns}' with arguments: #{inspect(Utils.unpack(args))}")

    change =
      %MachineStateChange{}
      |> put_aux_state(%{"arbitrary" => "arbitrary aux state data", "counter" => 0})
      |> put_events([:counter_created])

    action =
      %ComplexAction{}
      |> set_timer(1)

    {:ok, %SignalResult{change: change, action: action}}
  end

  defp process_timeout(%Machine{id: id, ns: ns} = machine) do
    Logger.debug("Timeouting machine #{id} of #{ns}")

    aux_state =
      machine
      |> get_aux_state()
      |> Map.update!("counter", &(&1 + 1))

    Logger.debug("New aux state #{inspect(aux_state)}")

    change =
      %MachineStateChange{}
      |> put_aux_state(aux_state)
      |> put_events([:counter_incremented])

    action =
      %ComplexAction{}
      |> set_timer(1)

    {:ok, %SignalResult{change: change, action: action}}
  end

  defp process_notification(_machine, _args) do
    throw(:not_implemented)
  end

  defp get_aux_state(%Machine{aux_state: aux_state}) do
    Utils.marshal(:aux_state, aux_state)
  end

  defp put_aux_state(change, data) do
    # Optional 'aux_state' technically can be 'nil' but this will
    # break machine, because it is not interpreted as msg_pack's 'nil'
    # but actually erlang's 'undefined'. In another words, default
    # 'nil' value of 'aux_state' does not leave previous value
    # unchanged but always expects it to be explicitly set.
    %MachineStateChange{change | aux_state: Utils.unmarshal(:aux_state, data)}
  end

  defp put_events(change, events) do
    wrapped_events =
      events
      |> Enum.map(&Utils.unmarshal(:content, &1))

    %MachineStateChange{change | events: wrapped_events}
  end

  defp set_timer(action, timeout, deadline \\ nil, range \\ nil) do
    timer = %SetTimerAction{
      timer: %Timer{timeout: timeout, deadline: deadline},
      range: maybe_last_n_range(range, 10),
      timeout: nil
    }

    %ComplexAction{action | timer: %TimerAction{set_timer: timer}}
  end

  defp maybe_last_n_range(nil, limit) do
    require Direction
    %HistoryRange{after: nil, limit: limit, direction: Direction.backward()}
  end

  defp maybe_last_n_range(range, _limit) do
    range
  end
end
