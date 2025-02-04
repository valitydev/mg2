defmodule LoadProcessor.ProcessorHandler do
  @moduledoc false
  alias Woody.Generated.MachinegunProto.StateProcessing.Processor
  @behaviour Processor.Handler

  require OpenTelemetry.Tracer, as: Tracer
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
    {hdlopts, options} =
      case Keyword.pop(options, :less_events, false) do
        {false, opts} -> {%{less_events: false}, opts}
        {true, opts} -> {%{less_events: true}, opts}
      end

    Processor.Handler.new({__MODULE__, hdlopts}, http_path, options)
  end

  @impl true
  def process_signal(%SignalArgs{signal: signal, machine: machine}, _ctx, hdlopts) do
    case signal do
      %Signal{init: %InitSignal{arg: args}} ->
        Tracer.with_span "initializing" do
          process_init(machine, Utils.unpack(args), hdlopts)
        end

      %Signal{timeout: %TimeoutSignal{}} ->
        Tracer.with_span "timeouting" do
          process_timeout(machine, hdlopts)
        end

      %Signal{notification: %NotificationSignal{arg: args}} ->
        Tracer.with_span "notifying" do
          process_notification(machine, Utils.unpack(args), hdlopts)
        end

      _uknown_signal ->
        throw(:not_implemented)
    end
  end

  @impl true
  def process_call(%CallArgs{arg: args, machine: machine}, _ctx, hdlopts) do
    Tracer.with_span "processing call" do
      Logger.debug("Calling machine #{machine.id} of #{machine.ns} with #{inspect(args)}")

      change =
        %MachineStateChange{}
        |> preserve_aux_state(machine)
        |> put_events([{:call_processed, args}], hdlopts)

      action =
        %ComplexAction{}
        |> set_timer(0)

      {:ok, %CallResult{response: Utils.pack("result"), change: change, action: action}}
    end
  end

  @impl true
  def process_repair(%RepairArgs{arg: _arg, machine: _machine}, _ctx, _hdlopts) do
    throw(:not_implemented)
  end

  defp process_init(%Machine{id: id, ns: ns} = _machine, args, hdlopts) do
    Logger.debug("Starting '#{id}' of '#{ns}' with arguments: #{inspect(args)}")

    change =
      %MachineStateChange{}
      |> put_aux_state(%{"arbitrary" => "arbitrary aux state data", :counter => 0})
      |> put_events([:counter_created], hdlopts)

    action =
      %ComplexAction{}
      |> set_timer(get_rand_sleep_time([3, 2, 1]))

    {:ok, %SignalResult{change: change, action: action}}
  end

  defp get_rand_sleep_time(seed) do
    # 0s 1s 2s etc occurrences in seed
    seed
    |> Enum.with_index()
    |> Enum.map(fn {occ, i} -> List.duplicate(i, occ) end)
    |> List.flatten()
    |> Enum.random()
  end

  defp process_timeout(%Machine{id: id, ns: ns} = machine, hdlopts) do
    Logger.debug("Timeouting machine #{id} of #{ns}")
    aux_state = get_aux_state(machine)

    case aux_state do
      %{notified: true} ->
        change =
          %MachineStateChange{}
          |> put_aux_state(aux_state)
          |> put_events([:counter_stopped], hdlopts)

        {:ok, %SignalResult{change: change, action: %ComplexAction{}}}

      %{counter: counter} when counter < 100 ->
        aux_state = Map.update!(aux_state, :counter, &(&1 + 1))
        Logger.debug("New aux state #{inspect(aux_state)}")

        change =
          %MachineStateChange{}
          |> put_aux_state(aux_state)
          |> put_events([{:counter_incremented, 1}], hdlopts)

        action =
          %ComplexAction{}
          |> set_timer(get_rand_sleep_time([3, 2, 1]))

        {:ok, %SignalResult{change: change, action: action}}

      _ ->
        change =
          %MachineStateChange{}
          |> put_aux_state(aux_state)
          |> put_events([:counter_stopped], hdlopts)

        {:ok, %SignalResult{change: change, action: %ComplexAction{}}}
    end
  end

  defp process_notification(%Machine{id: id, ns: ns} = machine, args, hdlopts) do
    Logger.debug("Notifying machine #{id} of #{ns}")

    change =
      %MachineStateChange{}
      |> put_aux_state(Map.put(get_aux_state(machine), :notified, true))
      |> put_events([{:counter_notified, args}], hdlopts)

    action =
      %ComplexAction{}
      |> set_timer(get_rand_sleep_time([3, 2, 1]))

    {:ok, %SignalResult{change: change, action: action}}
  end

  defp preserve_aux_state(change, %Machine{aux_state: aux_state}) do
    %MachineStateChange{change | aux_state: aux_state}
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

  defp put_events(change, _events, %{less_events: true}) do
    change
  end

  defp put_events(change, events, _hdlopts) do
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
