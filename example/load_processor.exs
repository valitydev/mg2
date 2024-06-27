Mix.install([
  {:thrift,
   git: "https://github.com/valitydev/elixir-thrift.git",
   branch: "ft/subst-reserved-vars",
   override: true},
  {:mg_proto, git: "https://github.com/valitydev/machinegun-proto", branch: "ft/elixir-support"},
  {:snowflake, git: "https://github.com/valitydev/snowflake.git", branch: "master"},
  {:genlib, git: "https://github.com/valitydev/genlib.git", branch: "master"}
])

defmodule LoadProcessor do
  defmodule Utils do
    @moduledoc false
    alias MachinegunProto.MsgPack

    def pack(data) do
      %MsgPack.Value{bin: :erlang.term_to_binary(data)}
    end

    def unpack(%MsgPack.Value{bin: bin}) do
      :erlang.binary_to_term(bin)
    end
  end

  defmodule ProcessorHandler do
    @moduledoc false
    alias Woody.Generated.MachinegunProto.StateProcessing.Processor
    @behaviour Processor.Handler

    require Logger

    alias LoadProcessor.Utils
    alias MachinegunProto.StateProcessing.{SignalArgs, CallArgs, RepairArgs}
    alias MachinegunProto.StateProcessing.{Signal, InitSignal, TimeoutSignal, NotificationSignal}
    alias MachinegunProto.StateProcessing.{SignalResult, CallResult, RepairResult, RepairFailed}
    alias MachinegunProto.StateProcessing.{Content, HistoryRange, Direction}
    alias MachinegunProto.StateProcessing.{Machine, MachineStatus, MachineStateChange}
    alias MachinegunProto.StateProcessing.{ComplexAction, TimerAction, SetTimerAction}
    alias MachinegunProto.Base.Timer

    def new(http_path, options \\ []) do
      Processor.Handler.new(__MODULE__, http_path, options)
    end

    @impl true
    def process_signal(%SignalArgs{signal: signal, machine: machine}, _ctx, _hdlops) do
      result =
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

      {:ok, result}
    end

    @impl true
    def process_call(%CallArgs{arg: _arg, machine: _machine}, _ctx, _hdlops) do
      throw(:not_implemented)
    end

    @impl true
    def process_repair(%RepairArgs{arg: _arg, machine: _machine}, _ctx, _hdlops) do
      throw(:not_implemented)
    end

    defp process_init(%Machine{id: id, ns: ns} = _machine, args) do
      Logger.info("Starting '#{id}' of '#{ns}' with arguments: #{inspect(Utils.unpack(args))}")

      change =
        %MachineStateChange{}
        |> put_aux_state(%{"arbitrary" => "arbitrary aux state data", "counter" => 0})
        |> put_events([:counter_incremented])

      action =
        %ComplexAction{}
        |> set_timer(1)

      %SignalResult{change: change, action: action}
    end

    defp process_timeout(%Machine{id: id, ns: ns} = machine) do
      Logger.info("Timeouting machine #{id} of #{ns}")

      aux_state = get_aux_state(machine)

      change =
        %MachineStateChange{}
        |> put_aux_state(%{aux_state | "counter" => aux_state["counter"] + 1})
        |> put_events([])

      action =
        %ComplexAction{}
        |> set_timer(1)

      %SignalResult{change: change, action: action}
    end

    defp process_notification(_machine, _args) do
      throw(:not_implemented)
    end

    defp get_aux_state(%Machine{aux_state: %Content{format_version: 1, data: data}}) do
      Utils.unpack(data)
    end

    defp get_aux_state(_machine) do
      nil
    end

    defp put_aux_state(change, data, format_version \\ 1) do
      %MachineStateChange{change | aux_state: to_content(data, format_version)}
    end

    defp put_events(change, events, format_version \\ 1) do
      wrapped_events =
        events
        |> Enum.map(&to_content(&1, format_version))

      %MachineStateChange{change | events: wrapped_events}
    end

    defp to_content(data, format_version) do
      %Content{format_version: format_version, data: Utils.pack(data)}
    end

    defp set_timer(action, timeout, deadline \\ nil, range \\ nil) do
      timer = %SetTimerAction{
        timer: %Timer{timeout: timeout, deadline: deadline},
        range: maybe_full_range(range),
        timeout: nil
      }

      %ComplexAction{action | timer: %TimerAction{set_timer: timer}}
    end

    defp maybe_full_range(nil) do
      require Direction
      %HistoryRange{after: nil, limit: nil, direction: Direction.forward()}
    end

    defp maybe_full_range(range) do
      range
    end
  end

  defmodule Machinery do
    @moduledoc false
    alias Woody.Generated.MachinegunProto.StateProcessing.Automaton.Client
    alias LoadProcessor.Utils

    defstruct url: nil, opts: nil

    def new(url, opts \\ nil) do
      %__MODULE__{url: url, opts: opts}
    end

    def start(%__MODULE__{url: url, opts: opts}, ns, id, args) do
      Woody.Context.new()
      |> Client.new(url, List.wrap(opts))
      |> Client.start(ns, id, Utils.pack(args))
    end
  end

  defmodule WebHandler do
    @moduledoc false
    alias LoadProcessor.Machinery

    def init(req, state) do
      result =
        Machinery.new("http://machinegun:8022/v1/automaton")
        |> Machinery.start("load-test", random_id(), "start please")

      IO.inspect(result)

      res =
        :cowboy_req.reply(200, %{"content-type" => "text/plain"}, "Starting machine now\n", req)

      {:ok, res, state}
    end

    def terminate(_, _, _) do
      :ok
    end

    defp random_id() do
      <<id::64>> = :snowflake.new()
      :genlib_format.format_int_base(id, 62)
    end
  end
end

require Logger

alias Woody.Server.Http, as: Server

endpoint =
  Server.Endpoint.any(:inet)
  |> Map.put(:port, 8022)

handlers = [
  {"/", LoadProcessor.WebHandler, []},
  LoadProcessor.ProcessorHandler.new("/v1/stateproc", event_handler: Woody.EventHandler.Default)
]

{:ok, _pid} =
  Server.child_spec(LoadProcessor, endpoint, handlers)
  |> List.wrap()
  |> Supervisor.start_link(strategy: :one_for_one)

Logger.info("Woody server now running on #{Server.endpoint(LoadProcessor)}")
Process.sleep(:infinity)
