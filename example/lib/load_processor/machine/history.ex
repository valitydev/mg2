defmodule LoadProcessor.Machine.History do
  alias MachinegunProto.StateProcessing.Machine, as: MachineState
  alias MachinegunProto.StateProcessing.{HistoryRange, Direction}
  alias LoadProcessor.Machine.Event
  require Direction

  @enforce_keys [:events, :range]
  defstruct events: nil, range: nil

  def from_machine_state(%MachineState{history: history, history_range: history_range}) do
    events = Event.from_machine_history(history)

    %__MODULE__{events: events, range: history_range}
  end
end
