defmodule LoadProcessor.Machine.Event do
  @moduledoc false

  alias MachinegunProto.StateProcessing.Event, as: MachineEvent
  alias LoadProcessor.Machine.Utils

  @enforce_keys [:id, :occurred_at, :body]
  defstruct id: nil, occurred_at: nil, body: nil

  def from_machine_history(history) do
    Enum.map(history, &construct_event/1)
  end

  defp construct_event(%MachineEvent{} = machine_event) do
    {:ok, occurred_at, _rest} = DateTime.from_iso8601(machine_event.created_at)

    %__MODULE__{
      id: machine_event.id,
      occurred_at: occurred_at,
      body: Utils.marshal(:event, machine_event)
    }
  end
end
