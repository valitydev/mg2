defmodule LoadProcessor.StfuWoodyHandler do
  @moduledoc false
  alias :woody_event_handler, as: WoodyEventHandler
  alias Woody.EventHandler.Formatter
  @behaviour WoodyEventHandler
  require Logger

  @exposed_meta [
    :event,
    :service,
    :function,
    :type,
    :metadata,
    :url,
    :deadline,
    :execution_duration_ms
  ]

  @allowed_severity [:warning, :error]

  def handle_event(event, rpc_id, meta, _opts) do
    case WoodyEventHandler.get_event_severity(event, meta) do
      level when level in @allowed_severity ->
        Logger.log(
          level,
          Formatter.format(rpc_id, event, meta),
          WoodyEventHandler.format_meta(event, meta, @exposed_meta)
        )

      _ ->
        :ok
    end
  end
end
