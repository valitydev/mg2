defmodule LoadProcessor.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  require Logger

  alias Woody.Server.Http, as: Server
  alias LoadProcessor.StfuWoodyHandler, as: WoodyHandler
  alias LoadProcessor.ProcessorHandler

  @impl true
  def start(_type, _args) do
    children = [
      server_spec([{"/", LoadProcessor.WebHandler, []}])
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: LoadProcessor.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.info("Woody server pnow running on #{server_endpoint()}")
        {:ok, pid}

      bad_ret ->
        bad_ret
    end
  end

  defp server_spec(additional_handlers) do
    endpoint =
      Server.Endpoint.any(:inet)
      |> Map.put(:port, 8022)

    Server.child_spec(LoadProcessor, endpoint, [
      ProcessorHandler.new("/v1/stateproc", event_handler: WoodyHandler) | additional_handlers
    ])
  end

  defp server_endpoint() do
    Server.endpoint(LoadProcessor)
  end
end
