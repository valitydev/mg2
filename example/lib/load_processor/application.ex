defmodule LoadProcessor.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  require Logger

  alias Woody.Server.Http, as: Server

  @impl true
  def start(_type, _args) do
    endpoint =
      Server.Endpoint.any(:inet)
      |> Map.put(:port, 8022)

    handlers = [
      {"/", LoadProcessor.WebHandler, []},
      LoadProcessor.ProcessorHandler.new("/v1/stateproc",
        event_handler: LoadProcessor.StfuWoodyHandler
      )
    ]

    children = [
      Server.child_spec(LoadProcessor, endpoint, handlers)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: LoadProcessor.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.info("Woody server now running on #{Server.endpoint(LoadProcessor)}")
        {:ok, pid}

      bad_ret ->
        bad_ret
    end
  end
end
