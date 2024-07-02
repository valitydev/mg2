defmodule LoadProcessor.WebHandler do
  @moduledoc false
  alias LoadProcessor.Machinery

  def init(req, state) do
    {:ok, machine} =
      Machinery.new("http://machinegun:8022/v1/automaton")
      |> Machinery.start("load-test", random_id(), "start please")

    req =
      :cowboy_req.reply(
        200,
        %{"content-type" => "text/plain"},
        "Starting machine now\n#{inspect(machine)}\n",
        req
      )

    {:ok, req, state}
  end

  def terminate(_, _, _) do
    :ok
  end

  defp random_id() do
    <<id::64>> = :snowflake.new()
    :genlib_format.format_int_base(id, 62)
  end
end
