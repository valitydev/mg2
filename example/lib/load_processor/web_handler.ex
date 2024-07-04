defmodule LoadProcessor.WebHandler do
  @moduledoc false

  def init(req, state) do
    req =
      :cowboy_req.reply(
        200,
        %{"content-type" => "text/plain"},
        "New random id\n#{inspect(random_id())}\n",
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
