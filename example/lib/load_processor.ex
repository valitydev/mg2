defmodule LoadProcessor do
  @moduledoc false
  alias LoadProcessor.Machine

  def run(count) do
    :timer.tc(fn -> do_run(count) end)
  end

  defp do_run(count) do
    1..count
    |> Task.async_stream(&try_start/1, max_concurrency: count, timeout: :infinity)
    |> Enum.filter(fn
      {:ok, {_i, nil}} -> false
      {:ok, _} -> true
    end)
    |> Enum.map(&elem(&1, 1))
  end

  defp try_start(i) do
    "load-test"
    |> Machine.new()
    |> Machine.start(%{"payload" => nil})

    {i, nil}
  rescue
    exception in Woody.BadResultError ->
      {i, exception}
  end
end
