defmodule LoadProcessor do
  @moduledoc false
  alias LoadProcessor.Machine

  def run(ns, count) do
    :timer.tc(fn -> do_run(ns, count) end)
  end

  defp do_run(ns, count) do
    1..count
    |> Task.async_stream(fn i -> try_start(ns, i) end, max_concurrency: count, timeout: :infinity)
    |> Enum.filter(fn
      {:ok, {_i, nil}} -> false
      {:ok, _} -> true
    end)
    |> Enum.map(&elem(&1, 1))
  end

  defp try_start(ns, i) do
    ns
    |> Machine.new()
    |> Machine.start(%{"payload" => nil})

    {i, nil}
  rescue
    exception in Woody.BadResultError ->
      {i, exception}
  end
end
