defmodule LoadProcessorTest do
  use ExUnit.Case
  doctest LoadProcessor

  test "greets the world" do
    assert LoadProcessor.hello() == :world
  end
end
