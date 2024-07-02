# LoadProcessor

**TODO: Add description**


## Receipts

``` elixir
# Alias machinery helper for convinence
alias LoadProcessor.Machinery, as: M

# Create automaton client
automaton = M.new("http://machinegun:8022/v1/automaton")

# Start machine
automaton |> M.start("load-test", "my-machine", "start payload")

# Call machine
automaton |> M.call("load-test", "my-machine", "call payload")

# Repair machine
automaton |> M.simple_repair("load-test", "my-machine")

# Get machine
automaton |> M.get("load-test", "my-machine")

# Get machine status
automaton |> M.get("load-test", "my-machine") |> elem(1) |> Map.get(:status)
```
