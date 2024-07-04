# LoadProcessor

**TODO: Add description**


## Receipts

``` elixir
# Alias machinery helper for convinence
alias LoadProcessor.Machine

# Create machine client
machine = Machine.new("http://machinegun:8022/v1/automaton", "load-test", "my-machine")

# Start machine
machine |> Machine.start("start payload")

# Call machine (returns call result, not machine client)
machine |> Machine.call("call payload")

# Repair machine
machine |> Machine.simple_repair()

# Get machine
machine |> Machine.get()

# Get machine status
machine |> Machine.get() |> Map.get(:status)
```
