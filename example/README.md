# LoadProcessor

**TODO: Add description**


## Receipts

``` elixir
# Alias machinery helper for convinence
alias LoadProcessor.Machine

# Create machine client
machine = Machine.new("load-test")

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
