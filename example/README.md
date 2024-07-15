# LoadProcessor

**TODO: Add description**


## Examples

### Per machine

``` elixir
# Alias machinery helper for convinence
alias LoadProcessor.Machine

# Create machine client
machine = Machine.new("eventful-counter")

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

### Start batch

``` elixir
LoadProcessor.run("simple-counter", 100)
```
