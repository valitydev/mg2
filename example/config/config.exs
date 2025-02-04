import Config

config :logger, level: :info

config :load_processor, :automaton,
  url: "http://machinegun:8022/v1/automaton",
  options: nil
