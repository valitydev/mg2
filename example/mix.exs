defmodule LoadProcessor.MixProject do
  use Mix.Project

  def project do
    [
      app: :load_processor,
      version: "0.1.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {LoadProcessor.Application, []}
    ]
  end

  defp releases do
    [
      api_key_mgmt: [
        version: "0.1.0",
        applications: [
          api_key_mgmt: :permanent,
          logstash_logger_formatter: :load,
          opentelemetry: :temporary
        ],
        include_executables_for: [:unix],
        include_erts: false
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:thrift,
       git: "https://github.com/valitydev/elixir-thrift.git",
       branch: "ft/subst-reserved-vars",
       override: true},
      {:mg_proto,
       git: "https://github.com/valitydev/machinegun-proto", branch: "ft/elixir-support"},
      {:snowflake, git: "https://github.com/valitydev/snowflake.git", branch: "master"},
      {:genlib, git: "https://github.com/valitydev/genlib.git", branch: "master"},
      {:logstash_logger_formatter,
       git: "https://github.com/valitydev/logstash_logger_formatter.git",
       branch: "master",
       only: [:prod],
       runtime: false},
      {:gproc, "~> 0.9.1", override: true},
      {:opentelemetry, "~> 1.3"},
      {:opentelemetry_api, "~> 1.2"},
      {:opentelemetry_exporter, "~> 1.6"}
    ]
  end
end
