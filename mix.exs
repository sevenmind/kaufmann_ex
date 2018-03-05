defmodule Kaufmann.MixProject do
  use Mix.Project

  def project do
    [
      app: :kaufmann,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :kafka_ex]
      # mod: {Kaufmann.Supervisor, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.16", only: :dev, runtime: false},
      {:flow, "~> 0.11"},
      # kafka Client
      {:kafka_ex, "~> 0.8.1"},
      # JSON lib
      {:poison, "~> 3.1"},
      # HTTP lib, overrride b/c some other libs specify older versions
      {:httpoison, "~> 1.0", override: true},
      {:nanoid, "~> 1.0"},
      {:avro_ex, "~> 0.1.0-beta.0"},
      {:schemex, "~> 0.1.0"},
      {:credo, "~> 0.9.0-rc2", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 0.5", only: [:dev], runtime: false},
      {:benchwarmer, ">= 0.0.0", only: [:dev]},
      {:mock, "~> 0.3.0", only: :test},
      {:bypass, "~> 0.8", only: :test},
      {:excoveralls, "~> 0.8", only: :test},
      {:distillery, "~> 1.5", runtime: false},
      {:mix_test_watch, "~> 0.5", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      test: "test --no-start"
    ]
  end
end
