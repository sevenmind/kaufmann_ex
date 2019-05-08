defmodule KaufmannEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :kaufmann_ex,
      version: "0.4.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls],
      docs: [
        main: "readme",
        extras: ["README.md"]
      ],
      description: "build microservices with Kafka + Avro schemas",
      package: package()
    ]
  end

  def package do
    [
      maintainers: ["sevenmind", "Grant McLendon"],
      links: %{
        GitHub: "https://github.com/sevenmind/kaufmann_ex"
      },
      licenses: ["MIT"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :snappy]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage, "~> 0.14"},
      {:kafka_ex_gen_stage_consumer,
       git: "https://github.com/sevenmind/kafka_ex_gen_stage_consumer"},
      # {:kafka_ex, "~> 0.9"},
      {:kafka_ex, git: "https://github.com/kafkaex/kafka_ex"},
      {:jason, "~> 1.1"},
      {:httpoison, "~> 1.5"},
      {:avro_ex, "~> 0.1.0-beta.6"},
      {:schemex, "~> 0.1.1"},
      {:nanoid, "~> 1.0"},
      {:memoize, "~> 1.2"},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:credo, "~> 1.0.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.4", only: [:dev], runtime: false},
      {:bypass, "~> 1.0", only: :test},
      {:excoveralls, "~> 0.10", only: :test},
      {:inch_ex, only: :docs},
      {:benchee, "~> 0.11", only: [:dev, :test]},
      {:mock, "~> 0.3.0", only: [:test]},
      {:snappy, git: "https://github.com/fdmanana/snappy-erlang-nif"},
      {:flow, "~> 0.14.3"},
      {:telemetry, "~> 0.4"},
      {:opencensus, "~> 0.9.0", override: true},
      {:opencensus_elixir, "~> 0.3.0"},
    ]
  end

  defp aliases do
    [
      test: "test --exclude integration --no-start",
      bench: "run --no-start"
    ]
  end
end
