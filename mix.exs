defmodule KaufmannEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :kaufmann_ex,
      # version is -dev while there are git dependencies
      version: "0.4.4-dev",
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
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage, "~> 0.14"},
      {:flow, "~> 0.15"},
      {:kafka_ex_gen_stage_consumer,
       git: "https://github.com/sevenmind/kafka_ex_gen_stage_consumer"},
      # Waiting Next release of kafka_ex > 0.10
      {:kafka_ex, git: "https://github.com/kafkaex/kafka_ex"},
      {:jason, "~> 1.1"},
      {:avro_ex_v0, "~> 0.1.0-beta.6.1"},
      {:avro_ex, git: "https://github.com/beam-community/avro_ex.git"},
      {:ecto, "~> 3.0", override: true},

      # use ex_json_schema for until merged: https://github.com/jonasschmidt/ex_json_schema/pull/43
      {:ex_json_schema, git: "https://github.com/woylie/ex_json_schema"},
      {:schemex, "~> 0.1.1"},
      {:nanoid, "~> 2.0"},
      {:memoize, "~> 1.2"},
      {:telemetry, "~> 0.4"},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:credo, "~> 1.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.4", only: [:dev], runtime: false},
      {:bypass, "~> 1.0", only: :test},
      {:excoveralls, "~> 0.11", only: :test},
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:mock, "~> 0.3.0", only: [:test]},
      {:inch_ex, only: :docs}
    ]
  end

  defp aliases do
    [
      test: [
        "test --exclude integration --no-start"
      ],
      bench: "run --no-start"
    ]
  end
end
