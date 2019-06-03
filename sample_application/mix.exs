defmodule Sample.Mixfile do
  use Mix.Project

  def project do
    [
      app: :sample,
      version: "0.0.0-alpha0",
      elixir: "~> 1.6",
      elixirc_paths: ["lib"],
      deps_path: "../deps",
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Sample.Application, []}
    ]
  end

  def deps do
    [
      {:kaufmann_ex, path: ".."}
    ]
  end

  defp aliases do
    [
      test: "test --no-start --exclude integration"
    ]
  end
end
