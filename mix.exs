defmodule Soy.MixProject do
  use Mix.Project

  def project do
    [
      app: :soy,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Soy.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:rustler, "~> 0.24.0"},
      {:briefly, "~> 0.3", only: [:test, :dev]},
      {:benchee, "~> 1.0", only: :dev},
      {:mnesia_rocksdb, git: "https://github.com/aeternity/mnesia_rocksdb", ref: "b70654d"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
