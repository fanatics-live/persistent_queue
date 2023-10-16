defmodule PersistentQueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :persistent_queue,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    []
  end

  defp deps do
    [
      {:credo, "~> 1.6", only: :dev}
    ]
  end
end
