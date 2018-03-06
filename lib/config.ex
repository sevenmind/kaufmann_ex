defmodule KaufmannEx.Config do
  @moduledoc """
  Convenience Getters for pulling config.exs values
  """

  @doc """
    `Application.get_env(:kaufmann_ex, :consumer_group)`
  """
  @spec consumer_group() :: String.t()
  def consumer_group, do: Application.get_env(:kaufmann_ex, :consumer_group)

  @doc """
    `Application.get_env(:kaufmann_ex, :default_topic)`
  """
  @spec default_topic() :: String.t()
  def default_topic, do: Application.get_env(:kaufmann_ex, :default_topic)

  @doc """
    `default_topic/0` in a list

    `[KaufmannEx.Config.default_topic()]`
  """
  @spec default_topics() :: [String.t()]
  def default_topics, do: [default_topic()]

  @doc """
  `Application.get_env(:kaufmann_ex, :event_handler_mod)`
  """
  @spec event_handler() :: String.t()
  def event_handler, do: Application.get_env(:kaufmann_ex, :event_handler_mod)

  @doc """
  `Application.get_env(:kaufmann_ex, :producer_mod)`
  """
  @spec producer_mod() :: String.t()
  def producer_mod, do: Application.get_env(:kaufmann_ex, :producer_mod)

  @doc """
  `Application.get_env(:kaufmann_ex, :schema_path)`
  """
  @spec schema_path() :: String.t()
  def schema_path, do: Application.get_env(:kaufmann_ex, :schema_path)

  @doc """
  `Application.get_env(:kaufmann_ex, :schema_registry_uri)`
  """
  @spec schema_registry_uri() :: String.t()
  def schema_registry_uri, do: Application.get_env(:kaufmann_ex, :schema_registry_uri)

  @doc """
  `System.get_env("SERVICE_NAME")`
  """
  @spec service_name() :: String.t()
  def service_name, do: System.get_env("SERVICE_NAME")

  @doc """
  `System.get_env("HOST_NAME")`
  """
  @spec service_id() :: String.t()
  def service_id, do: System.get_env("HOST_NAME")
end
