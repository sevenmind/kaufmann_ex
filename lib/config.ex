defmodule KaufmannEx.Config do
  @moduledoc """
  Convenience Getters for pulling config.exs values

  A config.exs may look like
  ```
  # test env
  config :kaufmann_ex,
    consumer_group: System.get_env("CONSUMER_GROUP"),
    default_topic: System.get_env("KAFKA_TOPIC"),
    event_handler_demand: 50,
    event_handler_mod: nil, # Be sure to specify your event handler
    gen_consumer_mod: KaufmannEx.Stages.GenConsumer,
    producer_mod: KaufmannEx.Publisher,
    schema_path: "priv/schemas",
    schema_registry_uri: System.get_env("SCHEMA_REGISTRY_PATH"),
    service_id: System.get_env("HOSTNAME"),
    service_name: "SampleService"
  ```
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
  @spec event_handler() :: atom
  def event_handler, do: Application.get_env(:kaufmann_ex, :event_handler_mod)

  @doc """
  `Application.get_env(:kaufmann_ex, :producer_mod)`
  """
  @spec producer_mod() :: atom
  def producer_mod, do: Application.get_env(:kaufmann_ex, :producer_mod, KaufmannEx.Publisher)

  @doc """
  `Application.get_env(:kaufmann_ex, :schema_path)`
  """
  @spec schema_path() :: String.t()
  def schema_path, do: Application.get_env(:kaufmann_ex, :schema_path, "priv/schemas")

  @doc """
  `Application.get_env(:kaufmann_ex, :schema_registry_uri)`
  """
  @spec schema_registry_uri() :: String.t()
  def schema_registry_uri, do: Application.get_env(:kaufmann_ex, :schema_registry_uri)

  @doc """
  `Application.get_env(:kaufmann_ex, :service_name)`
  """
  @spec service_name() :: String.t()
  def service_name, do: Application.get_env(:kaufmann_ex, :service_name)

  @doc """
  `Application.get_env(:kaufmann_ex, :service_id)`
  """
  @spec service_id() :: String.t()
  def service_id, do:  Application.get_env(:kaufmann_ex, :service_id)

  @doc """
  Application.get_env(:kaufmann_ex, :event_handler_demand, 50)
  """
  @spec event_handler_demand() :: integer()
  def event_handler_demand, do: Application.get_env(:kaufmann_ex, :event_handler_demand, 50)

 @doc """
  Application.get_env(:kaufmann_ex, :gen_consumer_mod)
  """
  def gen_consumer_mod , do:  Application.get_env(:kaufmann_ex, :gen_consumer_mod, KaufmannEx.Stages.GenConsumer)

end
