defmodule Kaufmann.Config do
  @moduledoc """
  Getters for pulling config.exs values
  """

  def consumer_group, do: Application.get_env(:kaufmann, :consumer_group)
  def default_topic, do: Application.get_env(:kaufmann, :default_topic)
  def default_topics, do: [default_topic()]
  def event_handler, do: Application.get_env(:kaufmann, :event_handler_mod)
  def producer_mod, do: Application.get_env(:kaufmann, :producer_mod)
  def schema_path, do: Application.get_env(:kaufmann, :schema_path)
  def schema_registry_uri, do: Application.get_env(:kaufmann, :schema_registry_uri)

  def service_name, do: System.get_env("SERVICE_NAME")
  def service_id, do: System.get_env("HOST_NAME")
end
