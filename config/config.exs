# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :kafka_ex,
  brokers: System.get_env("KAFKA_BROKERS"),
  use_ssl: false,
  consumer_group: System.get_env("CONSUMER_GROUP"),
  commit_threshold: 10,
  commit_interval: 100,
  sync_timeout: 10_000

config :kaufmann_ex,
  consumer_group: System.get_env("CONSUMER_GROUP"),
  default_topic: System.get_env("KAFKA_TOPIC"),
  event_handler_mod: nil,
  producer_mod: KaufmannEx.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: System.get_env("SCHEMA_REGISTRY_PATH"),
  service_name: "SampleService",
  service_id: "SampleHost",
  event_handler_demand: 50,
  commit_strategy: :async_commit

config :logger,
  level: :info

env_config = Path.expand("#{Mix.env()}.exs", __DIR__)

if File.exists?(env_config) do
  import_config(env_config)
end
