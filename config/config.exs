# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :kafka_ex,
  brokers: "kafka:9092",
  use_ssl: false,
  consumer_group: "kaufmann-consumer",
  commit_threshold: 10,
  commit_interval: 100,
  sync_timeout: 10_000

config :kaufmann_ex,
  consumer_group: "kaufmann-consumer",
  default_topic: "kaufmann-chat",
  event_handler_mod: nil,
  producer_mod: KaufmannEx.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: "http://schema_registry:8081",
  service_name: "SampleService",
  service_id: "SampleHost",
  max_demand: 50,
  commit_strategy: :async_commit,
  transcoder: [
    default: KaufmannEx.Transcoder.SevenAvro,
    json: KaufmannEx.Transcoder.Json
  ]

config :logger,
  level: :info

env_config = Path.expand("#{Mix.env()}.exs", __DIR__)

if File.exists?(env_config) do
  import_config(env_config)
end
