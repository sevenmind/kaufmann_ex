use Mix.Config

config :kafka_ex,
  brokers: [
    {
      System.get_env("KAFKA_HOST"),
      9092
    }
  ],
  use_ssl: false,
  consumer_group: System.get_env("CONSUMER_GROUP"),
  commit_interval: 100_000,
  commit_threshold: 100

config :kaufmann_ex,
  event_handler_mod: Sample.EventHandler,
  consumer_group: System.get_env("CONSUMER_GROUP"),
  default_topic: System.get_env("KAFKA_TOPIC"),
  producer_mod: KaufmannEx.Publisher,
  metadata_mod: Sample.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: System.get_env("SCHEMA_REGISTRY_PATH"),
  service_name: "SampleService",
  service_id: "SampleHost",
  event_handler_demand: 50
