use Mix.Config


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
  transcoder: [
    default: KaufmannEx.TestSupport.Transcoder.SevenAvro,
    json: KaufmannEx.Transcoder.Json
  ]
