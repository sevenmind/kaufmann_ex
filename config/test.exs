use Mix.Config

config :bypass, adapter: Plug.Adapters.Cowboy2


config :kaufmann_ex,
  consumer_group: "kaufmann-consumer",
  default_topic: "kaufmann-chat",
  event_handler_mod: nil,
  producer_mod: KaufmannEx.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: "http://schema_registry:8081",
  service_name: "SampleService",
  service_id: "SampleHost",
  transcoder: [
    default: KaufmannEx.TestSupport.Transcoder.SevenAvro,
    json: KaufmannEx.Transcoder.Json
  ]
