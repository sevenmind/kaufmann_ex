# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# 3rd-party users, it should be done in your "mix.exs" file.

# You can configure your application as:
#
#     config :kaufmann_ex, key: :value
#
# and access this configuration in your application as:
#
#     Application.get_env(:kaufmann_ex, :key)
#
# You can also configure a 3rd-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env}.exs"

config :kafka_ex,
  brokers: [
    {
      System.get_env("KAFKA_HOST"),
      9092
    }
  ],
  use_ssl: false,
  consumer_group: System.get_env("CONSUMER_GROUP")

config :kaufmann_ex,
  consumer_group: System.get_env("CONSUMER_GROUP"),
  default_topic: System.get_env("KAFKA_TOPIC"),
  event_handler_mod: nil,
  producer_mod: KaufmannEx.Publisher,
  schema_path: "priv/schemas",
  schema_registry_uri: System.get_env("SCHEMA_REGISTRY_PATH"),
  service_name: "SampleService",
  service_id: "SampleHost",
  event_handler_demand: 50
