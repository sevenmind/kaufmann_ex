# defmodule KaufmannEx.Schemas do
#   @moduledoc """
#     Handles registration, retrieval, validation and parsing of Avro Schemas.

#     Schemas are cached to ETS table using `Memoize`. Does not handle schema changes while running. Best practice is to redeploy all services using a message schema if the schema changes.

#     Depends on
#      - `Schemex` - calls to Confluent Schema Registry
#      - `AvroEx` - serializing and deserializing avro encoded messages
#      - `Memoize` - Cache loading schemas to an ETS table, prevent performance bottleneck at schema registry.
#   """

#   use Memoize
#   require Logger
#   require Map.Helpers
#   alias KaufmannEx.Schemas.Event

#   defp encode_message_with_schema(schema, message) do
#     AvroEx.encode(schema, message)
#   rescue
#     # avro_ex can become confused when trying to encode some schemas.
#     error ->
#       Logger.warn(["Could not encode schema \n\t", inspect(error)])
#       {:error, :unmatching_schema}
#   end

#   defp decode_message_with_schema(schema, encoded) do
#     AvroEx.decode(schema, encoded)
#   rescue
#     # avro_ex can become confused when trying to decode some schemas.
#     _ ->
#       {:error, :unmatching_schema}
#   end

#   defp atomize_keys({:ok, args}) do
#     {:ok, Map.Helpers.atomize_keys(args)}
#   end

#   defp atomize_keys(args), do: args
# end
