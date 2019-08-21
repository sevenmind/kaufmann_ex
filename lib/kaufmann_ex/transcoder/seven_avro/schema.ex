defmodule KaufmannEx.Transcoder.SevenAvro.Schema do
  @moduledoc """
  Convience module wrapping AvroEx behavior
  """

  require Logger
  import Map.Helpers, only: [stringify_keys: 1]

  def decode(schema, encoded) do
    AvroEx.decode(schema, encoded)
  rescue
    # avro_ex can become confused when trying to decode some schemas.
    error ->
      v0_decode(schema, encoded)
  end

  defp v0_decode(schema, encoded) do
    AvroExV0.decode(schema, encoded)
  rescue
    # avro_ex can become confused when trying to decode some schemas.
    error ->

      trace = Exception.format(:error, error, __STACKTRACE__)
      Logger.warn("Could not decode event \n\t #{trace}")

      {:error, :unmatching_schema}
  end

  def encode(schema, message) do
    AvroEx.encode(schema, stringify_keys(message))
  rescue
    # avro_ex can become confused when trying to encode some schemas.
    error ->
      trace = Exception.format(:error, error, __STACKTRACE__)

      Logger.warn("Could not encode event \n\t #{trace}")
      {:error, :unmatching_schema}
  end

  @spec encodable?(AvroEx.Schema.t(), any) :: boolean
  def encodable?(schema, payload) do
    AvroEx.encodable?(schema, stringify_keys(payload))
  end

  @doc """
  AvroEx.parse without the initial json decode step
  """
  @spec parse(Enum.t(), AvroEx.Schema.Context.t()) :: {:ok, AvroEx.Schema.t()} | {:error, term}
  def parse(schema, %AvroEx.Schema.Context{} = context \\ %AvroEx.Schema.Context{}) do
    with {:ok, schema} <- AvroEx.Schema.cast(schema),
         {:ok, schema} <- AvroEx.Schema.namespace(schema),
         {:ok, context} <- AvroEx.Schema.expand(schema, context) do
      {:ok, %AvroEx.Schema{schema: schema, context: context}}
    end
  end
end
