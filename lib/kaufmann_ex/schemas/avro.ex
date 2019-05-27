defmodule KaufmannEx.Schemas.Avro do
  require Logger
  alias KaufmannEx.Config

  def decode(schema, encoded) do
    AvroEx.decode(schema, encoded)
  rescue
    # avro_ex can become confused when trying to decode some schemas.
    _ ->
      {:error, :unmatching_schema}
  end

  def encode(schema, message) do
    AvroEx.encode(schema, Map.Helpers.stringify_keys(message))
  rescue
    # avro_ex can become confused when trying to encode some schemas.
    error ->
      Logger.warn(["Could not encode schema \n\t", inspect(error)])
      {:error, :unmatching_schema}
  end

  def read_local_schema(schema_name) do
    with path <- Path.join(Config.schema_path(), schema_name <> ".avsc"),
         {:ok, file} <- File.read(path),
         {:ok, raw_schema} <- Jason.decode(file),
         {:ok, schema} <- parse_schema_with_metadata(raw_schema) do
      {:ok, schema}
    else
      err -> err
    end
  end

  def parse_schema_with_metadata(raw_schema) do
    metadata_schema =
      Config.schema_path()
      |> Path.join("event_metadata.avsc")
      |> File.read!()
      |> Jason.decode!()

    [metadata_schema, raw_schema]
    |> parse()
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
