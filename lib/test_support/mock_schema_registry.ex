defmodule KaufmannEx.TestSupport.MockSchemaRegistry do
  @moduledoc """
  A simple immitation schema registry that verifies Test Events against the schemas we have saved to file

  Looks for Schemas at `Application.get_env(:kaufmann_ex, :schema_path)`
  """
  def fetch_event_schema(schema_name) do
    schema_name
    |> load_schema
    |> inject_metadata
    |> AvroEx.parse_schema!()
  end

  @spec defined_event?(String.t()) :: boolean
  def defined_event?(schema_name) do
    schema_path()
    |> build_paths(schema_name)
    |> Enum.any?(&File.exists?/1)
  end

  @spec encodable?(String.t(), any) :: boolean
  def encodable?(schema_name, payload) do
    schema_name
    |> fetch_event_schema
    |> AvroEx.encodable?(payload)
  end

  def encode_event(schema_name, payload) do
    {:ok, schema} =
      schema_name
      |> load_schema
      |> inject_metadata
      |> parse_schema

    AvroEx.encode(schema, payload)
  end

  defp inject_metadata(schema) do
    # Decode + encode here is dumb and time consuming
    schema
    |> Jason.decode!()
    |> List.wrap()
    |> List.insert_at(0, load_metadata())
    |> Jason.encode!()
  end

  defp load_schema(schema_name) do
    {:ok, schema} =
      schema_path()
      |> build_paths(schema_name)
      |> Enum.filter(&File.exists?/1)
      |> Enum.at(0)
      |> File.read()

    schema
  end

  defp parse_schema(schema) do
    AvroEx.parse_schema(schema)
  end

  defp load_metadata do
    load_schema("event_metadata")
    |> Jason.decode!()
  end

  defp schema_path do
    [
      KaufmannEx.Config.schema_path()
    ]
    |> List.flatten()
  end

  defp build_paths(schema_path, schema_name) when is_list(schema_path) do
    Enum.map(schema_path, &Path.join(&1, [schema_name |> to_string, ".avsc"]))
  end
end
