defmodule KaufmannEx.Schemas.Avro.Registry do
  @moduledoc """
    Interact with a remote Confluent Schema Registry
  """
  use Memoize

  defmemo parsed_schema(key) do
    with {:ok, %{"schema" => raw_schema}} <- get(if_partial_schema(key)) do
      AvroEx.parse_schema(raw_schema)
    end
  end

  @doc """
  Get schema from registry

  memoized permanetly
  """
  defmemo get(subject) do
    schema_registry_uri()
    |> Schemex.latest(subject)
  end

  def encodable?(subject, payload) do
    {:ok, schema} = parsed_schema(subject)
    AvroEx.encodable?(schema, payload)
  end

  defp if_partial_schema("query." <> <<_::binary-size(4)>> <> event_name), do: event_name
  defp if_partial_schema("event.error." <> event_name), do: event_name
  defp if_partial_schema(event_name), do: event_name

  def test(subject, schema) do
    schema_registry_uri()
    |> Schemex.test(subject, schema)
  end

  def register(subject, schema) do
    schema_registry_uri()
    |> Schemex.register(subject, schema)
  end

  def delete(subject) do
    schema_registry_uri()
    |> Schemex.delete(subject)
  end

  def check(subject, schema) do
    schema_registry_uri()
    |> Schemex.check(subject, schema)
  end

  def subjects do
    schema_registry_uri()
    |> Schemex.subjects()
  end

  defmemo defined_event?(subject) do
    case schema_registry_uri()
         |> Schemex.latest(subject) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp schema_registry_uri do
    KaufmannEx.Config.schema_registry_uri()
  end
end
