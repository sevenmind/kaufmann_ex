defmodule KaufmannEx.Transcoder.SevenAvro.Schema.Registry do
  @moduledoc """
    Interact with a remote Confluent Schema Registry. Wraps `Schemex`
  """
  use Memoize

  @doc """
  Loads a parsed scheam from a remote schema registry
  """
  defmemo parsed_schema(key) do
    with {:ok, %{"schema" => raw_schema}} <- latest(key) do
      AvroEx.parse_schema(raw_schema)
    end
  end

  defmemo latest(subject) do
    schema_registry_uri()
    |> Schemex.latest(subject)
  end

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
