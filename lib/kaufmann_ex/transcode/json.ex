defmodule KaufmannEx.Transcode.Json do
  @moduledoc """
  avro encoding & serialization in use in sevenmind
  """

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  @behaviour KaufmannEx.Transcode

  @impl true
  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    case Jason.decode(encoded) do
      {:ok, %{"meta" => meta, "payload" => payload}} ->
        %Event{
          event
          | name: key,
            meta: meta,
            payload: payload
        }

      {:ok, payload} ->
        %Event{
          event
          | name: key,
            payload: payload
        }

      other ->
        other
    end
  end

  @impl true
  def encode_event(%Request{format: :json, payload: payload} = request) do
    %Request{request | encoded: Jason.encode!(payload)}
  end

  @impl true
  def schema_extension, do: ".json"

  @impl true
  def encodable?(schema, map) do
    ExJsonSchema.Validator.valid?(schema, map)
  end

  @impl true
  def read_schema(path) do
    path
    |> File.read!()
    |> Jason.decode!()
    |> ExJsonSchema.Schema.resolve()
  end
end
