defmodule KaufmannEx.Transcoder.Json do
  @moduledoc """
  avro encoding & serialization in use in sevenmind
  """

  import Map.Helpers, only: [stringify_keys: 1]
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Telemetry.Logger, as: Telem

  @behaviour KaufmannEx.Transcode

  @impl true
  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    start_time = System.monotonic_time()
    response = case Jason.decode(encoded) do
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

    if not is_tuple(response) do
      Telem.report_decode_time(start_time: start_time, event: response)
    end

    response
  end

  @impl true
  def encode_event(%Request{format: :json, payload: payload} = request) do
    %Request{request | encoded: Jason.encode!(payload)}
  end

  @impl true
  def schema_extension, do: ".json"

  @impl true
  def encodable?(%Request{
        event_name: event_name,
        payload: payload
      }) do
    case find_schema(event_name) do
      nil ->
        false

      path ->
        path
        |> read_schema
        |> encodable?(payload)
    end
  end

  @impl true
  def encodable?(schema, map) do
    ExJsonSchema.Validator.valid?(schema, stringify_keys(map))
  end

  def defined_event?(event_name) do
    event_name
    |> find_schema
  end

  @impl true
  def read_schema(path) do
    path
    |> File.read!()
    |> Jason.decode!()
    |> ExJsonSchema.Schema.resolve()
  end

  defp all_schemas do
    KaufmannEx.Config.schema_path()
    |> List.flatten()
    |> Enum.flat_map(&Path.wildcard([&1, "/**/*.json"]))
  end

  defp find_schema(schema_name) do
    Enum.find(all_schemas(), &(Path.rootname(Path.basename(&1)) == schema_name))
  end
end
