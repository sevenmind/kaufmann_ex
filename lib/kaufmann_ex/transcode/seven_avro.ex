defmodule KaufmannEx.Transcode.SevenAvro do
  @moduledoc false
  # avro encoding & serialization in use in sevenmind

  require Logger

  import Map.Helpers, only: [atomize_keys: 1]

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Avro
  alias KaufmannEx.Schemas.Avro.Registry
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Telemetry.Logger, as: Telem

  @behaviour KaufmannEx.Transcode

  @impl true
  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    start_time = System.monotonic_time()

    res =
      with {:ok, schema} <- Registry.parsed_schema(key),
           {:ok, decoded} <- Avro.decode(schema, encoded) do
        %{meta: meta, payload: payload} =
          case atomize_keys(decoded) do
            %{meta: meta, payload: payload} -> %{meta: meta, payload: payload}
            %{metadata: meta, payload: payload} -> %{meta: meta, payload: payload}
            payload -> %{meta: %{}, payload: payload}
          end

        %KaufmannEx.Schemas.Event{
          event
          | name: key,
            meta: meta,
            payload: payload
        }
      else
        {:error, error} ->
          Logger.warn(fn -> "Error Decoding #{key} #{inspect(error)}" end)

          err_event =
            event
            |> Map.from_struct()
            |> Map.merge(%{name: key, error: error})

          struct(KaufmannEx.Schemas.ErrorEvent, err_event)
      end

    Telem.report_decode_time(start_time: start_time, event: event)

    res
  end

  @impl true
  def encode_event(
        %Request{format: _, payload: payload, event_name: event_name, metadata: meta} = request
      ) do
    start_time = System.monotonic_time()

    %{payload: payload, meta: meta} =
      case payload do
        %{payload: payload, meta: meta} -> %{payload: payload, meta: meta}
        payload -> %{payload: payload, meta: meta}
      end

    {:ok, encoded} =
      with {:ok, schema} <- Registry.parsed_schema(event_name),
           {:ok, encoded} <- Avro.encode(schema, %{payload: payload, meta: meta}) do
        Telem.report_encode_duration(
          start_time: start_time,
          encoded: encoded,
          message_name: event_name
        )

        {:ok, encoded}
      else
        {:error, error_message, to_encode, schema} ->
          Logger.warn(fn ->
            "Error Encoding #{event_name}, #{inspect(to_encode)} \n #{inspect(schema)}"
          end)

          {:error, {:schema_encoding_error, error_message}}

        {:error, error_message} ->
          Logger.warn(fn -> "Error Encoding #{event_name}, #{inspect(payload)}" end)

          {:error, {:schema_encoding_error, error_message}}
      end

    %Request{request | encoded: encoded}
  end

  @impl true
  def schema_extension, do: ".avsc"

  @impl true
  def encodable?(schema, map) do
    Avro.encodable?(schema, map)
  end

  @impl true
  def read_schema(path) do
    path
    |> File.read!()
    |> Jason.decode!()
    |> parse_schema_with_metadata
  end

  def parse_schema_with_metadata(raw_schema) do
    metadata_schema =
      KaufmannEx.Config.schema_path()
      |> Enum.at(0)
      |> Path.join("avro/event_metadata.avsc")
      |> File.read!()
      |> Jason.decode!()

    {:ok, schema} =
      [metadata_schema, raw_schema]
      |> Avro.parse()

    schema
  end
end
