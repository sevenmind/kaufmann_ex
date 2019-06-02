defmodule KaufmannEx.Transcoder.SevenAvro do
  @moduledoc false
  # avro encoding & serialization in use in sevenmind

  require Logger

  import Map.Helpers, only: [atomize_keys: 1]

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Telemetry.Logger, as: Telem
  alias KaufmannEx.Transcoder.SevenAvro.Schema
  alias KaufmannEx.Transcoder.SevenAvro.Schema.Registry

  @behaviour KaufmannEx.Transcoder

  @impl true
  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    start_time = System.monotonic_time()
    schema_name = scope_event_name(key)

    res =
      with {:ok, %{"schema" => raw_schema}} <- Registry.latest(schema_name),
           {:ok, schema} <- AvroEx.parse_schema(raw_schema),
           {:ok, decoded} <- Schema.decode(schema, encoded) do
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

    schema_name = scope_event_name(event_name)

    {:ok, encoded} =
      with {:ok, %{"schema" => raw_schema}} <- Registry.latest(schema_name),
           {:ok, schema} <- AvroEx.parse_schema(raw_schema),
           {:ok, encoded} <- Schema.encode(schema, %{payload: payload, meta: meta}) do
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

  def encodable?(%Request{event_name: event_name, payload: payload, metadata: meta}) do
    schema_name = scope_event_name(event_name)

    {:ok, %{"schema" => raw_schema}} = Registry.latest(schema_name)
    {:ok, schema} = AvroEx.parse_schema(raw_schema)

    %{payload: payload, meta: meta} =
      case payload do
        %{payload: payload, meta: meta} -> %{payload: payload, meta: meta}
        payload -> %{payload: payload, meta: meta}
      end

    encodable?(schema, %{payload: payload, meta: meta})
  end

  @impl true
  def encodable?(schema, map) do
    Schema.encodable?(schema, map)
  end

  @impl true
  def read_schema(nil), do: nil

  def read_schema(path) do
    path
    |> File.read!()
    |> Jason.decode!()
    |> parse_schema_with_metadata
  end

  @impl true
  def defined_event?(event_name) do
    Registry.defined_event?(event_name)
  end

  @spec parse_schema_with_metadata(any) :: any
  def parse_schema_with_metadata(raw_schema) do
    metadata_schema =
      KaufmannEx.Config.schema_path()
      |> Enum.at(0)
      |> Path.join("avro/event_metadata.avsc")
      |> File.read!()
      |> Jason.decode!()

    {:ok, schema} =
      [metadata_schema, raw_schema]
      |> Schema.parse()

    schema
  end

  def find_local_schema(event_name) do
    event_name
    |> scope_event_name
    |> find_schema
  end

  def read_local_schema(event_name) do
    event_name
    |> find_local_schema()
    |> read_schema
  end

  defp all_schemas do
    KaufmannEx.Config.schema_path()
    |> List.flatten()
    |> Enum.flat_map(&Path.wildcard([&1, "/**/*.avsc"]))
  end

  defp find_schema(nil), do: nil

  defp find_schema(schema_name) do
    Enum.find(all_schemas(), &(Path.rootname(Path.basename(&1)) == schema_name))
  end

  defp scope_event_name(event_name) when is_atom(event_name), do: Atom.to_string(event_name)

  defp scope_event_name("query." <> _ = event_name),
    do: scope_event_name(String.slice(event_name, 0..8))

  defp scope_event_name("event.error." <> _ = event_name),
    do: scope_event_name(String.slice(event_name, 0..10))

  defp scope_event_name(event_name),
    do:
      event_name
      |> String.split("#")
      |> Enum.at(0)
end
