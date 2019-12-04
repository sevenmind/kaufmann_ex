defmodule KaufmannEx.Transcoder.Json do
  @moduledoc """
  JSON Encoder wrapper
  """

  require Logger
  import Map.Helpers, only: [stringify_keys: 1]
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Telemetry.Logger, as: Telem

  @behaviour KaufmannEx.Transcoder

  @impl true
  def decode_event(%Event{raw_event: %{key: _key, value: encoded}} = event) do
    start_time = System.monotonic_time()

    encoded
    |> Jason.decode()
    |> format_decoded_event(event)
    |> report_decode_time(start_time)
  end

  defp format_decoded_event(
         {:ok, %{"meta" => meta, "payload" => payload}},
         %Event{raw_event: %{key: key}} = event
       ) do
    %Event{
      event
      | name: key,
        meta: Map.Helpers.atomize_keys(meta),
        payload: Map.drop(payload, ["meta", :meta])
    }
  end

  defp format_decoded_event(
         {:ok, %{"meta" => meta} = payload},
         %Event{raw_event: %{key: key}} = event
       ) do
    %Event{
      event
      | name: key,
        meta: Map.Helpers.atomize_keys(meta),
        payload: Map.drop(payload, ["meta", :meta])
    }
  end

  defp format_decoded_event({:ok, payload}, %Event{raw_event: %{key: key}} = event),
    do: %Event{
      event
      | name: key,
        payload: payload
    }

  defp format_decoded_event(other, _event), do: other

  defp report_decode_time(%Event{} = event, start_time) do
    Telem.report_decode_time(start_time: start_time, event: event)

    event
  end

  defp report_decode_time(other, _start_time), do: other

  @impl true

  def encode_event(%Request{format: _, payload: %{__struct__: _} = payload} = request),
    do: encode_event(%Request{request | payload: Map.from_struct(payload)})

  def encode_event(%Request{format: _, payload: payload, metadata: meta} = request) do
    start_time = System.monotonic_time()

    payload
    |> Map.put_new(:meta, meta)
    |> Jason.encode()
    |> format_encoded_event(request)
    |> report_encode_duration(start_time)
  end

  defp format_encoded_event({:ok, encoded}, %Request{} = request),
    do: %Request{request | encoded: encoded}

  defp format_encoded_event({:error, error_message}, %Request{} = request) do
    Logger.warn(fn ->
      "Error Encoding #{request.event_name} - #{inspect(error_message)}, #{
        inspect(request.payload)
      }"
    end)

    {:error, {:encoding_error, error_message}}
  end

  defp report_encode_duration(%Request{} = request, start_time) do
    Telem.report_encode_duration(
      start_time: start_time,
      encoded: request.encoded,
      message_name: request.event_name
    )

    request
  end

  defp report_encode_duration(other, _start_time), do: other

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

  @impl true
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
