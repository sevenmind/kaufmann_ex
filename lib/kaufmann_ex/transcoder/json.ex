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
  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    start_time = System.monotonic_time()

    response =
      case Jason.decode(encoded) do
        {:ok, %{"meta" => meta, "payload" => payload}} ->
          %Event{
            event
            | name: key,
              meta: Map.Helpers.atomize_keys(meta),
              payload: Map.drop(payload, ["meta", :meta])
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
  def encode_event(
        %Request{format: :json, payload: payload, metadata: meta, event_name: event_name} =
          request
      ) do
    start_time = System.monotonic_time()

    payload =
      case payload do
        %{meta: _meta} = _ -> payload
        payload -> Map.put(payload, :meta, meta)
      end

    with {:ok, encoded} <- Jason.encode(payload) do
      Telem.report_encode_duration(
        start_time: start_time,
        encoded: encoded,
        message_name: event_name
      )

      %Request{request | encoded: encoded}
    else
      {:error, error_message} ->
        Logger.warn(fn ->
          "Error Encoding #{event_name} - #{inspect(error_message)}, #{inspect(payload)}"
        end)

        {:error, {:encoding_error, error_message}}
    end
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
