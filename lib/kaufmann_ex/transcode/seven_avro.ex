defmodule KaufmannEx.Transcode.SevenAvro do
  @moduledoc false
  # avro encoding & serialization in use in sevenmind

  require Logger

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Avro
  alias KaufmannEx.Schemas.Avro.Registry
  alias KaufmannEx.Schemas.Event

  def decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    start_time = System.monotonic_time()

    res =
      with {:ok, schema} <- Registry.parsed_schema(key),
           {:ok, %{meta: meta, payload: payload}} <- Avro.decode(schema, encoded) do
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

    telemetry_report_decode_time(start_time: start_time, event: event)

    res
  end

  def encode_event(%Request{format: :json, payload: payload, event_name: event_name}) do
    start_time = System.monotonic_time()

    with {:ok, schema} <- Registry.parsed_schema(event_name),
         {:ok, encoded} <- Avro.encode(schema, payload) do
      telemetry_report_encode_duration(
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
  end

  def sniff_format(<<0>> <> _), do: [__MODULE__]

  defp telemetry_report_decode_time(start_time: start_time, event: event) do
    :telemetry.execute(
      [:kaufmann_ex, :schema, :decode],
      %{
        duration: System.monotonic_time() - start_time,
        offset: event.raw_event.offset,
        size: byte_size(event.raw_event.value)
      },
      %{event: event.raw_event.key, topic: event.topic, partition: event.partition}
    )
  end

  defp telemetry_report_encode_duration(
         start_time: start_time,
         encoded: encoded,
         message_name: message_name
       ) do
    :telemetry.execute(
      [:kaufmann_ex, :schema, :encode],
      %{
        duration: System.monotonic_time() - start_time,
        size: byte_size(encoded)
      },
      %{event: message_name}
    )
  end
end
