defmodule KaufmannEx.Stages.EventHandler do
  @moduledoc """
  Behavior module for consuming messages from Kafka bus.

  Spawns tasks to process each event. Should still be within the KaufmannEx supervision tree.
  """
  require Logger

  def start_link(event) do
    Task.start_link(fn ->
      handle_event(event)
    end)
  end

  def handle_event(event) do
    handler = KaufmannEx.Config.event_handler()

    event
    |> decode_event()
    |> handler.given_event()
  rescue
    error ->
      Logger.warn("Error Consuming #{event.key} #{inspect(error)}")
      handler = KaufmannEx.Config.event_handler()

      event
      |> decode_event()
      |> error_from_event(error)
      |> handler.given_event()

      reraise error, error.stacktrace()
  end

  @doc """
  Decodes Avro encoded message value

  Returns Event or Error
  """
  @spec decode_event(map) :: KaufmannEx.Schemas.Event.t() | KaufmannEx.Schemas.ErrorEvent.t()
  def decode_event(%{key: key, value: value} = event) do
    event_name = key |> String.to_atom()

    case KaufmannEx.Schemas.decode_message(key, value) do
      {:ok, %{meta: meta, payload: payload}} ->
        Logger.debug([
          meta[:message_name],
          " ",
          meta[:message_id],
          " from ",
          meta[:emitter_service_id]
        ])

        %KaufmannEx.Schemas.Event{
          name: event_name,
          meta: meta,
          payload: payload
        }

      {:error, error} ->
        Logger.warn(fn -> "Error Decoding #{key} #{inspect(error)}" end)

        %KaufmannEx.Schemas.ErrorEvent{
          name: key,
          error: error,
          message_payload: value
        }
    end
  end

  # if loop of error events, just emit whatever we got
  defp error_from_event(%KaufmannEx.Schemas.ErrorEvent{} = event, _error) do
    event
  end

  defp error_from_event(event, error) do
    %KaufmannEx.Schemas.ErrorEvent{
      name: event.name,
      error: inspect(error),
      message_payload: event.payload,
      meta: event.meta
    }
  end
end
