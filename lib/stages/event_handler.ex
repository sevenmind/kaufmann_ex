defmodule KaufmannEx.Stages.EventHandler do
  @moduledoc """
  Behavior module for consuming messages from Kafka bus.

  Spawns tasks to process each event. Should still be within the KaufmannEx supervision tree.
  """
  require Logger

  def start_link(event) do
    Task.start_link(fn ->
      event
      |> decode_event()
      |> handle_with_rescue()
    end)
  end

  def handle_with_rescue(event) do
    handle_event(event)
  rescue
    error ->
      Logger.warn("Error Publishing #{event.name} #{inspect(error)}")
      handler = KaufmannEx.Config.event_handler()

      event
      |> error_from_event(error)
      |> handler.given_event()
  end

  def handle_event(event) do
    handler = KaufmannEx.Config.event_handler()
    handler.given_event(event)
  end

  @doc """
  Decodes Avro encoded message value, transforms event into tuple of `{:message_name, %Payload{}}`

  Returns `{key, value}`
  """
  @spec decode_event(map) :: KaufmannEx.Schemas.Event.t() | KaufmannEx.Schemas.ErrorEvent.t()
  def decode_event(%{key: key, value: value}) do
    event_name = key |> String.to_atom()

    case KaufmannEx.Schemas.decode_message(key, value) do
      {:ok, parsed} ->
        %KaufmannEx.Schemas.Event{
          name: event_name,
          meta: parsed[:meta],
          payload: parsed[:payload]
        }

      {:error, error} ->
        Logger.warn(fn -> "Error Encoding #{key} #{inspect(error)}" end)

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
