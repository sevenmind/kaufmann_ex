defmodule KaufmannEx.Consumer.GenConsumer do
  @moduledoc """
  If Kaufmann were implemented as regular `KafkaEx.GenConsumer`
  """
  use KafkaEx.GenConsumer

  alias KaufmannEx.Config
  alias KaufmannEx.EventHandler
  alias KaufmannEx.Publisher
  alias KaufmannEx.Publisher.{Encoder, TopicSelector}
  alias KaufmannEx.Schemas.Event

  require Logger

  def init(topic, partition) do
    {:ok, %{topic: topic, partition: partition, event_handler: Config.event_handler()}}
  end

  # note - messages are delivered in batches
  def handle_message_set(
        message_set,
        %{topic: topic, partition: partition, event_handler: event_handler} = state
      ) do
    message_set
    |> Enum.map(fn event ->
      %Event{
        raw_event: event,
        topic: topic,
        partition: partition
      }
    end)
    # Filter out unhandled events before decoding
    # Decode each event
    |> Enum.map(&decode_event/1)
    |> Enum.flat_map(&EventHandler.handle_event(&1, event_handler))
    |> Enum.flat_map(&TopicSelector.resolve_topic/1)
    |> Enum.map(&Encoder.encode_event/1)
    |> Enum.each(&Publisher.publish_request/1)

     {:async_commit, state}
  end

  defp decode_event(%Event{raw_event: %{key: _, value: _}} = event) do
    # when in doubt try all the transcoders
    Enum.map(Config.transcoders(), fn trns -> trns.decode_event(event) end)
    |> Enum.find(fn
      %Event{} = _ -> true
      _ -> false
    end)
  end
end
