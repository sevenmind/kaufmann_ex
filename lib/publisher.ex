defmodule KaufmannEx.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic (`KaufmannEx.Config.default_topic/0`).
  """
  require Logger
  # alias KaufmannEx.Publisher.PartitionSelector
  # alias KaufmannEx.Publisher.Stage.TopicSelector

  # alias KafkaEx.Protocol.Produce.Message
  # alias KafkaEx.Protocol.Produce.Request
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  def publish(event_name, body, context \\ %{}, topic \\ :default) do
    message_body =
      case Map.has_key?(body, :meta) do
        true ->
          body

        _ ->
          %{
            payload: body,
            meta: Event.event_metadata(event_name, context)
          }
      end

    GenServer.cast(
      KaufmannEx.Publisher.Producer,
      {:publish,
       %Event{
         publish_request: %Request{
           event_name: event_name,
           body: message_body,
           context: context,
           topic: topic
         }
       }}
    )
  end
end
