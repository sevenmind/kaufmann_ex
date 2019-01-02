defmodule KaufmannEx.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic (`KaufmannEx.Config.default_topic/0`).
  """
  require Logger
  # alias KaufmannEx.Publisher.PartitionSelector
  # alias KaufmannEx.Publisher.TopicSelector

  # alias KafkaEx.Protocol.Produce.Message
  # alias KafkaEx.Protocol.Produce.Request

  def publish(event_name, body, context \\ %{}, topic \\ :default) do
    GenServer.cast(
      KaufmannEx.Publisher.Producer,
      {:publish,
       %{
         event_name: event_name,
         body: body,
         context: context,
         topic: topic
       }}
    )
  end
end
