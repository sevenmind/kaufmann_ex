defmodule KaufmannEx.Publisher.Publisher do
  @moduledoc """
  A Consumer worker supervised by `KaufmannEx.Publisher.Consumer` responsible for
  publishing events to kafka
  """
  require Logger
  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request

  def start_link(%{encoded: encoded, topic: topic, partition: partition, event_name: event_name}) do
    Task.start_link(fn ->
      Logger.debug(["Publishing Event ", event_name, " on ", topic, "@", partition])

      message = %Message{value: encoded, key: event_name}

      produce_request = %Request{
        partition: partition,
        topic: topic,
        messages: [message]
      }

      KafkaEx.produce(produce_request)
    end)
  end
end
