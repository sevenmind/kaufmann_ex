defmodule KaufmannEx.Publisher.Stage.Publisher do
  @moduledoc """
  A Consumer worker supervised by `KaufmannEx.Publisher.Stage.PublishSupervisor` responsible for
  publishing events to kafka
  """
  use Elixometer
  require Logger
  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request

  def start_link(args) do
    Task.start_link(fn ->
      publish(args)
    end)
  end

  @timed key: :auto
  def publish(%{encoded: encoded, topic: topic, partition: partition, event_name: event_name}) do
    Logger.debug([
      "Publishing Event ",
      event_name |> Atom.to_string(),
      " on ",
      topic,
      "#",
      to_string(partition)
    ])

    message = %Message{value: encoded, key: event_name |> Atom.to_string()}

    produce_request = %Request{
      partition: partition,
      topic: topic,
      messages: [message]
    }

    KafkaEx.produce(produce_request)
  end
end
