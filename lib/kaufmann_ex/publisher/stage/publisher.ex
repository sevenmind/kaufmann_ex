defmodule KaufmannEx.Publisher.Stage.Publisher do
  @moduledoc """
  A Consumer worker supervised by `KaufmannEx.Publisher.Stage.PublishSupervisor` responsible for
  publishing events to kafka
  """
  require Logger
  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request
  alias KaufmannEx.Publisher.Request, as: PRequest
  alias KaufmannEx.Schemas.Event

  def start_link(args) do
    Task.start_link(fn ->
      publish(args)
    end)
  end

  # def publish(
  #       %Event{
  #         publish_request: %PRequest{
  #           encoded: encoded,
  #           topic: topic,
  #           partition: partition,
  #           event_name: event_name
  #         }
  #       } = event
  #     ) do
  #   Logger.debug([
  #     "Publishing Event ",
  #     event_name |> Atom.to_string(),
  #     " on ",
  #     topic,
  #     "#",
  #     to_string(partition)
  #   ])

  #   message = %Message{value: encoded, key: event_name |> Atom.to_string()}

  #   produce_request = %Request{
  #     partition: partition,
  #     topic: topic,
  #     messages: [message]
  #   }

  #   KafkaEx.produce(produce_request)

  #   event
  # end

  def publish(event, workers \\ [:kafka_ex])

  def publish(
        %Event{
          publish_request: %PRequest{
            encoded: encoded,
            topic: topic,
            partition: partition,
            event_name: event_name
          }
        } = event,
        workers
      )
      when is_list(workers) do
    Logger.debug("Publishing Event #{event_name} on #{topic}##{partition}")

    message = %Message{value: encoded, key: event_name |> Atom.to_string()}

    produce_request = %Request{
      partition: partition,
      topic: topic,
      messages: [message],
      required_acks: 1
    }

    KafkaEx.produce(produce_request, worker_name: Enum.random(workers))

    event
  end

  def publish({_topic_partition, events}, workers) when is_list(events) do
    # events = Map.values(events) |> Map.flatten()
    %Event{
      publish_request: %PRequest{
        # encoded: encoded,
        topic: topic,
        partition: partition
        # event_name: event_name
      }
    } = Enum.at(events, 0)

    messages =
      events
      |> Enum.map(fn %Event{publish_request: %PRequest{encoded: encoded, event_name: event_name}} ->
        %Message{value: encoded, key: event_name |> Atom.to_string()}
      end)

    produce_request = %Request{
      partition: partition,
      topic: topic,
      messages: messages,
      required_acks: 1,
      compression: :snappy
    }

    KafkaEx.produce(produce_request, worker_name: Enum.random(workers))

    events
  end
end
