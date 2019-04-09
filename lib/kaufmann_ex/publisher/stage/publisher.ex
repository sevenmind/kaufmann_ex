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

    start_time = System.monotonic_time()

    KafkaEx.produce(produce_request, worker_name: Enum.random(workers))

    :telemetry.execute(
      [:kaufmann_ex, :publisher, :publish],
      %{
        duration: System.monotonic_time() - start_time,
        size: byte_size(encoded)
      },
      %{event: event_name, topic: topic, partition: partition}
    )

    event
  end
end
