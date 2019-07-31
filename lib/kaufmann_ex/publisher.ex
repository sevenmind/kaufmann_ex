defmodule KaufmannEx.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic (`KaufmannEx.Config.default_topic/0`).
  """
  require Logger

  alias KaufmannEx.Publisher.Encoder
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Publisher.TopicSelector
  alias KaufmannEx.Schemas.Event

  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request, as: KafkaRequest

  @doc """
  Execute Encode & publish inline, for when you just need to send something to
  kafka right now.
  """
  def publish(event_name, body, context \\ %{}, topic \\ :default, format \\ :default) do
    %Request{
      event_name: event_name,
      payload: body,
      context: context,
      topic: topic,
      format: format,
      metadata: Event.event_metadata(event_name, context)
    }
    |> TopicSelector.resolve_topic()
    |> Enum.map(&Encoder.encode_event/1)
    |> Enum.each(&publish_request/1)

    :ok
  end

  def publish_request(
        %Request{
          encoded: encoded,
          topic: topic,
          partition: partition,
          event_name: event_name
        } = request,
        workers \\ [:kafka_ex]
      )
      when is_list(workers) do
    Logger.debug("Publishing Event #{event_name} on #{topic}##{inspect(partition)}")

    message = %Message{value: encoded, key: event_name}

    produce_request = %KafkaRequest{
      partition: partition,
      topic: topic,
      messages: [message],
      required_acks: 1
    }

    start_time = System.monotonic_time()

    res = KafkaEx.produce(produce_request, worker_name: Enum.random(workers))

    report_publish_time(start_time: start_time, encoded: encoded, request: request)

    res
  end

  defp report_publish_time(
         start_time: start_time,
         encoded: encoded,
         request: %{topic: topic, partition: partition, event_name: event_name}
       ) do
    event_name = (event_name || "") |> String.split("#") |> Enum.at(0) |> String.split(":") |> Enum.at(0)

    :telemetry.execute(
      [:kaufmann_ex, :publisher, :publish],
      %{
        duration: System.monotonic_time() - start_time,
        size: byte_size(encoded)
      },
      %{event: event_name, topic: topic, partition: partition}
    )
  end
end
