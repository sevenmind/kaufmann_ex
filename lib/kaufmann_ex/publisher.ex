defmodule KaufmannEx.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic (`KaufmannEx.Config.default_topic/0`).
  """
  require Logger

  alias KaufmannEx.Publisher.Encoder
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Publisher.TopicSelector
  alias KaufmannEx.Schemas.Event

  alias KafkaEx.Protocol.Fetch.Message
  alias KafkaEx.Protocol.Produce.Request, as: KafkaRequest

  @doc """
  Execute Encode & publish inline, for when you just need to send something to
  kafka right now.
  """
  def publish(event_name, body, context \\ %{}, topic \\ :default, format \\ :default) do
    message_body =
      case is_map(body) and Map.has_key?(body, :meta) do
        true ->
          body

        _ ->
          %{
            payload: body,
            meta: Event.event_metadata(event_name, context)
          }
      end

    %Request{
      event_name: event_name,
      payload: message_body,
      context: context,
      topic: topic,
      format: format
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
    Logger.debug("Publishing Event #{event_name} on #{topic}##{partition}")

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
