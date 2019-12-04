defmodule KaufmannEx.Consumer.Flow do
  @moduledoc """
  A series of Flow for handling kafka events.
  """

  use Flow
  require Logger

  alias KaufmannEx.Config
  alias KaufmannEx.EventHandler
  alias KaufmannEx.Publisher
  alias KaufmannEx.Publisher.{Encoder, Request, TopicSelector}
  alias KaufmannEx.Schemas.Event

  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request, as: KafkaRequest

  def start_link({producer_stage, topic, partition, args}) do
    Logger.debug("starting consumer for #{topic}##{partition}")
    worker = String.to_atom("worker_#{topic}_#{partition}")
    {:ok, _pid} = create_worker(worker)

    metadata = KafkaEx.metadata(topic: topic, worker: worker)
    window = Flow.Window.trigger_periodically(Flow.Window.global(), 20, :millisecond)

    {:ok, link_pid} =
      [producer_stage]
      |> Flow.from_stages(
        stages: Config.stages(),
        max_demand: Config.max_demand(),
        window: window
      )
      # wrap events into our event struct
      |> Flow.map(fn event ->
        %Event{
          raw_event: event,
          topic: topic,
          partition: partition
        }
      end)
      # Decode each event
      |> Flow.map(&Event.decode_event/1)
      |> Flow.flat_map(&EventHandler.handle_event(&1, args))
      |> Flow.flat_map(&TopicSelector.resolve_topic/1)
      |> Flow.map(&Encoder.encode_event/1)
      |> Flow.map(fn
        %Request{
          encoded: encoded,
          event_name: event_name,
          topic: topic,
          partition: partition
        } ->
          %KafkaRequest{
            partition: partition,
            topic: topic,
            messages: [%KafkaEx.Protocol.Produce.Message{value: encoded, key: event_name}],
            required_acks: 1
          }
      end)
      |> Flow.map(&KafkaEx.DefaultPartitioner.assign_partition(&1, metadata))
      # partition events by topic & partition
      |> Flow.partition(
        min_demand: 1,
        key: &(&1.topic <> to_string(&1.partition)),
        window: window,
        stages: Config.stages()
      )
      # partition may publish events for multiple topics
      |> Flow.group_by(&(&1.topic <> to_string(&1.partition)))
      # publish events on window trigger
      |> Flow.on_trigger(fn
        events ->
          events
          |> Enum.map(fn {_partition_id, producer_requests} ->
            Enum.reduce(producer_requests, &combine_kafka_publish_requests/2)
          end)
          |> Enum.each(fn %KafkaRequest{} = produce_request ->
            start_time = System.monotonic_time()

            {:ok, _} = KafkaEx.produce(produce_request, worker_name: worker)

            log_produce_to_kafka(produce_request, System.monotonic_time() - start_time)
          end)

          {[], %{}}
      end)
      |> Flow.start_link(name: flow_name(producer_stage), demand: :forward)

    {:ok, link_pid}
  end

  defp create_worker(worker_name) do
    case KafkaEx.create_worker(worker_name) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  defp flow_name(producer) when is_atom(producer) do
    Module.concat([__MODULE__, producer])
  end

  defp flow_name(producer) when is_pid(producer) do
    Module.concat([__MODULE__, inspect(producer)])
  end

  defp log_produce_to_kafka(%{messages: messages, topic: topic, partition: partition}, duration) do
    time = :erlang.convert_time_unit(duration, :native, :microsecond)

    Logger.info(
      "Publishing #{length(messages)} events on #{topic}##{inspect(partition)} in #{time}μs",
      topic: topic,
      partition: partition
    )
  end

  defp combine_kafka_publish_requests(%KafkaRequest{} = value, %KafkaRequest{} = publish_request) do
    %KafkaRequest{
      publish_request
      | messages: Enum.concat(value.messages, publish_request.messages)
    }
  end
end
