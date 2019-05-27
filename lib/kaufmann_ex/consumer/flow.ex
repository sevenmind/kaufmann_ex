defmodule KaufmannEx.Consumer.Flow do
  @moduledoc """
  A series of Flow for handling kafka events.
  """

  use Flow
  require Logger

  alias KaufmannEx.Config
  alias KaufmannEx.EventHandler
  alias KaufmannEx.Publisher
  alias KaufmannEx.Publisher.{Encoder, TopicSelector}
  alias KaufmannEx.Schemas.Event

  def start_link({producer_stage, topic, partition, _extra_consumer_args}) do
    event_handler = Config.event_handler()
    stages = Config.stages()
    workers = spawn_workers(topic, partition, stages)

    {:ok, link_pid} =
      [producer_stage]
      |> Flow.from_stages(stages: stages, max_demand: Config.max_demand())
      # wrap events into our event struct
      |> Flow.map(fn event ->
        %Event{
          raw_event: event,
          topic: topic,
          partition: partition
        }
      end)
      # Filter out unhandled events before decoding
      # Decode each event
      |> Flow.map(&decode_event/1)
      |> Flow.flat_map(&EventHandler.handle_event(&1, event_handler))
      |> Flow.flat_map(&TopicSelector.resolve_topic/1)
      |> Flow.map(&Encoder.encode_event/1)
      |> Flow.map(&Publisher.publish_request(&1, workers))
      |> drain_flow_tail
      |> Flow.start_link()

    {:ok, link_pid}
  end

  defp spawn_workers(topic, partition, stages) do
    Enum.map(0..stages, fn n ->
      create_worker(String.to_atom("#{topic}_#{partition}_worker_#{n}"))
    end)
  end

  defp create_worker(worker_name) do
    case KafkaEx.create_worker(worker_name) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  defp drain_flow_tail(flow), do: Flow.map(flow, fn _ -> [] end)

  defp decode_event(%Event{raw_event: %{key: key, value: encoded}} = event) do
    # I guess we just try all the encoders?
  end
end
