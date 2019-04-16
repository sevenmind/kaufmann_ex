defmodule KaufmannEx.FlowConsumer do
  @moduledoc """
  A series of Flow for handling kafka events.
  """

  use Flow
  require Logger

  alias KaufmannEx.Config
  alias KaufmannEx.EventHandler
  alias KaufmannEx.Publisher.Stage.{Encoder, Publisher, TopicSelector}
  alias KaufmannEx.Schemas
  alias KaufmannEx.Schemas.Event

  def start_link({pid, topic, partition, _extra_consumer_args}) do
    event_handler = KaufmannEx.Config.event_handler()
    topic_metadata = TopicSelector.topic_metadata()
    stages = Config.stages()

    workers =
      Enum.map(0..stages, fn n ->
        create_worker(String.to_atom("#{topic}_#{partition}_worker_#{n}"))
      end)

    {:ok, link_pid} =
      [pid]
      # Initialize Flow from the stage provided in PID
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
      |> Flow.filter(&handled_event?(&1, event_handler.handled_events))
      # Decode each event
      |> Flow.map(&Schemas.decode_event/1)
      # Handle events in the Event Handler
      |> Flow.flat_map(&EventHandler.handle_event(&1, event_handler))
      # Encode each event
      |> Flow.map(&Encoder.encode_event/1)
      # Choose topic and partition from metadata
      |> Flow.flat_map(&TopicSelector.select_topic_and_partition(&1, topic_metadata))
      # Publish encoded events to kafka
      |> Flow.map(&Publisher.publish(&1, workers))
      # Empty out the Flow buffer. Prevent memory leak from processed events
      # accumulating in the Flow tail.
      |> Flow.map(fn _ -> [] end)
      |> Flow.start_link()

    {:ok, link_pid}
  end

  def handled_event?(_, [:all]), do: true
  def handled_event?(%{raw_event: %{key: key}} = _, handled_events), do: key in handled_events

  defp create_worker(worker_name) do
    case KafkaEx.create_worker(worker_name) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end
end
