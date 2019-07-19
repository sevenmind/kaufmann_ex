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
    Logger.debug("starting consumer for #{topic}##{partition}")
    event_handler = Config.event_handler()
    worker = String.to_atom("worker_#{topic}_#{partition}")
    {:ok, _pid} = create_worker(worker)

    {:ok, link_pid} =
      [producer_stage]
      |> Flow.from_stages(stages: Config.stages(), max_demand: Config.max_demand())
      # wrap events into our event struct
      |> Flow.map(fn event ->
        %Event{
          raw_event: event,
          topic: topic,
          partition: partition
        }
      end)
      # Decode each event
      |> Flow.map(&decode_event/1)
      |> Flow.flat_map(&EventHandler.handle_event(&1, event_handler))
      |> Flow.flat_map(&TopicSelector.resolve_topic/1)
      |> Flow.map(&Encoder.encode_event/1)
      |> Flow.map(&Publisher.publish_request(&1, [worker]))
      |> Flow.start_link(name: String.to_atom("flow_#{topic}_#{partition}"))

    {:ok, link_pid}
  end

  defp create_worker(worker_name) do
    case KafkaEx.create_worker(worker_name) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  defp decode_event(%Event{raw_event: %{key: _, value: _}} = event) do
    # when in doubt try all the transcoders
    Enum.map(Config.transcoders(), & &1.decode_event(event))
    |> Enum.find([], fn
      %Event{} = _event -> true
      _ -> false
    end)
  end
end
