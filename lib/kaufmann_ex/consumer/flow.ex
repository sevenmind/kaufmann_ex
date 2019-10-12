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

  def start_link({producer_stage, topic, partition, args}) do
    Logger.debug("starting consumer for #{topic}##{partition}")
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
      |> Flow.map(&Event.decode_event/1)
      |> Flow.flat_map(&EventHandler.handle_event(&1, args))
      |> Flow.flat_map(&TopicSelector.resolve_topic/1)
      |> Flow.map(&Encoder.encode_event/1)
      |> Flow.map(&Publisher.publish_request(&1, [worker]))
      |> Flow.start_link(name: Module.concat([__MODULE__, producer_stage]))

    {:ok, link_pid}
  end

  defp create_worker(worker_name) do
    case KafkaEx.create_worker(worker_name) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end
end
