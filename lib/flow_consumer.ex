defmodule KaufmannEx.FlowConsumer do
  use Flow
  require Logger

  alias KaufmannEx.Consumer.Stage.{Decoder, EventHandler, Producer}
  alias KaufmannEx.Publisher.Stage.{Encoder, Publisher, TopicSelector}

  def start_link(_) do
    event_handler = KaufmannEx.Config.event_handler()
    topic_metadata = TopicSelector.topic_metadata()
    workers = Enum.map(0..10, fn n -> create_worker(String.to_atom("worker_#{n}")) end)

    Flow.from_specs(
      producer_specs(topic_metadata),
      stages: 64
    )
    |> Flow.map(&timestamp(&1, :consumer_producer))
    |> Flow.filter(&handled_event?(&1, event_handler.handled_events))
    |> Flow.map(&timestamp(&1, :accepted_event))
    |> Flow.map(&Decoder.decode_event/1)
    |> Flow.map(&timestamp(&1, :decode_event))
    |> Flow.flat_map(&handle_event(&1, event_handler))
    |> Flow.map(&timestamp(&1, :event_handler))
    |> Flow.map(&Encoder.encode_event/1)
    |> Flow.map(&timestamp(&1, :encode_event))
    |> Flow.flat_map(&TopicSelector.select_topic_and_partition(&1, topic_metadata))
    |> Flow.map(&timestamp(&1, :select_topic_and_partition))
    |> Flow.partition(window: Flow.Window.periodic(4, :millisecond), stages: 128)
    # Group events in a 10 ms window by topic & partition
    |> Flow.group_by(fn event ->
      event.publish_request.topic <> to_string(event.publish_request.partition)
    end)
    |> Flow.flat_map(&Publisher.publish(&1, workers))
    |> Flow.map(&timestamp(&1, :publish))
    |> Flow.each(&print_timings/1)
    # KaufmannEx.StageSupervisor.stage_name(__MODULE__, topic, partition))
    |> Flow.start_link(name: __MODULE__)
  end

  def handle_event(event, event_handler) do
    case EventHandler.handle_event(event, event_handler) do
      nil -> []
      :ok -> []
      {:ok, p} when is_pid(p) -> []
      x -> x
    end
  end

  def handled_event?(_, [:all]), do: true
  def handled_event?(%{raw_event: %{key: key}} = _, handled_events), do: key in handled_events

  def timestamp(event, subject) do
    Map.put(event, :timestamps, [{subject, :erlang.monotonic_time()} | event.timestamps])
  end

  def print_timings(%{timestamps: timestamps} = event) do
    stage_timings =
      timestamps
      |> Enum.sort_by(fn {_, v} -> v end)
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.into(%{}, fn
        [{_first_n, first_time}, {second_n, second_time}] ->
          {second_n, time_diff(second_time, first_time)}
      end)
      |> Map.put(
        :overall,
        time_diff(
          Keyword.get(timestamps, :publish),
          Keyword.get(timestamps, :gen_consumer)
        )
      )

    Logger.info([to_string(event.name), " stage_timings: ", inspect(Enum.to_list(stage_timings))])
  end

  defp time_diff(finish, start, unit \\ :millisecond) do
    :erlang.convert_time_unit(finish - start, :native, unit)
  end

  defp create_worker(worker_name) do
    case KafkaEx.create_worker(worker_name) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  defp producer_specs(topic_metadata) do
    KaufmannEx.Config.default_topics()
    |> Enum.flat_map(fn topic ->
      partitions = TopicSelector.fetch_partitions_count(topic)

      Enum.map(0..partitions, fn partition ->
        {Producer,
         opts: [name: KaufmannEx.StageSupervisor.stage_name(Producer, topic, partition)],
         stage_opts: []}
      end)
    end)
  end
end
