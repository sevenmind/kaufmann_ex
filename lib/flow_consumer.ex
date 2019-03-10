defmodule KaufmannEx.FlowConsumer do
  use Flow
  require Logger

  alias KaufmannEx.Consumer.Stage.{Decoder, EventHandler, Producer}
  alias KaufmannEx.Publisher.Stage.{Encoder, Publisher, TopicSelector}
  alias KaufmannEx.Schemas.Event

  def start_link({pid, topic, partition, extra_consumer_args}) when is_pid(pid) do
    # def start_link({pids, topic, partition}) when is_list(pids) do
    event_handler = KaufmannEx.Config.event_handler()
    topic_metadata = TopicSelector.topic_metadata()
    workers = Enum.map(0..10, fn n -> create_worker(String.to_atom("worker_#{n}")) end)

    Flow.from_stages([pid], stages: 2)
    |> Flow.map(&inject_timestamp(&1, %{topic: topic, partition: partition}))
    |> flow_timestamp(:consumer_producer)
    |> Flow.filter(&handled_event?(&1, event_handler.handled_events))
    |> flow_timestamp(:accepted_event)
    |> Flow.map(&Decoder.decode_event/1)
    |> flow_timestamp(:decode_event)
    # |> Flow.each(
    #   &KafkaExGenStageConsumer.trigger_commit(pid, {:async_commit, &1.raw_event.offset})
    # )
    # |> flow_timestamp(:commit_offset)
    |> Flow.flat_map(&handle_event(&1, event_handler))
    |> flow_timestamp(:event_handler)
    |> Flow.map(&Encoder.encode_event/1)
    |> flow_timestamp(:encode_event)
    |> Flow.flat_map(&TopicSelector.select_topic_and_partition(&1, topic_metadata))
    |> flow_timestamp(:select_topic_and_partition)
    # |> Flow.partition(
    #   window: Flow.Window.periodic(10, :millisecond),
    #   stages: 4,
    #   key: &event_key/1
    # )
    # Group events in a 10 ms window by topic & partition
    # |> Flow.group_by(&event_key/1)
    |> Flow.map(&Publisher.publish(&1, workers))
    |> flow_timestamp(:publish)
    |> Flow.each(&print_timings/1)
    # KaufmannEx.StageSupervisor.stage_name(__MODULE__, topic, partition))
    |> Flow.start_link()
  end

  def inject_timestamp(event, %{topic: topic, partition: partition} = _) do
    %Event{
      raw_event: event,
      timestamps: [
        gen_consumer: :erlang.monotonic_time()
      ],
      topic: topic,
      partition: partition
    }
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

  def flow_timestamp(flow, subject) do
    Flow.map(flow, &timestamp(&1, subject))
  end

  def timestamp(event, subject) do
    Map.put(event, :timestamps, [{subject, :erlang.monotonic_time()} | event.timestamps])
  end

  defp event_key(event) do
    event.publish_request.topic <> to_string(event.publish_request.partition)
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
