defmodule KaufmannEx.FlowConsumer do
  defmodule DemandVacuum do
    @moduledoc """
    A GenStage Producer Consumer to inject demand into our GenStage Flow pipeline.

    There is no intuitive method to force a pipeline to propegate demand without
    an external consumer. But this pipeline doesn't have a consuming audience.

    Instead the `DemandVacuum` exerts a constant presure of 1000 events / sec on
    the Kafka Consumer stage
    """
    use GenStage
    require Logger

    def start_link(args \\ []) do
      GenStage.start(__MODULE__, args)
    end

    def init(_) do
      {:producer_consumer, %{}}
    end

    def handle_subscribe(:producer, opts, from, producers) do
      # We will only allow max_demand events every 500 milliseconds
      # max demand is 100 events /sec probably too low.
      demand = Keyword.get(opts, :max_demand, 1000)
      interval = Keyword.get(opts, :interval, 1000)

      producers =
        producers
        # Register the producer in the state
        |> Map.put(from, {demand, interval})
        |> ask_and_schedule(from)

      GenStage.ask(from, demand)

      # Returns manual as we want control over the demand
      # Automatic doesn't forward demand anyway :(
      {:manual, producers}
    end

    def handle_subscribe(:consumer, _opts, _from, producers) do
      {:automatic, producers}
    end

    def handle_cancel(_, from, producers) do
      # Remove the producers from the map on unsubscribe
      {:noreply, [], Map.delete(producers, from)}
    end

    def handle_events(events, _from, producers) do
      {:noreply, events, producers}
    end

    def handle_info({:ask, from}, producers) do
      # This callback is invoked by the Process.send_after/3 message below.
      {:noreply, [], ask_and_schedule(producers, from)}
    end

    defp ask_and_schedule(producers, from) do
      case producers do
        %{^from => {demand, interval}} ->
          # Logger.info("asking for #{demand} events")
          GenStage.ask(from, demand)
          Process.send_after(self(), {:ask, from}, interval)

          producers

        %{} ->
          producers
      end
    end
  end

  use Flow
  require Logger

  # alias KaufmannEx.Consumer.Stage.{Decoder, EventHandler, Producer}
  alias KaufmannEx.EventHandler
  alias KaufmannEx.Publisher.Stage.{Encoder, Publisher, TopicSelector}
  alias KaufmannEx.Schemas
  alias KaufmannEx.Schemas.Event

  def start_link({pid, topic, partition, _extra_consumer_args}) when is_pid(pid) do
    event_handler = KaufmannEx.Config.event_handler()
    topic_metadata = TopicSelector.topic_metadata()
    workers = Enum.map(0..10, fn n -> create_worker(String.to_atom("worker_#{n}")) end)

    {:ok, link_pid} =
      [pid]
      |> Flow.from_stages(stages: 16)
      |> Flow.through_specs([{DemandVacuum, []}])
      |> Flow.map(&inject_timestamp(&1, %{topic: topic, partition: partition}))
      |> flow_timestamp(:consumer_producer)
      |> Flow.filter(&handled_event?(&1, event_handler.handled_events))
      |> flow_timestamp(:accepted_event)
      |> Flow.map(&Schemas.decode_event/1)
      |> flow_timestamp(:decode_event)
      |> Flow.flat_map(&EventHandler.handle_event(&1, event_handler))
      |> flow_timestamp(:event_handler)
      |> Flow.map(&Encoder.encode_event/1)
      |> flow_timestamp(:encode_event)
      |> Flow.flat_map(&TopicSelector.select_topic_and_partition(&1, topic_metadata))
      |> flow_timestamp(:select_topic_and_partition)
      |> Flow.map(&Publisher.publish(&1, workers))
      |> flow_timestamp(:publish)
      |> Flow.each(&print_timings/1)
      |> Flow.start_link()

    {:ok, link_pid}
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
