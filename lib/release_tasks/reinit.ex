defmodule KaufmannEx.ReleaseTasks.ReInit do
  @moduledoc """
  A Release Task that can be used by implementing services to reinit their internal state

  Do not depend on the `target_offset` argument as an absolute. Events offsets are committed in batches, the final batch may include more than the final event.

  Caveat: This task has not been tested in a true production environment. It likely still has some bugs.
  """
  alias KaufmannEx.ReleaseTasks.ReInit

  defmodule PublishNothing do
    @moduledoc """
    dummy publisher. Does nothing.
    """
    def produce(name, _pl), do: IO.puts(name)
  end

  defmodule StateStore do
    @moduledoc """
    ETS table for storing target offset.
    """
    def init() do
      :ets.new(:reinit_store, [:named_table, :public])

      :ok
    end

    def set_target_offset(offset) do
      :ets.insert(:reinit_store, {:target_offset, offset})
    end

    def get_target_offset do
      :ets.lookup(:reinit_store, :target_offset)
    end
  end

  defmodule GenConsumer do
    @moduledoc """
    GenConsumer which terminates after target_offset is observed
    """
    use KafkaEx.GenConsumer

    def init(_topic, _partition) do
      {:ok, StateStore.get_target_offset()}
    end

    def handle_message_set(message_set, state) do
      # If all messages at offset >= target, shut it all down
      case Enum.any?(message_set, fn m -> m.offset >= state[:target_offset] - 1 end) do
        true ->
          message_set
          |> Enum.filter(fn m -> m.offset < state[:target_offset] - 1 end)
          |> KaufmannEx.Consumer.Stage.Producer.notify()

          # System.stop()
          send(:reinit, :shutdown)
          {:sync_commit, state}

        false ->
          KaufmannEx.Consumer.Stage.Producer.notify(message_set)
          {:async_commit, state}
      end
    end

    def handle_cast(:shutdown, state) do
      IO.puts("Shutdown")
      {:stop, :shutdown, state}
    end
  end

  defmodule Config do
    @moduledoc false
    defstruct starting_offset: 0,
              target_offset: :latest,
              publish: false,
              consumer_group: nil,
              default_topic: nil,
              worker: nil,
              metadata: nil
  end

  def run(app, starting_offset \\ 0, target_offset \\ :latest, publish \\ false) do
    starting_offset
    |> reset_offsets(target_offset, publish)
    |> consume_queued_messages(app)
  end

  def reset_offsets(starting_offset \\ 0, target_offset \\ :latest, publish \\ false) do
    start_services()

    starting_offset
    |> build_args(target_offset, publish)
    |> override_default_producer()
    |> configure_kafka_consumer_group()
    |> stop_services
  end

  def consume_queued_messages(%Config{} = _reinit, application) do
    ensure_loaded(application)
    :ok = Application.start(:kafka_ex)
    :ok = Application.start(application, :transient)
    Process.register(self(), :reinit)
    await_shutdown(application)

    # Application will run using ReInit.GenConsumer, which will terminate when target_offset is reached
  end

  def await_shutdown(application) do
    receive do
      :shutdown ->
        Process.sleep(500)
        Application.stop(application)
    after
      5000 ->
        await_shutdown(application)
    end
  end

  def start_services do
    ensure_loaded(:kaufmann_ex)
    :ok = Application.ensure_started(:kafka_ex)
    :ok = StateStore.init()

    override_default_gen_consumer()
  end

  defp build_args(starting_offset, target_offset, publish) do
    %Config{
      starting_offset: starting_offset,
      target_offset: target_offset,
      publish: publish,
      consumer_group: KaufmannEx.Config.consumer_group(),
      default_topic: KaufmannEx.Config.default_topic()
    }
  end

  @doc """
  Sets Publisher in kaufmann_ex Application Env to `PublishNothing`
  """
  def override_default_producer(%Config{publish: true} = args), do: args

  def override_default_producer(%Config{publish: false} = args) do
    Application.put_env(
      :kaufmann_ex,
      :producer_mod,
      KaufmannEx.ReleaseTasks.ReInit.PublishNothing
    )

    args
  end

  # Overwrites the configured GenConsumer with our custom GenServer that aborts when target_offset is reached
  defp override_default_gen_consumer do
    Application.put_env(
      :kaufmann_ex,
      :gen_consumer_mod,
      KaufmannEx.ReleaseTasks.ReInit.GenConsumer
    )
  end

  @doc """
  Set Offsets of consumergroup to 0 or ealiest available offset
  """
  def configure_kafka_consumer_group(%Config{} = args) do
    args
    |> create_worker()
    |> get_metadata()
    |> commit_earliest_offsets()
    |> destroy_worker()
    |> store_target_offset()
  end

  def stop_services(%Config{} = reinit) do
    :ok = Application.stop(:kafka_ex)

    reinit
  end

  defp create_worker(%Config{consumer_group: consumer_group} = args) do
    {:ok, worker} = KafkaEx.create_worker(:pr, consumer_group: consumer_group)

    %Config{args | worker: worker}
  end

  def get_metadata(%Config{default_topic: default_topic} = args) do
    metadata = KafkaEx.metadata(topic: default_topic, worker_name: :pr)
    earliest_offsets = Enum.flat_map(metadata.topic_metadatas, &get_earliest_offsets/1)
    latest_offsets = Enum.flat_map(metadata.topic_metadatas, &get_latest_offsets/1)

    target_offset =
      case args.target_offset do
        :latest -> Enum.max(latest_offsets)
        x -> Enum.max([x | latest_offsets])
      end

    starting_offset =
      case args.starting_offset do
        :earliest -> Enum.min(earliest_offsets)
        x -> Enum.min([x | earliest_offsets])
      end

    %Config{
      args
      | metadata: metadata,
        starting_offset: starting_offset,
        target_offset: target_offset
    }
  end

  defp get_earliest_offsets(%KafkaEx.Protocol.Metadata.TopicMetadata{} = topic_data) do
    topic_data.partition_metadatas
    |> Enum.flat_map(&KafkaEx.earliest_offset(topic_data.topic, &1.partition_id))
    |> Enum.flat_map(&extract_partition_offsets/1)
  end

  defp get_latest_offsets(%KafkaEx.Protocol.Metadata.TopicMetadata{partition_metadatas: md})
       when is_list(md) and md == [],
       do: [0]

  defp get_latest_offsets(%KafkaEx.Protocol.Metadata.TopicMetadata{} = topic_data) do
    topic_data.partition_metadatas
    |> Enum.flat_map(&KafkaEx.latest_offset(topic_data.topic, &1.partition_id))
    |> Enum.flat_map(&extract_partition_offsets/1)
  end

  defp extract_partition_offsets(response) do
    response.partition_offsets
    |> Enum.flat_map(fn offset -> offset.offset end)
  end

  defp commit_earliest_offsets(%Config{} = reinit) do
    reinit.metadata.topic_metadatas
    |> Enum.flat_map(&Map.get(&1, :partition_metadatas))
    |> Enum.map(&Map.get(&1, :partition_id))
    |> Enum.each(fn partition_id ->
      KafkaEx.offset_commit(reinit.worker, %KafkaEx.Protocol.OffsetCommit.Request{
        consumer_group: reinit.consumer_group,
        topic: reinit.default_topic,
        offset: reinit.starting_offset,
        partition: partition_id
      })
    end)

    reinit
  end

  defp destroy_worker(%Config{} = args) do
    KafkaEx.stop_worker(args.worker)

    %Config{args | worker: nil}
  end

  defp store_target_offset(%Config{target_offset: target_offset} = reinit) do
    StateStore.set_target_offset(target_offset)

    reinit
  end

  defp ensure_loaded(app) do
    case Application.load(app) do
      :ok -> nil
      {:error, {:already_loaded, _}} -> nil
      x -> raise RuntimeError, x
    end
  end

  # def fetch_offset(partition) do
  #   [
  #     %KafkaEx.Protocol.OffsetFetch.Response{
  #       partitions: [%{error_code: :no_error, offset: offset}],
  #       topic: "rapids"
  #     }
  #   ] =
  #     KafkaEx.offset_fetch(:kafka_ex, %KafkaEx.Protocol.OffsetFetch.Request{
  #       consumer_group: Config.consumer_group(),
  #       topic: "rapids",
  #       partition: 0
  #     })
  # end
end
