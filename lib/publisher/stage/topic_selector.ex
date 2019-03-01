defmodule KaufmannEx.Publisher.Stage.TopicSelector do
  @moduledoc """
  Topic and partition selection stage.

  If event context includes a callback topic, the passed message will be duplicated
  and published to the default topic and the callback topic, maybe it should only be
  published on the callback topic?

  TODO: Implement standard partition selection algo to match Java producer behavior
  """

  use GenStage
  use Elixometer
  require Logger
  alias KaufmannEx.Config
  alias KaufmannEx.Publisher.Request

  def start_link(opts \\ []) do
    GenStage.start_link(__MODULE__, opts, opts)
  end

  @impl true
  def init(opts) do
    state = %{
      partition_strategy: Config.partition_strategy(),
      topic_partitions: fetch_partitions_counts()
    }

    {:producer_consumer, state, Keyword.drop(opts, [:name])}
  end

  @impl true
  def handle_events(events, _from, state) do
    {:noreply, Enum.flat_map(events, &select_topic_and_partition(&1, state)), state}
  end

  @spec select_topic_and_partition(Request.t() | map(), map()) :: Request.t() | [Request.t()]
  def select_topic_and_partition(%{context: %{callback_topic: callback}} = event, state)
      when not is_nil(callback) and callback != %{} do
    # If context includes callback topic create duplicate publish request to callback topic
    [
      Map.merge(event, callback)
      | event
        |> Map.replace!(:context, Map.drop(event.context, [:callback_topic]))
        |> select_topic_and_partition(state)
    ]
  end

  def select_topic_and_partition(event, state) do
    topic =
      case event.topic do
        v when is_binary(v) -> v
        _ -> Config.default_publish_topic()
      end

    partitions_count = Map.get(state.topic_partitions, topic)

    partition =
      case state.partition_strategy do
        :md5 -> md5(event.encoded, partitions_count)
        _ -> random(partitions_count)
      end

    [Map.merge(event, %{topic: topic, partition: partition})]
  end

  @spec fetch_partitions_counts() :: map()
  def fetch_partitions_counts do
    KafkaEx.metadata()
    |> Map.get(:topic_metadatas)
    |> Enum.into(%{}, fn %{topic: topic_name, partition_metadatas: partitions} ->
      {topic_name, length(partitions)}
    end)
  end

  defp md5(key, partitions_count) do
    :crypto.hash(:md5, key)
    |> :binary.bin_to_list()
    |> Enum.sum()
    |> rem(partitions_count)
  end

  defp random(partitions_count) do
    :rand.uniform(partitions_count) - 1
  end
end
