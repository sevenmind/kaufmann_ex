defmodule KaufmannEx.Publisher.PartitionSelector do
  @moduledoc """
  Choose Kafka partition to publish to.

  if callback in metadata specifies a callback partition other options will be disregarded

  * :random
  * :md5
  * Callback metadata
  * function
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(opts \\ []) do
    {:ok,
     %{
       strategy: KaufmannEx.Config.topic_strategy(),
       topic_meta: fetch_partitions_counts()
     }}
  end

  def choose_partition(topic, metadata) do
    GenServer.call(__MODULE__, {:choose_partition, [topic, metadata]})
  end

  def handle_call(
        {:choose_partition, [topic, metadata]},
        _from,
        %{
          strategy: strategy,
          topic_meta: topic_meta
        } = state
      ) do
    partitions_count = Map.get(topic_meta, topic)
    partition = pick_partition(partitions_count, metadata, strategy)

    {:reply, partition, state}
  end

  @doc """
    Choose partition from specified strategy
  """
  @spec pick_partition(integer, term, atom | function) :: {atom, integer | atom}
  def pick_partition(partitions_count, %{callback_topic: %{partition: partition}}, _)
      when is_number(partition) do
    case partition do
      x when x < partitions_count -> {:ok, partition}
      _ -> {:error, :invalid_callback_partition}
    end
  end

  def pick_partition(partitions_count, metadata, :md5) do
    partition =
      metadata
      |> Map.get(:message_name)
      |> (fn x -> x <> Map.get(metadata, :message_id) end).()
      |> md5(partitions_count)

    {:ok, partition}
  end

  def pick_partition(partitions_count, metadata, strategy) when is_function(strategy) do
    strategy.(partitions_count, metadata)
  end

  def pick_partition(partitions_count, _metadata, _strategy) do
    {:ok, random(partitions_count)}
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

  def get_partitions_count(topic) do
    %KafkaEx.Protocol.Metadata.Response{
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          partition_metadatas: partition_metadatas
        }
      ]
    } = KafkaEx.metadata(topic: topic)

    length(partition_metadatas)
  end

  def fetch_partitions_counts do
    %KafkaEx.Protocol.Metadata.Response{topic_metadatas: topics} = KafkaEx.metadata()

    topics
    |> Enum.map(&extract_partition_count/1)
    |> Enum.into(%{})
  end

  defp extract_partition_count(topic) do
    %{topic: topic_name, partition_metadatas: partitions} = topic

    {topic_name, length(partitions)}
  end
end
