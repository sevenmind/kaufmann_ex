defmodule KaufmannEx.Publisher.PartitionSelector do
  @moduledoc """
  Choose Kafka partition to publish to.

  if callback in metadata specifies a callback partition other options will be disregarded

  * :random
  * :md5
  * Callback metadata
  * function
  """

  @doc """
    Choose partition from specified strategy
  """
  @spec choose_partition(integer, term, atom | function) :: {atom, integer | atom}
  def choose_partition(partitions_count, %{callback_topic: %{partition: partition}}, _)
      when is_number(partition) do
    case partition do
      x when x < partitions_count -> {:ok, partition}
      _ -> {:error, :invalid_callback_partition}
    end
  end

  def choose_partition(partitions_count, metadata, :md5) do
    partition =
      metadata
      |> Map.get(:message_name)
      |> (fn x -> x <> Map.get(metadata, :message_id) end).()
      |> md5(partitions_count)

    {:ok, partition}
  end

  def choose_partition(partitions_count, _metadata, :random) do
    {:ok, random(partitions_count)}
  end

  def choose_partition(partitions_count, metadata, strategy) when is_function(strategy) do
    strategy.(partitions_count, metadata)
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
