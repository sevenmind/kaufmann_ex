defmodule KaufmannEx.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic (`KaufmannEx.Config.default_topic/0`).


  """
  require Logger
  alias KaufmannEx.Publisher.PartitionSelector
  alias KaufmannEx.Publisher.TopicSelector

  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request

  @doc """
  Publishes encoded message

  Encodes messages into Avro Schema with ` KaufmannEx.Schemas.encode_message/2`

  Defaults to partition 0 for publication. This is less than ideal.
  """
  @spec produce(String.t(), String.t(), term(), term()) :: :ok | {:error, any}
  def produce(topic, message_name, data, context \\ %{})

  def produce(topic, message_name, data, context) when is_atom(message_name),
    do: produce(topic, message_name |> Atom.to_string(), data, context)

  def produce(topic, message_name, data, context) do
    with {:ok, payload} <- KaufmannEx.Schemas.encode_message(message_name, data),
         {:ok, partition} <- choose_partition(topic, context) do
      Logger.debug(fn -> "Publishing Event #{message_name} on #{topic}@#{partition}" end)

      message = %Message{value: payload, key: message_name}

      produce_request = %Request{
        partition: partition,
        topic: topic,
        messages: [message]
      }

      KafkaEx.produce(produce_request)
    else
      {:error, error} ->
        {:error, error}

      {:error, error, _} ->
        {:error, error}

      {:error, error, _payload, _schema} ->
        {:error, error}
    end
  end

  @doc """
  Produces message to configured producer

  Chooses publication topic from Topic Strategy

  Events with are produced to the Producer set in config `:kaufmann_ex, :producer_mod`. This defaults to `KaufmannEx.Publisher`
  """
  @spec publish(atom, map, map) :: :ok
  def publish(event_name, message_body, context \\ %{}) do
    log_time_took(context[:timestamp], event_name)
    {:ok, topic} = choose_topic(event_name, context)

    produce_to_topic(topic, event_name, message_body, context)
  end

  defp choose_partition(topic, metadata) do
    partitions_count = get_partitions_count(topic)
    strategy = KaufmannEx.Config.partition_strategy()

    PartitionSelector.choose_partition(
      partitions_count,
      metadata,
      strategy
    )
  end

  defp produce_to_topic(topics, event_name, message_body, context) when is_list(topics),
    do: Enum.map(topics, &produce_to_topic(&1, event_name, message_body, context))

  defp produce_to_topic(topic, event_name, message_body, context) do
    producer = Application.fetch_env!(:kaufmann_ex, :producer_mod)
    producer.produce(topic, event_name, message_body, context)
  end

  @spec choose_topic(atom, map) :: {atom, String.t()}
  def choose_topic(event_name, context) do
    strategy = KaufmannEx.Config.topic_strategy()
    TopicSelector.choose_topic(event_name, context, strategy)
  end

  defp get_partitions_count(topic) do
    %KafkaEx.Protocol.Metadata.Response{
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          partition_metadatas: partition_metadatas
        }
      ]
    } = KafkaEx.metadata(topic: topic)

    length(partition_metadatas)
  end

  defp log_time_took(nil, _), do: nil

  defp log_time_took(timestamp, event_name) do
    Logger.info(fn ->
      {:ok, published_at, _} = DateTime.from_iso8601(timestamp)
      took = DateTime.diff(DateTime.utc_now(), published_at, :millisecond)
      "Responded with #{event_name} in #{took}ms"
    end)
  end
end
