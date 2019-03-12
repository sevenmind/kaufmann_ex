defmodule KaufmannEx.Publisher.Stage.TopicSelector do
  @moduledoc """
  Topic and partition selection stage.

  If event context includes a callback topic, the passed message will be duplicated
  and published to the default topic and the callback topic, maybe it should only be
  published on the callback topic?

  TODO: Implement standard partition selection algo to match Java producer behavior
  """

  use GenStage
  require Logger
  alias KaufmannEx.Config
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  def start_link(opts: opts, stage_opts: stage_opts) do
    GenStage.start_link(__MODULE__, stage_opts, opts)
  end

  def start_link(opts: opts), do: start_link(opts: opts, stage_opts: [])
  def start_link([opts, args]), do: start_link(opts: opts, stage_opts: args)
  def start_link(opts), do: start_link(opts: opts, stage_opts: [])

  def init(stage_opts) do
    {:producer_consumer, topic_metadata, stage_opts}
  end

  def topic_metadata do
    %{
      partition_strategy: Config.partition_strategy(),
      topic_partitions: fetch_partitions_counts()
    }
  end

  @impl true
  def handle_events(events, _from, state) do
    {:noreply, Enum.flat_map(events, &select_topic_and_partition(&1, state)), state}
  end


  @spec select_topic_and_partition(Request.t() | map(), map()) :: Request.t() | [Request.t()]
  def select_topic_and_partition(event, state \\ %{})

  def select_topic_and_partition(
        %Event{publish_request: %Request{context: %{callback_topic: callback}} = publish_req} =
          event,
        state
      )
      when not is_nil(callback) and callback != %{} do
    # If context includes callback topic create duplicate publish request to callback topic

    [%Event{event | publish_request: Map.merge(publish_req, callback)}, event]
    |> Enum.map(&drop_callback_topic/1)
    |> Enum.flat_map(&select_topic_and_partition(&1, state))
  end

  defp drop_callback_topic(%Event{publish_request: publish_req} = event) do
    publish_request =
      publish_req
      |> Map.replace(
        :context,
        Map.drop(publish_req.context, [:callback_topic])
      )

    %Event{event | publish_request: publish_request}
  end

  def select_topic_and_partition(%Event{publish_request: publish_req} = event, state) do
    topic =
      case publish_req.topic do
        v when is_binary(v) -> v
        _ -> Config.default_publish_topic()
      end

    partitions_count =
      case state |> Map.get(:topic_partitions, %{}) |> Map.get(topic) do
        nil -> fetch_partitions_count(topic)
        n -> n
      end

    partition =
      case publish_req.partition do
        n when is_integer(n) ->
          n

        _ ->
          case Map.get(state, :partition_strategy, :random) do
            :md5 -> md5(publish_req.encoded, partitions_count)
            _ -> random(partitions_count)
          end
      end

    [
      %Event{
        event
        | publish_request:
            publish_req
            |> Map.put(:topic, topic)
            |> Map.put(:partition, partition)
      }
    ]
  end

  @spec fetch_partitions_counts() :: map()
  def fetch_partitions_counts do
    KafkaEx.metadata()
    |> Map.get(:topic_metadatas)
    |> Enum.into(%{}, fn %{topic: topic_name, partition_metadatas: partitions} ->
      {topic_name, length(partitions)}
    end)
  end

  @spec fetch_partitions_count(binary) :: integer
  def fetch_partitions_count(topic) do
    KafkaEx.metadata(topic: topic)
    |> Map.get(:topic_metadatas)
    |> Enum.at(0)
    |> Map.get(:partition_metadatas)
    |> length()
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
