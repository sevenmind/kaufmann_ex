defmodule KaufmannEx.Supervisor do
  @moduledoc """
  Supervisor that coordinates Kafka Subscription and event consumption

  Accepts `KafkaEx.ConsumerGroup` options
  """

  require Logger
  use Supervisor

  @spec start_link(any()) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link(opts \\ []) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts \\ []) do
    consumer_group_name = KaufmannEx.Config.consumer_group()
    topics = KaufmannEx.Config.subscription_topics()

    children = [
      {Registry, keys: :unique, name: Registry.ConsumerRegistry},
      %{
        id: KaufmannEx.ConsumerGroup,
        start:
          {KafkaEx.ConsumerGroup, :start_link,
           [
             {KafkaExGenStageConsumer, KaufmannEx.Consumer.Flow},
             consumer_group_name,
             topics,
             [
               heartbeat_interval: 1_000,
               commit_interval: 10_000,
               # Probably inadvisable in many uses
               auto_offset_reset: :latest,
               fetch_options: [
                 max_bytes: 1_971_520,
                 wait_time: 100
               ],
               commit_strategy: :async_commit
             ]
           ]},
        type: :supervisor
      },
      KaufmannEx.Publisher.Supervisor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
