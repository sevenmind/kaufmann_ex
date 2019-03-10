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

  def init(opts \\ []) do
    consumer_group_name = KaufmannEx.Config.consumer_group()
    topics = KaufmannEx.Config.default_topics()
    gen_consumer_mod = KaufmannEx.Config.gen_consumer_mod()

    consumer_group_opts = [
      gen_consumer_mod,
      consumer_group_name,
      topics,
      [
        heartbeat_interval: 1_000,
        commit_interval: 10_000,
        auto_offset_reset: :latest,
        fetch_options: [
          max_bytes: 1_971_520,
          wait_time: 1_000
        ]
      ]
      # opts
    ]

    children = [
      {Registry, keys: :unique, name: Registry.ConsumerRegistry},
      # KaufmannEx.FlowConsumer,
      # %{
      #   id: KafkaEx.ConsumerGroup,
      #   start: {KafkaEx.ConsumerGroup, :start_link, consumer_group_opts},
      #   type: :supervisor
      # },
      %{
        id: KafkaEx.ConsumerGroup,
        start:
          {KafkaEx.ConsumerGroup, :start_link,
           [
             KafkaExGenStageConsumer,
             KaufmannEx.FlowConsumer,
             consumer_group_name,
             topics,
             [
               heartbeat_interval: 1_000,
               commit_interval: 10_000,
               auto_offset_reset: :latest,
               fetch_options: [
                 max_bytes: 1_971_520,
                 wait_time: 30
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
