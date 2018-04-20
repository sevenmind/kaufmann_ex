defmodule KaufmannEx.Supervisor do
  @moduledoc """
  Supervisor that coordinates Kafka Subscription and event consumption

  Accepts `KafkaEx.ConsumerGroup` options
  """

  require Logger
  use Supervisor

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
      opts
    ]

    children = [
      %{
        id: KaufmannEx.Stages.Producer,
        start: {KaufmannEx.Stages.Producer, :start_link, []}
      },
      %{
        id: KaufmannEx.Stages.Consumer,
        start: {KaufmannEx.Stages.Consumer, :start_link, []}
      },
      %{
        id: KafkaEx.ConsumerGroup,
        start: {KafkaEx.ConsumerGroup, :start_link, consumer_group_opts},
        type: :supervisor
      }
    ]

    opts = [strategy: :one_for_one]
    Supervisor.init(children, opts)
  end
end
