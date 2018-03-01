defmodule Kaufmann.Supervisor do
  @moduledoc false

  require Logger
  use Supervisor

  def start_link(_) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    consumer_group = Kaufmann.Config.consumer_group()
    topics = Kaufmann.Config.default_topics()

    consumer_group_opts = [
      Kaufmann.GenConsumer,
      consumer_group,
      topics,
      [
        commit_interval: 200,
        heartbeat_interval: 200
      ]
    ]

    children = [
      %{
        id: KafkaEx.ConsumerGroup,
        start: {KafkaEx.ConsumerGroup, :start_link, consumer_group_opts},
        type: :supervisor
      },
      %{
        id: Kaufmann.Stages.Producer,
        start: {Kaufmann.Stages.Producer, :start_link, []}
      },
      %{
        id: Kaufmann.Subscriber,
        start: {Kaufmann.Subscriber, :start_link, []}
      }
    ]

    opts = [strategy: :one_for_one]
    Supervisor.init(children, opts)
  end
end
