defmodule KaufmannEx.Consumer.GenConsumer do
  @moduledoc """
    `KafkaEx.GenConsumer` listening for Kafka messages.

    Implementation based on Matthew Gardner's talk [Elixir with Kafka](https://www.youtube.com/watch?v=6ijgMvXJyuo)
  """
  require Logger
  use KafkaEx.GenConsumer
  alias KaufmannEx.Config
  alias KaufmannEx.Consumer.StageSupervisor

  @impl true
  def init(topic, partition) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting #{topic}@#{partition}" end)

    # Start Stage Supervisor
    {:ok, pid} = KaufmannEx.Consumer.StageSupervisor.start_link({topic, partition})

    {:ok,
     %{
       supervisor: pid,
       topic: topic,
       partition: partition,
       commit_strategy: Config.commit_strategy()
     }}
  end

  @impl true
  def handle_message_set(message_set, state) do
    GenStage.call(
      {:via, Registry,
       {Registry.ConsumerRegistry,
        StageSupervisor.stage_name(KaufmannEx.Consumer.Stage.Producer, state.topic, state.partition)}},
      {:notify, message_set}
    )

    {Map.get(state, :commit_strategy, :async_commit), state}
  end
end
