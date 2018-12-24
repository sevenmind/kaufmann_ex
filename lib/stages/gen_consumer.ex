defmodule KaufmannEx.Stages.GenConsumer do
  @moduledoc """
    `KafkaEx.GenConsumer` listening for Kafka messages.

    Implementation based on Matthew Gardner's talk [Elixir with Kafka](https://www.youtube.com/watch?v=6ijgMvXJyuo)
  """
  require Logger
  use KafkaEx.GenConsumer

  def init(topic, partition) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)

    # Start Stage Supervisor
    {:ok, pid} = KaufmannEx.Stage.Supervisor.start_link({topic, partition})

    {:ok, %{supervisor: pid, topic: topic, partition: partition}}
  end

  def handle_message_set(message_set, state) do
    GenStage.call(
      {:global, {KaufmannEx.Stages.Producer, state.topic, state.partition}},
      {:notify, message_set}
    )

    {:async_commit, state}
  end
end
