defmodule KaufmannEx.Consumer.GenConsumer do
  @moduledoc """
    `KafkaEx.GenConsumer` listening for Kafka messages.

    Implementation based on Matthew Gardner's talk [Elixir with Kafka](https://www.youtube.com/watch?v=6ijgMvXJyuo)
  """
  require Logger
  use KafkaEx.GenConsumer
  alias KaufmannEx.Config
  alias KaufmannEx.Consumer.Stage.Producer
  alias KaufmannEx.Consumer.StageSupervisor

  @impl true
  def init(topic, partition) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting #{topic}@#{partition}" end)

    # Start Stage Supervisor
    {:ok, pid} = start_stage_supervisor(topic, partition)

    {:ok,
     %{
       supervisor: pid,
       topic: topic,
       partition: partition,
       commit_strategy: Config.commit_strategy()
     }}
  end

  defp start_stage_supervisor(topic, partition) do
    case StageSupervisor.start_link({topic, partition}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @impl true
  def handle_message_set(message_set, %{topic: topic, partition: partition} = state) do
    :ok = Producer.notify(message_set, topic, partition)

    {Map.get(state, :commit_strategy, :async_commit), state}
  end
end
