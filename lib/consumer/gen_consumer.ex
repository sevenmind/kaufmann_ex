defmodule KaufmannEx.Consumer.GenConsumer do
  @moduledoc """
    `KafkaEx.GenConsumer` listening for Kafka messages.

    Implementation based on Matthew Gardner's talk [Elixir with Kafka](https://www.youtube.com/watch?v=6ijgMvXJyuo)
  """
  require Logger
  use KafkaEx.GenConsumer
  alias KaufmannEx.Config
  alias KaufmannEx.Consumer.Stage.Producer
  alias KaufmannEx.FlowConsumer
  alias KaufmannEx.Schemas.Event

  @impl true
  def init(topic, partition) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting #{topic}@#{partition}" end)

    # Start Stage Supervisor
    # {:ok, pid} = start_stage_supervisor(topic, partition)

    {:ok,
     %{
       #  supervisor: pid,
       topic: topic,
       partition: partition,
       commit_strategy: Config.commit_strategy()
     }}
  end

  defp start_stage_supervisor(topic, partition) do
    case FlowConsumer.start_link({topic, partition}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @impl true
  def handle_message_set(message_set, %{topic: topic, partition: partition} = state) do
    :ok =
      message_set
      |> Enum.map(&inject_timestamp(&1, state))
      |> Producer.notify(topic, partition)

    {Map.get(state, :commit_strategy, :async_commit), state}
  end

  def handle_info({_, :ok}, state), do: {:noreply, state}

  def terminate(reason, state) do
    Supervisor.stop(state.supervisor, reason, 1000)

    {:shutdown, reason}
  end

  def inject_timestamp(event, %{topic: topic, partition: partition} = _) do
    %Event{
      raw_event: event,
      timestamps: [
        gen_consumer: :erlang.monotonic_time()
      ],
      topic: topic,
      partition: partition
    }
  end
end
