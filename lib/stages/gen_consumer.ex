defmodule KaufmannEx.Stages.GenConsumer do
  @moduledoc """
    `KafkaEx.GenConsumer` listening for Kafka messages.

    Implementation based on Matthew Gardner's talk [Elixir with Kafka](https://www.youtube.com/watch?v=6ijgMvXJyuo)
  """
  require Logger
  use KafkaEx.GenConsumer

  def init(_topic, _partition) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)

    {:ok, {}}
  end

  def handle_message_set(message_set, state) do
    # What happens if this notify times out?
    # /!\ GenConsumer will be started once per topic partition What happens Then?
    KaufmannEx.Stages.Producer.notify(message_set)

    {:async_commit, state}
  end
end
