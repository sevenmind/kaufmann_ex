defmodule KaufmannEx.GenConsumer do
  @moduledoc """
    `KafkaEx.GenConsumer` listening for Kafka messages. 

    Implementation based on Matthew Gardner's talk [Elixir with Kafka](https://www.youtube.com/watch?v=6ijgMvXJyuo)
  """
  use KafkaEx.GenConsumer

  def handle_message_set(message_set, state) do
    # What happens if this notify times out?
    # /!\ GenConsumer will be started once per topic partition What happens Then?
    KaufmannEx.Stages.Producer.notify(message_set)

    {:async_commit, state}
  end
end
