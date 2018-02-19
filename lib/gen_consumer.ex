defmodule Kaufmann.GenConsumer do
  @moduledoc """
    GenConsumer listening for Kafka messages. 

    Implementation based on Matthew Gardner's talk: https://www.youtube.com/watch?v=6ijgMvXJyuo
  """
  use KafkaEx.GenConsumer

  def handle_message_set(message_set, state) do
    Kaufmann.Stages.Producer.notify(message_set)

    {:async_commit, state}
  end
end
