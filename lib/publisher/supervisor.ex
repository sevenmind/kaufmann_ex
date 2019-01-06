defmodule KaufmannEx.Publisher.Supervisor do
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

  def init(_opts \\ []) do
    children = [
      KaufmannEx.Publisher.Producer,
      KaufmannEx.Publisher.Encoder,
      KaufmannEx.Publisher.TopicSelector,
      KaufmannEx.Publisher.ConsumerSupervisor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
