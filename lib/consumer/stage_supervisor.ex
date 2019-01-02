defmodule KaufmannEx.Consumer.Stageupervisor do
  @moduledoc """
  Supervisor that coordinates Kafka Subscription and event consumption

  Accepts `KafkaEx.ConsumerGroup` options
  """

  require Logger
  use Supervisor

  def start_link({topic, partition}) do
    Supervisor.start_link(__MODULE__, {topic, partition},
      name: {:global, {__MODULE__, topic, partition}}
    )
  end

  def init({topic, partition}) do
    children = [
      {KaufmannEx.Consumer.Stage.Producer, {topic, partition}},
      {KaufmannEx.Consumer.Stage.Decoder, {topic, partition}},
      {KaufmannEx.Consumer.Stage.Consumer, {topic, partition}}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
