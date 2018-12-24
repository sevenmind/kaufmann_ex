defmodule KaufmannEx.Stage.Supervisor do
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
      {KaufmannEx.Stages.Producer, {topic, partition}},
      {KaufmannEx.Stages.Decoder, {topic, partition}},
      {KaufmannEx.Stages.Consumer, {topic, partition}}
      # %{
      #   id: ,
      #   start: {KaufmannEx.Stages.Producer, :start_link, {topic, partition}}
      # },
      # %{
      #   id: KaufmannEx.Stages.Consumer,
      #   start: {KaufmannEx.Stages.Consumer, :start_link, {topic, partition}}
      # }, %{
      #   id: KaufmannEx.Stages.Consumer,
      #   start: {KaufmannEx.Stages.Consumer, :start_link, {topic, partition}}
      # }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
