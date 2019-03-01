defmodule KaufmannEx.Publisher.Supervisor do
  @moduledoc """
  Supervisor that coordinates Kafka Event publication

  Should be used when a service (say an HTTP service) needs to publish messages
  but doesn't care about consuming messages.

  Do not use when `KaufmannEx.Supervisor` is also started
  """

  require Logger
  use Supervisor
  alias KaufmannEx.Publisher.Stage.{Encoder, PublishSupervisor, TopicSelector}

  def start_link(opts \\ []) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts \\ []) do
    children = [
      KaufmannEx.Publisher.Producer,
      {Encoder, [name: Encoder, subscribe_to: [KaufmannEx.Publisher.Producer]]},
      {TopicSelector, [name: TopicSelector, subscribe_to: [Encoder]]},
      {PublishSupervisor, [name: PublishSupervisor, subscribe_to: [TopicSelector]]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
