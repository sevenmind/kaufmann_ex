defmodule KaufmannEx.Publisher.Supervisor do
  @moduledoc """
  Supervisor that coordinates Kafka Event publication

  Should be used when a service (say an HTTP service) needs to publish messages
  but doesn't care about consuming messages.

  Do not use when `KaufmannEx.Supervisor` is also started
  """

  require Logger
  use Supervisor
  alias KaufmannEx.Config
  alias KaufmannEx.Publisher.Stage.{Encoder, PublisherConsumer, TopicSelector}

  def start_link(opts \\ []) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts \\ []) do
    children = [
      KaufmannEx.Publisher.Producer,
      {Encoder,
       [
         opts: [name: Encoder],
         stage_opts: [
           subscribe_to: [
             {KaufmannEx.Publisher.Producer, max_demand: Config.max_demand()}
           ]
         ]
       ]},
      {TopicSelector,
       [
         opts: [name: TopicSelector],
         stage_opts: [
           subscribe_to: [
             {Encoder, max_demand: Config.max_demand()}
           ]
         ]
       ]},
      {PublisherConsumer,
       [
         opts: [name: PublisherConsumer],
         stage_opts: [
           subscribe_to: [
             {TopicSelector, max_demand: Config.max_demand()}
           ]
         ]
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
