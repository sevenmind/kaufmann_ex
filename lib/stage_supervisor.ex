defmodule KaufmannEx.StageSupervisor do
  @moduledoc """

  A Supervisor responsible for managing event processing stages.

  Intended to be started from `KaufmannEx.Consumer.GenConsumer` and scoped to a
  topic and partition.

  StageSupervisor has the following children:
    - `KaufmannEx.Consumer.Stage.Producer`
    - `KaufmannEx.Consumer.Stage.Decoder`
    - `KaufmannEx.Consumer.Stage.EventHandler`
    - `KaufmannEx.Publisher.Stage.Encoder`
    - `KaufmannEx.Publisher.Stage.TopicSelector`
    - `KaufmannEx.Publisher.Stage.PublishSupervisor`
  """

  require Logger
  use Supervisor

  alias KaufmannEx.Config
  alias KaufmannEx.Consumer.Stage.{Decoder, EventHandler, Producer}
  alias KaufmannEx.Publisher.Stage.{Encoder, PublisherConsumer, TopicSelector}

  def start_link({topic, partition}) do
    Supervisor.start_link(__MODULE__, {topic, partition},
      name: stage_name(__MODULE__, topic, partition)
    )
  end

  def init({topic, partition}) do
    children = [
      # Consumption stages
      {Producer,
       opts: [
         name: stage_name(Producer, topic, partition)
       ]},
      {Decoder,
       [
         opts: [name: stage_name(Decoder, topic, partition)],
         stage_opts: [
           subscribe_to: [
             {stage_name(Producer, topic, partition), max_demand: Config.max_demand()}
           ]
         ]
       ]},
      {EventHandler,
       [
         opts: [name: stage_name(EventHandler, topic, partition)],
         stage_opts: [
           subscribe_to: [
             {stage_name(Decoder, topic, partition), max_demand: Config.max_demand()}
           ]
         ]
       ]},

      # Publish Stages
      {Encoder,
       [
         opts: [name: stage_name(Encoder, topic, partition)],
         stage_opts: [
           subscribe_to: [
             {stage_name(EventHandler, topic, partition), max_demand: Config.max_demand()},
             {KaufmannEx.Publisher.Producer, max_demand: Config.max_demand()}
           ]
         ]
       ]},
      {TopicSelector,
       [
         opts: [name: stage_name(TopicSelector, topic, partition)],
         stage_opts: [
           subscribe_to: [
             {stage_name(Encoder, topic, partition), max_demand: Config.max_demand()}
           ]
         ]
       ]},
      {PublisherConsumer,
       [
         opts: [name: stage_name(PublisherConsumer, topic, partition)],
         stage_opts: [
           subscribe_to: [
             {stage_name(TopicSelector, topic, partition), max_demand: Config.max_demand()}
           ]
         ]
       ]}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def stage_name(module, topic, partition),
    do: {:via, Registry, {Registry.ConsumerRegistry, build_stage_name(module, topic, partition)}}

  @spec build_stage_name(atom(), binary(), integer()) :: <<_::32, _::_*8>>
  def build_stage_name(module, topic, partition), do: "#{module}-t#{topic}-p#{partition}"
end
