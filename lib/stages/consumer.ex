defmodule KaufmannEx.Stages.Consumer do
  @moduledoc """
  A consumer will be a consumer supervisor that will
  Subscriber tasks for each event.
  """

  require Logger
  use ConsumerSupervisor

  def start_link({topic, partition}) do
    ConsumerSupervisor.start_link(__MODULE__, {topic, partition},
      name: {:global, {__MODULE__, topic, partition}}
    )
  end

  # Callbacks

  def init({topic, partition}) do
    children = [
      %{
        id: KaufmannEx.Stages.EventHandler,
        start: {KaufmannEx.Stages.EventHandler, :start_link, []},
        restart: :temporary
      }
    ]

    opts = [
      strategy: :one_for_one,
      subscribe_to: [
        {{:global, {KaufmannEx.Stages.Decoder, topic, partition}}, max_demand: 50}
      ]
    ]

    ConsumerSupervisor.init(children, opts)
  end
end
