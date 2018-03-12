defmodule KaufmannEx.Stages.Consumer do
  @moduledoc """
  A consumer will be a consumer supervisor that will
  Subscriber tasks for each event.
  """

  use ConsumerSupervisor

  def start_link() do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    children = [
      worker(KaufmannEx.Stages.EventHandler, [], restart: :temporary)
    ]

    # max_demand is highly resource dependent
    {:ok, children,
     strategy: :one_for_one,
     subscribe_to: [
       {KaufmannEx.Stages.Producer, max_demand: KaufmannEx.Config.event_handler_demand()}
     ]}
  end
end
