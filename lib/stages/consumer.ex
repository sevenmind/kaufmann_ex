defmodule KaufmannEx.Stages.Consumer do
  @moduledoc """
  A consumer will be a consumer supervisor that will
  Subscriber tasks for each event.
  """

  require Logger
  use ConsumerSupervisor

  def start_link(opts \\ []) do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    children = [%{
      id: KaufmannEx.Stages.EventHandler,
      start: {KaufmannEx.Stages.EventHandler, :start_link, []},
      restart: :temporary
    }]

    # max_demand is highly resource dependent
      opts = [strategy: :one_for_one,
     subscribe_to: [
       {KaufmannEx.Stages.Producer, max_demand: KaufmannEx.Config.event_handler_demand()}
     ]]

      ConsumerSupervisor.init(children, opts)
  end
end
