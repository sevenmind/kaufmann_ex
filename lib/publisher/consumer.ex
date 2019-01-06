defmodule KaufmannEx.Publisher.ConsumerSupervisor do
  require Logger
  use ConsumerSupervisor

  def start_link(_) do
    ConsumerSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      %{
        id: KaufmannEx.Publisher.Publisher,
        start: {KaufmannEx.Publisher.Publisher, :start_link, []},
        restart: :temporary
      }
    ]

    opts = [
      strategy: :one_for_one,
      subscribe_to: [KaufmannEx.Publisher.TopicSelector]
    ]

    ConsumerSupervisor.init(children, opts)
  end
end
