defmodule KaufmannEx.Publisher.Stage.PublishSupervisor do
  @moduledoc """
  Consumer Supervisor stage which handles end-stage publishing back to kafka.
  Each message received spawns a task responsible for publishing responses
  """
  require Logger
  use ConsumerSupervisor

  def start_link(opts) do
    ConsumerSupervisor.start_link(__MODULE__, opts, opts)
  end

  def init(opts) do
    children = [
      %{
        id: KaufmannEx.Publisher.Stage.Publisher,
        start: {KaufmannEx.Publisher.Stage.Publisher, :start_link, []},
        restart: :temporary
      }
    ]

    ConsumerSupervisor.init(children, [{:strategy, :one_for_one} | Keyword.drop(opts, [:name])])
  end
end
