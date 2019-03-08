defmodule KaufmannEx.Publisher.Stage.PublishSupervisor do
  @moduledoc """
  Consumer Supervisor stage which handles end-stage publishing back to kafka.
  Each message received spawns a task responsible for publishing responses
  """
  require Logger
  use ConsumerSupervisor

  def start_link(opts: opts, stage_opts: stage_opts) do
    ConsumerSupervisor.start_link(__MODULE__, stage_opts, opts)
  end

  def start_link(opts: opts), do: start_link(opts: opts, stage_opts: [])
  def start_link([opts, args]), do: start_link(opts: opts, stage_opts: args)
  def start_link(opts), do: start_link(opts: opts, stage_opts: [])

  def init(stage_opts \\ []) do
    children = [
      %{
        id: KaufmannEx.Publisher.Stage.Publisher,
        start: {KaufmannEx.Publisher.Stage.Publisher, :start_link, []},
        restart: :temporary
      }
    ]

    ConsumerSupervisor.init(children, [{:strategy, :one_for_one} | stage_opts])
  end
end
