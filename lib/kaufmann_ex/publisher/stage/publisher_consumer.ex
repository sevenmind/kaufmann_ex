defmodule KaufmannEx.Publisher.Stage.PublisherConsumer do
  @moduledoc false

  # this moule is a ConsuemrSupervisor for the publish.stage.publisher

  use GenStage
  require Logger

  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request
  alias KaufmannEx.Publisher.Stage.Publisher

  def start_link(opts: opts, stage_opts: stage_opts) do
    GenStage.start_link(__MODULE__, stage_opts, opts)
  end

  def start_link(opts: opts), do: start_link(opts: opts, stage_opts: [])
  def start_link([opts, args]), do: start_link(opts: opts, stage_opts: args)
  def start_link(opts), do: start_link(opts: opts, stage_opts: [])

  def init(stage_opts \\ []) do
    {:consumer, %{}, stage_opts}
  end

  @impl true
  def handle_events(events, _from, state) do
    Enum.each(events, &Publisher.publish/1)

    {:noreply, [], state}
  end
end
