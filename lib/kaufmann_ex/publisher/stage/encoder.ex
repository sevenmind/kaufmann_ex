defmodule KaufmannEx.Publisher.Stage.Encoder do
  @moduledoc """
  Publisher Encoder stage of
  """
  use GenStage
  require Logger
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas
  alias KaufmannEx.Schemas.Event

  def start_link(opts: opts, stage_opts: stage_opts) do
    GenStage.start_link(__MODULE__, stage_opts, opts)
  end

  def start_link(opts: opts), do: start_link(opts: opts, stage_opts: [])
  def start_link([opts, args]), do: start_link(opts: opts, stage_opts: args)
  def start_link(opts), do: start_link(opts: opts, stage_opts: [])

  def init(stage_opts \\ []) do
    {:producer_consumer, %{}, stage_opts}
  end

  def handle_events(events, _from, state) do
    {:noreply, Enum.map(events, &encode_event/1), state}
  end

  def encode_event(
        %Event{publish_request: %Request{event_name: event_name, body: body} = req} = event
      ) do
    {:ok, encoded} = Schemas.encode_message(event_name, body)
    %Event{event | publish_request: Map.put(req, :encoded, encoded)}
  end
end
