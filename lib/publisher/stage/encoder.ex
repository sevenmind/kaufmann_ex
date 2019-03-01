defmodule KaufmannEx.Publisher.Stage.Encoder do
  @moduledoc """
  Publisher Encoder stage of
  """
  use GenStage
  use Elixometer
  require Logger
  alias KaufmannEx.Schemas

  def start_link(opts \\ []) do
    GenStage.start_link(__MODULE__, opts, opts)
  end

  def init(opts \\ []) do
    {:producer_consumer, %{}, Keyword.drop(opts, [:name])}
  end

  def handle_events(events, _from, state) do
    {:noreply, Enum.map(events, &encode_event/1), state}
  end

  @timed key: :auto
  def encode_event(%{event_name: event_name, body: body} = event) do
    {:ok, encoded} = Schemas.encode_message(event_name, body)
    Map.put(event, :encoded, encoded)
  end
end
