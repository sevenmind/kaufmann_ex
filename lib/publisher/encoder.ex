defmodule KaufmannEx.Publisher.Encoder do
  use GenStage
  require Logger
  alias KaufmannEx.Schemas

  def start_link(_ \\ []) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:producer_consumer, [], subscribe_to: [KaufmannEx.Publisher.Producer]}
  end

  def handle_events(events, _from, state) do
    {:noreply, Enum.map(events, &encode_event/1), state}
  end

  def encode_event(%{event_name: event_name, body: body} = event) do
    {:ok, encoded} = Schemas.encode_message(event_name, body)
    Map.put(event, :encoded, encoded)
  end
end
