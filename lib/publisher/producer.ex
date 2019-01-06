defmodule KaufmannEx.Publisher.Producer do
  @moduledoc """
  The Producer stage of the publish GenStage. Accepts events to be published.

  This stage ignores demand and pushes all events directly to the next stage

  /!\ How to handle errors in any stage?
  """

  use GenStage
  alias KaufmannEx.Publisher.Request

  def start_link(_ \\ []) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:producer, []}
  end

  def publish(event_name, body, context \\ %{}, topic \\ :default),
    do:
      GenServer.cast(
        __MODULE__,
        {:publish,
         %Request{
           event_name: event_name,
           body: body,
           context: context,
           topic: topic
         }}
      )

  # just push events to consumers on adding
  @spec handle_cast({:publish, [Request.t()]}, any) :: {:noreply, [Request.t()], any()}
  def handle_cast({:publish, events}, state) when is_list(events) do
    {:noreply, events, state}
  end

  @spec handle_cast({:publish, Request.t()}, any) :: {:noreply, [Request.t()], any()}
  def handle_cast({:publish, events}, state), do: {:noreply, [events], state}

  # ignore any demand
  def handle_demand(_, state), do: {:noreply, [], state}
end
