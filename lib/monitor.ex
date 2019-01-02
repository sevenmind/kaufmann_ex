defmodule KaufmannEx.Monitor do
  @moduledoc """
  Monitor performance and behavior of Kaufmann

  measures
   - event consumption rate
   - stage throughput
   - decoding time
   - event handling time
   - publishing time
   - service latency on the bus
  """

  use GenServer
  require Logger

  def start_link(opts \\ nil) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  def init(opts \\ []) do
    {:ok, %{}}
  end

  def messages_from_kafka(messages) do
    GenServer.cast(
      __MODULE__,
      {:messages_from_kafka, messages, System.monotonic_time(:microsecond)}
    )
  end

  def event(name, crc) do
    GenServer.cast(__MODULE__, {:event, name, crc, System.monotonic_time(:microsecond)})
  end

  def stats do
    # GenServer.abcast(nodes \\ [node() | Node.list()], name, request)
    Kernel.send(__MODULE__, :stats)
  end

  def handle_cast({:messages_from_kafka, messages, time}, state) do
    state =
      messages
      |> Enum.into(%{}, fn v -> {Map.get(v, :crc), %{consumed_at: time}} end)
      |> Map.merge(state)

    {:noreply, state}
  end

  def handle_cast({:event, name, crc, time}, state) do
    {:noreply, Map.update(state, crc, %{}, fn ev -> Map.put(ev, name, time) end)}
  end

  def handle_info(:stats, state) do
    state
    |> Enum.map(fn {_, v} -> format_stats_intervals(v) end)
    |> IO.inspect()

    {:noreply, state}
  end

  def format_stats_intervals(event_times) do
    event_times
    |> Enum.sort_by(fn {_, v} -> v end)
    |> Enum.chunk_every(2, 1)
    |> Enum.map(fn
      [{k1, v1}, {k2, v2}] ->
        [k1, "to", k2, v2 - v1, :microsecond]

      _ ->
        []
    end)

    # [
    #   consumed_at: -576460724863455262,
    #   begin_decode: -576460724863241016,
    #   end_decode: -576460724863157329,
    #   start_handle_event: -576460724863114261,
    #   end_handle_event: -576460724863021783
    # ]
    # |>
  end
end
