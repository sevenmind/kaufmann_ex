defmodule KaufmannEx.Consumer.Stage.Decoder do
  @moduledoc """
  Consumer Deocoder stage, decodes AvroEx messages.
  """
  use GenStage
  use Elixometer
  require Logger
  alias KaufmannEx.StageSupervisor

  def start_link(opts \\ []) do
    GenStage.start_link(__MODULE__, opts, opts)
  end

  def init(opts) do
    {:producer_consumer, %{}, Keyword.drop(opts, [:name])}
  end

  @spec handle_events(any(), any(), any()) :: {:noreply, [any()], any()}
  def handle_events(events, _from, state) do
    {:noreply, Enum.map(events, &decode_event/1), state}
  end

  @doc """
  Decodes Avro encoded message value

  Returns Event or Error
  """
  @spec decode_event(map) :: KaufmannEx.Schemas.Event.t() | KaufmannEx.Schemas.ErrorEvent.t()
  @timed key: :auto
  def decode_event(%{key: key, value: value} = event) do
    event_name = key |> String.to_atom()
    crc = Map.get(event, :crc)
    now = DateTime.utc_now()

    case KaufmannEx.Schemas.decode_message(key, value) do
      {:ok, %{meta: meta, payload: payload}} ->
        {:ok, published_at, _} = DateTime.from_iso8601(meta[:timestamp])
        bus_time = DateTime.diff(now, published_at, :millisecond)

        Logger.debug([
          meta[:message_name],
          " ",
          meta[:message_id],
          " from ",
          meta[:emitter_service_id],
          " in ",
          to_string(bus_time),
          "ms"
        ])

        update_gauge("bus_latency", bus_time)

        %KaufmannEx.Schemas.Event{
          name: event_name,
          meta: Map.put(meta, :crc, crc),
          payload: payload
        }

      {:error, error} ->
        Logger.warn(fn -> "Error Decoding #{key} #{inspect(error)}" end)

        %KaufmannEx.Schemas.ErrorEvent{
          name: key,
          error: error,
          message_payload: value
        }
    end
  end
end
