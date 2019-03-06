defmodule KaufmannEx.Consumer.Stage.Decoder do
  @moduledoc """
  Consumer Deocoder stage, decodes AvroEx messages.
  """
  use GenStage
  require Logger
  alias KaufmannEx.StageSupervisor
  alias KaufmannEx.Schemas.Event

  def start_link(opts: opts, stage_opts: stage_opts) do
    GenStage.start_link(__MODULE__, stage_opts, opts)
  end

  def start_link(opts: opts), do: start_link(opts: opts, stage_opts: [])
  def start_link([opts, args]), do: start_link(opts: opts, stage_opts: args)
  def start_link([]), do: start_link(opts: [], stage_opts: [])
  def start_link(opts), do: start_link(opts: opts, stage_opts: [])

  def init(stage_opts \\ []) do
    {:producer_consumer, %{event_handler: KaufmannEx.Config.event_handler()}, stage_opts}
  end

  @spec handle_events(any(), any(), any()) :: {:noreply, [any()], any()}
  def handle_events(events, _from, state) do
    events =
      events
      |> Enum.filter(&(&1.key in state.event_handler.handled_events))
      |> Enum.map(&decode_event/1)

    {:noreply, events, state}
  end

  @doc """
  Decodes Avro encoded message value

  Returns Event or Error
  """
  @spec decode_event(map) :: KaufmannEx.Schemas.Event.t() | KaufmannEx.Schemas.ErrorEvent.t()

  def decode_event(%Event{raw_event: %{key: key, value: value}} = event) do
    event_name = key |> String.to_atom()
    crc = Map.get(event, :crc)
    now = DateTime.utc_now()
    size = byte_size(value)

    case KaufmannEx.Schemas.decode_message(key, value) do
      {:ok, %{meta: meta, payload: payload}} ->
        {:ok, published_at, _} = DateTime.from_iso8601(meta[:timestamp])
        bus_time = DateTime.diff(now, published_at, :millisecond)

        Logger.info([
          key,
          " ",
          meta[:message_id],
          " from ",
          meta[:emitter_service_id],
          " on ",
          event.topic,
          "@",
          to_string(event.partition),
          " in ",
          to_string(bus_time),
          "ms ",
          to_string(size),
          " bytes"
        ])

        %KaufmannEx.Schemas.Event{
          event
          | name: event_name,
            meta: Map.put(meta, :crc, crc),
            payload: payload
        }

      {:error, error} ->
        Logger.warn(fn -> "Error Decoding #{key} #{inspect(error)}" end)

        err_event =
          event
          |> Map.from_struct()
          |> Map.merge(%{name: key, error: error, message_payload: value})

        struct(KaufmannEx.Schemas.ErrorEvent, err_event)
    end
  end
end
