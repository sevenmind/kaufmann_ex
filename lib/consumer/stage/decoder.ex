defmodule KaufmannEx.Consumer.Stage.Decoder do
  use GenStage
  use Elixometer
  require Logger
  alias KaufmannEx.Consumer.StageSupervisor

  def start_link({topic, partition}) do
    GenStage.start_link(__MODULE__, {topic, partition},
      name:
        {:via, Registry,
         {Registry.ConsumerRegistry, StageSupervisor.stage_name(__MODULE__, topic, partition)}}
    )
  end

  def init({topic, partition}) do
    {:producer_consumer, %{partition: partition, topic: topic},
     subscribe_to: [
       {:via, Registry,
        {Registry.ConsumerRegistry,
         StageSupervisor.stage_name(KaufmannEx.Consumer.Stage.Producer, topic, partition)}}
     ]}
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

    case KaufmannEx.Schemas.decode_message(key, value) do
      {:ok, %{meta: meta, payload: payload}} ->
        {:ok, published_at, _} = DateTime.from_iso8601(meta[:timestamp])
        bus_time = DateTime.diff(DateTime.utc_now(), published_at, :millisecond)

        DateTime.utc_now()

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
