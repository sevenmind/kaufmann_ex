defmodule KaufmannEx.Stages.Decoder do
  use GenStage
  require Logger

  def start_link({topic, partition}) do
    GenStage.start_link(__MODULE__, {topic, partition},
      name: {:global, {__MODULE__, topic, partition}}
    )
  end

  def init({topic, partition}) do
    {:producer_consumer, %{partition: partition, topic: topic},
     subscribe_to: [{:global, {KaufmannEx.Stages.Producer, topic, partition}}]}
  end

  def handle_events(events, _from, state) do
    {:noreply, Enum.map(events, &decode_event/1), state}
  end

  @doc """
  Decodes Avro encoded message value

  Returns Event or Error
  """
  @spec decode_event(map) :: KaufmannEx.Schemas.Event.t() | KaufmannEx.Schemas.ErrorEvent.t()
  def decode_event(%{key: key, value: value} = event) do
    event_name = key |> String.to_atom()

    case KaufmannEx.Schemas.decode_message(key, value) do
      {:ok, %{meta: meta, payload: payload}} ->
        Logger.debug([
          meta[:message_name],
          " ",
          meta[:message_id],
          " from ",
          meta[:emitter_service_id]
        ])

        %KaufmannEx.Schemas.Event{
          name: event_name,
          meta: meta,
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
