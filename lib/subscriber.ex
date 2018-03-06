defmodule Kaufmann.Subscriber do
  @moduledoc """
  Behavior module for consuming messages from Kafka bus. 
  """
  require Logger
  use GenServer

  def start_link(_), do: start_link()

  def start_link() do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    GenServer.start_link(__MODULE__, [])
  end

  def init(_arg) do
    # Triggers immediate timeout, starts loop
    {:ok, [], 0}
  end

  # Use Timeout Loop to keep Flow running
  def handle_info(:timeout, []) do
    handle_messages()

    {:noreply, [], 0}
  end

  @doc """
  Receives messages from `Kaufmann.Stages.Producer`, uses `Flow`/`GenStage` to proccess messages in parallel in the module specified in `Kaufmann.Config.event_handler/0`
  """
  def handle_messages() do
    # Maybe use `ConsumerSupervisor` instead of flow (creates process per stage event)
    # |> Flow.map(&handle_with_rescue/1)
    Kaufmann.Stages.Producer
    |> Flow.from_stage()
    |> Flow.map(&decode_event/1)
    |> Flow.map(&handle_with_rescue/1)
    |> Flow.run()
  end

  def handle_with_rescue(event) do
    handle_event(event)
  rescue
    error ->
      Logger.warn("Error Publishing #{event.name} #{inspect(error)}")
      handler = Kaufmann.Config.event_handler()

      event
      |> error_from_event(error)
      |> handler.given_event()
  end

  def handle_event(event) do
    handler = Kaufmann.Config.event_handler()
    handler.given_event(event)
  end

  @doc """
  Decodes Avro encoded message value, transforms event into tuple of `{:message_name, %Payload{}}`

  Returns `{key, value}`
  """
  @spec decode_event(map) :: Kaufmann.Schemas.Event.t() | Kaufmann.Schemas.ErrorEvent.t()
  def decode_event(%{key: key, value: value}) do
    event_name = key |> String.to_atom()

    case Kaufmann.Schemas.decode_message(key, value) do
      {:ok, parsed} ->
        %Kaufmann.Schemas.Event{
          name: event_name,
          meta: parsed[:meta],
          payload: parsed[:payload]
        }

      {:error, error} ->
        Logger.warn(fn -> "Error Encoding #{key} #{inspect(error)}" end)

        %Kaufmann.Schemas.ErrorEvent{
          name: key,
          error: error,
          message_payload: value
        }
    end
  end

  defp error_from_event(event, error) do
    %Kaufmann.Schemas.ErrorEvent{
      name: event.name,
      error: error,
      message_payload: event.payload
    }
  end
end
