defmodule Kaufmann.Subscriber do
  @moduledoc """
  Behavior module for consuming messages from Kafka bus. 
  """
  require Logger
  use GenServer

  @event_handler_mod Kaufmann.Config.event_handler()

  def start_link() do
    :ok = Logger.info(fn -> "#{__MODULE__} Starting" end)
    GenServer.start_link(__MODULE__, [])
  end

  def init(_arg) do
    handle_messages()

    {:ok, []}
  end

  @doc """
  Receives messages from `Kaufmann.Stages.Producer`, uses `Flow`/`GenStage` to proccess messages in parallel in the module specified in `Kaufmann.Config.event_handler/0`
  """
  def handle_messages() do
    Kaufmann.Stages.Producer
    |> Flow.from_stage()
    |> Flow.map(&decode_event/1)
    |> Flow.map(&@event_handler_mod.given_event/1)
    |> Flow.start_link()
  end

  @doc """
  Decodes Avro encoded message value, transforms event into tuple of `{:message_name, %Payload{}}`

  Returns `{key, value}`
  """
  @spec decode_event(map) :: %Kaufmann.Schemas.Event{} | %Kaufmann.Schemas.ErrorEvent{}
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
        %Kaufmann.Schemas.ErrorEvent{
          name: key,
          error: error,
          message_payload: value
        }
    end
  end
end
