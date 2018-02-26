defmodule Kaufmann.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic.

  """
  require Logger
  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request

  @topic Kaufmann.Config.default_topic()

  def produce(message_name, payload) when is_atom(message_name) do
    produce(Atom.to_string(message_name), payload)
  end

  @spec produce(String.t(), term()) :: :ok | {:error, any}
  def produce(message_name, payload) when is_binary(message_name) do
    produce(@topic, message_name, payload)
  end

  @spec produce(String.t(), String.t(), term()) :: :ok | {:error, any}
  def produce(topic, message_name, data) do
    with {:ok, payload} <- Kaufmann.Schemas.encode_message(message_name, data) do
      Logger.debug(fn -> "Publishing Event #{message_name} on #{topic}" end)
      message = %Message{value: payload, key: message_name}

      produce_request = %Request{
        partition: 0,
        topic: topic,
        messages: [message]
      }

      KafkaEx.produce(produce_request)
    else
      {:error, error} -> {:error, error}
    end
  end

  def cmd_to_event(command_name) do
    command_name
    |> to_string
    |> String.replace_prefix("command.", "event.")
    |> String.to_atom()
  end

  @doc """
    Publishes error for a given event
  """
  def publish_error(event_name, error, _orig_payload, meta \\ %{}) do
    error_payload = %{
      error: error
    }

    publish(:"event.error.#{event_name}", error_payload, meta)
  end

  def publish_error(%Kaufmann.Schemas.Event{} = event, error) do
    error_payload = %{
      error: error
    }

    publish(:"event.error.#{event.name}", error_payload, event.meta)
  end

  @spec publish(atom, map, map) :: :ok
  def publish(event_name, payload, context \\ %{}) do
    message_body = %{
      payload: payload,
      meta: event_metadata(event_name, context)
    }

    producer = Application.fetch_env!(:kaufmann, :producer_mod)
    producer.produce(event_name, message_body)
  end

  @spec event_metadata(atom, map) :: map
  def event_metadata(event_name, context) do
    # TODO pull real emitter_service and service_id from ENV
    %{
      message_id: Nanoid.generate(),
      emitter_service: Kaufmann.Config.service_name(),
      emitter_service_id: Kaufmann.Config.service_id(),
      callback_id: context[:callback_id],
      message_name: event_name |> to_string,
      timestamp: DateTime.to_string(DateTime.utc_now())
    }
  end
end
