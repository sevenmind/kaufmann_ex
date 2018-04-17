defmodule Sample.EventHandler do
  alias KaufmannEx.Schemas.Event

  def given_event(%Event{name: :"command.test", payload: payload} = event) do
    publish(cmd_to_event(event.name), payload, event.meta)
  end

  def given_event(event), do: IO.inspect(event.name)

  def publish(event_name, payload, context) do
    message_body = %{
      payload: payload,
      meta: event_metadata(event_name, context)
    }

    KaufmannEx.Publisher.publish(event_name, message_body, context)
  end

  @spec event_metadata(atom, map) :: map
  def event_metadata(event_name, context) do
    %{
      message_id: Nanoid.generate(),
      emitter_service: KaufmannEx.Config.service_name(),
      emitter_service_id: KaufmannEx.Config.service_id(),
      callback_id: context[:callback_id],
      message_name: event_name |> to_string,
      timestamp: DateTime.to_string(DateTime.utc_now()),
      callback_topic: Map.get(context, :next_callback_topic, nil)
    }
  end

  @doc """
  Replace "command." with "event." in event names
  """
  @spec cmd_to_event(atom) :: atom
  def cmd_to_event(command_name) do
    command_name
    |> to_string
    |> String.replace_prefix("command.", "event.")
    |> String.to_atom()
  end
end
