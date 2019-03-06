defmodule Sample.Publisher do
  alias KaufmannEx.Publisher.Request

  def publish(event_name, payload, context \\ %{}, topic \\ :default) do
    message_body = %{
      payload: payload,
      meta: event_metadata(event_name, context)
    }

    # KaufmannEx.Publisher.publish(event_name, message_body, context)

    [%Request{
      event_name: event_name,
      body: message_body,
      context: context,
      topic: topic
    }]
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
  @spec coerce_event_name(atom) :: atom
  def coerce_event_name(command_name) do
    command_name
    |> to_string
    |> String.replace_prefix("command.", "event.")
    |> String.to_atom()
  end
end
