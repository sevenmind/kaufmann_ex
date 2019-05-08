defmodule KaufmannEx.Schemas.Event do
  @type t :: %KaufmannEx.Schemas.Event{
          name: atom,
          meta: map,
          payload: term,
          publish_request: KaufmannEx.Publisher.Request.t()
        }
  @moduledoc false
  defstruct [
    :name,
    :meta,
    :payload,
    :raw_event,
    :timestamps,
    :publish_request,
    :context,
    :topic,
    :partition
  ]

  @spec event_metadata(atom, map) :: map
  def event_metadata(event_name, context \\ %{}) do
    %{
      message_id: Nanoid.generate(),
      emitter_service: KaufmannEx.Config.service_name(),
      emitter_service_id: KaufmannEx.Config.service_id(),
      callback_id: context[:callback_id],
      message_name: event_name |> to_string,
      timestamp: DateTime.to_string(DateTime.utc_now()),
      callback_topic: Map.get(context, :next_callback_topic, nil),
      span_context: current_context()
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

  defp current_context do
    :ocp.current_span_ctx()
    |> :oc_propagation_binary.encode()
    |> :binary.bin_to_list()
  end
end

defmodule KaufmannEx.Schemas.ErrorEvent do
  @type t :: %KaufmannEx.Schemas.ErrorEvent{
          name: atom,
          error: term,
          message_payload: term,
          meta: term | nil
        }

  @moduledoc false
  defstruct [
    :name,
    :error,
    :message_payload,
    :meta,
    :raw_event,
    :timestamps,
    :publish_request,
    :context,
    :topic,
    :partition
  ]

  @doc """
  Append "error.event." to an event name
  """
  @spec coerce_event_name(atom) :: atom
  def coerce_event_name(command_name) do
    :"event.error.#{command_name}"
  end
end
