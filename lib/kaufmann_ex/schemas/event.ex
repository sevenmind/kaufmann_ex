defmodule KaufmannEx.Schemas.Event do
  alias KaufmannEx.Config

  @type t :: %KaufmannEx.Schemas.Event{
          name: atom | binary,
          meta: map | nil,
          payload: term
        }
  @moduledoc false
  defstruct [
    :name,
    :meta,
    :payload,
    :raw_event,
    :timestamps,
    :context,
    :topic,
    :partition,
    :consumer_group
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
      callback_topic: Map.get(context, :next_callback_topic, nil)
    }
  end

  def decode_event(%__MODULE__{raw_event: %{key: _, value: _}} = event) do
    # when in doubt try all the transcoders
    Enum.map(Config.transcoders(), & &1.decode_event(event))
    |> Enum.find(event, fn
      %__MODULE__{} = _event -> true
      _ -> false
    end)
  end
end

defmodule KaufmannEx.Schemas.ErrorEvent do
  @type t :: %KaufmannEx.Schemas.ErrorEvent{
          name: atom | binary,
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
    :partition,
    :consumer_group
  ]

  @doc """
  Append "error.event." to an event name
  """
  @spec coerce_event_name(atom) :: atom
  def coerce_event_name(command_name) do
    "event.error.#{command_name}"
  end
end
