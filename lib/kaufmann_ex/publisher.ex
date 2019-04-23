defmodule KaufmannEx.Publisher do
  @moduledoc """
    Publishes Avro encoded messages to the default topic (`KaufmannEx.Config.default_topic/0`).
  """
  require Logger

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Publisher.Stage.{Encoder, Publisher, TopicSelector}

  defmodule Request do
    @moduledoc """
    A Struct wrapping a publish request
    """
    defstruct [:event_name, :body, :context, :topic, :partition, :encoded]

    @type t :: %__MODULE__{
            # ?
            event_name: atom | binary,
            body: Map,
            context: Map,
            topic: binary | nil,
            partition: non_neg_integer | nil,
            encoded: binary | nil
          }
  end

  @doc """
  Execute Encode & publish inline, for when you just need to send something to
  kafka right now.
  """
  def publish(event_name, body, context \\ %{}, topic \\ :default) do
    message_body =
      case is_map(body) and Map.has_key?(body, :meta) do
        true ->
          body

        _ ->
          %{
            payload: body,
            meta: Event.event_metadata(event_name, context)
          }
      end

    %Event{
      publish_request: %Request{
        event_name: event_name,
        body: message_body,
        context: context,
        topic: topic
      }
    }
    |> Encoder.encode_event()
    |> TopicSelector.select_topic_and_partition()
    |> Enum.map(&Publisher.publish/1)
  end
end
