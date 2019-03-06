defmodule Sample.EventHandler do
  use KaufmannEx.EventHandler
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  # alias Sample.Publisher

  def given_event(%Event{name: :"command.test", payload: payload} = event) do
    # Do something with event payload 😁

    message_body = %{
      payload: payload,
      meta: Event.event_metadata(event.name, event.meta)
    }

    [
      %Event{
        event
        | publish_request: %Request{
            event_name: Event.coerce_event_name(event.name),
            body: message_body,
            context: event.meta,
            topic: :default
          }
      }
    ]
  end

  def given_event(%Event{name: :"another.event.here.test", payload: payload} = event) do
    # Do something with event payload 😁

    message_body = %{
      payload: payload,
      meta: Event.event_metadata(event.name, event)
    }

    [
      %Event{
        event
        | publish_request: %Request{
            event_name: Event.coerce_event_name(event.name),
            body: message_body,
            context: event.meta,
            topic: :default
          }
      }
    ]
  end

  # Handle unexpected Event
  def given_event(event) do
    IO.inspect(event.name, label: "unhandled_event")

    []
  end
end
