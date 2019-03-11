defmodule Sample.EventHandler do
  use KaufmannEx.EventHandler
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  # alias Sample.Publisher

  def given_event(%Event{name: :"command.test", payload: payload} = event) do
    # Do something with event payload 😁

    {:reply, [{Event.coerce_event_name(event.name), payload}]}
  end

  def given_event(%Event{name: :"another.event.here.test", payload: payload} = event) do
    # Do something with event payload 😁

    {:reply, [{Event.coerce_event_name(event.name), payload}]}
  end

  # Handle unexpected Event
  def given_event(event) do
    IO.inspect(event.name, label: "unhandled_event")

    {:noreply, []}
  end
end
