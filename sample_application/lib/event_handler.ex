defmodule Sample.EventHandler do
  alias KaufmannEx.Schemas.Event
  alias Sample.Publisher

  def given_event(%Event{name: :"command.test", payload: payload} = event) do
    # Do something with event payload 😁
    Publisher.publish(Publisher.coerce_event_name(event.name), payload, event.meta)
  end

  # Handle unexpected Event
  def given_event(event), do: IO.inspect(event.name)
end
