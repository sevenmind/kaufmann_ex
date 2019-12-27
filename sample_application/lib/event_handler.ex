defmodule Sample.EventHandler do
  use KaufmannEx.EventHandler
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event

  require Logger

  def given_event(%Event{name: "command.test", payload: payload} = event) do
    # Do something with event payload 😁

    {:reply, [{"event.test", payload}]}
  end

  def given_event(%Event{name: "event.test", payload: payload} = event) do
    # Do something with event payload 😁

    {:reply,
     [
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload},
       {"another.event.here.test", payload}
     ]}
  end

  def given_event(%Event{name: "another.event.here.test", payload: payload} = event) do
    # Do something with event payload 😁

    {:reply,
     [
       {"another.event.here", payload}
     ]}
  end

  # Handle unexpected Event
  def given_event(event) do
    Logger.debug("Unhandleded event: #{event.name}")

    {:noreply, []}
  end
end
