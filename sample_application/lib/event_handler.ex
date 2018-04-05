defmodule Sample.EventHandler do
  import KaufmannEx.Publisher
  alias KaufmannEx.Schemas.Event

  def given_event(%Event{name: :"command.test", payload: payload} = event) do
    publish(cmd_to_event(event.name), payload, event.meta)
  end

  def given_event(event), do: IO.inspect(event.name)
end
