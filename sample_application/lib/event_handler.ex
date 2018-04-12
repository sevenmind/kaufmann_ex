defmodule Sample.EventHandler do
  import KaufmannEx.Publisher
  alias KaufmannEx.Schemas.Event

  def given_event(%Event{name: :"command.test", payload: payload} = event) do
    publish(cmd_to_event(event.name), payload, event.meta)
  end

  def given_event(event), do: IO.inspect(event.name)


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
