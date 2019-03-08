defmodule KaufmannEx.EventHandler do
  alias KaufmannEx.Schemas.Event

  defmacro __using__(_mod) do
    quote do
      # import KaufmannEx.EventHandler
      Module.register_attribute(__MODULE__, :handled_events, accumulate: true)
      @before_compile KaufmannEx.EventHandler
      @on_definition KaufmannEx.EventHandler
    end
  end

  def __on_definition__(env, _kind, name, args, guards, body) do
    case extract_event_name(args) do
      [event_name] -> Module.put_attribute(env.module, :handled_events, event_name)
      _ -> nil
    end
  end

  defmacro __before_compile__(env) do
    handled_events =
      Module.get_attribute(env.module, :handled_events)
      |> Enum.map(&to_string/1)

    quote do
      @doc "lists handled events, used for filtering unhandled events in consumption"
      @callback handled_events :: [atom()]
      def handled_events do
        unquote(handled_events)
      end

      @callback given_event(Event.t()) :: any
      def given_event(event), do: nil

      defoverridable handled_events: 0,
                     given_event: 1
    end
  end

  defp extract_event_name({:name, name}), do: [name]
  defp extract_event_name(args) when is_list(args), do: Enum.flat_map(args, &extract_event_name/1)
  defp extract_event_name({_k, _o, t}), do: extract_event_name(t)
  defp extract_event_name({_k, t}), do: extract_event_name(t)
  defp extract_event_name(_), do: []
end
