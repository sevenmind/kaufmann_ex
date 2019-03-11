defmodule KaufmannEx.EventHandler do
  @moduledoc """
  Behavior and helpers for defining an EventHandler stage.

  ```elixir

  defmodule MyEventHandler do
    use KaufmannEx.EventHandler
    alias KaufmannEx.Schemas.Event

    @behaviour KaufmannEx.EventHandler


    @impl true
    def given_event(%Event{name: :"test.commnad", meta: meta} = event) do
      # :erlang.send(:subscriber, :event_recieved)
      message_body = do_some_work()

      {:reply, [%Request{
            event_name: "test.event",
            body: message_body,
            context: meta,
            topic: :default
          }]}
    end

  end
  ```
  """
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.ErrorEvent
  alias KaufmannEx.Schemas.Event

  require Logger

  defmacro __using__(_mod) do
    quote do
      # import KaufmannEx.EventHandler
      Module.register_attribute(__MODULE__, :handled_events, accumulate: true)
      @before_compile KaufmannEx.EventHandler
      @on_definition KaufmannEx.EventHandler
      @behaviour KaufmannEx.EventHandler
    end
  end

  def __on_definition__(env, _kind, :given_event, args, _guards, _body) do
    case extract_event_name(args) do
      [event_name] -> Module.put_attribute(env.module, :handled_events, event_name)
      _ -> nil
    end
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: nil

  defmacro __before_compile__(env) do
    handled_events =
      Module.get_attribute(env.module, :handled_events)
      |> Enum.map(&to_string/1)

    quote do
      def handled_events do
        unquote(handled_events)
      end

      def given_event(event), do: {:noreply, []}

      defoverridable handled_events: 0,
                     given_event: 1
    end
  end

  @doc "Event handler callback, accepts an Event, returns an Event with a Publish_request key or nothing"
  @callback given_event(Event.t()) ::
              {:reply | :noreply, [Request.t()]}

  @doc "lists handled events, used for filtering unhandled events in consumption"
  @callback handled_events :: [binary()]

  defp extract_event_name({:name, name}) when is_atom(name) or is_binary(name), do: [name]
  defp extract_event_name(args) when is_list(args), do: Enum.flat_map(args, &extract_event_name/1)
  defp extract_event_name({_k, _o, t}), do: extract_event_name(t)
  defp extract_event_name({_k, t}), do: extract_event_name(t)
  defp extract_event_name(_), do: []

  def handle_event(event, event_handler) do
    case event_handler.given_event(event) do
      {:noreply, _} -> []
      {:reply, events} when is_list(events) -> events
      {:reply, event} when is_tuple(event) or is_map(event) -> [event]
      _ -> []
    end
    |> Enum.map(&format_event(event, &1))
  rescue
    error ->
      Logger.warn(fn -> "Error handling #{event.name} #{inspect(error)}" end)

      [
        wrap_event(
          :"event.error.#{event.name}",
          %{
            error: inspect(error),
            message_payload: event.payload
          },
          event
        )
      ]
  end

  defp format_event(event, {event_name, payload}), do: wrap_event(event_name, payload, event)

  defp format_event(event, {event_name, payload, topic}),
    do: wrap_event(event_name, payload, event, topic)

  defp format_event(event, %{event_name: event_name, payload: payload, topic: topic}),
    do: wrap_event(event_name, payload, event, topic)

  defp wrap_event(event_name, body, event, topic \\ :default) do
    %Event{
      event
      | publish_request: %Request{
          event_name: event_name,
          body: %{
            payload: body,
            meta: Event.event_metadata(event_name, event.meta)
          },
          context: event.meta,
          topic: topic
        }
    }
  end
end
