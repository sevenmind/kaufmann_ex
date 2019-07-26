defmodule KaufmannEx.EventHandler do
  @moduledoc """
  Behavior and helpers for defining an EventHandler stage.

  ```elixir

  defmodule MyEventHandler do
    use KaufmannEx.EventHandler
    alias KaufmannEx.Schemas.Event

    @behaviour KaufmannEx.EventHandler

    @impl true
    def given_event(%Event{name: "test.commnad", meta: meta} = event) do
      message_body = do_some_work()

      {:reply, [{"test.event", message_body, topic}]}
    end
  end
  ```

  ## Defining accepted events

  At compile time the `KaufmannEx.EventHandler` will evaluate the arguments of
  all `given_event/1` functions and generate a list of accepted events. This
  list of events is used to discard unhandled events in
  `KaufmannEx.Consumer.Flow`. This is an optimization that's quite useful if you
  have a topic with many types of events you would prefer not to spend time
  decoding.

  Alternatively handled events can be defiened by overriding `handled_events/0`

  ```elixir
  def handled_events do
    ["this.other.event", "a.genericly.handled event"]
  end
  ```

  Or all events can be handled by returning `[:all]` from `handled_events/0`.
  This will ensure no events are discarded.

  ```elixir
  def handled_events, do: [:all]
  ```

  ## Error handling

  If you wish to have events an emit an error event (like to signal to a RPC
  subscriber or a query response) its as simple as returning an `{:error, error}`
  tuple from your event handler.

  ```
   def given_event(%Event{name: "somthing.bad.happends", meta: meta} = event) do
      message_body = do_some_work()

      {:reply, [{"test.event", message_body, topic}]}
    rescue
      error ->
        {:error, error}
    end
  ```

  """
  alias KaufmannEx.Publisher.Request
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
      _ -> Module.put_attribute(env.module, :handled_events, :all)
    end
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: nil

  defmacro __before_compile__(env) do
    handled_events =
      env.module
      |> Module.get_attribute(:handled_events)
      |> Enum.map(&to_string/1)

    quote do
      def handled_events do
        unquote(handled_events)
      end

      def given_event(event), do: {:unhandled, []}

      defoverridable handled_events: 0,
                     given_event: 1
    end
  end

  @doc "Event handler callback, accepts an Event, returns an Event with a Publish_request key or nothing"
  @callback given_event(Event.t()) ::
              {:reply | :noreply, [Request.t()]} | {:error, any}

  @doc "lists handled events, used for filtering unhandled events in consumption"
  @callback handled_events :: [binary() | :all]

  defp extract_event_name({:name, name}) when is_atom(name) or is_binary(name), do: [name]
  defp extract_event_name({:name, name}), do: dig_for_binary(name)
  defp extract_event_name(args) when is_list(args), do: Enum.flat_map(args, &extract_event_name/1)
  defp extract_event_name({_k, _o, t}), do: extract_event_name(t)
  defp extract_event_name({_k, t}), do: extract_event_name(t)
  defp extract_event_name(_), do: []

  defp dig_for_binary(tuple) when is_tuple(tuple),
    do:
      tuple
      |> Tuple.to_list()
      |> dig_for_binary()

  defp dig_for_binary(list) when is_list(list), do: Enum.flat_map(list, &dig_for_binary/1)
  defp dig_for_binary(bin) when is_binary(bin), do: [bin]
  defp dig_for_binary(_), do: []

  @spec handle_event(Event.t(), atom) :: [any]
  def handle_event(event, event_handler) do
    start_time = System.monotonic_time()

    results = handle_event_and_response(event, event_handler)

    report_telemetry(start_time: start_time, event: event, event_handler: event_handler)

    Enum.flat_map(results, &format_event(event, &1))
  end

  defp handle_event_and_response(event, event_handler) do
    event_name = event.name

    case event_handler.given_event(event) do
      {:reply, events} when is_list(events) ->
        events

      {:reply, event} when is_tuple(event) or is_map(event) ->
        [event]

      {:unhandled, _} when is_binary(event_name) ->
        # try casting the event name to atom
        # In case someone followed the legacy pattern
        event_name =
          event.name
          |> String.split("#")
          |> Enum.at(0)
          |> String.to_atom()

        handle_event_and_response(%Event{event | name: event_name}, event_handler)

      {:error, error} ->
        wrap_error_event(event, error)

      _ ->
        []
    end
  end

  defp report_telemetry(start_time: start_time, event: event, event_handler: event_handler) do
    event_name =
      (Map.get(event, :name) || "empty")
      |> to_string
      |> String.split("#")
      |> Enum.at(0)
      |> String.split(":")
      |> Enum.at(0)

    :telemetry.execute(
      [:kaufmann_ex, :event_handler, :handle_event],
      %{
        duration: System.monotonic_time() - start_time
      },
      %{
        event: event_name,
        topic: event.topic,
        partition: event.partition,
        handler: event_handler
      }
    )
  end

  def wrap_error_event(event, error) do
    Logger.warn("Error: #{inspect(error)}")

    [
      {"event.error.#{event.name}",
       %{
         error: error
       }, [:default, :callback]}
    ]
  end

  defp format_event(original_event, {event_name, payload}),
    do: format_event(original_event, %{event: event_name, payload: payload})

  defp format_event(original_event, {event_name, payload, topic})
       when is_map(topic) or is_binary(topic) or is_atom(topic),
       do: format_event(original_event, %{event: event_name, payload: payload, topics: [topic]})

  defp format_event(original_event, {event_name, payload, topics}) when is_list(topics),
    do: format_event(original_event, %{event: event_name, payload: payload, topics: topics})

  defp format_event(original_event, %{event: event_name, payload: payload} = result_event) do
    result_event
    |> Map.get(:topics, [:default])
    |> Enum.map(fn
      topic when is_binary(topic) or is_atom(topic) ->
        %Request{
          event_name: event_name,
          metadata: Event.event_metadata(event_name, original_event.meta),
          payload: payload,
          context: original_event.meta,
          topic: topic
        }

      topic when is_map(topic) ->
        %Request{
          event_name: event_name,
          metadata: Event.event_metadata(event_name, original_event.meta),
          payload: payload,
          context: original_event.meta
        }
        |> Map.merge(topic)
    end)
  end
end
