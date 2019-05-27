defmodule KaufmannEx.TestSupport.MockBus do
  use ExUnit.CaseTemplate

  alias KaufmannEx.EventHandler

  @moduledoc """
    Helper module for testing event handling.

    Cannot be run Async, relies on sending messages to `self()` via a single producer GenStage

    For every `given_event/2` and `then_event/2`.
     * Validates events against schemas registered with Schema registry
     * Injects generic Meta payload into given_event

    `then_event/2` asserts that the given event is emitted and verifies or returned the payload

    If you have a custom metadata schema or specific metadata handling, set a module exporting
    `event_metadata/2` in app_env `:kaufmann_ex, :metadata_mod`

    ### Example Usage

    ```
    defmodule EvenHandlerTests
      use KaufmannEx.TestSupport.MockBus

      test "event emission" do
        given_event(:"TestCommand", %{new_key: "test"})

        then_event(:"Testevent", %{new_key: "test"})

        then_no_event
      end
    end
    ```
  """

  using do
    quote do
      # Using doesn't just import itself
      import KaufmannEx.TestSupport.MockBus
    end
  end

  require Map.Helpers
  import ExUnit.Assertions
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.TestSupport.MockSchemaRegistry

  # handle case of unnamed event
  def given_event(payload) do
    handle_and_send_event(%Event{
      payload: payload
    })
  end

  defp event_consumer do
    Application.fetch_env!(:kaufmann_ex, :event_handler_mod)
  end

  @doc """
  Dispatches event to the default subscriber.

  Schema must be defined & payload must be valid/enocodable
  """
  @spec given_event(atom | binary, any, binary | nil) :: :ok
  def given_event(event_name, payload, callback_id \\ nil) do
    schema_name = schema_name_if_query(event_name)

    assert MockSchemaRegistry.defined_event?(schema_name),
           "Schema #{schema_name} not registered, Is the schema in #{
             KaufmannEx.Config.schema_path()
           }?"

    # Inject fake MetaData into event
    event = %Event{
      name: event_name,
      meta: event_metadata(event_name, %{callback_id: callback_id}),
      payload: payload
    }

    # If message isn't encodable, big problems
    assert_matches_schema(
      event_name,
      payload,
      event_metadata(event_name, %{callback_id: callback_id})
    )

    handle_and_send_event(event)
  end

  defp handle_and_send_event(event) do
    events = EventHandler.handle_event(event, event_consumer())

    for %{
          event_name: event_name,
          payload: payload,
          metadata: meta,
          context: _context,
          topic: topic
        } = ev <- events do
      send(self(), {:produce, {event_name, %{payload: payload, meta: meta}, topic}})
      handle_and_send_event(%Event{name: event_name, payload: payload, meta: meta})
    end

    :ok
  end

  @doc """
  Asserts an event will be emitted to the bus

  Will test emitted payload from event matches payload
  Asserts payload matches argument
  """
  @spec then_event(atom, any, integer) :: map
  def then_event(event_name, expected_payload \\ %{}, timeout \\ 500) do
    assert_receive(
      {:produce, {^event_name, %{payload: message_payload, meta: meta}, topic}},
      timeout
    )

    assert_matches_schema(event_name, message_payload, meta)

    if expected_payload != %{} do
      assert message_payload == expected_payload
    end

    %{payload: message_payload, meta: meta, topic: topic} |> Map.Helpers.atomize_keys()
  end

  @doc """
  Asserts no more events will be emitted
  """
  @spec then_no_event :: boolean
  def then_no_event do
    refute_received({:produce, _, _}, "No events expected")
  end

  @doc """
  Assert an event will not be emitted
  """
  @spec then_no_event(atom) :: boolean
  def then_no_event(message_name) do
    refute_received({:produce, {^message_name, _, _}}, "Unexpected #{message_name} recieved")
  end

  @doc """
  Assert a named event is encodable by its specified schema
  """
  @spec assert_matches_schema(atom, any, map) :: boolean
  def assert_matches_schema(event_name, payload, meta) do
    schema_name = schema_name_if_query(event_name)
    assert MockSchemaRegistry.defined_event?(schema_name), "Schema #{schema_name} not registered"

    encodable_payload = Map.Helpers.stringify_keys(%{payload: payload, meta: meta})

    assert MockSchemaRegistry.encodable?(schema_name, encodable_payload),
           "Payload does not match schema for #{schema_name}, #{inspect(encodable_payload)}"
  end

  def event_metadata(event_name, context \\ %{}) do
    metadata_mod = Application.get_env(:kaufmann_ex, :metadata_mod)

    if module_defined?(metadata_mod, :event_metadata, 2) do
      metadata_mod.event_metadata(event_name, context)
    else
      fake_meta(event_name, context[:callback_id])
    end
  end

  defp module_defined?(module, method, arity) do
    # runtime and compiled evaluation need different methods
    module &&
      (:erlang.function_exported(module, method, arity) ||
         Keyword.has_key?(module.__info__(:functions), method))
  end

  @doc false
  def fake_meta(event_name \\ "TestEvent", callback_id \\ nil) do
    %{
      message_id: Nanoid.generate(),
      emitter_service: Nanoid.generate(),
      emitter_service_id: Nanoid.generate(),
      callback_id: callback_id,
      callback_topic: nil,
      message_name: event_name |> to_string,
      timestamp: DateTime.to_string(DateTime.utc_now())
    }
  end

  # Rename events were we use a generic schema for entire classes of events
  @doc false
  def schema_name_if_query(event_name) when is_atom(event_name),
    do: event_name |> to_string |> schema_name_if_query

  def schema_name_if_query("query." <> _ = event_name), do: String.slice(event_name, 0..8)

  def schema_name_if_query("event.error." <> _ = event_name),
    do: String.slice(event_name, 0..10)

  def schema_name_if_query(event_name), do: event_name

end
