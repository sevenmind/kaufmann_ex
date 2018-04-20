defmodule KaufmannEx.TestSupport.MockBus do
  use ExUnit.CaseTemplate

  @moduledoc """
    Helper module for testing event flows.

    Cannot be run Async, relies on sending messages to `self()`

    For every `given_event/2` and `then_event/2`.
     * Validates events against schemas registered with Schema registry
     * Injects generic Meta payload into given_event

    `then_event/2` asserts that the given event is emitted and verifies or returned the payload

    If you have a custom metadata schema or specific metadata handling, set a module exporting `event_metadata/2` in app_env `:kaufmann_ex, :metadata_mod`

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

  setup do
    default_producer_mod = bus_setup()

    on_exit(fn -> teardown(default_producer_mod) end)

    :ok
  end

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

  # Setup Helper
  @doc false
  def bus_setup do
    default_producer_mod = Application.get_env(:kaufmann_ex, :producer_mod)
    Application.put_env(:kaufmann_ex, :producer_mod, KaufmannEx.TestSupport.MockBus)
    Process.register(self(), :producer)

    default_producer_mod
  end

  # Setup Helper
  @doc false
  def teardown(producer_mod) do
    Application.put_env(:kaufmann_ex, :producer_mod, producer_mod)
  end

  @doc """
  Dispatches event to the default subscriber.

  Schema must be defined & payload must be valid/enocodable
  """
  @spec given_event(atom, any, binary | nil) :: :ok
  def given_event(event_name, payload, callback_id \\ nil) do
    schema_name = schema_name_if_query(event_name)
    assert MockSchemaRegistry.defined_event?(schema_name), "Schema #{schema_name} not registered"

    # Inject fake MetaData into event
    event = %Event{
      name: event_name,
      meta: event_metadata(event_name, %{callback_id: callback_id}),
      payload: payload
    }

    encodable_payload =
      event |> Map.from_struct() |> Map.drop([:name]) |> Map.Helpers.stringify_keys()

    # If message isn't encodable, big problems
    assert MockSchemaRegistry.encodable?(schema_name, encodable_payload),
           "Payload does not match schema for #{schema_name}, #{inspect(encodable_payload)}"

    event_consumer = Application.fetch_env!(:kaufmann_ex, :event_handler_mod)
    event_consumer.given_event(event)
  end

  @doc """
  Asserts an event will be emitted to the bus

  Will test emitted payload from event matches payload
  Asserts payload matches argument
  """
  @spec then_event(atom, any) :: boolean
  def then_event(event_name, expected_payload) do
    assert_received(
      {:produce, {^event_name, %{payload: message_payload, meta: meta}}},
      "#{event_name} was not triggered"
    )

    assert_matches_schema(event_name, message_payload, meta)

    assert message_payload == expected_payload
  end

  @doc """
  Asserts an event has been emitted, returns the payload

  Returned payload will include `meta` metadata
  """
  @spec then_event(atom) :: %{meta: map, payload: any}
  def then_event(event_name) do
    assert_received(
      {:produce, {^event_name, %{payload: message_payload, meta: meta}}},
      "#{event_name} was not triggered"
    )

    assert_matches_schema(event_name, message_payload, meta)

    %{payload: message_payload, meta: meta} |> Map.Helpers.atomize_keys()
  end

  @doc """
  Asserts no more events will be emitted
  """
  @spec then_no_event :: boolean
  def then_no_event do
    refute_received({:produce, _}, "No events expected")
  end

  @doc """
  Assert an event will not be emitted
  """
  @spec then_no_event(atom) :: boolean
  def then_no_event(message_name) do
    refute_received({:produce, {^message_name, _}}, "Unexpected #{message_name} recieved")
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

  defp event_metadata(event_name, context) do
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
      message_name: event_name |> to_string,
      timestamp: DateTime.to_string(DateTime.utc_now())
    }
  end

  def produce(_topic, event_name, payload, _context), do: produce(event_name, payload)

  # Internal Produce call, sends to self for assertion
  @doc false
  def produce(event_name, payload) do
    send(:producer, {:produce, {event_name, payload}})

    :ok
  end

  # Rename events were we use a generic schema for entire classes of events
  @doc false
  def schema_name_if_query(event_name) do
    event_string = event_name |> to_string

    cond do
      Regex.match?(~r/^query\./, event_string) ->
        String.slice(event_string, 0..8)

      Regex.match?(~r/^event\.error\./, event_string) ->
        String.slice(event_string, 0..10)

      true ->
        event_string
    end
  end

  def encoded_event(event_name, payload, callback_id \\ nil) do
    %Event{
      meta: event_metadata(event_name, callback_id: callback_id),
      payload: payload
    }
    |> Map.from_struct()
    |> Map.drop([:name])
    |> Map.Helpers.stringify_keys()
    |> encode_payload(event_name)
  end

  defp encode_payload(payload, event_name) do
    schema_name = schema_name_if_query(event_name)
    MockSchemaRegistry.encode_event(schema_name, payload)
  end
end
