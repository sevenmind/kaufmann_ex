defmodule Kaufmann.TestSupport.MockBus do
  use ExUnit.CaseTemplate

  @moduledoc """
    Helper module for testing event flows. 

    Currently only works with one given_event and one then_event.
     * Validates events against schemas registered with Schema registry
     * Injects generic Meta payload into event

    ```
    use Kaufmann.TestSupport.MockBus

    test "event emission" do
      given_event(:"TestCommand", %{new_key: "test"})
      then_event(:"Testevent", %{new_key: "test"})
    end
    ```
  """
  # TODO rewrite into DSL

  setup do
    default_producer_mod = bus_setup()

    on_exit(fn -> teardown(default_producer_mod) end)

    :ok
  end

  using do
    quote do
      import Kaufmann.TestSupport.MockBus
    end
  end

  require Map.Helpers
  # require ExUnit.Assertions
  import ExUnit.Assertions
  alias Kaufmann.Schemas.Event
  alias Kaufmann.TestSupport.MockSchemaRegistry

  def bus_setup do
    default_producer_mod = Application.get_env(:kaufmann, :producer_mod)
    Application.put_env(:kaufmann, :producer_mod, Kaufmann.TestSupport.MockBus)
    Process.register(self(), :producer)
    # Register self so it can recieve calls?
    default_producer_mod
  end

  def teardown(producer_mod) do
    Application.put_env(:kaufmann, :producer_mod, producer_mod)
  end

  @doc """
  Dispatches event to the default subscriber
  """
  def given_event(event_name, payload, callback_id \\ nil) do
    schema_name = schema_name_if_query(event_name)
    assert MockSchemaRegistry.defined_event?(schema_name), "Schema #{schema_name} not registered"

    # Inject fake MetaData into event
    event = %Event{
      name: event_name,
      meta: fake_meta(event_name, callback_id),
      payload: payload
    }

    encodable_payload =
      event |> Map.from_struct() |> Map.drop([:name]) |> Map.Helpers.stringify_keys()

    # If message isn't encodable, big problems
    assert MockSchemaRegistry.encodable?(schema_name, encodable_payload),
           "Payload does not match schema for #{schema_name}, #{inspect(encodable_payload)}"

    event_consumer = Application.fetch_env!(:kaufmann, :event_handler_mod)
    event_consumer.given_event(event)
  end

  @doc """
  Asserts an event will be emitted to the bus

  Assumes the testHelper is overriding the default publisher, and will receive all of its calls. 
  """
  def then_event(event_name, payload) do
    assert_received(
      {:produce, {message_name, %{payload: message_payload, meta: meta}}},
      "#{event_name} was not triggered"
    )

    verify_event_confrorms_to_schema(event_name, message_payload, meta)

    assert {message_name, message_payload} == {event_name, payload}
  end

  def then_event(event_name) do
    assert_received(
      {:produce, {message_name, %{payload: message_payload, meta: meta}}},
      "#{event_name} was not triggered"
    )

    assert message_name == event_name

    verify_event_confrorms_to_schema(event_name, message_payload, meta)

    %{payload: message_payload, meta: meta} |> Map.Helpers.atomize_keys()
  end

  def verify_event_confrorms_to_schema(event_name, payload, meta) do
    schema_name = schema_name_if_query(event_name)
    assert MockSchemaRegistry.defined_event?(schema_name), "Schema #{schema_name} not registered"

    encodable_payload = Map.Helpers.stringify_keys(%{payload: payload, meta: meta})

    assert MockSchemaRegistry.encodable?(schema_name, encodable_payload),
           "Payload does not match schema for #{schema_name}, #{inspect(encodable_payload)}"
  end

  def fake_meta(event_name \\ "TestEvent", callback_id \\ nil) do
    %{
      message_id: Nanoid.generate(),
      emitter_service: Nanoid.generate(),
      emitter_service_id: Nanoid.generate(),
      callback_id: callback_id,
      message_name: event_name |> to_string
    }
  end

  def produce(event_name, payload) do
    send(:producer, {:produce, {event_name, payload}})
  end

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
end
