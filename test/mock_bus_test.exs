defmodule KaufmannEx.TestSupport.MockBusTest do
  use KaufmannEx.TestSupport.MockBus

  defmodule ExampleEventHandler do
    def given_event(event) do
      case event.payload do
        "no_event" -> :ok
        _ -> KaufmannEx.Publisher.publish(event.name, event.payload)
      end
    end
  end

  setup do
    # SERVICE_NAME and HOST_NAME must be set
    System.put_env("SERVICE_NAME", System.get_env("SERVICE_NAME") || Nanoid.generate())
    System.put_env("HOST_NAME", System.get_env("HOST_NAME") || Nanoid.generate())

    # event_handler_mod must be set
    Application.put_env(:kaufmann_ex, :event_handler_mod, ExampleEventHandler)
    Application.put_env(:kaufmann_ex, :schema_path, "test/support")

    :ok
  end

  describe "when given_event" do
    test "emits an event & payload" do
      given_event(:"test.event.publish", "Hello")

      then_event(:"test.event.publish", "Hello")
    end

    test "will raise execption if no schema" do
      message_name = :NOSCHEMA

      try do
        given_event(message_name, "Some kinda payload")
      rescue
        error in [ExUnit.AssertionError] ->
          "Schema NOSCHEMA not registered" = error.message
      end
    end

    test "validates payload" do
      try do
        given_event(:"test.event.publish", %{invalid_key: "unexpected value"})
      rescue
        error in [ExUnit.AssertionError] ->
          "Payload does not match schema for test.event.publish," =
            String.slice(error.message, 0..52)

          # Slicing b/c error includes random payload + metadata
      end
    end
  end

  describe "then_event" do
    test "/1 returns payload & metadata" do
      given_event(:"test.event.publish", "Hello")

      %{meta: meta, payload: payload} = then_event(:"test.event.publish")
      assert payload == "Hello"
    end

    test "/2 can assert a payload" do
      given_event(:"test.event.publish", "Hello")

      then_event(:"test.event.publish", "Hello")
    end

    test "/2 asserts then_event payload matches argument" do
      given_event(:"test.event.publish", "Hello")

      try do
        then_event(:"test.event.publish", "Bye")
      rescue
        error in [ExUnit.AssertionError] ->
          "Assertion with == failed" = error.message
          "Hello" = error.left
          "Bye" = error.right
      end
    end
  end

  describe "then_no_event" do
    test "/1 validates that event is never emitted" do
      given_event(:"test.event.publish", "no_event")
      then_no_event(:"test.event.publish")

      given_event(:"test.event.publish", "any_event")

      try do
        then_no_event(:"test.event.publish")
      rescue
        error in [ExUnit.AssertionError] ->
          "Unexpected test.event.publish recieved" = error.message
      end
    end

    test "/0 validates no events are ever emitted" do
      given_event(:"test.event.publish", "no_event")
      then_no_event()

      given_event(:"test.event.publish", "any_event")

      try do
        then_no_event()
      rescue
        error in [ExUnit.AssertionError] ->
          "No events expected" = error.message
      end
    end
  end
end
