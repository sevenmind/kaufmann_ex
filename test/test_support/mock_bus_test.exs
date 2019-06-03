defmodule KaufmannEx.TestSupport.MockBusTest.ExamplePublisher do
  alias KaufmannEx.TestSupport.MockBus

  @moduledoc false
  def publish(event_name, payload, context \\ %{}) do
    message_body = %{
      payload: payload,
      meta: event_metadata(event_name, context)
    }

    KaufmannEx.Publisher.publish(event_name, message_body, context)
  end

  def event_metadata(event_name, context), do: MockBus.fake_meta(event_name, nil)
end

defmodule KaufmannEx.TestSupport.MockBusTest do
  @moduledoc false
  use KaufmannEx.TestSupport.MockBus

  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.TestSupport.MockBusTest.ExamplePublisher
  alias KaufmannEx.TestSupport.MockSchemaRegistry

  defmodule ExampleEventHandler do
    @moduledoc false
    use KaufmannEx.EventHandler

    def given_event(%Event{payload: "no_event"} = event) do
      {:noreply, []}
    end

    def given_event(%Event{name: "event_a", payload: payload}) do
      {:reply, {"event_b", payload}}
    end

    def given_event(%Event{name: "event_b", payload: payload}) do
      {:reply, {"event_c", payload}}
    end

    def given_event(%Event{name: "test.event.publish", payload: "raise_error"} = event) do
      raise ArgumentError, "You know what you did"
    rescue
      error ->
        {:error, error.message}
    end

    def given_event(%Event{name: "test.event.publish", payload: pl} = event) do
      {:reply, {"test.event.another", pl}}
    end

    def given_event(error_event), do: []
  end

  setup do
    # event_handler_mod must be set
    Application.put_env(:kaufmann_ex, :event_handler_mod, ExampleEventHandler)
    Application.put_env(:kaufmann_ex, :metadata_mod, ExamplePublisher)
    Application.put_env(:kaufmann_ex, :schema_path, "test/support")

    :ok
  end

  describe "when given_event" do
    test "emits an event & payload" do
      given_event("test.event.publish", "Hello")

      then_event("test.event.another", "Hello")
    end

    test "will raise execption if no schema" do
      message_name = :NOSCHEMA

      assert_raise ExUnit.AssertionError,
                   "\n\nSchema NOSCHEMA not registered, Is the schema in test/support?\n",
                   fn ->
                     given_event(message_name, "Some kinda payload")
                   end
    end

    test "validates avro payload" do
      assert_raise ExUnit.AssertionError, fn ->
        given_event("test.event.publish", %{invalid_key: "unexpected value"})
      end
    end

    test "validates json payload" do
      assert_raise ExUnit.AssertionError, fn ->
        given_event("event_a", %{invalid_key: "unexpected value"})
      end
    end

    test "sends with :default topic of no topic passed" do
      given_event("test.event.publish", "Hello")
      then_event("test.event.another", "Hello")
    end

    test "when events trigger more events" do
      given_event("event_a", %{value: "Hello"})
      then_event("event_b", %{value: "Hello"})
      then_event("event_c", %{value: "Hello"})
    end
  end

  describe "then_event" do
    test "/1 returns payload & metadata" do
      given_event("test.event.publish", "Hello")

      assert %{meta: meta, payload: "Hello"} = then_event("test.event.another")
    end

    test "/2 can assert a payload" do
      given_event("test.event.publish", "Hello")

      then_event("test.event.another", "Hello")
    end

    test "/2 asserts then_event payload matches argument" do
      given_event("test.event.publish", "Hello")

      try do
        then_event("test.event.another", "Bye")
      rescue
        error in [ExUnit.AssertionError] ->
          "Assertion with == failed" = error.message
          "Hello" = error.left
          "Bye" = error.right
      end
    end

    test "/1 returns default topic" do
      given_event("test.event.publish", "Test")
      assert %{topic: :default} = then_event("test.event.another")
    end
  end

  describe "then_no_event" do
    test "/1 validates that event is never emitted" do
      given_event("test.event.publish", "no_event")
      then_no_event("test.event.another")

      given_event("test.event.publish", "any_event")

      try do
        then_no_event("test.event.another")
      rescue
        error in [ExUnit.AssertionError] ->
          "Unexpected test.event.another recieved" = error.message
      end
    end

    test "/0 validates no events are ever emitted" do
      given_event("test.event.publish", "no_event")
      then_no_event()

      given_event("test.event.publish", "any_event")

      try do
        then_no_event()
      rescue
        error in [ExUnit.AssertionError] ->
          "No events expected" = error.message
      end
    end
  end

  describe "if an event raises an error" do
    test "returns event.error" do
      given_event("test.event.publish", "raise_error")

      then_event("event.error.test.event.publish")
    end
  end
end
