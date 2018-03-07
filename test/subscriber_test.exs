defmodule KaufmannEx.SubscriberTest do
  use ExUnit.Case
  alias KaufmannEx.TestSupport.MockBus
  alias KafkaEx.Protocol.Fetch.Message

  defmodule TestEventHandler do
    def given_event(
          %KaufmannEx.Schemas.Event{name: :"test.event.publish", payload: "raise_error"} = event
        ) do
      raise ArgumentError, "You know what you did"

      :ok
    end

    def given_event(%KaufmannEx.Schemas.Event{} = event) do
      :erlang.send(:subscriber, :event_recieved)

      :ok
    end

    def given_event(%KaufmannEx.Schemas.ErrorEvent{} = event) do
      :erlang.send(:subscriber, :handled_error)

      :ok
    end
  end

  @topic "topic"
  @partition 0

  setup do
    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    Application.put_env(:kaufmann_ex, :event_handler_mod, TestEventHandler)
    Application.put_env(:kaufmann_ex, :schema_path, "test/support")

    Process.register(self(), :subscriber)

    {:ok, pid} = KaufmannEx.Stages.Producer.start_link([])
    {:ok, s_pid} = KaufmannEx.Subscriber.start_link([])
    {:ok, state} = KaufmannEx.GenConsumer.init(@topic, @partition)

    {:ok, bypass: bypass, state: state}
  end

  describe "when started" do
    test "Consumes Events to EventHandler", %{bypass: bypass, state: state} do
      mock_get_metadata_schema(bypass)
      mock_get_event_schema(bypass, "test.event.publish")

      event = encode_event(:"test.event.publish", "Hello")

      KaufmannEx.GenConsumer.handle_message_set([event], state)

      assert_receive :event_recieved
    end

    test "handles errors gracefully", %{bypass: bypass, state: state} do
      mock_get_metadata_schema(bypass)
      mock_get_event_schema(bypass, "test.event.publish")

      first_event = encode_event(:"test.event.publish", "raise_error")
      second_event = encode_event(:"test.event.publish", "Hello")

      KaufmannEx.GenConsumer.handle_message_set(
        [
          first_event,
          second_event,
          second_event,
          second_event
        ],
        state
      )

      assert_receive :event_recieved
      assert_receive :handled_error
    end
  end

  def mock_get_event_schema(bypass, event_name) do
    {:ok, schema} = File.read("test/support/#{event_name}.avsc")
    schema = schema |> Poison.decode!() |> Poison.encode!()

    Bypass.expect(bypass, "GET", "/subjects/#{event_name}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Poison.encode!(%{subject: event_name, version: 1, id: 1, schema: schema})
      )
    end)
  end

  def mock_get_metadata_schema(bypass) do
    {:ok, schema} = File.read('test/support/event_metadata.avsc')
    schema = schema |> Poison.decode!() |> Poison.encode!()

    Bypass.expect(bypass, "GET", "/subjects/event_metadata/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Poison.encode!(%{subject: "event_metadata", version: 1, id: 1, schema: schema})
      )
    end)
  end

  def encode_event(name, payload) do
    {:ok, encoded_event} = MockBus.encoded_event(name, payload)
    %Message{key: name |> to_string, value: encoded_event}
  end
end
