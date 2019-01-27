defmodule KaufmannEx.Consumer.Stage.EventHandlerTest do
  use ExUnit.Case
  alias KafkaEx.Protocol.Fetch.Message
  alias KaufmannEx.Consumer.Stage.{Producer, Decoder, Consumer}
  alias KaufmannEx.Consumer.GenConsumer
  alias KaufmannEx.TestSupport.MockBus

  @topic :rapids
  @partition 0

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

  setup do
    {:ok, memo_pid} = Application.ensure_all_started(:memoize)
    on_exit(fn -> Memoize.invalidate() end)

    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    # Mock calls to schema registry, only expected once
    # mock_get_metadata_schema(bypass)
    mock_get_event_schema(bypass, "test.event.publish")

    Application.put_env(:kaufmann_ex, :event_handler_mod, TestEventHandler)
    Application.put_env(:kaufmann_ex, :schema_path, "test/support")

    Process.register(self(), :subscriber)

    {:ok, _} = start_supervised({Registry, keys: :unique, name: Registry.ConsumerRegistry})

    assert {:ok, pid} = start_supervised({Producer, {@topic, @partition}})
    assert {:ok, _pid} = start_supervised({Decoder, {@topic, @partition}})
    assert {:ok, s_pid} = start_supervised({Consumer, {@topic, @partition}})

    {:ok, bypass: bypass, state: %{topic: @topic, partition: @partition}}
  end

  describe "when started" do
    test "Consumes Events to EventHandler", %{state: state} do
      event = encode_event(:"test.event.publish", "Hello")

      GenConsumer.handle_message_set([event], state)

      assert_receive :event_recieved, 1000
    end

    test "handles errors gracefully", %{state: state} do
      first_event = encode_event(:"test.event.publish", "raise_error")
      second_event = encode_event(:"test.event.publish", "Hello")

      GenConsumer.handle_message_set(
        [
          first_event,
          second_event,
          second_event,
          second_event
        ],
        state
      )

      assert_receive :event_recieved, 1000
      assert_receive :handled_error, 1000
    end
  end

  def mock_get_event_schema(bypass, event_name) do
    {:ok, schema} = File.read("test/support/#{event_name}.avsc")
    {:ok, meta_schema} = File.read('test/support/event_metadata.avsc')

    schema = schema |> Poison.decode!()
    meta_schema = meta_schema |> Poison.decode!()

    schemas = [meta_schema, schema] |> Poison.encode!()

    Bypass.expect(bypass, "GET", "/subjects/#{event_name}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Poison.encode!(%{subject: event_name, version: 1, id: 1, schema: schemas})
      )
    end)
  end

  def mock_get_metadata_schema(bypass) do
    {:ok, schema} = File.read('test/support/event_metadata.avsc')
    schema = schema |> Poison.decode!() |> Poison.encode!()

    Bypass.expect_once(bypass, "GET", "/subjects/event_metadata/versions/latest", fn conn ->
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
