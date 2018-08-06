defmodule KaufmannEx.Stages.EventHandlerTest do
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

  setup_all do
    {:ok, memo_pid} = Application.ensure_all_started(:memoize)
    # Clear cached schemas on exit
    on_exit(&Memoize.invalidate/0)

    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    # Mock calls to schema registry, only expected once
    # mock_get_metadata_schema(bypass)
    mock_get_event_schema(bypass, "test.event.publish")

    [bypass: bypass]
  end

  setup %{bypass: bypass} = context do
    Application.put_env(:kaufmann_ex, :event_handler_mod, TestEventHandler)
    Application.put_env(:kaufmann_ex, :schema_path, "test/support")

    Process.register(self(), :subscriber)

    {:ok, pid} = KaufmannEx.Stages.Producer.start_link([])
    {:ok, s_pid} = KaufmannEx.Stages.Consumer.start_link()
    {:ok, state} = KaufmannEx.Stages.GenConsumer.init(@topic, @partition)

    {:ok, bypass: bypass, state: state}
  end

  describe "when started" do
    test "Consumes Events to EventHandler", %{state: state} do
      event = encode_event(:"test.event.publish", "Hello")

      KaufmannEx.Stages.GenConsumer.handle_message_set([event], state)

      assert_receive :event_recieved
    end

    test "handles errors gracefully", %{state: state} do
      first_event = encode_event(:"test.event.publish", "raise_error")
      second_event = encode_event(:"test.event.publish", "Hello")

      KaufmannEx.Stages.GenConsumer.handle_message_set(
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
    {:ok, meta_schema} = File.read('test/support/event_metadata.avsc')

    schema = schema |> Poison.decode!()
    meta_schema = meta_schema |> Poison.decode!()

    schemas = [meta_schema, schema] |> Poison.encode!()

    Bypass.expect_once(bypass, "GET", "/subjects/#{event_name}/versions/latest", fn conn ->
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
