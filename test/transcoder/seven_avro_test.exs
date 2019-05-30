defmodule KaufmannEx.Transcoder.SevenAvroTest do
  use ExUnit.Case

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.TestSupport.MockBus
  alias KaufmannEx.Transcoder.SevenAvro

  setup_all do
    # {:ok, memo_pid} = Application.ensure_all_started(:memoize)

    # Clear cached schemas
    on_exit(fn -> Memoize.invalidate() end)

    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    event_name = "test.event.publish"
    # Mock calls to schema registry, only expected once
    TestHelper.init_schema_cache(bypass)
    TestHelper.mock_get_schema(bypass, event_name)

    [bypass: bypass, event_name: event_name]
  end

  setup %{bypass: bypass, event_name: event_name} do
    Application.put_env(:kaufmann_ex, :schema_path, "test/support")

    encoded =
      <<2, 42, 85, 118, 103, 51, 87, 74, 98, 117, 85, 55, 105, 80, 95, 48, 79, 121, 77, 112, 71,
        48, 86, 42, 106, 118, 119, 74, 89, 86, 45, 118, 70, 75, 76, 97, 104, 107, 103, 81, 87, 87,
        55, 110, 115, 42, 101, 82, 85, 51, 86, 70, 66, 79, 107, 98, 119, 100, 80, 118, 115, 113,
        67, 98, 120, 116, 73, 0, 36, 116, 101, 115, 116, 46, 101, 118, 101, 110, 116, 46, 112,
        117, 98, 108, 105, 115, 104, 54, 50, 48, 49, 57, 45, 48, 53, 45, 50, 55, 32, 50, 51, 58,
        48, 48, 58, 53, 54, 46, 56, 52, 52, 52, 52, 56, 90, 0, 22, 72, 101, 108, 108, 111, 32, 87,
        111, 114, 108, 100>>

    {:ok, bypass: bypass, encoded: encoded, event_name: event_name}
  end

  describe "decode_event/1" do
    test "resolves schema from registry and parses event", %{
      encoded: encoded,
      event_name: event_name
    } do
      assert %Event{
               name: "test.event.publish",
               payload: "Hello World",
               raw_event: %{
                 key: "test.event.publish",
                 value: ^encoded
               }
             } =
               SevenAvro.decode_event(%Event{
                 raw_event: %{key: event_name, value: encoded, offset: nil}
               })
    end

    test "when missing event", %{encoded: encoded} do
      assert %KaufmannEx.Schemas.ErrorEvent{
               error: %{
                 "error_code" => "40401",
                 "message" => "Subject not found."
               }
             } =
               SevenAvro.decode_event(%Event{
                 raw_event: %{key: "UnknownEvent", value: encoded, offset: nil}
               })
    end

    test "when invalid encoding", %{
      encoded: encoded,
      event_name: event_name
    } do
      assert %KaufmannEx.Schemas.ErrorEvent{
               error: :unmatching_schema
             } =
               SevenAvro.decode_event(%Event{
                 raw_event: %{key: event_name, value: encoded <> <<55>>, offset: nil}
               })
    end
  end

  describe "&encode_event/1" do
    test "when schema exists and is encodable", %{event_name: event_name} do
      assert %Request{encoded: <<2>> <> _} =
               SevenAvro.encode_event(%Request{
                 event_name: event_name,
                 payload: "Hello World",
                 metadata: MockBus.fake_meta(event_name)
               })
    end

    test "when schema isn't registered" do
      # {:error, message} = SevenAvro.encode_event("UnknownEvent", %{hello: "world"})

      assert_raise MatchError,
                   "no match of right hand side value: {:error, {:schema_encoding_error, %{\"error_code\" => \"40401\", \"message\" => \"Subject not found.\"}}}",
                   fn ->
                     SevenAvro.encode_event(%Request{
                       event_name: "UnknownEvent",
                       payload: "Hello World",
                       metadata: MockBus.fake_meta()
                     })
                   end
    end

    test "when message doesn't match schema", %{event_name: event_name} do
      assert_raise MatchError,
                   "no match of right hand side value: {:error, {:schema_encoding_error, :data_does_not_match_schema}}",
                   fn ->
                     SevenAvro.encode_event(%Request{
                       event_name: event_name,
                       payload: %{Hello: "World"},
                       metadata: MockBus.fake_meta()
                     })
                   end
    end
  end
end
