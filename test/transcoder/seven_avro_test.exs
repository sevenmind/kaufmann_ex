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
    TestHelper.mock_get_schema(bypass, "query.req")
    TestHelper.mock_get_schema(bypass, "event.error")

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

    encoded_query =
      <<2, 42, 80, 82, 119, 85, 99, 78, 66, 112, 56, 50, 100, 103, 52, 108, 106, 67, 45, 122, 67,
        113, 116, 42, 121, 111, 57, 48, 108, 121, 104, 83, 120, 75, 119, 86, 98, 76, 100, 52, 90,
        101, 78, 116, 122, 42, 79, 50, 97, 113, 55, 95, 84, 54, 76, 103, 110, 74, 49, 86, 111,
        120, 101, 48, 101, 81, 117, 0, 50, 113, 117, 101, 114, 121, 46, 114, 101, 113, 46, 115,
        111, 109, 101, 116, 104, 105, 110, 103, 46, 111, 116, 104, 101, 114, 54, 50, 48, 49, 57,
        45, 48, 54, 45, 48, 51, 32, 49, 55, 58, 52, 51, 58, 53, 48, 46, 50, 52, 50, 49, 50, 56,
        90, 0, 2, 22, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100>>

    encoded_error =
      <<2, 42, 113, 98, 66, 87, 117, 115, 101, 88, 73, 115, 90, 104, 72, 51, 118, 66, 54, 119,
        111, 54, 120, 42, 95, 104, 84, 121, 113, 86, 86, 69, 107, 116, 121, 105, 54, 77, 45, 82,
        106, 53, 53, 49, 97, 42, 74, 122, 114, 80, 89, 55, 71, 67, 84, 83, 106, 87, 117, 83, 89,
        104, 98, 49, 97, 77, 88, 0, 74, 101, 118, 101, 110, 116, 46, 101, 114, 114, 111, 114, 46,
        113, 117, 101, 114, 121, 46, 114, 101, 113, 46, 115, 111, 109, 101, 116, 104, 105, 110,
        103, 46, 111, 116, 104, 101, 114, 54, 50, 48, 49, 57, 45, 48, 54, 45, 48, 51, 32, 49, 55,
        58, 52, 54, 58, 53, 50, 46, 50, 56, 49, 49, 54, 51, 90, 0, 2, 10, 101, 114, 114, 111,
        114>>

    {:ok,
     bypass: bypass,
     encoded: encoded,
     encoded_query: encoded_query,
     encoded_error: encoded_error,
     event_name: event_name}
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

    test "when event with generalized schema", %{encoded_query: encoded_query} do
      assert %Event{
               name: "query.req.something.other",
               payload: %{query_params: "Hello World"},
               raw_event: %{
                 key: "query.req.something.other",
                 value: ^encoded_query
               }
             } =
               SevenAvro.decode_event(%Event{
                 raw_event: %{key: "query.req.something.other", value: encoded_query, offset: nil}
               })
    end

    test "when error event", %{encoded_error: encoded_error} do
      assert %Event{
               name: "event.error.query.req.something.other",
               payload: %{error: "error"},
               raw_event: %{
                 key: "event.error.query.req.something.other",
                 value: ^encoded_error
               }
             } =
               SevenAvro.decode_event(%Event{
                 raw_event: %{
                   key: "event.error.query.req.something.other",
                   value: encoded_error,
                   offset: nil
                 }
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

    test "when event with generalized schema", %{encoded: encoded} do
      assert %Request{encoded: <<2>> <> _} =
               SevenAvro.encode_event(%Request{
                 event_name: "query.req.something.other",
                 payload: %{query_params: "Hello World"},
                 metadata: MockBus.fake_meta("query.req.something.other")
               })

      assert %Request{encoded: <<2>> <> _} =
               SevenAvro.encode_event(%Request{
                 event_name: "event.error.query.req.something.other",
                 payload: %{error: "error"},
                 metadata: MockBus.fake_meta("event.error.query.req.something.other")
               })
    end
  end
end
