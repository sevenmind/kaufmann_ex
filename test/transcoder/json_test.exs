defmodule KaufmannEx.Transcoder.JsonTest do
  use ExUnit.Case

  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.Transcoder.Json

  describe "decode_event/1" do
    test "parses event" do
      assert %Event{
               name: nil,
               meta: nil,
               payload: %{"some" => "event"}
             } =
               Json.decode_event(%Event{
                 raw_event: %{
                   key: nil,
                   value: ~s({"some":"event"})
                 }
               })
    end

    test "maps meta key if present" do
      assert %Event{
               name: nil,
               meta: %{"key" => "value"},
               payload: %{"some" => "field"}
             } =
               Json.decode_event(%Event{
                 raw_event: %{
                   key: nil,
                   value: ~s({"meta":{"key":"value"},"payload":{"some":"field"}})
                 }
               })
    end
  end

  describe "encode_event" do
    test "encodes event" do
      assert %Request{
               encoded: "{\"this\":\"value\"}",
               format: :json
             } = Json.encode_event(%Request{format: :json, payload: %{this: "value"}})
    end
  end
end
