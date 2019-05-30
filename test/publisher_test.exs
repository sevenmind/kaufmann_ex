defmodule KaufmannEx.PublisherTest do
  use ExUnit.Case

  import Mock

  setup do
    Application.put_env(:kaufmann_ex, :transcoder,
      default: KaufmannEx.Transcoder.SevenAvro,
      json: KaufmannEx.Transcoder.Json
    )

    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")
    TestHelper.mock_get_schema(bypass, "event.test")
  end

  describe "publish/1" do
    test "publishes an event to kafka" do
      with_mock KafkaEx, [],
        produce: fn _, _ -> nil end,
        metadata: fn _ -> %{topic_metadatas: [%{topic: "rapids", partition_metadatas: [%{}]}]} end do
        assert KaufmannEx.Publisher.publish("event.test", %{message: "hello"})
      end
    end
  end
end
