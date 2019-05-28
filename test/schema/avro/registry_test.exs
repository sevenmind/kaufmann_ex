defmodule KaufmannEx.Schemas.Avro.RegistryTest do
  use ExUnit.Case

  setup_all do
    on_exit(fn -> Memoize.invalidate() end)
    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    event_name = "test.event.publish"
    # Mock calls to schema registry, only expected once
    TestHelper.init_schema_cache(bypass)
    TestHelper.mock_get_schema(bypass, event_name)

    [bypass: bypass, event_name: event_name]
  end

  describe "&parsed_schema" do
    test "when valid event" do
    end
  end

  describe "&get/1" do
    test "gets a remote schema"
  end

  describe "encodable?" do
    test ""
  end
end
