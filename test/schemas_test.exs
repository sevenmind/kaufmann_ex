defmodule KaufmannEx.SchemasTest do
  use ExUnit.Case
  alias KaufmannEx.Schemas

  setup_all do
    # {:ok, memo_pid} = Application.ensure_all_started(:memoize)

    # Clear cached schemas
    on_exit(fn -> Memoize.invalidate() end)

    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    # Mock calls to schema registry, only expected once
    TestHelper.init_schema_cache(bypass, "test_event")

    [bypass: bypass]
  end

  setup %{bypass: bypass} do
    encoded = <<22, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>

    {:ok, bypass: bypass, encoded: encoded, event_name: "test_event"}
  end

  describe "encode message" do
    test "when schema isn't registered" do
      {:error, message} = Schemas.encode_message("UnknownEvent", %{hello: "world"})

      assert message ==
               {:schema_encoding_error,
                %{"error_code" => "40401", "message" => "Subject not found."}}
    end

    test "when message doesn't match schema", %{event_name: event_name} do
      {:error, {:schema_encoding_error, :data_does_not_match_schema}} =
        Schemas.encode_message(event_name, %{"hello" => "world"})
    end

    test "when schema exists and is encodable", %{event_name: event_name, encoded: encoded} do
      {:ok, encoded_message} = Schemas.encode_message(event_name, "hello world")
      assert encoded_message == encoded
    end
  end

  describe "decode message" do
    test "when schema isn't registered", %{encoded: encoded} do
      {:error, message} = Schemas.decode_message("UnknownEvent", encoded)

      assert message ==
               {:schema_decoding_error,
                %{"error_code" => "40401", "message" => "Subject not found."}}
    end

    test "when message doesn't match schema", %{bypass: bypass, encoded: encoded} do
      event_name = "test_complex_event"

      fake_schema =
        Jason.encode!(%{
          type: "record",
          name: "fields",
          fields: [%{type: "string", name: "name"}, %{type: "string", name: "address"}]
        })

      TestHelper.mock_get_fake_event(bypass, event_name, fake_schema)

      {:error, :unmatching_schema} = Schemas.decode_message(event_name, encoded)
    end

    test "when schema exists and message valid", %{
      encoded: encoded,
      event_name: event_name
    } do
      {:ok, message} = Schemas.decode_message(event_name, encoded)
      assert message == "hello world"
    end

    test "when schema is complex and valid", %{bypass: bypass} do
      event_name = "test_record"

      fake_schema =
        Jason.encode!(%{
          type: "record",
          name: event_name,
          fields: [
            %{type: "string", name: "hello"}
          ]
        })

      TestHelper.mock_get_fake_event(bypass, event_name, fake_schema)

      {:ok, encoded} = Schemas.encode_message(event_name, %{hello: "world"})
      {:ok, decoded} = Schemas.decode_message(event_name, encoded)

      assert decoded == %{hello: "world"}
    end
  end


end
