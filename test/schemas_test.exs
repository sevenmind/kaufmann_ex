defmodule KaufmannEx.SchemasTest do
  use ExUnit.Case
  alias KaufmannEx.Schemas

  setup do
    previous_schema_uri = Application.get_env(:kaufmann_ex, :schema_registry_uri)
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:1188")

    on_exit(fn -> Application.put_env(:kaufmann_ex, :schema_registry_uri, previous_schema_uri) end)

    bypass = Bypass.open(port: 1188)

    encoded = <<2, 22, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>
    {:ok, bypass: bypass, encoded: encoded}
  end

  describe "encode message" do
    test "when schema isn't registered", %{bypass: bypass} do
      Bypass.expect_once(bypass, "GET", "/subjects/UnknownEvent/versions/latest", fn conn ->
        Plug.Conn.resp(conn, 404, ~s<{"error_code": "40401", "message": "Subject not found."}>)
      end)

      {:error, message} = Schemas.encode_message("UnknownEvent", %{hello: "world"})

      assert message ==
               {:schema_encoding_error,
                %{"error_code" => "40401", "message" => "Subject not found."}}
    end

    test "when message doesn't match schema", %{bypass: bypass} do
      event_name = "test_event"

      fake_schema = Poison.encode!(%{type: "string", name: "field"})

      mock_get_fake_event(bypass, event_name, fake_schema)
      mock_get_metadata_schema(bypass)

      {:error, :data_does_not_match_schema, _, _} =
        Schemas.encode_message(event_name, %{"hello" => "world"})
    end

    test "when schema exists and is encodable", %{bypass: bypass} do
      event_name = "test_event"

      fake_schema = Poison.encode!(%{type: "string", name: "field"})

      mock_get_metadata_schema(bypass)
      mock_get_fake_event(bypass, event_name, fake_schema)

      {:ok, encoded} = Schemas.encode_message(event_name, "hello world")
      assert encoded == <<2, 22, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>
    end
  end

  describe "decode message" do
    test "when schema isn't registered", %{bypass: bypass, encoded: encoded} do
      Bypass.expect_once(bypass, "GET", "/subjects/UnknownEvent/versions/latest", fn conn ->
        Plug.Conn.resp(conn, 404, ~s<{"error_code": "40401", "message": "Subject not found."}>)
      end)

      {:error, message} = Schemas.decode_message("UnknownEvent", encoded)

      assert message ==
               {:schema_decoding_error,
                %{"error_code" => "40401", "message" => "Subject not found."}}
    end

    test "when message doesn't match schema", %{bypass: bypass, encoded: encoded} do
      event_name = "test_event"

      fake_schema =
        Poison.encode!(%{
          type: "record",
          name: "fields",
          fields: [%{type: "string", name: "name"}, %{type: "string", name: "address"}]
        })

      mock_get_fake_event(bypass, event_name, fake_schema)
      mock_get_metadata_schema(bypass)

      {:error, :unmatching_schema} = Schemas.decode_message(event_name, encoded)
    end

    test "when schema exists and message valid", %{bypass: bypass, encoded: encoded} do
      event_name = "test_event"

      fake_schema = Poison.encode!(%{type: "string", name: "field"})

      mock_get_metadata_schema(bypass)
      mock_get_fake_event(bypass, event_name, fake_schema)

      {:ok, message} = Schemas.decode_message(event_name, encoded)
      assert message == "hello world"
    end

    test "when schema is complex and valid", %{bypass: bypass} do
      event_name = "test_record"

      fake_schema =
        Poison.encode!(%{
          type: "record",
          name: event_name,
          fields: [
            %{type: "string", name: "hello"}
          ]
        })

      mock_get_metadata_schema(bypass)
      mock_get_fake_event(bypass, event_name, fake_schema)

      {:ok, encoded} = Schemas.encode_message(event_name, %{hello: "world"})
      {:ok, decoded} = Schemas.decode_message(event_name, encoded)

      assert decoded == %{hello: "world"}
    end
  end

  def mock_get_fake_event(bypass, event_name, fake_schema) do
    Bypass.expect(bypass, "GET", "/subjects/#{event_name}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Poison.encode!(%{subject: event_name, version: 1, id: 1, schema: fake_schema})
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
end
