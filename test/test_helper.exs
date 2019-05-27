ExUnit.start()

{:ok, _} = Application.ensure_all_started(:httpoison)
{:ok, _} = Application.ensure_all_started(:bypass)
{:ok, _} = Application.ensure_all_started(:telemetry)
{:ok, _} = Application.ensure_all_started(:memoize)

defmodule TestHelper do
  def init_schema_cache(bypass, event_name \\ "test_event") do
    fake_schema = Jason.encode!(%{type: "string", name: "field"})

    # bypass any http calls called time
    # mock_get_metadata_schema(bypass)
    mock_get_fake_event(bypass, event_name, fake_schema)
    mock_get_unkown_event(bypass)
  end

  def mock_get_fake_event(bypass, event_name, fake_schema) do
    Bypass.stub(bypass, "GET", "/subjects/#{event_name}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Jason.encode!(%{subject: event_name, version: 1, id: 1, schema: fake_schema})
      )
    end)
  end

  def mock_get_schema(bypass, schema_name) do
    metadata_schema =
      File.read!("test/support/avro/event_metadata.avsc")
      |> Jason.decode!()

    schema =
      File.read!("test/support/avro/#{schema_name}.avsc")
      |> Jason.decode!()

    schema = [metadata_schema, schema] |> Jason.encode!()

    bypass
    |> Bypass.stub("GET", "/subjects/#{schema_name}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Jason.encode!(%{subject: schema_name, version: 1, id: 1, schema: schema})
      )
    end)
  end

  def mock_get_metadata_schema(bypass) do
    {:ok, schema} = File.read("test/support/avro/event_metadata.avsc")
    schema = schema |> Jason.decode!() |> Jason.encode!()

    Bypass.stub(bypass, "GET", "/subjects/event_metadata/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Jason.encode!(%{subject: "event_metadata", version: 1, id: 1, schema: schema})
      )
    end)
  end

  def mock_get_unkown_event(bypass) do
    Bypass.stub(bypass, "GET", "/subjects/UnknownEvent/versions/latest", fn conn ->
      Plug.Conn.resp(conn, 404, ~s<{"error_code": "40401", "message": "Subject not found."}>)
    end)
  end
end
