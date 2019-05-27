defmodule KaufmannEx.Consumer.FlowTest do
  use ExUnit.Case

  import Mock

  defmodule GenProducer do
    use GenStage

    def start_link(number) do
      GenStage.start_link(__MODULE__, number)
    end

    def init(counter) do
      {:producer, counter}
    end

    def handle_cast(message, state) do
      {:noreply, [message], state}
    end

    def handle_demand(demand, state) do
      {:noreply, [], state}
    end
  end

  defmodule EventHandler do
    use KaufmannEx.EventHandler
    alias KaufmannEx.Schemas.Event

    def given_event(%Event{name: :"event.test", payload: pl}) do
      IO.puts "REACHEEEED"
      {:reply, [{:"event.test", pl}]}
    end
  end

  defmodule KafkaMock do
    @moduledoc "Just a mock for other named genservers"
    use GenServer

    def start_link(name) do
      GenServer.start_link(__MODULE__, [], name: name)
    end

    def init(_), do: {:ok, []}

    def handle_call({:metadata, _}, _from, state),
      do: {:reply, %{topic_metadatas: [%{topic: "rapids", partition_metadatas: [%{}]}]}, state}

    def handle_call(_, _from, state), do: {:reply, :ok, state}
    def handle_cast(_, _from, state), do: {:noreply, state}
  end

  alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse
  alias KaufmannEx.Consumer.FlowTest.EventHandler
  alias KaufmannEx.Consumer.FlowTest.GenProducer
  alias KaufmannEx.Consumer.FlowTest.KafkaMock
  alias KaufmannEx.Schemas.Avro
  alias KaufmannEx.Schemas.Avro.Registry
  alias KaufmannEx.TestSupport.MockBus

  setup_all do
    # Clear cached schemas

    bypass = Bypass.open()
    Application.put_env(:kaufmann_ex, :schema_registry_uri, "http://localhost:#{bypass.port}")

    event_name = "event.test"

    init_schema_cache(bypass, event_name)
    [bypass: bypass, event_name: event_name]
  end

  setup %{bypass: bypass, event_name: event_name} do
    {:ok, kafka_mock} = start_supervised({KafkaMock, :kafka_ex})

    on_exit(fn ->
      Memoize.invalidate()
    end)

    Application.put_env(:kaufmann_ex, :event_handler_mod, EventHandler)

    {:ok, schema} = Registry.parsed_schema(event_name)

    {:ok, encoded} =
      Avro.encode(schema, %{
        meta: MockBus.event_metadata(event_name),
        payload: %{message: "hello world"}
      })

    {:ok, bypass: bypass, encoded: encoded, event_name: event_name, kafka_mock: kafka_mock}
  end

  describe "start_link" do
    test "tries to start" do
      with_mock KafkaEx, [],
        metadata: fn _ -> %{topic_metadatas: [%{topic: "rapids", partition_metadatas: [%{}]}]} end,
        metadata: fn -> %{topic_metadatas: [%{topic: "rapids", partition_metadatas: [%{}]}]} end,
        create_worker: fn _, _ -> {:ok, spawn(fn -> 1 + 2 end)} end,
        create_worker: fn _ -> {:ok, spawn(fn -> 1 + 2 end)} end,
        stop_worker: fn _ -> :ok end,
        produce: fn _, _ -> :ok end,
        offset_fetch: fn _, _ ->
          [
            %OffsetFetchResponse{
              topic: "rapids",
              partitions: [
                %{partition: 0, error_code: :no_error, offset: 0}
              ]
            }
          ]
        end do
        {:ok, pid} = start_supervised({GenProducer, 0})

        assert {:ok, flow_pid} =
                 start_supervised({KaufmannEx.Consumer.Flow, {pid, "rapids", 0, []}})
      end
    end

    test "sends an event", %{encoded: encoded, event_name: event_name, kafka_mock: kafka_mock} do
      with_mock KafkaEx, [],
        metadata: fn _ -> :ok end,
        metadata: fn -> %{topic_metadatas: [%{topic: "rapids", partition_metadatas: [%{}]}]} end,
        create_worker: fn _, _ -> {:ok, kafka_mock} end,
        create_worker: fn _ -> {:ok, kafka_mock} end,
        stop_worker: fn _ -> :ok end,
        offset_fetch: fn _, _ ->
          [
            %OffsetFetchResponse{
              topic: "rapids",
              partitions: [
                %{partition: 0, error_code: :no_error, offset: 0}
              ]
            }
          ]
        end do
        {:ok, pid} = start_supervised({GenProducer, 0})

        assert {:ok, flow_pid} =
                 start_supervised({KaufmannEx.Consumer.Flow, {pid, "rapids", 0, []}})

        GenServer.cast(pid, %{key: event_name, value: encoded, offset: 0})
      end
    end
  end

  def init_schema_cache(bypass, event_name) do
    # bypass any http calls called time
    mock_get_metadata_schema(bypass)
    mock_get_event(bypass, event_name)

    assert {:ok, %{}} = Registry.parsed_schema(event_name)
    # mock_get_unkown_event(bypass)
  end

  def mock_get_event(bypass, event_name) do
    {:ok, schema} = File.read("test/support/avro/#{event_name}.avsc")
    {:ok, meta_schema} = File.read("test/support/avro/event_metadata.avsc")

    Bypass.stub(bypass, "GET", "/subjects/#{event_name}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Jason.encode!(%{
          subject: event_name,
          version: 1,
          id: 1,
          schema: "[#{meta_schema}, #{schema}]"
        })
      )
    end)
  end

  def mock_get_metadata_schema(bypass) do
    {:ok, schema} = File.read("test/support/avro/event_metadata.avsc")

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
