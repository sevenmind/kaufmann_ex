defmodule KaufmannEx.EncoderBench do
  @moduledoc """
  Benchmarks of event encodencodinging. Trying to understand how differemt patterns of
  schema cacheing effect performance
  """

  defmodule SchemaCacheencoder do
    use GenServer

    def start_link(path) do
      {:ok, pid} = GenServer.start_link(__MODULE__, path, name: __MODULE__)
    end

    @impl true
    def init(schemas_path) do
      {:ok, load_schemas(schemas_path)}
    end

    def handle_call({:encode, schema_name, raw}, _from, state) do
      schema = Map.get(state, schema_name)
      {:ok, encoded} = AvroEx.encode(schema, raw)

      {:reply, encoded, state}
    end

    def load_schemas(schemas_path) do
      schemas_path
      |> KaufmannEx.ReleaseTasks.MigrateSchemas.read_schemas()
      |> Enum.into(%{}, fn {k, schema} ->
        {:ok, parsed} = KaufmannEx.Schemas.parse(schema)
        {k, parsed}
      end)
    end

    def encode(schema_name, raw) do
      GenServer.call(__MODULE__, {:encode, schema_name, raw})
    end
  end

  @doc """
  Comparison of encode w/ different schema cacheing patterns
  """
  def run do
    setup()

    Benchee.run(
      %{
        "encode w inline schema" => fn %{schema: schema, raw: raw} ->
          {:ok, _} = AvroEx.encode(schema, raw)
        end,
        "encode w read schema file" => fn %{schema_name: schema_name, raw: raw} ->
          {:ok, schema} = KaufmannEx.Schemas.read_local_schema(schema_name)
          {:ok, _} = AvroEx.encode(schema, raw)
        end,
        "encode w schema registry" => fn %{
                                           schema_name: schema_name,
                                           raw: raw,
                                           schema: o_schema
                                         } ->
          {:ok, %{"schema" => raw_schema}} = get_schema_from_registry(schema_name)
          {:ok, schema} = AvroEx.parse_schema(raw_schema)
          {:ok, _} = AvroEx.encode(schema, raw)
        end,
        "encode w schema registry (memoized) " => fn %{
                                                       schema_name: schema_name,
                                                       raw: raw
                                                     } ->
          # parsed_schema is memoized
          {:ok, schema} = KaufmannEx.Schemas.parsed_schema(schema_name)
          {:ok, _} = AvroEx.encode(schema, raw)
        end,
        "encode default (memoized)" => fn %{
                                            schema_name: schema_name,
                                            raw: raw
                                          } ->
          {:ok, _} = KaufmannEx.Schemas.encode_message(schema_name, raw)
        end,
        "encode w/ genserver cacheing schemas" => fn %{
                                                       schema_name: schema_name,
                                                       raw: raw
                                                     } ->
          KaufmannEx.SchemasBench.SchemaCacheencoder.encode(schema_name, raw)
        end
        # "encode w/ ets cached schema" => nil,
        # "encode w/ poison encoded" => nil
      },
      before_scenario: fn %{raw: raw} = args ->
        %{schema: schema} = init_caches(args)

        # {:ok, encoded} = encoded_payload(schema, noise)

        Map.merge(args, %{schema: schema, encoded: encoded})
      end,
      parallel: 4,
      memory_time: 2,
      after_scenario: fn _ -> nil end,
      inputs: %{
        "standard" => %{
          schema_name: "command.test",
          noise: "ok"
        }
      }
    )
  end

  def setup do
    {:ok, _} = Application.ensure_all_started(:httpoison)
    {:ok, _} = Application.ensure_all_started(:memoize)

    Application.put_env(:kaufmann_ex, :schema_path, "test/support")
    KaufmannEx.ReleaseTasks.MigrateSchemas.migrate_schemas("test/support")
    KaufmannEx.SchemasBench.SchemaCacheencoder.start_link("test/support")
  end

  def init_caches(%{schema_name: name} = args) do
    # read schema from file
    {:ok, schema} = KaufmannEx.Schemas.read_local_schema(name)

    # init memoized caches
    # KaufmannEx.Schemas.get(name)
    # KaufmannEx.Schemas.parsed_schema(name)

    Map.merge(%{schema: schema}, args)
  end

  def encoded_payload(schema, noise \\ "") do
    message_body =
      %{
        payload: %{message: noise},
        meta: %{
          message_id: Nanoid.generate(),
          emitter_service: Nanoid.generate(),
          emitter_service_id: Nanoid.generate(),
          callback_id: nil,
          message_name: "command.test",
          timestamp: DateTime.to_string(DateTime.utc_now())
        }
      }
      |> Map.Helpers.stringify_keys()

    AvroEx.encode(schema, message_body)
  end

  def get_schema_from_registry(schema_name) do
    KaufmannEx.Config.schema_registry_uri()
    |> Schemex.latest(schema_name)
  end

  def seed_schemas do
  end
end

KaufmannEx.SchemasBench.run()
