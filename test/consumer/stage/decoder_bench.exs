defmodule KaufmannEx.DecoderBench do
  @moduledoc """
  Benchmarks of event decoding. Trying to understand how differemt patterns of
  schema cacheing effect performance

  ## Benchmark results

      Operating System: Linux
      CPU Information: Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
      Number of Available Cores: 8
      Available memory: 15.40 GB
      Elixir 1.8.1
      Erlang 21.2.5

      Benchmark suite executing with the following configuration:
      warmup: 2 s
      time: 5 s
      memory time: 2 s
      parallel: 4
      inputs: standard
      Estimated total run time: 54 s


      Benchmarking decode default (memoized) with input standard...
      Benchmarking decode w inline schema with input standard...
      Benchmarking decode w read schema file with input standard...
      Benchmarking decode w schema registry with input standard...
      Benchmarking decode w schema registry (memoized)  with input standard...
      Benchmarking decode w/ genserver cacheing schemas with input standard...

      ##### With input standard #####
      Name                                           ips        average  deviation         median         99th %
      decode w inline schema                      9.52 K      105.00 μs  ±2703.73%       11.64 μs       33.56 μs
      decode w schema registry (memoized)         5.36 K      186.45 μs  ±2038.04%       20.04 μs       66.51 μs
      decode w/ genserver cacheing schemas        4.69 K      213.42 μs  ±1669.58%       44.32 μs      125.67 μs
      decode default (memoized)                   3.56 K      280.74 μs  ±1662.09%       29.21 μs       83.68 μs
      decode w read schema file                  0.147 K     6799.41 μs   ±313.30%      891.73 μs    87400.18 μs
      decode w schema registry                  0.0501 K    19959.98 μs   ±159.84%     4017.51 μs    91713.03 μs

      Comparison:
      decode w inline schema                      9.52 K
      decode w schema registry (memoized)         5.36 K - 1.78x slower
      decode w/ genserver cacheing schemas        4.69 K - 2.03x slower
      decode default (memoized)                   3.56 K - 2.67x slower
      decode w read schema file                  0.147 K - 64.75x slower
      decode w schema registry                  0.0501 K - 190.09x slower

      Memory usage statistics:

      Name                                         average  deviation         median         99th %
      decode w inline schema                       4.47 KB     ±0.00%        4.47 KB        4.47 KB
      decode w schema registry (memoized)          4.76 KB     ±0.00%        4.76 KB        4.76 KB
      decode w/ genserver cacheing schemas         0.96 KB     ±1.57%        0.96 KB        0.96 KB
      decode default (memoized)                    6.50 KB     ±0.00%        6.50 KB        6.50 KB
      decode w read schema file                   97.56 KB     ±0.00%       97.56 KB       97.56 KB
      decode w schema registry                   188.07 KB     ±0.11%      188.01 KB      188.75 KB

      Comparison:
      decode w inline schema                       4.47 KB
      decode w schema registry (memoized)          4.76 KB - 1.06x memory usage
      decode w/ genserver cacheing schemas         0.96 KB - 0.22x memory usage
      decode default (memoized)                    6.50 KB - 1.45x memory usage
      decode w read schema file                   97.56 KB - 21.83x memory usage
      decode w schema registry                   188.01 KB - 42.07x memory usage
  """

  defmodule SchemaCacheDecoder do
    use GenServer

    def start_link(path) do
      {:ok, pid} = GenServer.start_link(__MODULE__, path, name: __MODULE__)
    end

    @impl true
    def init(schemas_path) do
      {:ok, load_schemas(schemas_path)}
    end

    def handle_call({:decode, schema_name, encoded}, _from, state) do
      schema = Map.get(state, schema_name)
      {:ok, decoded} = AvroEx.decode(schema, encoded)

      {:reply, decoded, state}
    end

    def load_schemas(schemas_path) do
      schemas_path
      |> KaufmannEx.ReleaseTasks.MigrateSchemas.read_schemas()
      |> Enum.into(%{}, fn {k, schema} ->
        {:ok, parsed} = KaufmannEx.Schemas.parse(schema)
        {k, parsed}
      end)
    end

    def decode(schema_name, encoded) do
      GenServer.call(__MODULE__, {:decode, schema_name, encoded})
    end
  end

  @doc """
  Comparison of decode w/ different schema cacheing patterns
  """
  def run do
    setup()

    Benchee.run(
      %{
        "decode w inline schema" => fn %{schema: schema, encoded: encoded} ->
          {:ok, _} = AvroEx.decode(schema, encoded)
        end,
        "decode w read schema file" => fn %{schema_name: schema_name, encoded: encoded} ->
          {:ok, schema} = KaufmannEx.Schemas.read_local_schema(schema_name)
          {:ok, _} = AvroEx.decode(schema, encoded)
        end,
        "decode w schema registry" => fn %{
                                           schema_name: schema_name,
                                           encoded: encoded,
                                           schema: o_schema
                                         } ->
          {:ok, %{"schema" => raw_schema}} = get_schema_from_registry(schema_name)
          {:ok, schema} = AvroEx.parse_schema(raw_schema)
          {:ok, _} = AvroEx.decode(schema, encoded)
        end,
        "decode w schema registry (memoized) " => fn %{
                                                       schema_name: schema_name,
                                                       encoded: encoded
                                                     } ->
          # parsed_schema is memoized
          {:ok, schema} = KaufmannEx.Schemas.parsed_schema(schema_name)
          {:ok, _} = AvroEx.decode(schema, encoded)
        end,
        "decode default (memoized)" => fn %{
                                            schema_name: schema_name,
                                            encoded: encoded
                                          } ->
          {:ok, _} = KaufmannEx.Schemas.decode_message(schema_name, encoded)
        end,
        "decode w/ genserver cacheing schemas" => fn %{
                                                       schema_name: schema_name,
                                                       encoded: encoded
                                                     } ->
          KaufmannEx.SchemasBench.SchemaCacheDecoder.decode(schema_name, encoded)
        end
        # "decode w/ ets cached schema" => nil,
        # "decode w/ poison encoded" => nil
      },
      before_scenario: fn %{noise: noise} = args ->
        %{schema: schema} = init_caches(args)

        {:ok, encoded} = encoded_payload(schema, noise)

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
    KaufmannEx.SchemasBench.SchemaCacheDecoder.start_link("test/support")
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
