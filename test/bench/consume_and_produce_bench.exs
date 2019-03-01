# Benchmark roundtrip publishing and consuming an event.
# Should provide a measure of best case performance with instant event.

# Operating System: Linux"
# CPU Information: Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
# Number of Available Cores: 8
# Available memory: 15.40 GB
# Elixir 1.7.4
# Erlang 21.1.4

# Benchmark suite executing with the following configuration:
# warmup: 5 s
# time: 10 s
# memory time: 0 μs
# parallel: 2
# inputs: none specified
# Estimated total run time: 15 s

# Benchmarking round trip...

# Name                 ips        average  deviation         median         99th %
# round trip        1.32 K      760.31 μs    ±47.78%         687 μs     2227.58 μs

defmodule Bench.ConsumeAndProduce do
  @moduledoc false

  ### Benchmark Decoding an event and passing it to an EventHandler
  require Logger

  @partition 1
  @topic "rapids"

  defmodule BenchEventHandler do
    @moduledoc false

    alias KaufmannEx.Schemas
    @partition 1
    @topic "rapids"

    def given_event(%{name: :"command.test", payload: %{message: msg}} = event) do
      [pid, string] = String.split(msg, "::")

      # Simulate waiting for work
      # Process.sleep(10)

      pid
      |> pid_from_string()
      |> send({:ack, pid, string})
    end

    def given_event(other) do
      IO.inspect(other, label: "Unhandled event")
    end

    # Thanks to https://github.com/koudelka/visualixir/blob/master/lib/visualixir/tracer.ex
    defp pid_to_binary(pid) when is_pid(pid) do
      "#PID" <> (pid |> :erlang.pid_to_list() |> :erlang.list_to_binary())
    end

    def pid_from_string("#PID" <> string) do
      string
      |> :erlang.binary_to_list()
      |> :erlang.list_to_pid()
    end

    def encoded_payload(pid, noise \\ "") do
      message_body = %{
        payload: %{message: pid_to_binary(pid) <> "::" <> noise},
        meta: %{
          message_id: Nanoid.generate(),
          emitter_service: KaufmannEx.Config.service_name(),
          emitter_service_id: KaufmannEx.Config.service_id(),
          callback_id: nil,
          message_name: "command.test",
          timestamp: DateTime.to_string(DateTime.utc_now()),
          callback_topic: nil
        }
      }

      {:ok, encoded} = Schemas.encode_message(:"command.test", message_body)

      %{key: "command.test", value: encoded, crc: ""}
    end

    def publish(pid, noise) do
      message_body = %{
        payload: %{message: pid_to_binary(pid) <> "::" <> noise},
        meta: %{
          message_id: Nanoid.generate(),
          emitter_service: KaufmannEx.Config.service_name(),
          emitter_service_id: KaufmannEx.Config.service_id(),
          callback_id: nil,
          message_name: "command.test",
          timestamp: DateTime.to_string(DateTime.utc_now()),
          callback_topic: nil
        }
      }

      KaufmannEx.Publisher.publish(:"command.test", message_body, %{})
    end
  end

  def random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  def run do
    KaufmannEx.ReleaseTasks.migrate_schemas("sample_application/priv/schemas/")

    Application.put_env(
      :kaufmann_ex,
      :event_handler_mod,
      BenchEventHandler
    )

    KafkaEx.metadata(topic: "rapids")

    {:ok, kaufmann_supervisor} = KaufmannEx.Supervisor.start_link([])

    IO.puts("Sleeping while everything starts \n")

    Process.sleep(10_000)

    Benchee.run(
      %{
        "round trip" => fn [pid, string] ->
          BenchEventHandler.publish(pid, string)

          receive do
            {:ack, pid, ^string} ->
              :ok
          after
            2000 ->
              {:error, :timeout}
          end
        end
      },
      parallel: 1,
      before_scenario: fn length ->
        [self(), random_string(16)]
      end,
      time: 20,
      warmup: 5,
      formatter_options: %{
        console: %{comparison: true, extended_statistics: true}
      }
    )
  end
end

Bench.ConsumeAndProduce.run()
