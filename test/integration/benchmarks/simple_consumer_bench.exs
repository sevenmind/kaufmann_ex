# Simple benchmark of standrad KafkaEx GenConsumer vs KaufmannEx Stages.Consumer.
# In Sequential mode, get very similar performance

# Expects to be run in integration test mode with running kafka, zookeeper and schema registry
# $ mix test test/integration/benchmarks/simple_consumer_bench.exs --include integration
# Name                                  ips        average  deviation         median         99th %
# kaufmannex publish and wait        841.71        1.19 ms  ±1324.85%        0.64 ms        4.40 ms
# simple publish and wait            797.63        1.25 ms  ±1283.08%        0.67 ms        4.31 ms

# Name                                  ips        average  deviation         median         99th %
# kaufmannex publish and wait        861.38        1.16 ms  ±1318.39%        0.62 ms        4.11 ms
# simple publish and wait            785.88        1.27 ms  ±1275.77%        0.69 ms        4.43 ms

# Comparison:
# kaufmannex publish and wait        861.38
# simple publish and wait            785.88 - 1.10x slower

defmodule IntegrationTest.SimpleGenConsumerSubscriber do
  use KafkaEx.GenConsumer
  require Logger

  def init(_topic, _partition) do
    {:ok, {}}
  end

  def handle_message_set(message_set, state) do
    message_set
    |> Enum.map(&KaufmannEx.Consumer.Stage.EventHandler.handle_event/1)

    {:async_commit, state}
  end

  def publish(pid) do
    message_body = %{
      payload: %{message: pid_to_binary(pid)},
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

    :ok = KaufmannEx.Publisher.publish(:"command.test", message_body)
  end

  def given_event(%{name: :"command.test", payload: %{message: pid}} = event) do
    # Process.sleep(400)

    pid
    |> pid_from_string()
    |> send({:hello, pid})
  end

  def given_event(other) do
    IO.inspect(other, label: "Unhandled event")
  end

  def publish_and_wait(__), do: publish_and_wait()

  def publish_and_wait do
    :ok = publish(self())

    receive do
      {:hello, pid} -> Logger.debug("Publish Callback received")
    after
      2000 ->
        {:error, :timeout}
    end
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
end

defmodule IntegrationTest.KaufmannExSubscriberListener do
  require Logger

  def publish(pid) do
    message_body = %{
      payload: %{message: pid_to_binary(pid)},
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

    :ok = KaufmannEx.Publisher.publish(:"command.test", message_body)
  end

  def given_event(%{name: :"command.test", payload: %{message: pid}} = event) do
    # Process.sleep(400)

    pid
    |> pid_from_string()
    |> send({:hello, pid})
  end

  def given_event(other) do
    IO.inspect(other, label: "Unhandled event")
  end

  def publish_and_wait(__), do: publish_and_wait()

  def publish_and_wait do
    :ok = publish(self())

    receive do
      {:hello, pid} ->
        {:ok, nil}
    after
      2000 ->
        {:error, :timeout}
    end
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
end

defmodule IntegrationTest.IngrationBenchmarkTest do
  use ExUnit.Case
  import Mock

  @moduletag :integration

  setup_all do
    KaufmannEx.ReleaseTasks.migrate_schemas("sample_application/priv/schemas/")

    # Ensure topic is defined, raise error if not
    KafkaEx.metadata(topic: "rapids")

    Process.sleep(1000)
  end

  def setup_kaufmann_supervisor(event_handler_mod, consumer_mod) do
    Application.put_env(
      :kaufmann_ex,
      :event_handler_mod,
      event_handler_mod
    )

    Application.put_env(
      :kaufmann_ex,
      :gen_consumer_mod,
      consumer_mod
    )
  end

  # @tag skip: "Enable to bench throughput of publish to consumer"
  test "Bench Run" do
    # Name                                  ips        average  deviation         median         99th %
    # kaufmannex publish and wait        841.71        1.19 ms  ±1324.85%        0.64 ms        4.40 ms
    # simple publish and wait            797.63        1.25 ms  ±1283.08%        0.67 ms        4.31 ms

    {:ok, sup} =
      Supervisor.start_link([], strategy: :one_for_one, max_restarts: 1_000_000, max_seconds: 1)

    Benchee.run(
      %{
        "simple publish and wait" =>
          {&IntegrationTest.SimpleGenConsumerSubscriber.publish_and_wait/1,
           input: {
             IntegrationTest.SimpleGenConsumerSubscriber,
             IntegrationTest.SimpleGenConsumerSubscriber
           }},
        "kaufmannex publish and wait" => {
          &IntegrationTest.KaufmannExSubscriberListener.publish_and_wait/1,
          input: {
            IntegrationTest.KaufmannExSubscriberListener,
            KaufmannEx.Consumer.Stage.GenConsumer
          }
        }
      },
      # parallel: 3,
      before_scenario: fn {event_mod, consumer_mod} ->
        setup_kaufmann_supervisor(
          event_mod,
          consumer_mod
        )

        Supervisor.start_child(sup, Supervisor.child_spec(KaufmannEx.Supervisor, []))
      end,
      after_scenario: fn _ ->
        id = KaufmannEx.Supervisor

        with :ok <- Supervisor.terminate_child(sup, id),
             :ok <- Supervisor.delete_child(sup, id),
             do: :ok
      end
    )
  end
end
