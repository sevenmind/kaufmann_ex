defmodule IntegrationTest.SubscriberListener do
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

defmodule IntegrationTest do
  use ExUnit.Case
  import Mock

  @moduletag :integration

  setup_all do
    KaufmannEx.ReleaseTasks.migrate_schemas("sample_application/priv/schemas/")

    Application.put_env(
      :kaufmann_ex,
      :event_handler_mod,
      IntegrationTest.SubscriberListener
    )

    # Ensure topic is defined, raise error if not 
    KafkaEx.metadata(topic: "rapids")

    Process.sleep(1000)

    # Start supervision tree
    {:ok, kaufmann_supervisor} = start_supervised(KaufmannEx.Supervisor)

    Process.sleep(4000)

    # Ensure subscriber is working 
    IntegrationTest.SubscriberListener.publish_and_wait()

    [kaufmann_supervisor: kaufmann_supervisor]
  end

  test "publish and consume" do
    assert :ok = IntegrationTest.SubscriberListener.publish(self())

    assert_receive {:hello, _}
  end

  describe "GenConsumer handles timeout" do
    test "inspection of supervision tree", %{kaufmann_supervisor: kaufmann_supervisor} do
      {KafkaEx.ConsumerGroup, k_consumer_group, :supervisor, [KafkaEx.ConsumerGroup]} =
        kaufmann_supervisor
        |> Supervisor.which_children()
        |> Enum.find(fn
          {KafkaEx.ConsumerGroup, _, _, _} -> true
          _ -> false
        end)

      assert [
               {:consumer, _, :supervisor, [KafkaEx.GenConsumer.Supervisor]},
               {KafkaEx.ConsumerGroup.Manager, _, :worker, [KafkaEx.ConsumerGroup.Manager]}
             ] = Supervisor.which_children(k_consumer_group)

      assert %{active: 4, specs: 4, supervisors: 1, workers: 3} =
               Supervisor.count_children(kaufmann_supervisor)
    end

    test "when KafakEx.GenConsumer receives timeout", %{kaufmann_supervisor: kaufmann_supervisor} do
      # Updates state, continues
      {KafkaEx.ConsumerGroup, k_consumer_group, :supervisor, [KafkaEx.ConsumerGroup]} =
        kaufmann_supervisor
        |> Supervisor.which_children()
        |> Enum.find(fn
          {KafkaEx.ConsumerGroup, _, _, _} -> true
          _ -> false
        end)

      consumer =
        k_consumer_group
        |> KafkaEx.ConsumerGroup.consumer_pids()
        |> Enum.at(0)

      send(consumer, :timeout)
    end
  end
end
