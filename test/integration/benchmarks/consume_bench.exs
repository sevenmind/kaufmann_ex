### Benchmark Decoding an event and passing it to an EventHandler
require Logger

partition = 1
topic = "rapids"

defmodule BenchEventPublisher do
  @moduledoc false
  # Send Events to the benchmark subject

  alias KaufmannEx.Consumer.StageSupervisor
  alias KaufmannEx.Schemas

  @partition 1
  @topic "rapids"
  def publish(message_set) do
    GenStage.call(
      {:via, Registry,
       {Registry.ConsumerRegistry,
        StageSupervisor.stage_name(KaufmannEx.Consumer.Stage.Producer, @topic, @partition)}},
      {:notify, message_set}
    )
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

    %{key: "command.test", value: encoded, crc: "a"}
  end

  # Thanks to https://github.com/koudelka/visualixir/blob/master/lib/visualixir/tracer.ex
  defp pid_to_binary(pid) when is_pid(pid) do
    "#PID" <> (pid |> :erlang.pid_to_list() |> :erlang.list_to_binary())
  end
end

defmodule BenchEventHandler do
  @moduledoc false
  def given_event(%{name: :"command.test", payload: %{message: pid}} = event) do
    pid
    |> String.split("::")
    |> Enum.at(0)
    |> pid_from_string()
    |> send({:ack, pid})
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
end

Application.put_env(
  :kaufmann_ex,
  :event_handler_mod,
  BenchEventHandler
)

inputs = %{
  "Just PID" => "",
  "PID & some text" => String.duplicate("x", 500),
  "PID & lost of random data" => String.duplicate("x", 5_000)
}

Registry.start_link(keys: :unique, name: Registry.ConsumerRegistry)

Benchee.run(
  %{
    "Consume * decode" => fn messages ->
      BenchEventPublisher.publish(messages)

      receive do
        {:ack, pid} -> Logger.debug("Publish Callback received")
      after
        2000 ->
          {:error, :timeout}
      end
    end
  },
  # parallel: 3,
  before_scenario: fn input ->
    KaufmannEx.Consumer.StageSupervisor.start_link({topic, partition})

    [BenchEventPublisher.encoded_payload(self(), input)]
  end,
  after_scenario: fn _ ->
    Registry.unregister(
      Registry.ConsumerRegistry,
      StageSupervisor.stage_name(KaufmannEx.Consumer.Stage.Producer, topic, partition)
    )
  end,
  warmup: 5,
  inputs: inputs
)
