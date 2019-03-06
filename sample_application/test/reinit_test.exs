defmodule Sample.ReInitTest do
  use ExUnit.Case
  alias KaufmannEx.ReleaseTasks.ReInit
  alias KaufmannEx.ReleaseTasks.ReInit.Config
  @moduletag :integration

  # Testing Reini here instead of in base tests
  #   Reinit_service assumes it will be run in a service using kaufmann

  setup_all do
    Sample.ReleaseTasks.migrate_schemas()

    []
  end

  setup context do
    topic =  Nanoid.generate(16, "qwertyuiop")
    override_consumer_group(Nanoid.generate())
    init_topic(topic)

    :ok = Application.start(:sample, :transient)

    {:ok, [topic: topic]}
  end

  describe "ReInit.run" do
    test "Runs and does not reemit events", %{topic: topic} do
      initial_offset = latest_offset_number(topic, 0)

      # Publish commands, will trigger events
      publish_events()
      publish_events()
      publish_events()
      Process.sleep(200)

      # Offset should have moved by 2 commands + 2 events
      assert latest_offset_number(topic, 0) == initial_offset + 3

      Application.stop(:sample)
      stop_kafka()

      Sample.ReleaseTasks.reinit_service()


      # Run ReInit should have rebuilt internal state, but not moved the offset
      start_kafka()
      assert latest_offset_number(topic, 0) == initial_offset + 3
    end
  end

  def start_kafka do
    :ok = Application.ensure_started(:kafka_ex)
  end

  def stop_kafka do
    Application.stop(:kafka_ex)
  end

  def publish_events do
    KaufmannEx.Publisher.publish(:"event.test",
      %{
        meta: %{
          callback_id: nil,
          callback_topic: nil,
          emitter_service: "SampleService",
          emitter_service_id: "SampleHost",
          message_id: "AfMz7EXG_hMfihB~YQ~1i",
          message_name: "event.test",
          timestamp: "2019-03-06 22:46:07.160817Z"
        },
        payload: %{message: "Test"}
      }
    )
  end

  def init_topic(topic) do
    override_default_topic(topic)

    start_kafka()
    KafkaEx.metadata(topic: topic)
  end

  def override_default_topic(topic) do
    Application.put_env(
      :kaufmann_ex,
      :default_topic,
      topic
    )
  end

  def override_consumer_group(group) do
    Application.put_env(
      :kaufmann_ex,
      :consumer_group,
      group
    )
    Application.put_env(
      :kafka_ex,
      :consumer_group,
      group
    )
  end

  def get_max_offset(topic) do
    KafkaEx.create_worker(
      :pr,
      consumer_group: KaufmannEx.Config.consumer_group(),
      metadata_update_interval: 10
    )

    :timer.sleep(15)

    %Config{
      target_offset: target_offset
    } = ReInit.get_metadata(%Config{default_topic: topic})

    KafkaEx.stop_worker(:pr)

    target_offset
  end


  def latest_offset_number(topic, partition_id, worker \\ :kafka_ex) do
    offset = KafkaEx.latest_offset(topic, partition_id, worker)
      |> first_partition_offset

    offset || 0
end

 defp first_partition_offset(:topic_not_found) do
    nil
  end
  defp first_partition_offset(response) do
    [%KafkaEx.Protocol.Offset.Response{partition_offsets: partition_offsets}] =
      response
    first_partition = hd(partition_offsets)
    first_partition.offset |> hd
end
end
