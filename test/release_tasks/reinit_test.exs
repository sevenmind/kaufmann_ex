defmodule KaufmannEx.ReleaseTasks.ReInitTest do
  use ExUnit.Case
  alias KafkaEx.Protocol.Fetch.Message
  alias KaufmannEx.Config
  alias KaufmannEx.ReleaseTasks.ReInit
  @moduletag :integration

  setup do
    ensure_topic_exists("rapids")
    :ok
  end

  describe "&reset_offsets" do
    test "it sets the  producer_mod to the publish_nothing" do
      ReInit.override_default_producer(%ReInit.Config{})
      assert Application.get_env(:kaufmann_ex, :producer_mod) == ReInit.PublishNothing
    end

    test "it resets consumer offset  to 0" do
      set_partition_0_offset(14)
      assert partition_0_offset == 14

      ReInit.reset_offsets(0, :latest)

      assert partition_0_offset == 0
    end
  end

  describe "reinit.genconsumer" do
    @tag skip: "terminates test process"
    test "will terminate when passed message set all >= target_offset in state" do
      {:ok, _} = KaufmannEx.Stages.Producer.start_link([])

      :ok = ReInit.StateStore.init()
      ReInit.StateStore.set_target_offset(15)

      {:ok, state} = ReInit.GenConsumer.init("topic", 0)

      message_set = [
        %Message{offset: 14, value: "whatevs"},
        %Message{offset: 15, value: "whatevs"}
      ]

      Process.register(self(), :reinit)
      :ok = ReInit.GenConsumer.handle_message_set(message_set, state)

      assert_received :shutdown,
                      ":reinit process should recieve shutdown once max offset is reached"
    end
  end

  def set_partition_0_offset(offset) do
    :ok = Application.ensure_started(:kafka_ex)

    res =
      KafkaEx.offset_commit(:kafka_ex, %KafkaEx.Protocol.OffsetCommit.Request{
        consumer_group: Config.consumer_group(),
        topic: "rapids",
        offset: offset,
        partition: 0
      })

    Application.stop(:kafka_ex)
  end

  def partition_0_offset do
    :ok = Application.ensure_started(:kafka_ex)

    [
      %KafkaEx.Protocol.OffsetFetch.Response{
        partitions: [%{error_code: :no_error, offset: offset}],
        topic: "rapids"
      }
    ] =
      KafkaEx.offset_fetch(:kafka_ex, %KafkaEx.Protocol.OffsetFetch.Request{
        consumer_group: Config.consumer_group(),
        topic: "rapids",
        partition: 0
      })

    Application.stop(:kafka_ex)

    offset
  end

  def ensure_topic_exists(topic) do
    :ok = Application.ensure_started(:kafka_ex)

    # Ensure topic exists
    KafkaEx.metadata(topic: topic)
    Application.stop(:kafka_ex)
  end
end
