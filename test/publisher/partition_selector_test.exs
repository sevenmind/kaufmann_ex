defmodule KaufmannEx.Publisher.PartitionSelectorTest do
  use ExUnit.Case
  alias KaufmannEx.Publisher.PartitionSelector

  setup do
    default_metadata = %{
      message_id: Nanoid.generate(),
      emitter_service: KaufmannEx.Config.service_name(),
      emitter_service_id: KaufmannEx.Config.service_id(),
      message_name: "command.test.event",
      timestamp: DateTime.to_string(DateTime.utc_now()),
      callback: %{}
    }

    {:ok, %{default_metadata: default_metadata}}
  end

  describe "partition from callback metadata" do
    test "success", %{default_metadata: default_metadata} do
      metadata =
        Map.put(default_metadata, :callback_topic, %{
          topic: "rapids",
          partition: 1
        })

      {:ok, partition} = PartitionSelector.choose_partition(2, metadata, :callback)
      assert partition == 1
    end

    test "invalid partition", %{default_metadata: default_metadata} do
      metadata =
        Map.put(default_metadata, :callback_topic, %{
          topic: "rapids",
          partition: 200
        })

      assert {:error, :invalid_callback_partition} =
        PartitionSelector.choose_partition(2, metadata, :callback)
    end
  end

  describe "md5 (default behavior)" do
    test "uses md5 of message to choose partition", %{default_metadata: metadata} do
      {:ok, partition} = PartitionSelector.choose_partition(10, metadata, :md5)
      assert Enum.member?(0..9, partition)
    end

    test "partition should be deterministic", %{default_metadata: metadata} do
      assert PartitionSelector.choose_partition(10, metadata, :md5) ==
               PartitionSelector.choose_partition(10, metadata, :md5)
    end

    test "partition should not be fixed", %{default_metadata: metadata} do
      mutated_metadata = Map.put(metadata, :message_name, "command.test.event2")

      refute PartitionSelector.choose_partition(100, metadata, :md5) ==
               PartitionSelector.choose_partition(100, mutated_metadata, :md5)
    end
  end

  describe "random partition" do
    test "chooses random partition between and total-1", %{default_metadata: metadata} do
      {:ok, partition} = PartitionSelector.choose_partition(4, metadata, :random)
      assert Enum.member?(0..3, partition)
    end
  end
end
