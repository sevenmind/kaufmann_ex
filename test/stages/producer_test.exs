defmodule KaufmannEx.Stages.ProducerTest do
  use ExUnit.Case

  @topic :rapids
  @partition 0

  setup do
    {:ok, pid} = start_supervised({KaufmannEx.Stages.Producer, {@topic, @partition}})

    {:ok, %{pid: pid}}
  end

  describe "handle_demand" do
    test "when no messages and demand > 0" do
      # Is this actually a useful thing to test
      assert {:noreply, [], %{demand: 10, message_set: []}} =
               KaufmannEx.Stages.Producer.handle_demand(10, %{message_set: [], demand: 10})
    end

    test "when messages > demand && demand > 0" do
      assert {:noreply, [1, 2, 3, 4, 5], %{demand: 0, message_set: [6]}} =
               KaufmannEx.Stages.Producer.handle_demand(5, %{
                 message_set: [1, 2, 3, 4, 5, 6],
                 demand: 5
               })
    end

    test "when messages and 0 demand" do
      res =
        KaufmannEx.Stages.Producer.handle_demand(0, %{message_set: [1, 2, 3, 4, 5], demand: 10})

      assert res == {:noreply, [], %{demand: 0, message_set: [1, 2, 3, 4, 5]}}
    end

    test "when no messages, and  0 demand" do
      res = KaufmannEx.Stages.Producer.handle_demand(0, %{message_set: [], demand: 10})
      assert res == {:noreply, [], %{demand: 0, message_set: []}}
    end

    test "Sends reply when more demand than event supply" do
      from = MapSet.new([{self(), :test}])

      {reply, next_message_set, state} =
        KaufmannEx.Stages.Producer.handle_demand(5, %{
          message_set: [1, 2, 3, 4, 5, 6],
          demand: 5,
          from: from
        })

      assert {:noreply, [], %{demand: 0, message_set: [6], from: ^from}} =
               KaufmannEx.Stages.Producer.handle_demand(0, state)

      assert {:noreply, [6], %{demand: 0, message_set: [], from: %MapSet{}}} =
               KaufmannEx.Stages.Producer.handle_demand(1, state)

      # Should happen
      assert_receive({:test, :ok})
    end
  end

  defmodule TestEventHandler do
    def child_spec(opts) do
      %{
        id: TestEventHandler,
        start: {TestEventHandler, :start_link, opts},
        restart: :temporary
      }
    end

    def start_link(parent, z) do
      Task.start_link(fn ->
        Process.send_after(parent, z, 5)
      end)
    end
  end

  defmodule TestConsumerSupervisor do
    use ConsumerSupervisor
    @topic :rapids
    @partition 0

    def start_link(parent) do
      ConsumerSupervisor.start_link(__MODULE__, parent)
    end

    def init(parent) do
      children = [{TestEventHandler, [parent]}]

      producer = {:global, {KaufmannEx.Stages.Producer, @topic, @partition}}

      # max_demand is highly resource dependent
      ConsumerSupervisor.init(children,
        strategy: :one_for_one,
        subscribe_to: [{producer, max_demand: 50}]
      )
    end
  end

  describe "when in flow from_stage" do
    test "will process just a few events" do
      assert {:ok, _} = start_supervised({TestConsumerSupervisor, self()})

      :ok =
        GenStage.call(
          {:global, {KaufmannEx.Stages.Producer, @topic, @partition}},
          {:notify, Enum.to_list(1..10)}
        )

      assert_receive 1
      assert_receive 2
      assert_receive 10
    end

    test "will consume as as many events as there is demand " do
      _ = start_supervised({TestConsumerSupervisor, self()})

      events = Enum.to_list(1..10_000)

      :ok =
        GenStage.call(
          {:global, {KaufmannEx.Stages.Producer, @topic, @partition}},
          {:notify, events}
        )

      assert_receive 1
      assert_receive 2
      assert_receive 3
      assert_receive 10_000
    end

    test "When more demand than events" do
      parent = self()
      _ = start_supervised({TestConsumerSupervisor, parent})

      events = Enum.to_list(1..100)

      :ok =
        GenStage.call(
          {:global, {KaufmannEx.Stages.Producer, @topic, @partition}},
          {:notify, events}
        )

      assert_receive 1
      assert_receive 2
      assert_receive 100
    end

    test "when less events than demand" do
      parent = self()
      _ = start_supervised({TestConsumerSupervisor, parent})

      events = Enum.to_list(1..100)

      :ok =
        GenStage.call(
          {:global, {KaufmannEx.Stages.Producer, @topic, @partition}},
          {:notify, events}
        )

      assert_receive 1
      assert_receive 10
      assert_receive 100
    end
  end
end
