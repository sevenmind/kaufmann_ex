defmodule KaufmannEx.Consumer.Stage.ProducerTest do
  defmodule StateTests do
    @moduledoc false

    use ExUnit.Case
    alias KaufmannEx.Consumer.Stage.Producer, as: ProducerStage

    describe "handle_demand" do
      test "when no messages and demand > 0" do
        # Is this actually a useful thing to test
        assert {:noreply, [], %{demand: 10, message_set: []}} =
                 ProducerStage.handle_demand(10, %{message_set: [], demand: 10})
      end

      test "when messages > demand && demand > 0" do
        from = {self(), :called}

        assert {:noreply, [1, 2, 3, 4, 5], %{demand: 0, message_set: [[6, ^from]]}} =
                 ProducerStage.handle_demand(5, %{
                   message_set: [[1, from], [2, from], [3, from], [4, from], [5, from], [6, from]],
                   demand: 5
                 })

        assert_receive({:called, :ok})
      end

      test "when messages and 0 demand" do
        res = ProducerStage.handle_demand(0, %{message_set: [1, 2, 3, 4, 5], demand: 10})

        assert res == {:noreply, [], %{demand: 0, message_set: [1, 2, 3, 4, 5]}}
      end

      test "when no messages, and  0 demand" do
        res = ProducerStage.handle_demand(0, %{message_set: [], demand: 10})
        assert res == {:noreply, [], %{demand: 0, message_set: []}}
      end

      test "Sends reply when more demand than event supply" do
        from = {self(), :test}

        {reply, next_message_set, state} =
          ProducerStage.handle_demand(5, %{
            message_set: [[1, from], [2, from], [3, from], [4, from], [5, from], [6, from]],
            demand: 5
          })

        assert {:noreply, [], %{demand: 0, message_set: [[6, ^from]]}} =
                 ProducerStage.handle_demand(0, state)

        assert {:noreply, [6], %{demand: 0, message_set: []}} =
                 ProducerStage.handle_demand(1, state)

        # Should happen
        assert_receive({:test, :ok})
      end
    end

    describe "call notify message_set" do
      test "notify messages with no demand" do
        from = {self(), :uncalled}
        messages = ["a"]

        assert {:noreply, [], %{message_set: [["a", ^from]], demand: 0}} =
                 ProducerStage.handle_call({:notify, messages}, from, %{
                   demand: 0,
                   message_set: []
                 })
      end

      test "notify more messages than demand" do
        uncalled = {self(), :uncalled}
        called = {self(), :called}

        assert {:noreply, ["a"], %{message_set: [["b", ^uncalled], ["c", ^uncalled]], demand: 0}} =
                 ProducerStage.handle_call({:notify, ["c"]}, uncalled, %{
                   demand: 1,
                   message_set: [["a", called], ["b", uncalled]],
                   from: MapSet.new()
                 })

        assert_receive({:called, :ok})
      end

      test "notify with messages equal to demand" do
        uncalled = {self(), :uncalled}
        called = {self(), :called}

        assert {:noreply, ["a", "b"],
                %{message_set: [["c", ^uncalled], ["d", ^uncalled]], demand: 0}} =
                 ProducerStage.handle_call({:notify, ["c", "d"]}, uncalled, %{
                   demand: 2,
                   message_set: [["a", called], ["b", called]],
                   from: MapSet.new()
                 })

        assert_receive({:called, :ok})
      end

      test "notify with messages less than demand" do
        called = {self(), :called}

        assert {:reply, :ok, ["a", "b", "c", "d"], %{message_set: [], demand: 6}} =
                 ProducerStage.handle_call({:notify, ["c", "d"]}, called, %{
                   demand: 10,
                   message_set: [["a", called], ["b", called]],
                   from: MapSet.new()
                 })

        assert_receive({:called, :ok})
      end
    end

    test "call message_set is returned when the message set is sent to the next stage" do
      from = {self(), :called}

      assert {:noreply, ["a", "b"],
              %{message_set: [["c", {self, :cd}], ["d", {self, :cd}]], demand: 0}} =
               ProducerStage.handle_call({:notify, ["c", "d"]}, {self, :cd}, %{
                 demand: 2,
                 message_set: [["a", {self, :a}], ["b", {self, :b}]]
               })

      assert_receive({:a, :ok})
      assert_receive({:b, :ok})

      # assert {:noreply}
    end
  end

  use ExUnit.Case

  alias KaufmannEx.Consumer.Stage.Producer, as: ProducerStage
  alias KaufmannEx.Consumer.Stage.ProducerTest.TestConsumerSupervisor
  alias KaufmannEx.Consumer.Stage.ProducerTest.TestConsumerSupTestEventHandlerervisor
  alias KaufmannEx.StageSupervisor

  @topic "rapids"
  @partition 0
  @producer_name ProducerStage

  setup do
    # {:ok, _} = start_supervised({Registry, keys: :unique, name: Registry.ConsumerRegistry})

    {:ok, pid} = start_supervised({ProducerStage, [name: ProducerStage]})

    {:ok, %{pid: pid}}
  end

  defmodule TestEventHandler do
    @moduledoc false
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
    @moduledoc false
    use ConsumerSupervisor
    @topic "rapids"
    @partition 0

    def start_link(parent) do
      ConsumerSupervisor.start_link(__MODULE__, parent)
    end

    def init(parent) do
      children = [{TestEventHandler, [parent]}]

      # max_demand is highly resource dependent
      ConsumerSupervisor.init(children,
        strategy: :one_for_one,
        subscribe_to: [{ProducerStage, max_demand: 50}]
      )
    end
  end

  describe "when in flow from_stage" do
    test "will process just a few events" do
      assert {:ok, _} = start_supervised({TestConsumerSupervisor, self()})

      :ok =
        GenStage.call(
          @producer_name,
          {:notify, Enum.to_list(1..10)}
        )

      assert_receive 1
      assert_receive 2
      assert_receive 10
    end

    test "will consume as as many events as there is demand" do
      _ = start_supervised({TestConsumerSupervisor, self()})

      events = Enum.to_list(1..10_000)

      :ok =
        GenStage.call(
          @producer_name,
          {:notify, events}
        )

      assert_receive 1
      assert_receive 2
      assert_receive 3
      assert_receive 10_000, 300
    end

    test "When more demand than events" do
      parent = self()
      _ = start_supervised({TestConsumerSupervisor, parent})

      events = Enum.to_list(1..100)

      :ok =
        GenStage.call(
          @producer_name,
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
          @producer_name,
          {:notify, events}
        )

      assert_receive 1
      assert_receive 10
      assert_receive 100
    end
  end
end
