defmodule KaufmannEx.Stages.ProducerTest do
  use ExUnit.Case

  setup do
    case KaufmannEx.Stages.Producer.start_link([]) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      _ -> raise RuntimeError
    end

    :ok
  end

  describe "handle_demand" do
    test "when no messages and demand > 0" do
      # Is this actually a useful thing to test
      res = KaufmannEx.Stages.Producer.handle_demand(10, %{message_set: [], demand: 10})
      assert res == {:noreply, [], %{demand: 10, message_set: []}}
    end

    test "when messages > demand && demand > 0" do
      res =
        KaufmannEx.Stages.Producer.handle_demand(5, %{message_set: [1, 2, 3, 4, 5, 6], demand: 5})

      assert res == {:noreply, [1, 2, 3, 4, 5], %{demand: 0, message_set: [6]}}
      # State is the remaining message less than demand
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

    test "When demand changes over multiple calls" do
      from = MapSet.new([{self(), :test}])

      {reply, next_message_set, state} =
        KaufmannEx.Stages.Producer.handle_demand(5, %{
          message_set: [1, 2, 3, 4, 5, 6],
          demand: 5,
          from: from
        })

      assert {:noreply, [], %{demand: 0, message_set: [6], from: ^from}} = KaufmannEx.Stages.Producer.handle_demand(0, state)

      assert {:noreply, [6], %{demand: 0, message_set: [], from: %MapSet{}}} = KaufmannEx.Stages.Producer.handle_demand(1, state)

      # Should happen
      assert_receive({:test, :ok})
    end
  end

  defmodule TestEventHandler do
    def start_link(parent, n) do
      Task.start_link(fn ->
        send(parent, n)
      end)
    end
  end

  defmodule TestConsumer do
    use ConsumerSupervisor

    def start_link(parent) do
      ConsumerSupervisor.start_link(__MODULE__, parent)
    end

    def init(parent) do
      children = [
        worker(TestEventHandler, [parent], restart: :temporary)
      ]

      # max_demand is highly resource dependent
      {:ok, children,
       strategy: :one_for_one, subscribe_to: [{KaufmannEx.Stages.Producer, max_demand: 50}]}
    end
  end

  describe "when in flow from_stage" do
    test "will process just a few events" do
      parent = self()
      _ = TestConsumer.start_link(parent)

      events = Enum.to_list(1..10)

      :ok = KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 10
    end

    test "will consume as as many events as there is demand " do
      parent = self()
      _ = TestConsumer.start_link(parent)

      events = Enum.to_list(1..10_000)

      KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 3
      assert_receive 10_000, 100, "consumes all available events"
    end

    test "When more demand than events" do
      parent = self()
      _ = TestConsumer.start_link(parent)

      events = Enum.to_list(1..100)

      KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 100, 100, "consumes all available events"
    end

    test "when less events than demand" do
      parent = self()
      _ = TestConsumer.start_link(parent)

      events = Enum.to_list(1..100)

      KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 10
      assert_receive 100, 100, "consumes all available events"
    end
  end
end
