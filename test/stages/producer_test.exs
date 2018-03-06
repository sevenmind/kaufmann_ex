defmodule KaufmannEx.Stages.ProducerTest do
  use ExUnit.Case

  setup do
    {:ok, _} = KaufmannEx.Stages.Producer.start_link([])

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
      from = {self(), :test}

      {reply, next_message_set, state} =
        KaufmannEx.Stages.Producer.handle_demand(5, %{
          message_set: [1, 2, 3, 4, 5, 6],
          demand: 5,
          from: from
        })

      {reply, next_message_set, state} = KaufmannEx.Stages.Producer.handle_demand(0, state)
      assert next_message_set == []
      assert state == %{demand: 0, message_set: [6], from: from}

      # Should happen
      # assert_receive({:ok, :test}, 100, "reply if all events consumed")

      {reply, next_message_set, state} = KaufmannEx.Stages.Producer.handle_demand(1, state)
      assert next_message_set == [6]
      assert state == %{demand: 0, message_set: [], from: from}
    end
  end

  defmodule TestSubscriber do
    use GenServer

    def start_link(parent, demand \\ 5) do
      GenServer.start_link(__MODULE__, {parent, demand}, name: __MODULE__)
    end

    def init({parent, demand}) do
      {:ok, {parent, demand}, 0}
    end

    def handle_info(:timeout, {parent, demand}) do
      handle_messages({parent, demand})

      {:noreply, {parent, demand}}
    end

    def handle_messages({parent, demand}) do
      Flow.from_stage(KaufmannEx.Stages.Producer, stages: 1)
      |> Flow.each(&send(parent, &1))
      |> Enum.take(demand)
    end
  end

  describe "when in flow from_stage" do
    test "will process just a few events" do
      parent = self()
      {:ok, pid} = TestSubscriber.start_link(parent)

      events = Enum.to_list(1..100)

      :ok = KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 100
    end

    test "will consume as as many events as there is demand " do
      parent = self()
      {:ok, pid} = TestSubscriber.start_link(parent, 10_000)

      events = Enum.to_list(1..10_000)

      KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 3
      assert_receive 10_000, 100, "consumes all available events"
    end

    test "When more demand than events" do
      parent = self()
      {:ok, pid} = TestSubscriber.start_link(parent, 10_000)

      events = Enum.to_list(1..100)

      KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 100, 100, "consumes all available events"
    end

    test "when less events than demand" do
      parent = self()
      {:ok, pid} = TestSubscriber.start_link(parent, 10)

      events = Enum.to_list(1..100)

      KaufmannEx.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 10
      assert_receive 100, 100, "consumes all available events"
    end
  end
end
