defmodule Kaufmann.Stages.ProducerTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = Kaufmann.Stages.Producer.start_link([])

    :ok
  end

  # describe "handle_demand" do
  #   test "when no messages and demand > 0" do
  #     # Is this actually a useful thing to test
  #     res = Kaufmann.Stages.Producer.handle_demand(10, %{message_set: [], demand: 0})
  #     assert res == {:noreply, [], %{demand: 10, message_set: []}}
  #   end

  #   test "when messages > demand && demand > 0" do
  #     # Shouldn't request more
  #     res =
  #       Kaufmann.Stages.Producer.handle_demand(5, %{message_set: [1, 2, 3, 4, 5, 6], demand: 0})

  #     assert res == {:noreply, [], %{demand: 10, message_set: []}}
  #   end

  #   test "when messages and demand == 0"
  #   test "when no messages, and  no demand"
  # end

  # describe "notify" do
  # end

  # describe "when in series" do
  # end

  # What are the real-world cases we care about here

  # 1. When nothing being processed and nothing demandedc
  # 2. when events accumulating faster than demand can cope (GenServer thing?)
  # 3. What if handling demand takes longer than call timeout?

  # ------------------------------------
  # Need to Test that backpressure is applied all the way to the notifying module

  # test "Times out without a consumer" do
  #   #  Should Be based on Consumer demand on the other side of the GenStage
  #   # assert_raise(TimeOut, fn ->
  #   #   Kaufmann.Stages.Producer.notify([1, 2, 3, 4, 5, 6, 7, 8, 9], 100)
  #   # end)
  # end

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
      Flow.from_stage(Kaufmann.Stages.Producer, stages: 1)
      |> Flow.each(&send(parent, &1))
      |> Enum.take(demand)
    end
  end

  ## Can We use the FlowStage `Kaufmann.Stages.Producer`
  ##
  describe "with a configured consumer" do
    test "just a few events" do
      parent = self()
      {:ok, pid} = TestSubscriber.start_link(parent)

      events = Enum.to_list(1..100)

      :ok = Kaufmann.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 100
    end

    test "will consume as as many events as there is demand " do
      parent = self()
      {:ok, pid} = TestSubscriber.start_link(parent, 10000)

      events = Enum.to_list(1..10000)

      Kaufmann.Stages.Producer.notify(events)

      assert_receive 1
      assert_receive 2
      assert_receive 3
      assert_receive 10000
    end
  end
end
