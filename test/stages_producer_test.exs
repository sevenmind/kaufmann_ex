defmodule Kaufmann.Stages.ProducerTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = Kaufmann.Stages.Producer.start_link([])

    on_exit(fn ->
      GenStage.stop(Kaufmann.Stages.Producer)
    end)

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

  test "Times out without a consumer" do
    #  Should Be based on Consumer demand on the other side of the GenStage
    # assert_raise(TimeOut, fn ->
    #   Kaufmann.Stages.Producer.notify([1, 2, 3, 4, 5, 6, 7, 8, 9], 100)
    # end)
  end

  test "With a configured consumer" do
    parent = self()
    # |> Flow.start_link()
    res =
      Flow.from_stage(Kaufmann.Stages.Producer, stages: 1)
      |> Flow.each(&send(parent, &1))
      |> Enum.take(5)

    # IO.puts("before notify")
    res_1 = Kaufmann.Stages.Producer.notify([1, 2, 3, 4, 5, 6, 7, 8, 9])

    assert_received 1
    assert_received 2
    assert_received 3

    IO.inspect(res)
    IO.inspect(res_1)
  end
end
