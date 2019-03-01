defmodule KaufmannEx.Consumer.GenConsumerTest do
  use ExUnit.Case

  @topic "rapids"
  @partition 0

  setup do
    {:ok, _} = start_supervised({Registry, keys: :unique, name: Registry.ConsumerRegistry})

    :ok
  end

  describe "init/2" do
    test "starts stage supervisor & children" do
      start_supervised(KaufmannEx.Publisher.Producer)

      assert {:ok,
              %{
                commit_strategy: :async_commit,
                partition: @partition,
                supervisor: supervisor_pid,
                topic: @topic
              }} = KaufmannEx.Consumer.GenConsumer.init(@topic, @partition)

      assert [
               {KaufmannEx.Publisher.Stage.PublishSupervisor, _, :supervisor,
                [KaufmannEx.Publisher.Stage.PublishSupervisor]},
               {KaufmannEx.Publisher.Stage.TopicSelector, _, :worker,
                [KaufmannEx.Publisher.Stage.TopicSelector]},
               {KaufmannEx.Publisher.Stage.Encoder, _, :worker,
                [KaufmannEx.Publisher.Stage.Encoder]},
               {KaufmannEx.Consumer.Stage.EventHandler, _, :worker,
                [KaufmannEx.Consumer.Stage.EventHandler]},
               {KaufmannEx.Consumer.Stage.Decoder, _, :worker,
                [KaufmannEx.Consumer.Stage.Decoder]},
               {KaufmannEx.Consumer.Stage.Producer, _, :worker,
                [KaufmannEx.Consumer.Stage.Producer]}
             ] = Supervisor.which_children(supervisor_pid)
    end
  end

  describe "handle_message_set/2" do
    test "notifies producer stage" do
      Registry.register(
        Registry.ConsumerRegistry,
        KaufmannEx.StageSupervisor.build_stage_name(
          KaufmannEx.Consumer.Stage.Producer,
          @topic,
          @partition
        ),
        self()
      )

      state = %{
        topic: @topic,
        partition: @partition
      }

      message_set = ["hi"]

      # launder genserver call to self through spawned process.
      spawn_link(fn ->
        assert {:async_commit, ^state} =
                 KaufmannEx.Consumer.GenConsumer.handle_message_set(message_set, state)
      end)

      assert_receive {:"$gen_call", {_pid, _reg}, {:notify, message_set}}
    end
  end
end
