defmodule KaufmannEx.Publisher.Stage.TopicSelectorTest do
  @moduledoc false

  use ExUnit.Case
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Publisher.Stage.TopicSelector
  alias KaufmannEx.Schemas.Event

  setup do
    Application.put_env(:kaufmann_ex, :default_topic, "default_topic")
    :ok
  end

  describe "topic selection" do
    test "selects default topic" do
      event = %Event{
        publish_request: %Request{
          event_name: :whatever,
          body: %{},
          context: %{}
        }
      }

      state = %{
        partition_strategy: :default,
        topic_partitions: %{"default_topic" => 1}
      }

      assert {:noreply, [event], _} = TopicSelector.handle_events([event], nil, state)

      assert event.publish_request.topic == "default_topic"
      assert event.publish_request.partition == 0
    end

    test "respects passed topic" do
      event = %Event{
        publish_request: %Request{
          event_name: :whatever,
          body: %{},
          context: %{},
          topic: "specified_topic"
        }
      }

      state = %{
        partition_strategy: :default,
        topic_partitions: %{"specified_topic" => 1}
      }

      assert {:noreply, [event], _} = TopicSelector.handle_events([event], nil, state)

      assert event.publish_request.topic == "specified_topic"
      assert event.publish_request.partition == 0
    end

    test "selects callback topic and default topic" do
      event = %Event{
        publish_request: %Request{
          event_name: :whatever,
          body: %{},
          context: %{
            callback_topic: %{
              topic: "test_callback",
              partition: 0
            }
          }
        }
      }

      state = %{
        partition_strategy: :default,
        topic_partitions: %{"default_topic" => 1}
      }

      assert {:noreply, [callback, event], _} = TopicSelector.handle_events([event], nil, state)

      assert callback.publish_request.topic == "test_callback"
      assert callback.publish_request.partition == 0
      assert event.publish_request.topic == "default_topic"
      assert event.publish_request.partition == 0
    end
  end

  describe "partition selection" do
    test "selects random partition by default" do
      event = %Event{
        publish_request: %Request{
          event_name: :whatever,
          body: %{},
          context: %{}
        }
      }

      state = %{
        partition_strategy: :default,
        topic_partitions: %{"default_topic" => 10}
      }

      assert {:noreply, [publish_request], _} = TopicSelector.handle_events([event], nil, state)

      assert publish_request.publish_request.topic == "default_topic"
      assert publish_request.publish_request.partition >= 0
      assert publish_request.publish_request.partition <= 9
    end

    test "uses md5 to compute partition" do
      event = %Event{
        publish_request: %Request{
          event_name: :whatever,
          body: %{},
          context: %{},
          encoded: "some binary bytes"
        }
      }

      state = %{
        partition_strategy: :md5,
        topic_partitions: %{"default_topic" => 10}
      }

      assert {:noreply, [event], _} = TopicSelector.handle_events([event], nil, state)

      assert event.publish_request.topic == "default_topic"
      assert event.publish_request.partition == 3
    end
  end
end
