defmodule KaufmannEx.Publisher.TopicSelectorTest do
  use ExUnit.Case
  alias KaufmannEx.Publisher.TopicSelector

  describe "topic from metadata" do
    test "publishes valid message" do
      context = %{
        callback_topic: %{topic: "test_topic"}
      }

      assert {:ok, "test_topic"} ==
               TopicSelector.choose_topic("event.test", context, :ignored)
    end
  end

  describe "topic from message namespace" do
    test "from query event" do
      event_name = :"query.req.library.catalog.bibliographies.search"
      {:ok, topic} = TopicSelector.choose_topic(event_name, %{}, :event_namespace)
      assert topic == "library.catalog"
    end

    test "from error event" do
      event_name = :"event.error.library.catalog"

      {:ok, topic} = TopicSelector.choose_topic(event_name, %{}, :event_namespace)
      assert topic == "error.library"
    end

    test "from event" do
      event_name = :"command.library.catalog.bibliographies.update"

      {:ok, topic} = TopicSelector.choose_topic(event_name, %{}, :event_namespace)
      assert topic == "library.catalog"
    end
  end

  describe "default" do
    test "default" do
      default = KaufmannEx.Config.default_topic()
      {:ok, topic} = TopicSelector.choose_topic("", %{}, :default)
      assert default == topic
    end
  end
end
