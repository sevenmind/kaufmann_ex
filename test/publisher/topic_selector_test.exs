defmodule KaufmannEx.Publisher.TopicSelectorTest do
  use ExUnit.Case
  alias KaufmannEx.Publisher.TopicSelector

  setup do
     Application.put_env(:kaufmann_ex, :default_topic, "default_topic")
    :ok
  end

  describe "topic from metadata" do
    test "publishes valid message" do
      context = %{
        callback_topic: %{topic: "test_callback"}
      }

      assert {:ok, ["test_callback", "default_topic"]} =
               TopicSelector.choose_topic("event.test", context, :default)
    end
  end

  describe "topic from message namespace" do
    test "from query event" do
      event_name = :"query.req.library.catalog.bibliographies.search"
      assert {:ok,  "library.catalog"} = TopicSelector.choose_topic(event_name, %{}, :event_namespace)
    end

    test "from error event" do
      event_name = :"event.error.library.catalog"

      assert {:ok, "error.library"} = TopicSelector.choose_topic(event_name, %{}, :event_namespace)
    end

    test "from event" do
      event_name = :"command.library.catalog.bibliographies.update"

      assert {:ok, "library.catalog"} = TopicSelector.choose_topic(event_name, %{}, :event_namespace)
    end
  end

  describe "default" do
    test "default" do
      default = KaufmannEx.Config.default_topic()
      assert {:ok, ^default} = TopicSelector.choose_topic("", %{}, :default)
    end
  end
end
