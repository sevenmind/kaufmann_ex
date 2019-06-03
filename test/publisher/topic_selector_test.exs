defmodule KaufmannEx.Publisher.Stage.TopicSelectorTest do
  @moduledoc false

  use ExUnit.Case
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Publisher.TopicSelector
  alias KaufmannEx.Schemas.Event

  import Mock

  setup do
    Application.put_env(:kaufmann_ex, :default_topic, "default_topic")
    :ok
  end

  describe "resolve_topic/1" do
    test "selects default topic when no topic specified" do
      assert [
               %KaufmannEx.Publisher.Request{
                 context: %{},
                 encoded: nil,
                 event_name: :whatever,
                 format: nil,
                 metadata: nil,
                 partition: nil,
                 payload: %{},
                 topic: "default_topic"
               }
             ] =
               TopicSelector.resolve_topic(%Request{
                 event_name: :whatever,
                 payload: %{},
                 context: %{}
               })
    end

    test "selects default topic when :default" do
      assert [
               %KaufmannEx.Publisher.Request{
                 context: %{},
                 encoded: nil,
                 event_name: :whatever,
                 format: nil,
                 metadata: nil,
                 partition: nil,
                 payload: %{},
                 topic: "default_topic"
               }
             ] =
               TopicSelector.resolve_topic(%Request{
                 event_name: :whatever,
                 payload: %{},
                 context: %{},
                 topic: :default
               })
    end

    test "respects passed topic" do
      assert [
               %KaufmannEx.Publisher.Request{
                 format: nil,
                 partition: nil,
                 topic: "specified_topic"
               }
             ] =
               TopicSelector.resolve_topic(%Request{
                 event_name: :whatever,
                 topic: "specified_topic"
               })
    end

    test "selects callback topic when :callback" do
      assert [
               %KaufmannEx.Publisher.Request{
                 partition: 0,
                 context: %{},
                 topic: "test_callback"
               }
             ] =
               TopicSelector.resolve_topic(%Request{
                 event_name: :whatever,
                 payload: %{},
                 topic: :callback,
                 context: %{
                   callback_topic: %{
                     topic: "test_callback",
                     partition: 0
                   }
                 }
               })
    end

    test "selects topic specifying topics and format" do
      assert [
               %KaufmannEx.Publisher.Request{
                 format: :json,
                 topic: "specified_topic"
               }
             ] =
               TopicSelector.resolve_topic(%Request{
                 event_name: :whatever,
                 payload: %{},
                 topic: %{
                   topic: "specified_topic",
                   format: :json
                 }
               })
    end
  end
end
