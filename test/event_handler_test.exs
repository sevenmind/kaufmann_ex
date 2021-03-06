defmodule KaufmannEx.EventHandlerTest do
  use ExUnit.Case
  alias KafkaEx.Protocol.Fetch.Message
  alias KaufmannEx.EventHandler
  alias KaufmannEx.Publisher.Request
  alias KaufmannEx.Schemas.ErrorEvent
  alias KaufmannEx.Schemas.Event
  alias KaufmannEx.TestSupport.MockBus

  @topic :rapids
  @partition 11

  defmodule TestEventHandler do
    use KaufmannEx.EventHandler
    alias KaufmannEx.Schemas.Event

    def given_event(%Event{name: :"event.with.response", payload: pl}) do
      {:reply, [{:response_event, pl}]}
    end

    def given_event(%Event{name: :"event.with.response.topic", payload: pl}) do
      {:reply, [{:response_event, pl, "some_topic"}]}
    end

    def given_event(%Event{name: :"test.event.error", payload: "raise_error"} = event) do
      raise ArgumentError, "You know what you did"
    rescue
      error ->
        {:error, error.message}
    end

    def given_event(%{name: :"another.test.event"}), do: {:noreply, []}

    def given_event(%{name: "a.string.name" <> _}), do: {:reply, [{:response_event, %{}}]}

    def given_event(%{name: "with.a.json.response"}),
      do:
        {:reply,
         [
           %{
             event: "json.response.event",
             payload: %{timestamp: DateTime.utc_now()},
             topics: [:callback, "rapids", %{topic: :default, format: :json}]
           }
         ]}

    def given_event(%Event{name: _name} = event) do
      {:noreply, []}
    end
  end

  test "defines &handled_events/0" do
    assert TestEventHandler.handled_events() |> Enum.sort() == [
             "a.string.name",
             "all",
             "another.test.event",
             "event.with.response",
             "event.with.response.topic",
             "test.event.error",
             "with.a.json.response"
           ]
  end

  describe "&given_event/1" do
    test "returns noreply tuple" do
      assert TestEventHandler.given_event(%Event{name: :"another.test.event"}) == {:noreply, []}

      assert TestEventHandler.given_event(%Event{name: :"completely.unhandled.event"}) ==
               {:noreply, []}
    end

    test "returns reply tuple" do
      pl = %{}

      assert TestEventHandler.given_event(%Event{name: :"event.with.response", payload: pl}) ==
               {:reply, [{:response_event, pl}]}

      assert TestEventHandler.given_event(%Event{name: :"event.with.response.topic", payload: pl}) ==
               {:reply, [{:response_event, pl, "some_topic"}]}
    end
  end

  describe "&handle_event/1" do
    test "transforms reply tuple to response events" do
      assert [
               %Request{
                 event_name: :response_event,
                 payload: %{},
                 metadata: _
               }
             ] =
               EventHandler.handle_event(
                 %Event{name: :"event.with.response", payload: %{}, meta: %{}},
                 event_handler: TestEventHandler
               )
    end

    test "transforms reply tuple with topic" do
      assert [%Request{event_name: :response_event, topic: "some_topic"}] =
               EventHandler.handle_event(
                 %Event{
                   name: :"event.with.response.topic",
                   payload: %{},
                   meta: %{}
                 },
                 event_handler: TestEventHandler
               )
    end

    test "wraps exceptions into ErrorEvent" do
      assert [
               %Request{
                 event_name: "event.error.test.event.error",
                 payload: %{
                   error: "You know what you did"
                 }
               }
               | _
             ] =
               EventHandler.handle_event(
                 %Event{
                   name: :"test.event.error",
                   payload: "raise_error",
                   meta: %{}
                 },
                 event_handler: TestEventHandler
               )
    end

    test "handles binary event names" do
      assert [
               %Request{
                 event_name: :response_event,
                 payload: %{}
               }
             ] =
               EventHandler.handle_event(
                 %Event{
                   name: "a.string.name#with_an_id",
                   payload: %{},
                   meta: %{}
                 },
                 event_handler: TestEventHandler
               )
    end

    test "when multiple topics are specified" do
      res =
        EventHandler.handle_event(
          %Event{
            name: "with.a.json.response",
            payload: %{},
            meta: %{}
          },
          event_handler: TestEventHandler
        )

      assert length(res) == 3

      assert Enum.map(res, &Map.get(&1, :topic)) == [
               :callback,
               "rapids",
               :default
             ]
    end
  end
end
