defmodule Sample.EventHandlerTest do
  use KaufmannEx.TestSupport.MockBus

  test "Events Can Be published & observed" do
    given_event("command.test", %{message: "Hello World"})

    assert %{
             payload: %{message: "Hello World"}
           } = then_event("event.test")

    Enum.each(0..10, fn _ ->
      then_event("another.event.here.test")
    end)
  end
end
