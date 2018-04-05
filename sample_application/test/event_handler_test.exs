defmodule Sample.EventHandlerTest do
  use KaufmannEx.TestSupport.MockBus

  test "Events Can Be published & observed" do
    given_event(:"command.test", %{message: "Hello World"})

    %{
      payload: %{message: "Hello World"}
    } = then_event(:"event.test")
  end
end
