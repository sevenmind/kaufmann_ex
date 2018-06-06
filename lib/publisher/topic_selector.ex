defmodule KaufmannEx.Publisher.TopicSelector do
  @moduledoc """
  Methods a Publisher may use to choose which topic to publish an event to

  If a message context includes a callback_topic, the specified callback_topic will be used over :namespace
  """

  @doc """
  Chooses topic from metadata for publishing an event,
  """
  @spec choose_topic(atom, map, atom | function) :: {atom, String.t()}
  def choose_topic(event_name, context, strategy) when is_function(strategy),
    do: strategy.(event_name, context)

  def choose_topic(event_name, %{callback_topic: %{topic: callback_topic}}, strategy)
      when is_binary(callback_topic) do
    {:ok, default_topic} = choose_topic(event_name, %{}, strategy)
    {:ok, [callback_topic, default_topic]}
  end

  def choose_topic(event_name, _context, :event_namespace) do
    {:ok, event_name_to_namespace(event_name)}
  end

  def choose_topic(_, _, _) do
    {:ok, KaufmannEx.Config.default_publish_topic()}
  end

  defp event_name_to_namespace(event_name) do
    event_name
    |> Atom.to_string()
    |> String.split(".")
    |> slice_event_name
    |> Enum.join(".")
  end

  defp slice_event_name(["query" | _] = event_name), do: Enum.slice(event_name, 2, 2)
  defp slice_event_name(event_name), do: Enum.slice(event_name, 1, 2)
end
