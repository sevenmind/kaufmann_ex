defmodule KaufmannEx.Publisher.TopicSelector do
  @moduledoc """
  Topic and partition selection stage.

  if `topic: :callback` the value of `context: %{callback_topic: callback_topic}`
  will be used as the publication topic. If callback_topic is nil or empty then
  no callback event will be published.

  Topics may be specified as a map or binary.


  ## Examples

      # when map topic
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: %{topic: "test", partition: 0}})
      [%KaufmannEx.Publisher.Request{topic: "test", partition: 0}]

      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: %{topic: "test", format: :json}})
      [%KaufmannEx.Publisher.Request{topic: "test", format: :json}]

      # When binary topic
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: "Test"})
      [%KaufmannEx.Publisher.Request{topic: "Test"}]

      # When nil callback topic
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: :callback, context: %{callback_topic: nil}})
      []

      # When nil context
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: :callback, context: nil})
      []

      # when defined callback_topic
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: :callback, context: %{callback_topic: %{topic: "topic", partition: 10}}})
      [%KaufmannEx.Publisher.Request{topic: "topic", partition: 10, context: %{callback_topic: %{partition: 10, topic: "topic"}}}]

      # when topic :default
      iex> Application.put_env(:kaufmann_ex, :default_topic, "default_topic")
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: :default})
      [%KaufmannEx.Publisher.Request{topic: "default_topic"}]

      # when topic nil use default
      iex> Application.put_env(:kaufmann_ex, :default_topic, "default_topic")
      iex> KaufmannEx.Publisher.TopicSelector.resolve_topic(%KaufmannEx.Publisher.Request{topic: nil})
      [%KaufmannEx.Publisher.Request{topic: "default_topic"}]
  """

  require Logger
  alias KaufmannEx.Config
  alias KaufmannEx.Publisher.Request

  @spec resolve_topic(KaufmannEx.Publisher.Request.t()) :: [KaufmannEx.Publisher.Request.t()]

  def resolve_topic(%Request{topic: topic} = request) when is_map(topic) do
    [Map.merge(request, topic)]
  end

  def resolve_topic(%Request{topic: topic} = request) when is_binary(topic), do: [request]

  def resolve_topic(%Request{topic: :callback, context: %{callback_topic: callback}} = request)
      when not is_nil(callback) and callback != %{} do
    [Map.merge(request, callback)]
  end

  def resolve_topic(%Request{topic: :callback, context: _} = _), do: []

  def resolve_topic(%Request{topic: topic} = request) when topic == :default or is_nil(topic) do
    [%Request{request | topic: Config.default_topic()}]
  end
end
