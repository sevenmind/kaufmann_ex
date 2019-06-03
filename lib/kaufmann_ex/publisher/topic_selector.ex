defmodule KaufmannEx.Publisher.TopicSelector do
  @moduledoc """
  Topic and partition selection stage.

  If event context includes a callback topic, the passed message will be duplicated
  and published to the default topic and the callback topic, maybe it should only be
  published on the callback topic?
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

  def resolve_topic(%Request{topic: topic} = request) when topic == :default or is_nil(topic) do
    [%Request{request | topic: Config.default_topic()}]
  end
end
