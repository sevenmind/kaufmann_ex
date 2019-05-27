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

  def resolve_topic(%Request{topic: :callback, context: %{callback_topic: callback}} = request)
      when not is_nil(callback) and callback != %{} do
    [Map.merge(request, callback)]
  end

  def resolve_topic(%Request{topic: :default, context: %{callback_topic: callback}} = request)
      when not is_nil(callback) and callback != %{} do
    defaults =
      Enum.map(KaufmannEx.Config.default_topics(), fn topic ->
        %Request{request | topic: topic}
      end)

    [Map.merge(request, callback) | defaults]
  end

  def resolve_topic(%Request{topic: :default} = request) do
    Enum.map(KaufmannEx.Config.default_topics(), fn topic ->
      %Request{request | topic: topic}
    end)
  end

  def resolve_topic(%Request{topic: %{topic: topic, format: format}} = request) do
    [%Request{request | topic: topic, format: format}]
  end

  def resolve_topic(%Request{topic: topic} = request) when is_binary(topic), do: request
end
