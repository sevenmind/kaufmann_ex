defmodule Sample.Application do
  use Application
  require KaufmannEx

  def start(_type, _args) do
    # Ensure default topic exists
    KafkaEx.metadata(topic: KaufmannEx.Config.default_topic())

    children = [KaufmannEx.Supervisor, KaufmannEx.TelemetryLogger]

    opts = [strategy: :one_for_one, name: Sample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
