defmodule Sample.Application do
  use Application
  require KaufmannEx

  def start(_type, _args) do
    children = [KaufmannEx.Supervisor]

    opts = [strategy: :one_for_one, name: Sample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
