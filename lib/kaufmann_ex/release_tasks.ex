defmodule KaufmannEx.ReleaseTasks do
  @moduledoc """
  Release Tasks intended to be used as [distillery custom commands](https://hexdocs.pm/distillery/custom-commands.html#content).

  """
  def migrate_schemas(app) do
    KaufmannEx.ReleaseTasks.MigrateSchemas.migrate_schemas(app)
  end

  def reset_schemas(app) do
    KaufmannEx.ReleaseTasks.MigrateSchemas.reset_schemas(app)
  end

  @doc """
  Release Task for recreating a service.

  Will replay events without emission until the specified offset.
  """
  @spec reinit_service(atom, number, number | atom, boolean) :: {:ok, any}
  def reinit_service(app, starting_offset \\ 0, target_offset \\ :latest, publish \\ false) do
    KaufmannEx.ReleaseTasks.ReInit.run(app, starting_offset, target_offset, publish)
  end
end
