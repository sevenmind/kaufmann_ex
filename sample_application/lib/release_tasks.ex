defmodule Sample.ReleaseTasks do
  def migrate_schemas do
    Application.load(:kaufmann_ex)
    KaufmannEx.ReleaseTasks.migrate_schemas(:sample)
  end

  def reinit_service do
    Application.load(:kaufmann_ex)
    KaufmannEx.ReleaseTasks.reinit_service(:sample)
  end
end
