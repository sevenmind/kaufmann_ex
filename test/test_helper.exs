ExUnit.start()

{:ok, _} = Application.ensure_all_started(:httpoison)
{:ok, _} = Application.ensure_all_started(:bypass)
