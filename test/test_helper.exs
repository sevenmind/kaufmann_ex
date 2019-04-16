ExUnit.start()

{:ok, _} = Application.ensure_all_started(:httpoison)
{:ok, _} = Application.ensure_all_started(:bypass)
{:ok, _} = Application.ensure_all_started(:telemetry)
{:ok, _} = Application.ensure_all_started(:memoize)
