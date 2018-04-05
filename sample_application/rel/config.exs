# Import all plugins from `rel/plugins`
# They can then be used by adding `plugin MyPlugin` to
# either an environment, or release definition, where
# `MyPlugin` is the name of the plugin module.
Path.join(["rel", "plugins", "*.exs"])
|> Path.wildcard()
|> Enum.map(&Code.eval_file(&1))

use Mix.Releases.Config,
    # This sets the default release built by `mix release`
    default_release: :default,
    # This sets the default environment used by `mix release`
    default_environment: Mix.env()

# For a full list of config options for both releases
# and environments, visit https://hexdocs.pm/distillery/configuration.html


# You may define one or more environments in this file,
# an environment's settings will override those of a release
# when building in that environment, this combination of release
# and environment configuration is called a profile

environment :dev do
  # If you are running Phoenix, you should make sure that
  # server: true is set and the code reloader is disabled,
  # even in dev mode.
  # It is recommended that you build with MIX_ENV=prod and pass
  # the --env flag to Distillery explicitly if you want to use
  # dev mode.
  set dev_mode: true
  set include_erts: false
  set cookie: :"6S_Ux4=y/nl[7wodOX&>Ra92^rj!u4|>H~[YtPk4S8v!FjE.=ZciF2}g(!Do;F6q"
end

environment :prod do
  set include_erts: true
  set include_src: false
  set cookie: :"m/bp%`2fP*VSlB;LLun4WU.8>yR[x=(|yI,f%Tv8$j1;c2HDgY^z:;F(bLM5rvx`"
end

# You may define one or more releases in this file.
# If you have not set a default release, or selected one
# when running `mix release`, the first release in the file
# will be used by default

release :sample do
  set version: current_version(:sample)
  set applications: [
    :runtime_tools
  ]

  set(
      commands: [
        migrate_schemas: "rel/commands/migrate_schemas.sh",
        reinit_service: "rel/commands/reinit_service.sh"
      ]
    )
end

