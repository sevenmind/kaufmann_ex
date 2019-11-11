defmodule KaufmannEx.Publisher.Request do
  @moduledoc """
  A Struct wrapping a publish request
  """
  defstruct(
    event_name: nil,
    payload: nil,
    metadata: nil,
    partition: nil,
    encoded: nil,
    context: %{},
    format: :default,
    topic: :default,
    worker_name: nil
  )

  @type t :: %__MODULE__{
          event_name: atom | binary,
          payload: Map | binary,
          metadata: Map,
          context: Map,
          topic: binary | atom | map,
          partition: non_neg_integer | nil,
          format: atom,
          encoded: binary | nil,
          worker_name: atom | nil
        }
end
