defmodule KaufmannEx.Publisher.Request do
  @moduledoc """
  A Struct wrapping a publish request
  """
  defstruct [:event_name, :body, :context, :topic, :partition, :encoded]

  @type t :: %__MODULE__{
          # ?
          event_name: atom | binary,
          body: Map,
          context: Map,
          topic: binary | nil,
          partition: non_neg_integer | nil,
          encoded: binary | nil
        }
end
