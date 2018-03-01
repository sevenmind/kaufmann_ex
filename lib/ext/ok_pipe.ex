defmodule OkPipe do
  @moduledoc """
    Convience method for piping results from functions that return `{:ok, result}`

    For Example: 
    ```
    def mymethod(args) do
      {:ok, res_a} = ModuleM.method(args)
      {:ok, res_b} = ModuleN.read_from_file(res_a)
      {:ok, final_res} = ModuleO.another_method(res_b)
    end
    ```

    can become 
    ```
    defmodule MyModule do
      use OkPipe
      
      def mymethod(args) do
        args
        |> ModuleM.method()
        ~> ModuleN.read_from_file()
        ~> ModuleO.another_method()
      end
    end
    ```
  """

  defmacro __using__(_) do
    quote do
      require unquote(__MODULE__)
      import unquote(__MODULE__)
    end
  end

  @doc """
  Asserts left produces a result of `{:ok, result}`.  Pipes `result` as first arg to righthand side.

  Mostly usedful if you really don't want to use a `with/1` form.
  """
  defmacro left ~> right do
    quote do
      case unquote(left) do
        {:ok, product} ->
          product |> unquote(right)

        product ->
          product |> unquote(right)
      end
    end
  end
end
