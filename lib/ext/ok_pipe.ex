defmodule OkPipe do
  @moduledoc """
    Convience method for piping results from functions that return {:ok, result}

    Instead of 
    ```
    def mymethod(args) do
      {:ok, res_a} = ModuleM.method(args)
      {:ok, res_b} = ModuleN.read_from_file(res_a)
      {:ok, final_res} = ModuleO.another_method(res_b)
    end
    ```

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
