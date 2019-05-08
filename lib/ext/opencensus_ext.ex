defmodule OpencensusExt.Trace do
  defmacro with_trace(do: block) do
    line = __CALLER__.line
    module = __CALLER__.module
    file = __CALLER__.file
    function = format_function(__CALLER__.function)

    label = "#{module}##{function}"

    computed_attributes =
      compute_attributes([quote(do: %{}), :default], %{
        line: line,
        module: module,
        file: file,
        function: function
      })

    quote do
      parent_span_ctx = :ocp.current_span_ctx()

      new_span_ctx =
        :oc_trace.start_span(unquote(label), parent_span_ctx, %{
          :attributes => unquote(computed_attributes)
        })

      :ocp.with_span_ctx(new_span_ctx)

      try do
        unquote(block)
      after
        :oc_trace.finish_span(new_span_ctx)
        :ocp.with_span_ctx(parent_span_ctx)
      end
    end
  end

  @doc "Wraps the given block in a tracing child span with the given label/name and optional attributes"
  defmacro with_child_span(label, attributes \\ quote(do: %{}), do: block) do
    line = __CALLER__.line
    module = __CALLER__.module
    file = __CALLER__.file
    function = format_function(__CALLER__.function)

    computed_attributes =
      compute_attributes(attributes, %{
        line: line,
        module: module,
        file: file,
        function: function
      })

    quote do
      parent_span_ctx = :ocp.current_span_ctx()

      new_span_ctx =
        :oc_trace.start_span(unquote(label), parent_span_ctx, %{
          :attributes => unquote(computed_attributes)
        })

      :ocp.with_span_ctx(new_span_ctx)

      try do
        unquote(block)
      after
        :oc_trace.finish_span(new_span_ctx)
        :ocp.with_span_ctx(parent_span_ctx)
      end
    end
  end

  defp compute_attributes(attributes, default_attributes) when is_list(attributes) do
    {atoms, custom_attributes} = Enum.split_with(attributes, &is_atom/1)

    default_attributes = compute_default_attributes(atoms, default_attributes)

    case Enum.split_with(custom_attributes, fn
           ## map ast
           {:%{}, _, _} -> true
           _ -> false
         end) do
      {[ca_map | ca_maps], []} ->
        ## custom attributes are literal maps, merge 'em
        {:%{}, meta, custom_attributes} =
          List.foldl(ca_maps, ca_map, fn {:%{}, _, new_pairs}, {:%{}, meta, old_pairs} ->
            {:%{}, meta,
             :maps.to_list(:maps.merge(:maps.from_list(old_pairs), :maps.from_list(new_pairs)))}
          end)

        {:%{}, meta,
         :maps.to_list(:maps.merge(:maps.from_list(custom_attributes), default_attributes))}

      {_ca_maps, _other_calls} ->
        [f_ca | r_ca] = custom_attributes

        quote do
          unquote(
            List.foldl(r_ca ++ [Macro.escape(default_attributes)], f_ca, fn ca, acc ->
              quote do
                Map.merge(unquote(acc), unquote(ca))
              end
            end)
          )
        end
    end
  end

  defp compute_attributes(attributes, _default_attributes) do
    attributes
  end

  defp compute_default_attributes(atoms, default_attributes) do
    List.foldl(atoms, %{}, fn
      :default, _acc ->
        default_attributes

      atom, acc ->
        Map.put(acc, atom, Map.fetch!(default_attributes, atom))
    end)
  end

  defp format_function(nil), do: nil
  defp format_function({name, arity}), do: "#{name}/#{arity}"
end
