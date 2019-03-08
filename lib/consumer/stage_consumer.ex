# defmodule KaufmannEx.GenStageConsumer do
#   use GenStage

#   alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
#   alias KafkaEx.Protocol.OffsetCommit.Response, as: OffsetCommitResponse
#   alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
#   alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse
#   alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
#   alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
#   alias KafkaEx.Protocol.Fetch.Message

#   defmodule State do
#     @moduledoc false
#     defstruct [
#       :consumer_state,
#       :commit_interval,
#       :commit_threshold,
#       :worker_name,
#       :group,
#       :topic,
#       :partition,
#       :current_offset,
#       :committed_offset,
#       :acked_offset,
#       :last_commit,
#       :auto_offset_reset,
#       :fetch_options,
#       :sync_status
#     ]
#   end

#   @commit_interval 5_000
#   @commit_threshold 100
#   @auto_offset_reset :none

#   @producer_options []
#   @sync_status :async_commit

#   def init({group_name, topic, partition, opts}) do
#     producer_options =
#       Keyword.get(
#         opts,
#         :producer_options,
#         Application.get_env(:kaufmann_ex, :producer_options, @producer_options)
#       )

#     commit_interval =
#       Keyword.get(
#         opts,
#         :commit_interval,
#         Application.get_env(:kafka_ex, :commit_interval, @commit_interval)
#       )

#     commit_threshold =
#       Keyword.get(
#         opts,
#         :commit_threshold,
#         Application.get_env(:kafka_ex, :commit_threshold, @commit_threshold)
#       )

#     auto_offset_reset =
#       Keyword.get(
#         opts,
#         :auto_offset_reset,
#         Application.get_env(:kafka_ex, :auto_offset_reset, @auto_offset_reset)
#       )

#     sync_status =
#       Keyword.get(
#         opts,
#         :sync_status,
#         Application.get_env(:kafka_ex, :sync_status, @sync_status)
#       )

#     extra_consumer_args =
#       Keyword.get(
#         opts,
#         :extra_consumer_args
#       )

#     worker_opts = Keyword.take(opts, [:uris])

#     {:ok, worker_name} =
#       KafkaEx.create_worker(
#         :no_name,
#         [consumer_group: group_name] ++ worker_opts
#       )

#     default_fetch_options = [
#       auto_commit: false,
#       worker_name: worker_name
#     ]

#     given_fetch_options = Keyword.get(opts, :fetch_options, [])
#     fetch_options = Keyword.merge(default_fetch_options, given_fetch_options)

#     state = %State{
#       consumer_state: consumer_state,
#       commit_interval: commit_interval,
#       commit_threshold: commit_threshold,
#       auto_offset_reset: auto_offset_reset,
#       worker_name: worker_name,
#       group: group_name,
#       topic: topic,
#       partition: partition,
#       fetch_options: fetch_options,
#       sync_status: sync_status
#     }

#     Process.flag(:trap_exit, true)

#     {:producer, state, producer_options}
#   end

#   def handle_demand(
#         demand,
#         %State{
#           topic: topic,
#           partition: partition,
#           current_offset: offset,
#           fetch_options: fetch_options
#         } = state
#       )
#       when demand > 0 do
#     [
#       %FetchResponse{
#         topic: ^topic,
#         partitions: [
#           response = %{error_code: error_code, partition: ^partition, message_set: message_set}
#         ]
#       }
#     ] =
#       KafkaEx.fetch(
#         topic,
#         partition,
#         Keyword.merge(fetch_options, offset: offset)
#       )

#     state =
#       case error_code do
#         :offset_out_of_range ->
#           handle_offset_out_of_range(state)

#         :no_error ->
#           state
#       end

#     [state, sync_status] =
#       case List.last(message_set) do
#         %Message{offset: nil} ->
#           [state, :async_commit]

#         %Message{offset: last_offset} ->
#           [
#             %State{
#               state
#               | acked_offset: last_offset + 1,
#                 current_offset: last_offset + 1
#             },
#             state.sync_status
#           ]
#       end

#     # Todo allow more flexible sync_status
#     {:noreply, message_set, handle_commit(sync_status, state)}
#   end

#   @doc """
#   Returns the topic and partition id for this consumer process
#   """
#   @spec partition(GenServer.server()) ::
#           {topic :: binary, partition_id :: non_neg_integer}
#   def partition(gen_consumer) do
#     GenServer.call(gen_consumer, :partition)
#   end

#   def handle_call(:partition, _from, state) do
#     {:reply, {state.topic, state.partition}, state, 0}
#   end

#   def handle_info(
#         :timeout,
#         %State{current_offset: nil, last_commit: nil} = state
#       ) do
#     new_state = %State{
#       load_offsets(state)
#       | last_commit: :erlang.monotonic_time(:milli_seconds)
#     }

#     {:noreply, new_state, 0}
#   end

#   def handle_info(:timeout, %State{} = state) do
#     {:noreply, state, 0}
#   end

#   def terminate(_reason, %State{} = state) do
#     commit(state)
#     Process.unlink(state.worker_name)
#     KafkaEx.stop_worker(state.worker_name)
#   end

#   # Helpers

#   defp handle_offset_out_of_range(
#          %State{
#            worker_name: worker_name,
#            topic: topic,
#            partition: partition,
#            auto_offset_reset: auto_offset_reset
#          } = state
#        ) do
#     [
#       %OffsetResponse{
#         topic: ^topic,
#         partition_offsets: [
#           %{partition: ^partition, error_code: :no_error, offset: [offset]}
#         ]
#       }
#     ] =
#       case auto_offset_reset do
#         :earliest ->
#           KafkaEx.earliest_offset(topic, partition, worker_name)

#         :latest ->
#           KafkaEx.latest_offset(topic, partition, worker_name)

#         _ ->
#           raise "Offset out of range while consuming topic #{topic}, partition #{partition}."
#       end

#     %State{
#       state
#       | current_offset: offset,
#         committed_offset: offset,
#         acked_offset: offset
#     }
#   end

#   defp handle_commit(:sync_commit, %State{} = state), do: commit(state)

#   defp handle_commit(
#          :async_commit,
#          %State{
#            acked_offset: acked,
#            committed_offset: committed,
#            commit_threshold: threshold,
#            last_commit: last_commit,
#            commit_interval: interval
#          } = state
#        ) do
#     case acked - committed do
#       0 ->
#         %State{state | last_commit: :erlang.monotonic_time(:milli_seconds)}

#       n when n >= threshold ->
#         commit(state)

#       _ ->
#         if :erlang.monotonic_time(:milli_seconds) - last_commit >= interval do
#           commit(state)
#         else
#           state
#         end
#     end
#   end

#   defp commit(%State{acked_offset: offset, committed_offset: offset} = state) do
#     state
#   end

#   defp commit(
#          %State{
#            worker_name: worker_name,
#            group: group,
#            topic: topic,
#            partition: partition,
#            acked_offset: offset
#          } = state
#        ) do
#     request = %OffsetCommitRequest{
#       consumer_group: group,
#       topic: topic,
#       partition: partition,
#       offset: offset
#     }

#     [%OffsetCommitResponse{topic: ^topic, partitions: [^partition]}] =
#       KafkaEx.offset_commit(worker_name, request)

#     Logger.debug(fn ->
#       "Committed offset #{topic}/#{partition}@#{offset} for #{group}"
#     end)

#     %State{
#       state
#       | committed_offset: offset,
#         last_commit: :erlang.monotonic_time(:milli_seconds)
#     }
#   end

#   defp load_offsets(
#          %State{
#            worker_name: worker_name,
#            group: group,
#            topic: topic,
#            partition: partition
#          } = state
#        ) do
#     request = %OffsetFetchRequest{
#       consumer_group: group,
#       topic: topic,
#       partition: partition
#     }

#     [
#       %OffsetFetchResponse{
#         topic: ^topic,
#         partitions: [
#           %{partition: ^partition, error_code: error_code, offset: offset}
#         ]
#       }
#     ] = KafkaEx.offset_fetch(worker_name, request)

#     case error_code do
#       :no_error ->
#         %State{
#           state
#           | current_offset: offset,
#             committed_offset: offset,
#             acked_offset: offset
#         }

#       :unknown_topic_or_partition ->
#         [
#           %OffsetResponse{
#             topic: ^topic,
#             partition_offsets: [
#               %{partition: ^partition, error_code: :no_error, offset: [offset]}
#             ]
#           }
#         ] = KafkaEx.earliest_offset(topic, partition, worker_name)

#         %State{
#           state
#           | current_offset: offset,
#             committed_offset: offset,
#             acked_offset: offset
#         }
#     end
#   end
# end
