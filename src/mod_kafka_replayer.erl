%%%-------------------------------------------------------------------
%%% @doc
%%% Kafka Replayer - Sequential background resending of RocksDB outbox.
%%%
%%% KEY FIXES vs original:
%%%   1. Messages deleted from RocksDB per-message AFTER confirmed Kafka ack
%%%      (not bulk-delete before acks arrive) — prevents message loss on
%%%      partial batch failure or process crash mid-batch.
%%%   2. update_db_handle/2 API: manager calls this on all surviving replayers
%%%      when RocksDB is restarted, so no replayer ever holds a stale handle.
%%%   3. do_replay/1 guards against undefined db_handle.
%%%   4. process_shard wraps iterator open in catch to handle stale handle.
%%%   5. Partial-batch produce failures: only truly failed messages stay in
%%%      RocksDB; successfully acked ones are deleted individually.
%%%   6. brod ack format: handles both brod_produce_reply tuple and {Ref,ok}.
%%%   7. status field properly managed — no premature idle reset during replay.
%%%   8. report_failure API: uses correct 1-arity form.
%%%-------------------------------------------------------------------
-module(mod_kafka_replayer).
-behaviour(gen_server).

%% API
-export([start_link/7, trigger_replay/1, update_db_handle/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("logger.hrl").

-record(state, {
    host                :: binary(),
    index               :: integer(),
    client_name         :: atom(),
    topic               :: binary(),
    batch_size          :: pos_integer(),
    replay_interval     :: pos_integer(),
    status = idle       :: idle | replaying | pending,
    timer               :: reference() | undefined,
    pending_timer       :: reference() | undefined,
    db_handle           :: rocksdb:db_handle() | undefined,
    manager_pid         :: pid() | undefined,

    %% Metrics
    total_replayed = 0  :: non_neg_integer(),
    total_failed = 0    :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, ClientName, Topic, BatchSize, ReplayInterval, Index, DbHandle) ->
    Name = list_to_atom("kafka_replayer_" ++ binary_to_list(Host) ++ "_" ++ integer_to_list(Index)),
    gen_server:start_link({local, Name}, ?MODULE,
                         [Host, ClientName, Topic, BatchSize, ReplayInterval, Index, DbHandle], []).

trigger_replay(ReplayerPid) ->
    gen_server:cast(ReplayerPid, trigger).

%% FIX: Called by manager when RocksDB is restarted with a fresh handle.
%% Without this, a surviving replayer would keep using the closed (stale) handle.
update_db_handle(ReplayerPid, NewDbHandle) ->
    gen_server:cast(ReplayerPid, {update_db_handle, NewDbHandle}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, ClientName, Topic, BatchSize, ReplayInterval, Index, DbHandle]) ->
    process_flag(trap_exit, true),
    ManagerName = list_to_atom(binary_to_list(Host) ++ "_kafka_manager"),
    State = #state{
        host = Host,
        index = Index,
        client_name = ClientName,
        topic = Topic,
        batch_size = BatchSize,
        replay_interval = ReplayInterval,
        manager_pid = whereis(ManagerName),
        db_handle = DbHandle
    },
    TRef = erlang:send_after(ReplayInterval, self(), check_spool),
    {ok, State#state{timer = TRef}}.

handle_call(get_stats, _From, State) ->
    {reply, #{
        status         => State#state.status,
        total_replayed => State#state.total_replayed,
        total_failed   => State#state.total_failed
    }, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% Schedule a deferred replay only when truly idle.
handle_cast(trigger, #state{status = idle, pending_timer = undefined} = State) ->
    TRef = erlang:send_after(20, self(), deferred_trigger),
    {noreply, State#state{status = pending, pending_timer = TRef}};
handle_cast(trigger, State) ->
    %% Already replaying/pending — next scheduled tick will catch it.
    {noreply, State};

%% FIX: propagate fresh db_handle from manager to this replayer.
handle_cast({update_db_handle, NewDbHandle}, State) ->
    ?INFO_MSG("Replayer ~p (host ~s) received new db_handle", [State#state.index, State#state.host]),
    {noreply, State#state{db_handle = NewDbHandle}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(deferred_trigger, State) ->
    NewState = do_replay(State),
    {noreply, NewState#state{status = idle, pending_timer = undefined}};

%% FIX: keep re-arming timer even when replaying so we never miss a cycle.
handle_info(check_spool, #state{status = replaying} = State) ->
    TRef = erlang:send_after(State#state.replay_interval, self(), check_spool),
    {noreply, State#state{timer = TRef}};
handle_info(check_spool, #state{status = pending} = State) ->
    TRef = erlang:send_after(State#state.replay_interval, self(), check_spool),
    {noreply, State#state{timer = TRef}};
handle_info(check_spool, State) ->
    NewState = do_replay(State),
    TRef = erlang:send_after(State#state.replay_interval, self(), check_spool),
    {noreply, NewState#state{status = idle, timer = TRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private helpers
%%%===================================================================

%% FIX: guard against undefined handle — prevents crash on startup race.
do_replay(#state{db_handle = undefined} = State) ->
    ?WARNING_MSG("Replayer ~p (host ~s): db_handle undefined, skipping replay",
                 [State#state.index, State#state.host]),
    State;
do_replay(State) ->
    try_start_replay(State).

try_start_replay(State) ->
    case mod_kafka_manager:get_circuit_status(State#state.manager_pid) of
        open -> State;
        _ ->
            Prefix = <<(State#state.index):16>>,
            process_shard(Prefix, State)
    end.

process_shard(Prefix, State) ->
    %% FIX: wrap iterator open in catch — a stale handle raises an exception.
    case catch rocksdb:iterator(State#state.db_handle, []) of
        {ok, Iterator} ->
            try
                case rocksdb:iterator_move(Iterator, {seek, Prefix}) of
                    {ok, Key, Value} ->
                        case Key of
                            <<Prefix:2/binary, _/binary>> ->
                                replay_batch(Iterator, Key, Value,
                                             State#state.batch_size, State);
                            _ -> State
                        end;
                    _ -> State
                end
            after
                catch rocksdb:iterator_close(Iterator)
            end;
        Error ->
            ?ERROR_MSG("Replayer ~p (host ~s): cannot open RocksDB iterator: ~p",
                       [State#state.index, State#state.host, Error]),
            State
    end.

replay_batch(_Iterator, _Key, _Value, 0, State) ->
    State;
replay_batch(Iterator, Key, Value, Count, State) ->
    Prefix = <<(State#state.index):16>>,
    {Batch, _LastKey} = fetch_batch(Iterator, Key, Value, Count, Prefix, [], State#state.db_handle),

    case Batch of
        [] -> State;
        _ ->
            %% FIX: per-message produce + ack + delete (see send_batch_with_ack).
            {Sent, Failed} = send_batch_with_ack(Batch, State),
            NewState = State#state{
                total_replayed = State#state.total_replayed + Sent,
                total_failed   = State#state.total_failed + Failed
            },

            case Failed > 0 of
                true ->
                    ?WARNING_MSG("Replay partial failure host ~s worker ~p: ~p sent, ~p failed",
                                 [State#state.host, State#state.index, Sent, Failed]),
                    %% Report only once per batch (circuit-breaker logic).
                    mod_kafka_manager:report_failure(State#state.manager_pid),
                    NewState;
                false ->
                    ?INFO_MSG("Replayed batch of ~p messages for host ~s (worker ~p)",
                              [Sent, State#state.host, State#state.index]),
                    %% If we filled the batch, there might be more — continue immediately.
                    case length(Batch) =:= Count of
                        true ->
                            case rocksdb:iterator_move(Iterator, next) of
                                {ok, NextKey, NextValue} ->
                                    case NextKey of
                                        <<Prefix:2/binary, _/binary>> ->
                                            replay_batch(Iterator, NextKey, NextValue,
                                                         Count, NewState);
                                        _ -> NewState
                                    end;
                                _ -> NewState
                            end;
                        false -> NewState
                    end
            end
    end.

fetch_batch(_Iterator, Key, _Value, 0, _Prefix, Acc, _DbHandle) ->
    {lists:reverse(Acc), Key};
fetch_batch(Iterator, Key, Value, Count, Prefix, Acc, DbHandle) ->
    try
        <<PKLen:32, PartitionKey:PKLen/binary, Payload/binary>> = Value,
        NewAcc = [{Key, PartitionKey, Payload} | Acc],
        case rocksdb:iterator_move(Iterator, next) of
            {ok, NextKey, NextValue} ->
                case NextKey of
                    <<Prefix:2/binary, _/binary>> ->
                        fetch_batch(Iterator, NextKey, NextValue, Count - 1, Prefix, NewAcc, DbHandle);
                    _ ->
                        {lists:reverse(NewAcc), NextKey}
                end;
            _ ->
                {lists:reverse(NewAcc), Key}
        end
    catch
        _:ParseErr ->
            ?ERROR_MSG("Corrupt RocksDB entry ~p for host, skipping: ~p", [Key, ParseErr]),
            %% FIX: delete corrupt entry so it doesn't block replay forever.
            catch rocksdb:delete(DbHandle, Key, [{sync, true}]),
            {lists:reverse(Acc), Key}
    end.

%%--------------------------------------------------------------------
%% FIX: send_batch_with_ack/2
%%
%% Original code bulk-deleted messages BEFORE waiting for acks.
%% This version:
%%   - Produces each message to Kafka (with one retry on producer_not_found).
%%   - Waits for each Kafka ack individually.
%%   - Deletes from RocksDB only on confirmed ack.
%%   - Messages that fail produce or ack are left in RocksDB for the
%%     next replay cycle — zero message loss.
%%
%% Returns {SuccessCount, FailCount}.
%%--------------------------------------------------------------------
send_batch_with_ack([], _State) ->
    {0, 0};
send_batch_with_ack(Batch, State) ->
    Partitions = case ets:lookup(mod_kafka, {partitions, State#state.host}) of
        [{_, P}] when is_integer(P), P > 0 -> P;
        _ -> 1
    end,

    %% Phase 1: submit produces, collect {RocksKey, CallRef} pairs.
    {Pending, ImmFails} =
        lists:foldl(fun({RocksKey, PKey, Payload}, {PAcc, FAcc}) ->
            Partition = erlang:phash2(PKey, Partitions),
            case produce_with_retry(State, Partition, PKey, Payload) of
                {ok, CallRef} -> {[{RocksKey, CallRef} | PAcc], FAcc};
                {error, _}    -> {PAcc, FAcc + 1}
            end
        end, {[], 0}, Batch),

    %% Phase 2: wait acks in submission order, delete on success.
    {Sent, AckFails} = wait_acks_and_delete(lists:reverse(Pending), State, 0, 0),
    {Sent, ImmFails + AckFails}.

produce_with_retry(State, Partition, Key, Payload) ->
    case brod:produce(State#state.client_name, State#state.topic, Partition, Key, Payload) of
        {ok, _} = OK ->
            OK;
        {error, Reason} ->
            case is_kafka_down(Reason) of
                true ->
                    %% Ask manager to ensure producer exists, then retry once.
                    catch gen_server:call(State#state.manager_pid, ensure_producer, 5000),
                    brod:produce(State#state.client_name, State#state.topic, Partition, Key, Payload);
                false ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% wait_acks_and_delete/4
%%
%% Waits for brod ack for each pending {RocksKey, CallRef} in order.
%% brod sends acks as:
%%   {brod_produce_reply, Ref, Partition, brod_produce_reply_acked}  -- success
%%   {brod_produce_reply, Ref, Partition, {error, Reason}}           -- failure
%%   {Ref, ok}                                                        -- alt form
%%   {Ref, {error, Reason}}                                           -- alt form
%%
%% FIX: We wait for each ref explicitly. On timeout the message is left
%% in RocksDB (not deleted), and all remaining pending refs are drained
%% as failed without waiting further — preventing indefinite blocking.
%%--------------------------------------------------------------------
wait_acks_and_delete([], _State, Sent, Fails) ->
    {Sent, Fails};
wait_acks_and_delete([{RocksKey, Ref} | Rest], State, Sent, Fails) ->
    receive
        {brod_produce_reply, Ref, _Part, brod_produce_reply_acked} ->
            delete_key(State, RocksKey),
            mod_kafka_manager:report_success(State#state.manager_pid),
            wait_acks_and_delete(Rest, State, Sent + 1, Fails);
        {Ref, ok} ->
            delete_key(State, RocksKey),
            mod_kafka_manager:report_success(State#state.manager_pid),
            wait_acks_and_delete(Rest, State, Sent + 1, Fails);
        {brod_produce_reply, Ref, _Part, {error, AckReason}} ->
            ?WARNING_MSG("Kafka ack error host ~s worker ~p: ~p",
                         [State#state.host, State#state.index, AckReason]),
            wait_acks_and_delete(Rest, State, Sent, Fails + 1);
        {Ref, {error, AckReason}} ->
            ?WARNING_MSG("Kafka ack error host ~s worker ~p: ~p",
                         [State#state.host, State#state.index, AckReason]),
            wait_acks_and_delete(Rest, State, Sent, Fails + 1)
    after 30000 ->
        ?WARNING_MSG("Kafka ack timeout host ~s worker ~p — ~p remaining refs not waited",
                     [State#state.host, State#state.index, length(Rest) + 1]),
        %% Do NOT delete RocksKey — message stays for retry.
        %% Count all remaining (including current) as failed.
        {Sent, Fails + 1 + length(Rest)}
    end.

delete_key(State, Key) ->
    case catch rocksdb:delete(State#state.db_handle, Key, [{sync, true}]) of
        ok -> ok;
        Err ->
            ?ERROR_MSG("Failed to delete key from RocksDB host ~s: ~p",
                       [State#state.host, Err])
    end.

is_kafka_down(Reason) ->
    Reason =:= producer_not_found orelse
    (is_tuple(Reason) andalso element(1, Reason) =:= producer_not_found) orelse
    Reason =:= client_down orelse
    (is_tuple(Reason) andalso element(1, Reason) =:= client_down).
