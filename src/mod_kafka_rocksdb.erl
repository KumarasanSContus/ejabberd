%%%-------------------------------------------------------------------
%%% @doc
%%% RocksDB management for Kafka outbox.
%%%
%%% KEY FIXES vs original:
%%%   1. RocksDB writes use {sync, true} — fsync on every write to prevent
%%%      message loss on crash (outbox durability guarantee).
%%%   2. enqueue/3 falls back to gen_server call if ETS handle is stale/missing,
%%%      so a transient ETS miss during startup doesn't silently drop messages.
%%%   3. get_stats handle pattern-match errors gracefully (rocksdb property
%%%      reads can return {error,_} if DB is under heavy compaction).
%%%-------------------------------------------------------------------
-module(mod_kafka_rocksdb).
-behaviour(gen_server).

%% API
-export([start_link/3, enqueue/3, get_db/1, get_stats/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("logger.hrl").

-record(state, {
    host        :: binary(),
    db_path     :: string(),
    db_handle   :: rocksdb:db_handle() | undefined,
    topic       :: binary(),
    client_name :: atom()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, DBPath, Opts) ->
    Name = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:start_link({local, Name}, ?MODULE,
                          [iolist_to_binary(Host), DBPath, Opts], []).

%%--------------------------------------------------------------------
%% enqueue/3
%%
%% Fast path: reads DbHandle from ETS to avoid a gen_server roundtrip.
%% FIX: on ETS miss (startup race or stale handle after restart), falls
%% back to a direct gen_server call instead of silently returning an error.
%% FIX: RocksDB writes use {sync, true} to guarantee durability.
%%--------------------------------------------------------------------
enqueue(HostStr, PartitionKey, Payload) ->
    Host = iolist_to_binary(HostStr),
    case ets:lookup(mod_kafka, {disk_low, Host}) of
        [{_, true}] ->
            ?ERROR_MSG("RocksDB write suspended for ~p: Disk space critically low", [Host]),
            {error, disk_full};
        _ ->
            do_enqueue(Host, PartitionKey, Payload)
    end.

do_enqueue(Host, PartitionKey, Payload) ->
    DbHandle = case ets:lookup(mod_kafka, {rocksdb, Host}) of
        [{_, H}] -> H;
        _ ->
            %% ETS miss — ask the gen_server directly (handles startup race).
            Name = gen_mod:get_module_proc(Host, ?MODULE),
            case catch gen_server:call(Name, get_db_handle, 5000) of
                {ok, H} ->
                    ets:insert(mod_kafka, {{rocksdb, Host}, H}),
                    H;
                _ -> undefined
            end
    end,
    case DbHandle of
        undefined ->
            ?ERROR_MSG("No RocksDB handle available for host ~p — message dropped", [Host]),
            {error, db_not_found};
        _ ->
            write_to_rocksdb(Host, DbHandle, PartitionKey, Payload)
    end.

write_to_rocksdb(Host, DbHandle, PartitionKey, Payload) ->
    TS    = erlang:system_time(nanosecond),
    Uniq  = erlang:unique_integer([positive, monotonic]),

    {WorkerIdx, Replayers} = case ets:lookup(mod_kafka, {replayers, Host}) of
        [{_, R}] -> {erlang:phash2(PartitionKey, maps:size(R)), R};
        _        -> {0, #{}}
    end,

    Key   = <<WorkerIdx:16, TS:64, Uniq:64>>,
    PKLen = byte_size(PartitionKey),
    Value = <<PKLen:32, PartitionKey/binary, Payload/binary>>,

    %% FIX: {sync, true} — fsync before returning ok so message is durable.
    case catch rocksdb:put(DbHandle, Key, Value, [{sync, true}]) of
        ok ->
            ?DEBUG("Enqueued message for host ~p worker ~p", [Host, WorkerIdx]),
            notify_worker(Host, WorkerIdx, Replayers),
            ok;
        {error, Reason} ->
            ?ERROR_MSG("Failed to write to RocksDB for host ~p: ~p", [Host, Reason]),
            {error, Reason};
        {'EXIT', {badarg, _}} ->
            ?ERROR_MSG("Stale RocksDB handle for host ~p — clearing from ETS", [Host]),
            ets:delete(mod_kafka, {rocksdb, Host}),
            {error, stale_handle};
        Other ->
            ?ERROR_MSG("Unexpected RocksDB error for host ~p: ~p", [Host, Other]),
            {error, Other}
    end.

get_db(Host) ->
    case ets:lookup(mod_kafka, {rocksdb, iolist_to_binary(Host)}) of
        [{_, DbHandle}] -> {ok, DbHandle};
        _               -> {error, not_found}
    end.

get_stats(Host) ->
    Name = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Name, get_stats, 5000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

open_db_with_retry(_DBPath, _DbOpts, 0) ->
    {error, max_retries_exceeded};
open_db_with_retry(DBPath, DbOpts, Tries) ->
    case rocksdb:open(DBPath, DbOpts) of
        {ok, DB} -> {ok, DB};
        {error, Reason} ->
            ?WARNING_MSG("Failed to open RocksDB at ~s: ~p. Retrying in 1s... (~p tries left)",
                         [DBPath, Reason, Tries]),
            timer:sleep(1000),
            open_db_with_retry(DBPath, DbOpts, Tries - 1)
    end.

init([Host, DBPath, Opts]) ->
    process_flag(trap_exit, true),
    ok = filelib:ensure_dir(filename:join(DBPath, "dummy")),

    DbOpts = [
        {create_if_missing, true},
        {max_open_files, 1000},
        {write_buffer_size, 64 * 1024 * 1024},
        {max_write_buffer_number, 3},
        {target_file_size_base, 64 * 1024 * 1024},
        {block_size, 32 * 1024}
    ],

    case open_db_with_retry(DBPath, DbOpts, 5) of
        {ok, DB} ->
            ?INFO_MSG("Opened RocksDB at ~s for host ~p", [DBPath, Host]),
            try ets:new(mod_kafka, [named_table, public, set, {read_concurrency, true}])
            catch _:_ -> ok end,
            ets:insert(mod_kafka, {{rocksdb, Host}, DB}),
            State = #state{
                host        = Host,
                db_path     = DBPath,
                db_handle   = DB,
                topic       = maps:get(topic, Opts, <<"ejabberd.chat">>),
                client_name = list_to_atom("kafka_client_" ++ binary_to_list(Host))
            },
            {ok, State};
        {error, Reason} ->
            ?CRITICAL_MSG("Failed to open RocksDB at ~s: ~p", [DBPath, Reason]),
            {stop, Reason}
    end.

handle_call(get_db_handle, _From, State) ->
    {reply, {ok, State#state.db_handle}, State};

%% FIX: guard rocksdb property reads — they can fail during compaction.
handle_call(get_stats, _From, State) ->
    LevelStats = case catch rocksdb:get_property(State#state.db_handle, <<"rocksdb.levelstats">>) of
        {ok, V1} -> V1;
        _        -> <<"unavailable">>
    end,
    CurMem = case catch rocksdb:get_property(State#state.db_handle, <<"rocksdb.cur-size-all-mem-tables">>) of
        {ok, V2} -> V2;
        _        -> 0
    end,
    {reply, #{levelstats => LevelStats, mem_bytes => CurMem}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.db_handle of
        undefined -> ok;
        DB ->
            %% Remove from ETS before closing so no new writers use stale handle.
            ets:delete(mod_kafka, {rocksdb, State#state.host}),
            rocksdb:close(DB),
            ?INFO_MSG("Closed RocksDB for host ~p", [State#state.host])
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

notify_worker(_Host, Index, Replayers) ->
    case maps:get(Index, Replayers, undefined) of
        Pid when is_pid(Pid) -> mod_kafka_replayer:trigger_replay(Pid);
        _                    -> ok
    end.
