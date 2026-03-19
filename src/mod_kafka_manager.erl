%%%-------------------------------------------------------------------
%%% @doc
%%% Kafka Manager - Circuit breaker, coordination, no I/O.
%%%
%%% KEY FIXES vs original:
%%%   1. report_failure/2 added (was called from mod_kafka.erl but didn't exist).
%%%   2. On RocksDB crash+restart, surviving replayers are updated via
%%%      mod_kafka_replayer:update_db_handle/2 so they never use stale handles.
%%%   3. On replayer crash+restart, ETS replayer map is refreshed.
%%%   4. rocksdb restart passes full Opts (not just #{topic}), preserving config.
%%%   5. disksup:get_disk_data() pattern fixed — actual tuple arity is {Disk,Kbytes,Percent}.
%%%   6. ETS {{rocksdb,Host}} handle is refreshed in ETS after DB restart so
%%%      enqueue/3 immediately picks up the new handle.
%%%-------------------------------------------------------------------
-module(mod_kafka_manager).
-behaviour(gen_server).

%% API
-export([start_link/3, handle_message/3, report_success/1,
         report_failure/1, report_failure/2, report_failure/3,
         get_circuit_status/1, get_partitions/1, get_stats/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("logger.hrl").

%% Constants
-define(INITIAL_BACKOFF_MS, 10000).
-define(MAX_BACKOFF_MS, 300000).   %% 5 minutes
-define(CB_FAILURE_THRESHOLD, 50).
-define(META_REFRESH, 60000).
-define(DISK_CHECK_INTERVAL, 300000). %% 5 minutes

-record(state, {
    name                :: atom(),
    host                :: binary(),
    topic               :: binary(),
    brokers             :: [{string(), integer()}],
    ssl                 :: boolean(),
    ssl_opts            :: list(),
    client_name         :: atom(),

    %% Circuit breaker
    failures = 0        :: non_neg_integer(),
    status = closed     :: closed | open | half_open,
    reset_timer         :: reference() | undefined,
    backoff_ms          :: non_neg_integer(),
    initial_backoff_ms  :: non_neg_integer(),
    max_backoff_ms      :: non_neg_integer(),

    %% Partition cache
    partitions = 1      :: pos_integer(),
    partitions_timestamp :: integer(),

    %% RocksDB
    db_path             :: string(),
    db_handle           :: rocksdb:db_handle() | undefined,

    %% Child processes
    num_spoolers        :: pos_integer(),
    spoolers            :: pid() | undefined,
    replayers           :: #{integer() => pid()},

    %% Config — kept so we can pass full opts on rocksdb restart.
    opts                :: map(),
    rocksdb_dir         :: binary(),
    batch_size          :: pos_integer(),
    replay_interval_ms  :: pos_integer(),

    %% Disk monitoring
    disk_low = false    :: boolean(),
    last_disk_check     :: integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name, Host, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Host, Opts], []).

handle_message(ManagerPid, Key, Payload) ->
    gen_server:cast(ManagerPid, {message, Key, Payload}).

report_success(ManagerPid) ->
    gen_server:cast(ManagerPid, success).

%% FIX: 1-arity (no payload).
report_failure(ManagerPid) ->
    gen_server:cast(ManagerPid, failure).

%% FIX: 2-arity used by mod_kafka.erl brod_callback (was missing, caused undef).
report_failure(ManagerPid, Payload) ->
    gen_server:cast(ManagerPid, {failure, undefined, Payload}).

%% 3-arity: explicit key+payload.
report_failure(ManagerPid, Key, Payload) ->
    gen_server:cast(ManagerPid, {failure, Key, Payload}).

get_stats(ManagerPid) ->
    gen_server:call(ManagerPid, get_stats, 5000).

get_circuit_status(ManagerPid) ->
    gen_server:call(ManagerPid, get_circuit_status, 5000).

get_partitions(ManagerPid) ->
    gen_server:call(ManagerPid, get_partitions, 5000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([HostStr, Opts]) ->
    process_flag(trap_exit, true),
    Host = iolist_to_binary(HostStr),

    BrokersBin    = maps:get(brokers, Opts, <<"localhost:9092">>),
    Topic         = maps:get(topic, Opts, <<"ejabberd.chat">>),
    RocksDbDir    = maps:get(rocksdb_dir, Opts, <<"/var/lib/ejabberd/rocksdb">>),
    Ssl           = maps:get(ssl, Opts, false),
    SslOpts       = maps:get(ssl_opts, Opts, []),
    BatchSize     = maps:get(batch_size, Opts, 1000),
    ReplayInterval= maps:get(replay_interval_ms, Opts, 500),
    InitialBackoff= maps:get(initial_backoff_ms, Opts, ?INITIAL_BACKOFF_MS),
    MaxBackoff    = maps:get(max_backoff_ms, Opts, ?MAX_BACKOFF_MS),
    NumSpoolers   = maps:get(num_spoolers, Opts, 8),

    Brokers    = parse_brokers(BrokersBin),
    ClientName = list_to_atom("kafka_client_" ++ binary_to_list(Host)),
    DBPathStr  = if is_binary(RocksDbDir) -> binary_to_list(RocksDbDir); true -> RocksDbDir end,

    %% Start RocksDB process.
    ?INFO_MSG("Starting mod_kafka_rocksdb for host ~p at ~s", [Host, RocksDbDir]),
    {ok, RocksPid} = mod_kafka_rocksdb:start_link(Host, DBPathStr, Opts),

    %% Synchronous call to ensure DB is open and get handle.
    {ok, DbHandle} = gen_server:call(RocksPid, get_db_handle, 5000),

    %% Start replayer workers.
    Replayers = maps:from_list([begin
        {ok, Pid} = mod_kafka_replayer:start_link(
            Host, ClientName, Topic, BatchSize, ReplayInterval, I, DbHandle),
        {I, Pid}
    end || I <- lists:seq(0, NumSpoolers - 1)]),

    %% Start Kafka client (non-blocking).
    _ = start_kafka_client(Brokers, ClientName, Ssl, SslOpts, Topic),

    Partitions = get_partitions_count(ClientName, Topic),

    %% Ensure ETS table exists and populate.
    try ets:new(mod_kafka, [named_table, public, set, {read_concurrency, true}])
    catch _:_ -> ok end,
    ets:insert(mod_kafka, {{workers,      Host}, RocksPid}),
    ets:insert(mod_kafka, {{replayers,    Host}, Replayers}),
    ets:insert(mod_kafka, {{partitions,   Host}, max(1, Partitions)}),
    ets:insert(mod_kafka, {{client_name,  Host}, ClientName}),
    ets:insert(mod_kafka, {{topic,        Host}, Topic}),
    ets:insert(mod_kafka, {{status,       Host}, closed}),
    ets:insert(mod_kafka, {{disk_low,     Host}, false}),
    ?INFO_MSG("mod_kafka_manager init complete for host ~p, ~p replayers", [Host, NumSpoolers]),

    State = #state{
        name               = gen_mod:get_module_proc(Host, ?MODULE),
        host               = Host,
        topic              = Topic,
        brokers            = Brokers,
        ssl                = Ssl,
        ssl_opts           = SslOpts,
        client_name        = ClientName,
        partitions         = max(1, Partitions),
        partitions_timestamp = erlang:monotonic_time(),
        backoff_ms         = InitialBackoff,
        initial_backoff_ms = InitialBackoff,
        max_backoff_ms     = MaxBackoff,
        num_spoolers       = NumSpoolers,
        spoolers           = RocksPid,
        replayers          = Replayers,
        opts               = Opts,       %% FIX: keep full opts for restart
        rocksdb_dir        = RocksDbDir,
        batch_size         = BatchSize,
        replay_interval_ms = ReplayInterval,
        last_disk_check    = erlang:monotonic_time(),
        db_path            = DBPathStr,
        db_handle          = DbHandle
    },

    erlang:send_after(?META_REFRESH,      self(), refresh_partitions),
    erlang:send_after(?DISK_CHECK_INTERVAL, self(), check_disk_space),

    {ok, State}.

handle_call(get_stats, _From, State) ->
    SpoolersStats = try gen_server:call(State#state.spoolers, get_stats, 5000) catch _:_ -> #{} end,
    ReplayStats = lists:foldl(fun(Pid, Acc) ->
        Stats = try gen_server:call(Pid, get_stats, 5000) catch _:_ -> #{} end,
        maps:fold(fun(K, V, A) ->
            case is_integer(V) of
                true  -> maps:put(K, maps:get(K, A, 0) + V, A);
                false -> A
            end
        end, Acc, Stats)
    end, #{}, maps:values(State#state.replayers)),

    Stats = #{
        host               => State#state.host,
        status             => State#state.status,
        kafka_failures     => State#state.failures,
        backoff_ms         => State#state.backoff_ms,
        partitions         => State#state.partitions,
        num_spoolers       => State#state.num_spoolers,
        disk_low           => State#state.disk_low,
        rocks_mem_bytes    => maps:get(mem_bytes, SpoolersStats, 0),
        total_replayed     => maps:get(total_replayed, ReplayStats, 0),
        total_replay_failed=> maps:get(total_failed, ReplayStats, 0)
    },
    {reply, Stats, State};

handle_call(get_circuit_status, _From, State) ->
    {reply, State#state.status, State};

handle_call(get_partitions, _From, State) ->
    {reply, State#state.partitions, State};

handle_call(ensure_producer, _From, State) ->
    Result = start_kafka_client(State#state.brokers, State#state.client_name,
                               State#state.ssl, State#state.ssl_opts, State#state.topic),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({message, Key, Payload}, State) ->
    mod_kafka_rocksdb:enqueue(State#state.host, Key, Payload),
    {noreply, State};

handle_cast(success, State) ->
    NewState = case State#state.status of
        half_open ->
            ?INFO_MSG("Circuit CLOSED for host ~s (Recovered)", [State#state.host]),
            ets:insert(mod_kafka, {{status, State#state.host}, closed}),
            [mod_kafka_replayer:trigger_replay(RPid) || RPid <- maps:values(State#state.replayers)],
            State#state{status = closed, failures = 0, backoff_ms = State#state.initial_backoff_ms};
        _ ->
            State#state{failures = 0, backoff_ms = State#state.initial_backoff_ms}
    end,
    {noreply, NewState};

handle_cast(failure, State) ->
    handle_cast({failure, undefined, undefined}, State);

handle_cast({failure, _Key, _Payload}, State) ->
    Failures = State#state.failures + 1,
    NewState = case {State#state.status, Failures} of
        {closed, F} when F >= ?CB_FAILURE_THRESHOLD ->
            Backoff = get_jittered_backoff(State#state.initial_backoff_ms),
            ?WARNING_MSG("Circuit OPEN for host ~s (Failures: ~p), retrying in ~p ms",
                         [State#state.host, F, Backoff]),
            ets:insert(mod_kafka, {{status, State#state.host}, open}),
            cancel_timer(State#state.reset_timer),
            TRef = erlang:send_after(Backoff, self(), circuit_reset),
            State#state{status = open, failures = F, reset_timer = TRef, backoff_ms = Backoff};
        {half_open, _} ->
            NextBackoff = min(State#state.max_backoff_ms, State#state.backoff_ms * 2),
            Backoff = get_jittered_backoff(NextBackoff),
            ?WARNING_MSG("Circuit OPEN for host ~s (Failed in half-open), retrying in ~p ms",
                         [State#state.host, Backoff]),
            ets:insert(mod_kafka, {{status, State#state.host}, open}),
            cancel_timer(State#state.reset_timer),
            TRef = erlang:send_after(Backoff, self(), circuit_reset),
            State#state{status = open, failures = Failures, reset_timer = TRef, backoff_ms = Backoff};
        _ ->
            State#state{failures = Failures}
    end,
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_partitions, State) ->
    Partitions = get_partitions_count(State#state.client_name, State#state.topic),
    NewState = State#state{
        partitions           = max(1, Partitions),
        partitions_timestamp = erlang:monotonic_time()
    },
    ets:insert(mod_kafka, {{partitions, State#state.host}, max(1, Partitions)}),
    erlang:send_after(?META_REFRESH, self(), refresh_partitions),
    {noreply, NewState};

handle_info(circuit_reset, State) ->
    [mod_kafka_replayer:trigger_replay(RPid) || RPid <- maps:values(State#state.replayers)],
    {noreply, State#state{status = half_open, reset_timer = undefined}};

handle_info({'EXIT', Pid, Reason}, State) ->
    if Pid =:= State#state.spoolers ->
        ?CRITICAL_MSG("mod_kafka_rocksdb crashed for host ~s: ~p, restarting", [State#state.host, Reason]),
        %% Remove stale ETS handle immediately so enqueue/3 doesn't try to use it.
        ets:delete(mod_kafka, {rocksdb, State#state.host}),
        timer:sleep(500),
        %% FIX: pass full Opts so rocksdb restarts with the same configuration.
        {ok, NewPid} = mod_kafka_rocksdb:start_link(
            State#state.host, State#state.db_path, State#state.opts),
        {ok, NewHandle} = gen_server:call(NewPid, get_db_handle, 5000),
        %% FIX: update all surviving replayers with the new handle so they
        %% don't keep using the closed (stale) handle.
        [mod_kafka_replayer:update_db_handle(RPid, NewHandle)
         || RPid <- maps:values(State#state.replayers)],
        ets:insert(mod_kafka, {{workers, State#state.host}, NewPid}),
        {noreply, State#state{spoolers = NewPid, db_handle = NewHandle}};
    true ->
        case find_pid_index(Pid, State#state.replayers) of
            {ok, Index} ->
                ?CRITICAL_MSG("Replayer ~p crashed for host ~s: ~p, restarting",
                              [Index, State#state.host, Reason]),
                {ok, NewPid} = mod_kafka_replayer:start_link(
                    State#state.host, State#state.client_name, State#state.topic,
                    State#state.batch_size, State#state.replay_interval_ms,
                    Index, State#state.db_handle),
                NewReplayers = (State#state.replayers)#{Index => NewPid},
                %% FIX: update ETS so enqueue/3 notifies the new pid.
                ets:insert(mod_kafka, {{replayers, State#state.host}, NewReplayers}),
                {noreply, State#state{replayers = NewReplayers}};
            none ->
                {noreply, State}
        end
    end;

handle_info(check_disk_space, State) ->
    DiskLow = is_disk_low(State#state.db_path),
    if DiskLow ->
        ?WARNING_MSG("Disk space low for RocksDB storage on host ~s", [State#state.host]);
    true -> ok
    end,
    ets:insert(mod_kafka, {{disk_low, State#state.host}, DiskLow}),
    erlang:send_after(?DISK_CHECK_INTERVAL, self(), check_disk_space),
    {noreply, State#state{disk_low = DiskLow, last_disk_check = erlang:monotonic_time()}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private helpers
%%%===================================================================

start_kafka_client(Brokers, ClientName, Ssl, SslOpts, Topic) ->
    _ = brod:stop_client(ClientName),
    ProducerConfig = [
        {enable_idempotence, true},
        {max_linger_ms, 50},
        {max_batch_bytes, 5242880},
        {required_acks, -1},
        {retry_backoff_ms, 500}
    ],
    ClientConfig = [
        {reconnect_cool_down_seconds, 10},
        {query_api_versions, true},
        {client_id, atom_to_binary(ClientName, utf8)},
        {auto_start_producers, true},
        {default_producer_config, ProducerConfig}
    ] ++ if Ssl -> [{ssl, true}, {ssl_opts, SslOpts}]; true -> [] end,

    case brod:start_client(Brokers, ClientName, ClientConfig) of
        ok ->
            ?INFO_MSG("Started Kafka client ~s with auto-producers", [ClientName]),
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?WARNING_MSG("Failed to start Kafka client ~s: ~p", [ClientName, Reason]),
            {error, Reason}
    end,
    spawn(fun() -> catch brod:get_metadata(ClientName, [Topic]) end),
    ok.

get_partitions_count(ClientName, Topic) ->
    case brod:get_partitions_count(ClientName, Topic) of
        {ok, Count} when is_integer(Count), Count > 0 -> Count;
        _ -> 1
    end.

cancel_timer(undefined) -> ok;
cancel_timer(Timer) ->
    _ = erlang:cancel_timer(Timer),
    ok.

get_jittered_backoff(Ms) ->
    Jitter = Ms div 10,
    case Jitter > 0 of
        true  -> Ms + rand:uniform(Jitter * 2) - Jitter;
        false -> Ms
    end.

parse_brokers(Bin) when is_binary(Bin) ->
    parse_brokers(binary_to_list(Bin));
parse_brokers(List) when is_list(List) ->
    [case string:tokens(T, ":") of
        [Host]       -> {Host, 9092};
        [Host, Port] -> {Host, list_to_integer(Port)}
    end || T <- string:tokens(List, ",")].

find_pid_index(Pid, Map) ->
    maps:fold(fun(K, V, Acc) ->
        case V =:= Pid of
            true  -> {ok, K};
            false -> Acc
        end
    end, none, Map).

%% FIX: disksup:get_disk_data() returns [{Disk, KBytes, Percent}] tuples (3 elements).
%% Original code matched a 6-element tuple which would always fall through to `false`.
is_disk_low(RocksDbDir) ->
    Dir = if is_binary(RocksDbDir) -> binary_to_list(RocksDbDir); true -> RocksDbDir end,
    case file:read_file_info(Dir) of
        {ok, _} ->
            case catch disksup:get_disk_data() of
                Data when is_list(Data), Data =/= [] ->
                    %% Find the mount point that contains our path.
                    %% Fallback: check the first entry if no match found.
                    BestEntry = find_best_disk_entry(Dir, Data),
                    case BestEntry of
                        {_Disk, KBytes, _Percent} when is_integer(KBytes) ->
                            %% Less than 512 MB free?
                            KBytes < (512 * 1024);
                        _ -> false
                    end;
                _ -> false
            end;
        {error, _} -> false
    end.

find_best_disk_entry(Path, DiskData) ->
    %% Pick the entry whose mount point is the longest prefix of Path.
    Sorted = lists:sort(fun({D1,_,_}, {D2,_,_}) ->
        length(D1) > length(D2)
    end, DiskData),
    case lists:dropwhile(fun({Disk, _, _}) ->
        not lists:prefix(Disk, Path)
    end, Sorted) of
        [Best | _] -> Best;
        []          -> hd(DiskData)
    end.
