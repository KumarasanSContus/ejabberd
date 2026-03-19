%%%-------------------------------------------------------------------
%%% @author Antigravity
%%% @copyright (C) 2026, Antigravity
%%% @doc
%%% Enterprise-grade Kafka Integration for ejabberd - Kubernetes & Cluster Ready.
%%% Split architecture: Manager + RocksDB + Replayer for maximum scalability.
%%%
%%% KEY FIXES vs original:
%%%   1. brod_callback calls report_failure/2 which now exists in manager.
%%%   2. serialize_packet returns <<>> on empty body — handled correctly in
%%%      maybe_process (existing check preserved, comment clarified).
%%%   3. maybe_process returns ok (not {error,_}) on non-chat messages so
%%%      fly_mod_stanza_ack process_acknowledgment can distinguish properly.
%%%-------------------------------------------------------------------
-module(mod_kafka).
-behaviour(gen_mod).

%% API
-export([start/2, stop/1, reload/3, depends/2, mod_opt_type/1, mod_options/1, mod_doc/0]).
-export([on_user_send_packet/1, on_user_receive_packet/1, on_muc_filter_message/3, maybe_process/2]).
-export([brod_callback/4, get_manager_pid/1, get_circuit_status/1, get_stats/1]).

-include_lib("xmpp/include/xmpp.hrl").
-include("logger.hrl").

%% Constants
-define(MANAGER_SUFFIX, "_kafka_manager").
-define(DEFAULT_TOPIC, <<"ejabberd.chat">>).
-define(DEFAULT_BROKERS, <<"localhost:9092">>).

%%%===================================================================
%%% gen_mod API
%%%===================================================================

start(Host, Opts) ->
    ?INFO_MSG("Starting mod_kafka on host ~s", [Host]),
    try ets:new(?MODULE, [named_table, public, set, {read_concurrency, true}])
    catch _:_ -> ok end,
    ManagerPid = start_manager(Host, Opts),
    ?INFO_MSG("mod_kafka_manager started with PID: ~p", [ManagerPid]),
    ets:insert(?MODULE, {{manager, Host}, ManagerPid}),
    ok.

stop(Host) ->
    case get_manager_pid(Host) of
        undefined -> ok;
        Pid       -> gen_server:stop(Pid)
    end,
    ok.

reload(_Host, _NewOpts, _OldOpts) -> ok.

depends(_Host, _Opts) -> [].

mod_options(_) ->
    [{brokers,           ?DEFAULT_BROKERS},
     {topic,             ?DEFAULT_TOPIC},
     {rocksdb_dir,       <<"/var/lib/ejabberd/rocksdb">>},
     {ssl,               false},
     {ssl_opts,          []},
     {batch_size,        1000},
     {replay_interval_ms,10000},
     {initial_backoff_ms,10000},
     {max_backoff_ms,    300000},
     {num_spoolers,      8},
     {max_payload_size,  1048576}].

mod_doc() -> #{}.

mod_opt_type(brokers)           -> fun(B) -> iolist_to_binary(B) end;
mod_opt_type(topic)             -> fun(T) -> iolist_to_binary(T) end;
mod_opt_type(rocksdb_dir)       -> fun(D) -> iolist_to_binary(D) end;
mod_opt_type(ssl)               -> fun(B) -> is_boolean(B) end;
mod_opt_type(ssl_opts)          -> fun(L) when is_list(L) -> L end;
mod_opt_type(batch_size)        -> fun(I) when is_integer(I), I > 0 -> I end;
mod_opt_type(replay_interval_ms)-> fun(I) when is_integer(I), I > 0 -> I end;
mod_opt_type(initial_backoff_ms)-> fun(I) when is_integer(I), I > 0 -> I end;
mod_opt_type(max_backoff_ms)    -> fun(I) when is_integer(I), I > 0 -> I end;
mod_opt_type(num_spoolers)      -> fun(I) when is_integer(I), I > 0 -> I end;
mod_opt_type(max_payload_size)  -> fun(I) when is_integer(I), I > 0 -> I end;
mod_opt_type(_) ->
    [brokers, topic, rocksdb_dir, ssl, ssl_opts,
     batch_size, replay_interval_ms,
     initial_backoff_ms, max_backoff_ms, num_spoolers,
     max_payload_size].

%%%===================================================================
%%% Process Management
%%%===================================================================

start_manager(Host, Opts) ->
    ManagerName = list_to_atom(binary_to_list(Host) ++ ?MANAGER_SUFFIX),
    {ok, Pid} = mod_kafka_manager:start_link(ManagerName, Host, Opts),
    Pid.

get_manager_pid(Host) ->
    case ets:lookup(?MODULE, {manager, Host}) of
        [{_, Pid}] when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true  -> Pid;
                false -> undefined
            end;
        _ -> undefined
    end.

%%%===================================================================
%%% Hooks
%%%===================================================================

on_user_send_packet({Packet, State}) ->
    JID  = maps:get(jid, State),
    Host = iolist_to_binary(JID#jid.lserver),
    maybe_process(Host, Packet),
    {Packet, State};
on_user_send_packet(Acc) -> Acc.

on_user_receive_packet({Packet, State}) ->
    JID  = maps:get(jid, State),
    Host = iolist_to_binary(JID#jid.lserver),
    maybe_process(Host, Packet),
    {Packet, State};
on_user_receive_packet(Acc) -> Acc.

on_muc_filter_message(Packet, _MUCState, Host) ->
    maybe_process(iolist_to_binary(Host), Packet),
    Packet.

maybe_process(undefined, _Packet) ->
    ok;
maybe_process(Host, Packet) ->
    ?DEBUG("mod_kafka: maybe_process Host=~p", [Host]),
    try
        case is_chat_message(Packet) of
            true ->
                case extract_body(Packet) of
                    <<>> ->
                        ok;
                    Body when byte_size(Body) > 0 ->
                        From         = xmpp:get_from(Packet),
                        PartitionKey = jid:encode(jid:remove_resource(From)),
                        Payload      = serialize_packet(From, Packet),
                        case Payload of
                            <<>> ->
                                ?ERROR_MSG("serialize_packet returned empty for host ~p — skipping", [Host]),
                                {error, serialize_failed};
                            _ ->
                                case check_payload_size(Host, Payload) of
                                    ok ->
                                        case mod_kafka_rocksdb:enqueue(Host, PartitionKey, Payload) of
                                            ok           -> ok;
                                            {error, Rsn} ->
                                                ?ERROR_MSG("Failed to enqueue to RocksDB for ~p: ~p",
                                                           [Host, Rsn]),
                                                {error, Rsn}
                                        end;
                                    {error, too_large} ->
                                        ?WARNING_MSG("Message dropped for ~p: size ~p exceeds limit",
                                                     [Host, byte_size(Payload)]),
                                        {error, too_large}
                                end
                        end;
                    _ -> ok
                end;
            false -> ok
        end
    catch
        E:R:S ->
            ?ERROR_MSG("maybe_process failed: ~p:~p~n~p", [E, R, S]),
            ok
    end.

is_chat_message(Packet) ->
    Type = xmpp:get_type(Packet),
    lists:member(Type, [chat, groupchat, normal]).

extract_body(#message{body = Body}) ->
    try xmpp:get_text(Body) catch _:_ -> <<>> end;
extract_body(_) ->
    <<>>.

serialize_packet(From, Packet) ->
    try
        To = case xmpp:get_to(Packet) of
                 undefined -> <<>>;
                 TJID      -> jid:encode(TJID)
             end,
        JObj = #{
            <<"id">>        => xmpp:get_id(Packet),
            <<"from">>      => jid:encode(From),
            <<"to">>        => To,
            <<"type">>      => atom_to_binary(xmpp:get_type(Packet), utf8),
            <<"body">>      => extract_body(Packet),
            <<"timestamp">> => erlang:system_time(millisecond),
            <<"node">>      => atom_to_binary(node(), utf8),
            <<"pod">>       => get_pod_name()
        },
        misc:json_encode(JObj)
    catch
        E:R:S ->
            ?ERROR_MSG("serialize_packet failed: ~p:~p~n~p", [E, R, S]),
            <<>>
    end.

get_pod_name() ->
    case os:getenv("POD_NAME") of
        false ->
            case os:getenv("HOSTNAME") of
                false -> list_to_binary(atom_to_list(node()));
                H     -> list_to_binary(H)
            end;
        Val -> list_to_binary(Val)
    end.

check_payload_size(Host, Payload) ->
    Limit = gen_mod:get_module_opt(Host, ?MODULE, max_payload_size),
    case byte_size(Payload) > Limit of
        true  -> {error, too_large};
        false -> ok
    end.

get_circuit_status(Host) ->
    case get_manager_pid(Host) of
        undefined -> closed;
        Pid       -> mod_kafka_manager:get_circuit_status(Pid)
    end.

get_stats(Host) ->
    case get_manager_pid(Host) of
        undefined  -> #{error => no_manager};
        ManagerPid -> mod_kafka_manager:get_stats(ManagerPid)
    end.

%%%===================================================================
%%% Kafka Callback
%%%===================================================================

-spec brod_callback(Host :: binary(), Payload :: binary(), Timestamp :: integer(),
                    Result :: {ok, brod:call_ref()} | {error, term()}) -> ok.
brod_callback(Host, Payload, _Timestamp, Result) ->
    case get_manager_pid(Host) of
        undefined -> ok;
        ManagerPid ->
            case Result of
                {ok, _CallRef} ->
                    mod_kafka_manager:report_success(ManagerPid);
                {error, Reason} ->
                    ?ERROR_MSG("Kafka produce callback failed for ~s: ~p", [Host, Reason]),
                    %% FIX: report_failure/2 now exists in manager (was undef before).
                    mod_kafka_manager:report_failure(ManagerPid, Payload)
            end
    end.
