%%%----------------------------------------------------------------------
%%% File    : fly_mod_stanza_ack.erl
%%% Purpose : Message Receipts XEP-0184
%%%----------------------------------------------------------------------

-module(fly_mod_stanza_ack).
-behaviour(gen_mod).

-include("logger.hrl").
-include_lib("xmpp/include/xmpp.hrl").
-include("../include/fly_xmpp_records.hrl").

-define(EJABBERD_DEBUG, true).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/2, stop/1, depends/2, mod_options/1, mod_doc/0, mod_opt_type/1]).
-export([
    on_user_send_packet/1,
    send_ack_response/6,
    is_any_resource_online/2,
    should_store_in_offline/2,
    should_acknowledge_on_send/1,
    should_acknowledge_on_receive/1,
    should_acknowledge_on_receipts/1,
    add_timestamp/1]).
%% gen_server callbacks.
-export([init/1,
     handle_call/3,
     handle_cast/2,
     handle_info/2,
     terminate/2,
     code_change/3,
     start_link/0]).

-record(state,
      { server_host = <<"">> :: binary(),
        permissions = dict:new() :: dict:dict()
      }).

%%%===================================================================
%%% API
%%%===================================================================
start(Host, Opts) ->
    gen_mod:start_child(?MODULE, Host, Opts).

stop(Host) ->
    gen_mod:stop_child(?MODULE, Host).

mod_doc() ->
    #{desc => <<"Message Receipts XEP-0184 Implementation">>}.

mod_opt_type(_) ->
    econf:any().

mod_options(_Host) ->
    [].

depends(_Host, _Opts) ->
    [].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([Host, _Opts]) ->
    ejabberd_hooks:add(user_send_packet, Host, ?MODULE, on_user_send_packet, 100),
    {ok, #state{server_host = Host}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({send_ack, Msg}, State) ->
    ejabberd_router:route(Msg),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Host = State#state.server_host,
    ejabberd_hooks:delete(user_send_packet, Host, ?MODULE, on_user_send_packet, 100).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

on_user_send_packet({#message{from = From, to = To} = IncomingPacket, C2SState}) ->
    case {From, To} of
        {#jid{user = FUser, lserver = FServer},
         #jid{user = TUser, lserver = TServer}} ->
            Packet = add_timestamp(IncomingPacket),
            case should_acknowledge_on_receive(Packet) of
                chat_acknowledge ->
                    handle_acknowledgment(Packet, FUser, FServer, TUser, TServer, From, To);
                message_delivered ->
                    handle_message_delivered(Packet, From, To, FUser, TUser, TServer);
                message_seen ->
                    handle_message_seen(Packet, From, To, FUser, TUser, TServer);
                message_recall ->
                    handle_message_recall(Packet, From, To, FUser, TUser, TServer);
                _ -> ok
            end,
            case should_acknowledge_on_receipts(Packet) of
                ReceiptType when ReceiptType =:= received_receipt orelse
                                 ReceiptType =:= seen_receipt     orelse
                                 ReceiptType =:= recall_receipt   ->
                    ok;
                _ -> ok
            end,
            {Packet, C2SState};
        _ ->
            {IncomingPacket, C2SState}
    end;
on_user_send_packet(Acc) -> Acc.

handle_acknowledgment(Packet, _FUser, FServer, TUser, TServer, From, To) ->
    case should_acknowledge_on_send(Packet) of
        message_acknowledge ->
            process_acknowledgment(Packet, FServer, TUser, TServer, From, To);
        false -> ok
    end.

process_acknowledgment(Packet, FServer, TUser, TServer, From, To) ->
    IdValue = xmpp:get_id(Packet),
    {BroadcastId, BroadcastMsgId, _MessageType} = extract_broadcast_info(Packet),

    case mod_kafka:maybe_process(FServer, Packet) of
        ok ->
            send_ack_response(From, To, IdValue, <<"acknowledge">>, BroadcastMsgId, BroadcastId),
            NewPacket = xmpp:set_from_to(Packet, From, To),
            {TimeStampedPacket, _FlatTimeStamp} = get_timestamp_from_packet(NewPacket),
            handle_mobile_user_messages(TUser, TServer, TimeStampedPacket, true);
        {error, Reason} ->
            ?ERROR_MSG("Failed to durably store message for ~p (id=~p): ~p — ACK withheld",
                       [FServer, IdValue, Reason])
    end.

extract_broadcast_info(Packet) ->
    case get_chatcontent(Packet) of
        false ->
            {<<"">>, <<"">>, <<"">>};
        #chatcontent{broadcast_id = BId, broadcast_msgid = BMsgId, message_type = MType} ->
            {BId, BMsgId, MType}
    end.

handle_message_delivered(Packet, From, To, _FUser, TUser, TServer) ->
    case get_delivered(Packet) of
        #delivered{id = Id} ->
            send_ack_response(From, To, Id, <<"delivered">>, <<"">>, <<"">>),
            update_timestamp_and_store_packet(Packet, From, To, TUser, TServer, false);
        false -> ok
    end.

handle_message_seen(Packet, From, To, _FUser, TUser, TServer) ->
    case get_seen(Packet) of
        #seen{id = Id} ->
            send_ack_response(From, To, Id, <<"seen">>, <<"">>, <<"">>),
            update_timestamp_and_store_packet(Packet, From, To, TUser, TServer, false);
        false -> ok
    end.

handle_message_recall(Packet, From, To, _FUser, TUser, TServer) ->
    case get_recall(Packet) of
        #recall{id = Id, prev_message_id = PrevId} ->
            send_recall_ack_response(From, To, Id, <<"recall">>, <<"">>, <<"">>, PrevId),
            update_timestamp_and_store_packet(Packet, From, To, TUser, TServer, true);
        false -> ok
    end.

update_timestamp_and_store_packet(Packet, From, To, TUser, TServer, SendPush) ->
    FromToUpdatedPacket = xmpp:set_from_to(Packet, From, To),
    {TimeStampedPacket, _} = get_timestamp_from_packet(FromToUpdatedPacket),
    handle_mobile_user_messages(TUser, TServer, TimeStampedPacket, SendPush).

get_timestamp_from_packet(Packet) ->
    case get_timestampmsg(Packet) of
        false ->
            {XMLTag, FlatTimestamp} = create_timestamp_element(),
            {xmpp:append_subtags(Packet, [XMLTag]), FlatTimestamp};
        #timestampmsg{time = Time} ->
            {Packet, Time}
    end.

create_timestamp_element() ->
    FlatTimeStamp = generate_timestamp(),
    TimeStampElement = #xmlel{
        name  = <<"timestamp">>,
        attrs = [
            {<<"xmlns">>, <<"urn:xmpp:messagetime">>},
            {<<"time">>,  FlatTimeStamp}
        ],
        children = []
    },
    {TimeStampElement, FlatTimeStamp}.

generate_timestamp() ->
    Timestamp = erlang:system_time(microsecond),
    list_to_binary(integer_to_list(Timestamp)).

handle_mobile_user_messages(_TUser, _TServer, _TimeStampedPacket, _SendPushFromCaller) -> ok.

is_any_resource_online(TUser, TServer) ->
    length(ejabberd_sm:get_user_resources(TUser, TServer)) > 0.

should_store_in_offline(TUser, TServer) ->
    ActiveSessions = ejabberd_sm:get_user_present_resources(TUser, TServer),
    case ActiveSessions of
        [] -> {false, false};
        Resources ->
            HasMobile = lists:any(
                fun({_Priority, Resource}) ->
                    is_mobile_resource(Resource)
                end,
                Resources
            ),
            case HasMobile of
                true  -> {true, false};
                false -> {<<"true">>, <<"true">>}
            end
    end.

is_mobile_resource(Resource) ->
    LowerRes = string:lowercase(Resource),
    case binary:split(LowerRes, <<"-">>) of
        [Prefix | _] -> lists:member(Prefix, [<<"mobile">>]);
        _            -> false
    end.

should_acknowledge_on_send(Packet) ->
    case Packet#message.body =:= [] of
        true  -> false;
        false -> message_acknowledge
    end.

should_acknowledge_on_receive(Packet) ->
    if
        is_record(Packet, message) ->
            ChatAcknowledge = get_chatcontent(Packet),
            Delivered       = get_delivered(Packet),
            Seen            = get_seen(Packet),
            Recall          = get_recall(Packet),
            if
                ChatAcknowledge =/= false -> chat_acknowledge;
                Delivered       =/= false -> message_delivered;
                Seen            =/= false -> message_seen;
                Recall          =/= false -> message_recall;
                true                      -> ok
            end;
        true -> ok
    end.

should_acknowledge_on_receipts(Packet) ->
    case get_acknowledge(Packet) of
        false -> ok;
        #acknowledge{type = Type} ->
            case Type of
                <<"delivered">> -> received_receipt;
                <<"seen">>      -> seen_receipt;
                <<"recall">>    -> recall_receipt;
                _               -> ok
            end
    end.

add_timestamp(Packet) ->
    {UpdatedPacket, _} = get_timestamp_from_packet(Packet),
    UpdatedPacket.

send_ack_response(From, To, ReceiptId, Type, BroadcastMsgId, BroadcastId) ->
    send_response(From, To, ReceiptId, Type, BroadcastMsgId, BroadcastId, undefined).

send_recall_ack_response(From, To, ReceiptId, Type, BroadcastMsgId, BroadcastId, PrevMessageId) ->
    send_response(From, To, ReceiptId, Type, BroadcastMsgId, BroadcastId, PrevMessageId).

send_response(From, To, ReceiptId, Type, BroadcastMsgId, BroadcastId, PrevMessageId) ->
    ServerHost = jid:get_lserver(From),
    SentTo     = jid:to_string(To),

    XmlBodyAttrs = [
        {<<"xmlns">>,         ?NS_RECEIPTS},
        {<<"id">>,            ReceiptId},
        {<<"sent_to">>,       SentTo},
        {<<"broadcast_msgid">>,BroadcastMsgId},
        {<<"broadcast_id">>,  BroadcastId},
        {<<"type">>,          Type}
    ],

    UpdatedXmlBodyAttrs = case PrevMessageId of
        undefined -> XmlBodyAttrs;
        _         -> [{<<"prev_message_id">>, PrevMessageId} | XmlBodyAttrs]
    end,

    XmlBody = #xmlel{name = <<"acknowledge">>, attrs = UpdatedXmlBodyAttrs, children = []},
    Proc    = gen_mod:get_module_proc(ServerHost, ?MODULE),
    Msg     = #message{from = To, to = From, type = chat, sub_els = [XmlBody]},
    gen_server:cast(Proc, {send_ack, Msg}).

%% ====================================================================
%% Manual Decoding Helpers for MirrorFly Custom Records
%% ====================================================================

get_chatcontent(Packet) ->
    case fxml:get_subtag(Packet, <<"chatcontent">>) of
        false -> false;
        El ->
            Attrs = El#xmlel.attrs,
            #chatcontent{
                block_user        = fxml:get_attr_s(<<"block_user">>, Attrs),
                broadcast_id      = fxml:get_attr_s(<<"broadcast_id">>, Attrs),
                broadcast_msgid   = fxml:get_attr_s(<<"broadcast_msgid">>, Attrs),
                notification      = fxml:get_attr_s(<<"notification">>, Attrs),
                organization_type = fxml:get_attr_s(<<"organization_type">>, Attrs),
                offline_enable    = fxml:get_attr_s(<<"offline_enable">>, Attrs),
                is_broadcast      = fxml:get_attr_s(<<"is_broadcast">>, Attrs),
                detail            = fxml:get_attr_s(<<"detail">>, Attrs),
                type              = fxml:get_attr_s(<<"type">>, Attrs),
                title             = fxml:get_attr_s(<<"title">>, Attrs),
                bname             = fxml:get_attr_s(<<"bname">>, Attrs),
                message_type      = fxml:get_attr_s(<<"message_type">>, Attrs)
            }
    end.

get_timestampmsg(Packet) ->
    case fxml:get_subtag(Packet, <<"timestamp">>) of
        false -> false;
        El ->
            Attrs = El#xmlel.attrs,
            #timestampmsg{
                id   = fxml:get_attr_s(<<"id">>, Attrs),
                time = fxml:get_attr_s(<<"time">>, Attrs)
            }
    end.

get_delivered(Packet) ->
    case fxml:get_subtag(Packet, <<"delivered">>) of
        false -> false;
        El ->
            Attrs = El#xmlel.attrs,
            #delivered{
                id             = fxml:get_attr_s(<<"id">>, Attrs),
                time           = fxml:get_attr_s(<<"time">>, Attrs),
                message_status = fxml:get_attr_s(<<"message_status">>, Attrs),
                group_id       = fxml:get_attr_s(<<"group_id">>, Attrs)
            }
    end.

get_seen(Packet) ->
    case fxml:get_subtag(Packet, <<"seen">>) of
        false -> false;
        El ->
            Attrs = El#xmlel.attrs,
            #seen{
                id             = fxml:get_attr_s(<<"id">>, Attrs),
                time           = fxml:get_attr_s(<<"time">>, Attrs),
                message_status = fxml:get_attr_s(<<"message_status">>, Attrs),
                group_id       = fxml:get_attr_s(<<"group_id">>, Attrs)
            }
    end.

get_recall(Packet) ->
    case fxml:get_subtag(Packet, <<"recall">>) of
        false -> false;
        El ->
            Attrs = El#xmlel.attrs,
            #recall{
                id              = fxml:get_attr_s(<<"id">>, Attrs),
                time            = fxml:get_attr_s(<<"time">>, Attrs),
                chat_type       = fxml:get_attr_s(<<"chat_type">>, Attrs),
                prev_message_id = fxml:get_attr_s(<<"prev_message_id">>, Attrs),
                group_id        = fxml:get_attr_s(<<"group_id">>, Attrs)
            }
    end.

get_acknowledge(Packet) ->
    case fxml:get_subtag(Packet, <<"acknowledge">>) of
        false -> false;
        El ->
            Attrs = El#xmlel.attrs,
            #acknowledge{
                id              = fxml:get_attr_s(<<"id">>, Attrs),
                broadcast_msgid = fxml:get_attr_s(<<"broadcast_msgid">>, Attrs),
                sent_to         = fxml:get_attr_s(<<"sent_to">>, Attrs),
                type            = fxml:get_attr_s(<<"type">>, Attrs)
            }
    end.
