%%%----------------------------------------------------------------------
%%% File    : fly_xmpp_records.hrl
%%% Purpose : Custom MirrorFly XMPP records
%%%----------------------------------------------------------------------

-record(chatcontent, {
    block_user = <<"0">> :: binary(),
    broadcast_id = <<"">> :: binary(),
    broadcast_msgid = <<"">> :: binary(),
    notification = <<"0">> :: binary(),
    organization_type = <<"">> :: binary(),
    offline_enable = <<"">> :: binary(),
    is_broadcast = <<"">> :: binary(),
    detail = <<"">> :: binary(),
    type = <<"">> :: binary(),
    title = <<"">> :: binary(),
    bname = <<"">> :: binary(),
    message_type = <<"">> :: binary()
}).

-record(delivered, {
    id = <<"">> :: binary(),
    time = <<"">> :: binary(),
    message_status = <<"">> :: binary(),
    group_id = <<"">> :: binary()
}).

-record(seen, {
    id = <<"">> :: binary(),
    time = <<"">> :: binary(),
    message_status = <<"">> :: binary(),
    group_id = <<"">> :: binary()
}).

-record(recall, {
    id = <<"">> :: binary(),
    time = <<"">> :: binary(),
    chat_type = <<"">> :: binary(),
    prev_message_id = <<"">> :: binary(),
    group_id = <<"">> :: binary()
}).

-record(timestampmsg, {
    id = <<"">> :: binary(),
    time = <<"">> :: binary()
}).

-record(acknowledge, {
    id = <<"">> :: binary(),
    broadcast_msgid = <<"">> :: binary(),
    sent_to = <<"">> :: binary(),
    type = <<"">> :: binary()
}).
