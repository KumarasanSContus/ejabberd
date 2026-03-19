-include("/home/kumarasan/Documents/services/zf/aiflow/chat-platform/ejabberd/deps/xmpp/include/ns.hrl").
-include("/home/kumarasan/Documents/services/zf/aiflow/chat-platform/ejabberd/deps/xmpp/include/jid.hrl").
-include("/home/kumarasan/Documents/services/zf/aiflow/chat-platform/ejabberd/deps/fast_xml/include/fxml.hrl").
-include("/home/kumarasan/Documents/services/zf/aiflow/chat-platform/ejabberd/deps/xmpp/include/xmpp_codec.hrl").

-type stanza() :: iq() | presence() | message().

-define(is_stanza(Pkt),
	(is_record(Pkt, iq) or
	 is_record(Pkt, message) or
	 is_record(Pkt, presence))).

-define(stanza_type(Pkt), element(3, Pkt)).
-define(stanza_from(Pkt), element(5, Pkt)).
-define(stanza_to(Pkt), element(6, Pkt)).
