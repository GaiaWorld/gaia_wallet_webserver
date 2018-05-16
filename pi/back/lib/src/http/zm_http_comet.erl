%%@doc http长连接模块，用于处理http的服务器推送
%%@end


-module(zm_http_comet).

-description("http comet").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([bind/5, get_msgs/5, msg_transform/2]).

%%%=======================DEFINE=======================
-define(PREFIX, "<HTML><BODY>").
-define(Q, 34).

-define(SERVER_INTERNAL_ERROR, 500).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 处理客户端向服务器端发起建立http长连接的请求，将长连接与会话绑定，并将会话标识发送给客户端
%% @spec bind(Args, Con, Info, Headers, any()) -> return()
%% where
%%      return() = {break, [], [], []}
%%@end
%% -----------------------------------------------------------------
bind(Args, Con, Info, Headers, _Body) ->
	{_M, _F, _A} = MFA = z_lib:get_value(Args, msg_transform,
		{?MODULE, msg_transform, {"<SCRIPT>", "</SCRIPT>", "route(", $,, ");"}}),
	{ID, Pid} = zm_http_session:get_session(Info, Headers),
	send_comet(Args, Con, Info, Headers, ID),
	try
		zm_http_session:bind(Pid, Con, MFA)
	catch
		Error:Reason ->
			zm_http:send_chunk(Con, io_lib:write(
				{Error, Reason, erlang:get_stacktrace()}), none),
			zm_http:send_chunk(Con, [], none)
	end,
	{break, [], [], []}.

%% -----------------------------------------------------------------
%%@doc 客户端向服务器端获取推送的消息队列
%% @spec get_msgs(any(), any(), Info, Headers, any()) -> return()
%% where
%%      return() = {ok, Info, Headers, Body}
%%@end
%% -----------------------------------------------------------------
get_msgs(_Args, _Con, Info, Headers, _Body) ->
	RespHeaders = zm_http:resp_headers(Info, Headers, ".js"),
	ML = zm_http_session:get_msgs(Headers),
	{ok, Info, RespHeaders, zm_json:encode(ML)}.

%% -----------------------------------------------------------------
%%@doc 对发送到客户端的消息进行编码转换
%%```
%%  IOList::list()
%%'''
%% @spec msg_transform(Argus::{Prefix, Suffix, MsgPrefix, Separator, MsgSuffix},L::list()) -> IOList
%%@end
%% -----------------------------------------------------------------
msg_transform(_, []) ->
	[];
msg_transform({Prefix, Suffix, MsgPrefix, Separator, MsgSuffix}, L) ->
	[Prefix, [[MsgPrefix, ?Q, Type, ?Q, Separator, zm_json:encode(ML), MsgSuffix]
		|| {Type, ML} <- L, is_list(Type)], Suffix].

%%%===================LOCAL FUNCTIONS==================
%发送comet
send_comet(Args, Con, Info, Headers, ID) ->
	RespHeaders = case ID of
		none ->
			zm_http:set_header(zm_http:resp_headers(
				Info, Headers, ".html"), "Transfer-Encoding", "chunked");
		_ ->
			zm_http_session:set_scid(zm_http:set_header(zm_http:resp_headers(
				Info, Headers, ".html"), "Transfer-Encoding", "chunked"), ID)
	end,
	case z_lib:get_value(Args, zip, false) of
		false ->
			zm_http:send(Con, Info, zm_http:del_header(
				RespHeaders, "Content-Encoding"), none);
		_ ->
			zm_http:send(Con, Info, RespHeaders, none)
	end,
	case z_lib:get_value(Args, prefix, ?PREFIX) of
		"" ->
			ok;
		Prefix ->
			zm_http:send_chunk(Con, Prefix, none)
	end.
