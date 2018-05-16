%%@doc HTTP路由模块和工具模块
%%```
%%%由连接进程调用接收消息，负责根据配置表执行消息处理，
%%%配置表的格式为：{{消息源, 消息路径}, MFA列表, 错误处理MFA}
%%%如果路径没有找到，则寻找上层路径配置进行处理
%%'''
%%  @type sb_trees() = tuple()
%%@end


-module(zm_http).

-description("http route & util").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([get/1, set/3, unset/3, is_alive/1, recv/5, recv_sse/5]).
-export([send_chunk/3, send_event/4, send/3, send/4]).
-export([get_peer/2, get_range/1, get_cookies/1, get_cookie/3]).
-export([get_accept_charset/1, get_accept_language/1, get_accept_encoding/1]).
-export([main_domain/1, parse_url_path/1, parse_url_query/1]).
-export([parse_range/1, parse_cookies/1, parse_qs/2, parse_qvalues/1]).
-export([trim_hex/1, new_scid/3, new_scid/6, resp_headers/3, resp_headers/4, set_header/3, set_content_type/3]).
-export([set_cookie/3, set_cookie/4, set_cookie2/3, set_cookie2/4, del_header/2]).
-export([mime_extension/1, reason_phrase/1]).
-export([week/1, month/1, convert_month/1, rfc1123_date/1, convert_rfc1123_date/1]).

%%%=======================DEFINE=======================
-define(QUOTE, 34).

-define(IS_WHITESPACE(C),
	(C =:= $\s orelse C =:= $\t orelse C =:= $\r orelse C =:= $\n)).

%% RFC 2616 separators (called tspecials in RFC 2068)
-define(IS_SEPARATOR(C),
	(C < 32 orelse
	C =:= $\s orelse C =:= $\t orelse
	C =:= $( orelse C =:= $) orelse C =:= $< orelse C =:= $> orelse
	C =:= $@ orelse C =:= $, orelse C =:= $; orelse C =:= $: orelse
	C =:= $\\ orelse C =:= ?QUOTE orelse C =:= $/ orelse
	C =:= $[ orelse C =:= $] orelse C =:= $? orelse C =:= $= orelse
	C =:= ${ orelse C =:= $})).

-define(IS_HEX(C),
	((C >= $0 andalso C =< $9) orelse
	(C >= $a andalso C =< $f) orelse
	(C >= $A andalso C =< $F))).

-define(SSE_HEADER(Origin), sb_trees:from_dict([{"Content-Type", "text/event-stream"}, 
												{"Cache-Control", "no-cache"}, 
												{"Connection", "keep-alive"}, 
												{"Access-Control-Allow-Origin", Origin}])).

%%%=======================RECORD=======================
-record(?MODULE, {pid}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 获得指定消息路径的配置
%% @spec get(Key::atom()) -> return()
%% where
%%      return() = none | tuple()
%%@end
%% -----------------------------------------------------------------
get(Key) ->
	zm_config:get(?MODULE, Key).

%% -----------------------------------------------------------------
%%@doc 设置指定消息路径的配置
%% @spec set(Type::{Src::atom(),MsgPath::list()},MFAList::list(),ErrorHandler::tuple()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set({Src, MsgPath} = Type, [{_M, _F, _A} | _] = MFAList,
	{_M1, _F1, _A1} = ErrorHandler) when is_atom(Src), is_list(MsgPath) ->
	zm_config:set(?MODULE, {Type, MFAList, ErrorHandler}).

%% -----------------------------------------------------------------
%%@doc 取消设置指定消息路径的配置
%% @spec unset(Type::{Src::atom(),MsgPath::list()},MFAList::list(),ErrorHandler::tuple()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Type, MFAList, ErrorHandler) ->
	zm_config:unset(?MODULE, {Type, MFAList, ErrorHandler}).

%% -----------------------------------------------------------------
%%@doc 判断连接是否活动
%% @spec is_alive(Record::tuple()) -> boolean()
%%@end
%% -----------------------------------------------------------------
-spec is_alive(tuple()) ->
	true | false.
%% -----------------------------------------------------------------
is_alive(#?MODULE{pid = Pid}) ->
	is_process_alive(Pid).

%% -----------------------------------------------------------------
%%@doc recv
%% @spec recv(Con, Src, Info, Headers, Body) -> ok
%%@end
%% -----------------------------------------------------------------
recv(Con, Src, Info, Headers, Body) ->
	Cookies = parse_cookies(z_lib:get_value(Headers, "cookie", undefined)),
	route(Con, Src, Info, sb_trees:enter("cookies", Cookies, Headers), Body).

%% -----------------------------------------------------------------
%%@doc recv
%% @spec recv(Con, Src, Info, Headers, Body) -> ok
%%@end
%% -----------------------------------------------------------------
recv_sse(Con, Src, Info, Headers, Body) ->
	Cookies = parse_cookies(z_lib:get_value(Headers, "cookie", undefined)),
	route_sse(Con, Src, Info, sb_trees:enter("cookies", Cookies, Headers), Body).

%% -----------------------------------------------------------------
%%@doc 通过连接发送chunk数据
%% @spec send_chunk(Record::tuple(),Body,MFA) -> ok
%%@end
%% -----------------------------------------------------------------
send_chunk(#?MODULE{pid = Pid}, Body, MFA) when is_pid(Pid) ->
	Pid ! {http_response_body_chunk, Body, MFA}.

%% -----------------------------------------------------------------
%%@doc 通过连接发送事件数据
%% @spec send_event(Record::tuple(),Body,MFA) -> ok
%%@end
%% -----------------------------------------------------------------
send_event(#?MODULE{pid = Pid}, Info, Headers, Body) when is_pid(Pid) ->
	Pid ! {http_event, Info, Headers, new_http_event(Body, [])}.

%% -----------------------------------------------------------------
%%@doc 通过连接发送内容数据
%% @spec send(Record::tuple(),Body,MFA) -> ok
%%@end
%% -----------------------------------------------------------------
send(#?MODULE{pid = Pid}, Body, MFA) when is_pid(Pid) ->
	Pid ! {http_response_body, Body, MFA}.

%% -----------------------------------------------------------------
%%@doc 通过连接发送头信息数据，或消息体数据
%% @spec send(Record::tuple(),Info::list(),Headers,Body_MFA) -> return()
%% where
%%      return() = false | ok
%%@end
%% -----------------------------------------------------------------
send(_Con, [], _Headers, _Body) ->
	false;
send(#?MODULE{pid = Pid}, Info, Headers, Body_MFA) when is_pid(Pid) ->
	Pid ! {http_response, Info, Headers, Body_MFA}.

%% -----------------------------------------------------------------
%%@doc get_peer
%% @spec get_peer(Info,Headers) -> list()
%%@end
%% -----------------------------------------------------------------
get_peer(Info, Headers) ->
	IP = z_lib:get_value(Info, ip, {0, 0, 0, 0}),
	case element(1, IP) of
		N when N =:= 0 orelse N =:= 10 orelse N =:= 127 orelse N =:= 192 ->
			case z_lib:get_value(Headers, "x-forwarded-for", undefined) of
				undefined ->
					inet_parse:ntoa(IP);
				Hosts ->
					string:strip(lists:last(string:tokens(Hosts, ",")))
			end;
		_ ->
			inet_parse:ntoa(IP)
	end.

%% -----------------------------------------------------------------
%%@doc get_range
%% @spec get_range(Headers) -> return()
%% where
%%      return() = [] | undefined | fail | [{Min | none, Max | none}]
%%@end
%% -----------------------------------------------------------------
get_range(Headers) ->
	case z_lib:get_value(Headers, "range", undefined) of
		undefined ->
			undefined;
		Value ->
			parse_range(Value)
	end.

%% -----------------------------------------------------------------
%%@doc get_cookies
%% @spec get_cookies(Headers) -> return()
%% where
%%      return() = undefined | sb_trees
%%@end
%% -----------------------------------------------------------------
get_cookies(Headers) ->
	z_lib:get_value(Headers, "cookie", undefined).

%% -----------------------------------------------------------------
%%@doc get_cookie
%% @spec get_cookie(Headers, Key, Default) -> return()
%% where
%%      return() = undefined | Value
%%@end
%% -----------------------------------------------------------------
get_cookie(Headers, Key, Default) ->
	case z_lib:get_value(Headers, "cookies", undefined) of
		undefined ->
			Default;
		Cookies ->
			z_lib:get_value(Cookies, Key, Default)
	end.

%% -----------------------------------------------------------------
%%@doc get_accept_encoding
%% @spec get_accept_charset(Headers) -> return()
%% where
%%      return() = undefined | fail | [{charset, 1.0}]
%%@end
%% -----------------------------------------------------------------
get_accept_charset(Headers) ->
	case z_lib:get_value(Headers, "accept-charset", undefined) of
		undefined ->
			undefined;
		Value ->
			parse_qvalues(Value)
	end.

%% -----------------------------------------------------------------
%%@doc get_accept_encoding
%% @spec get_accept_language(Headers) -> return()
%% where
%%      return() = undefined | fail | [{language, 1.0}]
%%@end
%% -----------------------------------------------------------------
get_accept_language(Headers) ->
	case z_lib:get_value(Headers, "accept-encoding", undefined) of
		undefined ->
			undefined;
		Value ->
			parse_qvalues(Value)
	end.

%% -----------------------------------------------------------------
%%@doc get_accept_encoding
%% @spec get_accept_encoding(Headers) -> return()
%% where
%%      return() = undefined | fail | [{encoding, 1.0}]
%%@end
%% -----------------------------------------------------------------
get_accept_encoding(Headers) ->
	case z_lib:get_value(Headers, "accept-encoding", undefined) of
		undefined ->
			undefined;
		Value ->
			parse_qvalues(Value)
	end.

%% -----------------------------------------------------------------
%%@doc main_domain
%% @spec main_domain(Domain::list()) -> return()
%% where
%%      return() = Domain::list()
%%@end
%% -----------------------------------------------------------------
main_domain([H | _] = Domain) when H > $0, H =< $9 ->
	Domain;
main_domain(Domain) ->
	case string:chr(Domain, $.) of
		0 -> Domain;
		Index -> main_domain1(Domain, Index)
	end.

main_domain1(Domain, Index) ->
	Main = string:substr(Domain, Index + 1),
	case string:chr(Main, $.) of
		0 -> Domain;
		Index1 -> main_domain1(Main, Index1)
	end.

%% -----------------------------------------------------------------
%%@doc parse_url_path
%% @spec parse_url_path(File::list()) -> return()
%% where
%%      return() = {Path::list(), Other::list()}
%%@end
%% -----------------------------------------------------------------
parse_url_path(File) ->
	parse_url_path(File, []).

parse_url_path([$%, Hi, Lo | T], L) when ?IS_HEX(Lo), ?IS_HEX(Hi) ->
	parse_url_path(T, [(z_lib:hex_digit(Lo) bor (z_lib:hex_digit(Hi) bsl 4)) | L]);
parse_url_path([$# | _] = T, L) ->
	{z_lib:rutf8_to_characters(L), T};
parse_url_path([$? | T], L) ->
	{z_lib:rutf8_to_characters(L), T};
parse_url_path([H | T], L) ->
	parse_url_path(T, [H | L]);
parse_url_path([], L) ->
	{z_lib:rutf8_to_characters(L), ""}.

%% -----------------------------------------------------------------
%%@doc url_split_query
%% @spec parse_url_query(Query::list()) -> return()
%% where
%%      return() = {Query::list(), Fragment}
%%@end
%% -----------------------------------------------------------------
parse_url_query(Query) ->
	parse_url_query(Query, []).

parse_url_query([$# | T], L) ->
	{lists:reverse(L), T};
parse_url_query([H | T], L) ->
	parse_url_query(T, [H | L]);
parse_url_query([], L) ->
	{lists:reverse(L), ""}.

%% -----------------------------------------------------------------
%%@doc parse_range
%% @spec parse_range(Args::list()) -> return()
%% where
%%      return() = [] | undefined | fail | [{Min | none, Max | none}]
%%@end
%% -----------------------------------------------------------------
parse_range("bytes=") ->
	[];
parse_range("bytes=0-") ->
	undefined;
parse_range("bytes=" ++ Range) ->
	parse_range(z_lib:split(Range, $,), []);
parse_range(_) ->
	fail.

parse_range([H | T], L) ->
	case z_lib:split(H, $-) of
		["", N] ->
			parse_range(T, [{none, list_to_integer(N)} | L]);
		[N, ""] ->
			parse_range(T, [{list_to_integer(N), none} | L]);
		[N1, N2] ->
			parse_range(T, [{list_to_integer(N1), list_to_integer(N2)} | L]);
		_ ->
			fail
	end;
parse_range([], L) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc parse_cookies
%% @spec parse_cookies(Args::undefined | tuple() | list()) -> return()
%% where
%%      return() = sb_trees() | nil
%%@end
%% -----------------------------------------------------------------
parse_cookies(undefined) ->
	sb_trees:empty();
parse_cookies({_, String}) ->
	parse_cookies(String, sb_trees:empty());
parse_cookies(String) ->
	parse_cookies(String, sb_trees:empty()).

parse_cookies("", Tree) ->
	Tree;
parse_cookies(String, Tree) ->
	{Key, Value, R} = parse_cookie(String),
	case Key of
		[$$ | _] ->
			parse_cookies(R, Tree);
		"" ->
			parse_cookies(R, Tree);
		_ ->
			parse_cookies(R, sb_trees:enter(Key, Value, Tree))
	end.

%% -----------------------------------------------------------------
%%@doc 分析请求的字符串数据，返回倒序的键值对列表
%%```
%%  KVList::list()
%%'''
%% @spec parse_qs(String::list(),L::list()) -> KVList
%%@end
%% -----------------------------------------------------------------
parse_qs([_ | _] = String, L) ->
	{Key, R} = parse_qs_key(String, []),
	{Value, R1} = parse_qs_value(R, []),
	parse_qs(R1, [{Key, Value} | L]);
parse_qs([], L) ->
	L.

parse_qs_key([$= | T], L) ->
	{z_lib:rutf8_to_characters(L), T};
parse_qs_key([$; | _] = Q, L) ->
	{z_lib:rutf8_to_characters(L), Q};
parse_qs_key([$& | _] = Q, L) ->
	{z_lib:rutf8_to_characters(L), Q};
parse_qs_key([$+ | T], L) ->
	parse_qs_key(T, [$\s | L]);
parse_qs_key([$%, Hi, Lo | T], L) when ?IS_HEX(Lo), ?IS_HEX(Hi) ->
	parse_qs_key(T, [(z_lib:hex_digit(Lo) bor (z_lib:hex_digit(Hi) bsl 4)) | L]);
parse_qs_key([C | T], L) ->
	parse_qs_key(T, [C | L]);
parse_qs_key([], L) ->
	{z_lib:rutf8_to_characters(L), ""}.

parse_qs_value([$; | T], L) ->
	{z_lib:rutf8_to_characters(L), T};
parse_qs_value([$& | T], L) ->
	{z_lib:rutf8_to_characters(L), T};
parse_qs_value([$+ | T], L) ->
	parse_qs_value(T, [$\s | L]);
parse_qs_value([$%, Hi, Lo | T], L) when ?IS_HEX(Lo), ?IS_HEX(Hi) ->
	parse_qs_value(T, [(z_lib:hex_digit(Lo) bor (z_lib:hex_digit(Hi) bsl 4)) | L]);
parse_qs_value([H | T], L) ->
	parse_qs_value(T, [H | L]);
parse_qs_value([], L) ->
	{z_lib:rutf8_to_characters(L), ""}.

%% -----------------------------------------------------------------
%%@doc 分析accepted 请求中的Q值
%% @spec parse_qvalues(Str::string()) -> return()
%% where
%%      return() = fail | list()
%%@end
%% -----------------------------------------------------------------
parse_qvalues(Str) ->
	try
		L = z_lib:split_filter(Str, $,, $\s),
		lists:sort(fun(X, Y) -> element(2, X) >= element(2, Y) end, [parse_qe(E) || E <- L])
	catch
		_:_ ->
			fail
	end.

parse_qe(Str) ->
	case z_lib:split(Str, $;) of
		[S] ->
			{S, 1.0};
		[S, "q=" ++ Q] ->
			case Q of
				"1" ->
					{S, 1.0};
				"0" ->
					{S, 0.0};
				[$0, $. | _] ->
					{S, list_to_float(Q)}
			end
	end.

%% -----------------------------------------------------------------
%%@doc 获取字符串中HEX字符，遇到第一个非HEX字符就返回
%% @spec trim_hex(L::list()) -> list()
%%@end
%% -----------------------------------------------------------------
trim_hex(L) ->
	trim_hex(L, []).

trim_hex([H | T], L) when ?IS_HEX(H) ->
	trim_hex(T, [H | L]);
trim_hex(_, L) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc 生成一个新的session_cookie_id
%% @spec new_scid(IP::tuple(),Port::port(),Pid::pid()) -> list()
%%@end
%% -----------------------------------------------------------------
new_scid(IP, Port, Pid) ->
	new_scid(IP, Port, zm_time:now_millisecond(), random:uniform(16#7fffffff), node(), Pid).

%% -----------------------------------------------------------------
%%@doc 生成一个新的session_cookie_id
%% @spec new_scid(NTuple::tuple(),Port::port(),MilliSecond::integer(),Random,Node,Pid) -> list()
%%@end
%% -----------------------------------------------------------------
new_scid({N1, N2, N3, N4}, Port, MilliSecond, Random, Node, Pid) ->
	H1 = erlang:phash2(Node, 16#ffffffff),
	H2 = erlang:phash2(Pid, 16#ffffffff),
	Bin = <<N1:8, N2:8, N3:8, N4:8, Port:16, MilliSecond:64, Random:32, H1:32, H2:32>>,
	base58:encode(Bin);
new_scid({N1, N2, N3, N4, N5, N6, N7, N8}, Port, MilliSecond, Random, Node, Pid) ->
	H1 = erlang:phash2(Node, 16#ffffffff),
	H2 = erlang:phash2(Pid, 16#ffffffff),
	Bin = <<N1:16, N2:16, N3:16, N4:16, N5:16, N6:16, N7:16, N8:16,
		Port:16, MilliSecond:64, Random:32, H1:32, H2:32>>,
	base58:encode(Bin).

%% -----------------------------------------------------------------
%%@doc 创建回应消息头
%% @spec resp_headers(Info, ReqHeaders, Ext::string()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
resp_headers(Info, ReqHeaders, Ext) ->
	resp_headers(Info, ReqHeaders, Ext, none).

%% -----------------------------------------------------------------
%%@doc 创建回应消息头
%% @spec resp_headers(Info, ReqHeaders, Ext::string()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
resp_headers(Info, ReqHeaders, Ext, SidKey) ->
	H1 = sb_trees:enter("Date", rfc1123_date(erlang:universaltime()),
		sb_trees:enter("Server", "zm_http", sb_trees:empty())),
	H2 = case mime_extension(Ext) of
		"text/" ++ _ = Mime ->
			sb_trees:enter("Content-Type", Mime ++ "; charset=utf-8", H1);
		Mime ->
			sb_trees:enter("Content-Type", Mime, H1)
	end,
	Con = case z_lib:get_value(Info, version, {1, 0}) of
		{1, 1} ->
			case string:to_lower(z_lib:get_value(ReqHeaders, "connection", "")) of
				"keep-alive" = C ->
					C;
				"keep-alive" ++ _ ->
					"keep-alive";
				_ ->
					"close"
			end;
		_ ->
			"close"
	end,
	H3 = sb_trees:enter("Connection", Con, H2),
	H4 = case get_accept_encoding(ReqHeaders) of
		[{"gzip", _} | _] ->
			sb_trees:enter("Content-Encoding", "gzip", H3);
		[{"deflate", _} | _] ->
			sb_trees:enter("Content-Encoding", "deflate", H3);
		_ ->
			H3
	end,
	Domain = main_domain(z_lib:get_value(Info, host, "")),
	case SidKey of
		{Key, ID} ->
			set_cookie(H4, Key, ID, [{"path", "/"}, {"domain", Domain}, {"expires", "Thu, 30 Dec 2100 16:00:00 GMT"}, "HttpOnly"]);
		[_|_] ->
			case get_cookie(ReqHeaders, SidKey, 0) of
				0 ->
					ID = new_scid(z_lib:get_value(Info, ip, {0, 0, 0, 0}), z_lib:get_value(Info, port, 0), self()),
					set_cookie(H4, SidKey, ID, [{"path", "/"}, {"domain", Domain}, {"expires", "Thu, 30 Dec 2100 16:00:00 GMT"}, "HttpOnly"]);
				_ ->
					H4
			end;
		_ ->
			H4
	end.

%% -----------------------------------------------------------------
%%@doc 设置头信息的键值
%% @spec set_header(Tree::tuple(),K::list(),V::term()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
set_header(Tree, K, V) when is_list(K) ->
	sb_trees:enter(K, z_lib:any_to_iolist(V), Tree).

%% -----------------------------------------------------------------
%%@doc 设置回应消息的Content-Type
%% @spec set_content_type(Headers,Type::string(),Charset) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
set_content_type(Headers, [$. | _] = Type, Charset) ->
	set_content_type(Headers, mime_extension(Type), Charset);
set_content_type(Headers, Type, Charset) ->
	case Type of
		"text/" ++ _ = Mime ->
			sb_trees:enter("Content-Type", Mime ++ "; charset=" ++ Charset, Headers);
		Mime ->
			sb_trees:enter("Content-Type", Mime, Headers)
	end.

%% -----------------------------------------------------------------
%%@doc 设置头信息的cookie
%% @spec set_cookie(Tree::tuple(), K, V) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
set_cookie(Tree, K, V) ->
	set_cookie(Tree, "Set-Cookie", K, V, []).

%% -----------------------------------------------------------------
%%@doc 设置头信息的cookie
%% @spec set_cookie(Tree::tuple(), K, V, Opts::list()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
set_cookie(Tree, K, V, Opts) ->
	set_cookie(Tree, "Set-Cookie", K, V, Opts).

%% -----------------------------------------------------------------
%%@doc 设置头信息的cookie2
%% @spec set_cookie2(Tree::tuple(), K, V) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
set_cookie2(Tree, K, V) ->
	set_cookie(Tree, "Set-Cookie2", K, V, []).

%% -----------------------------------------------------------------
%%@doc 设置头信息的cookie2
%% @spec set_cookie2(Tree::tuple(), K, V, Opts::list()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
set_cookie2(Tree, K, V, Opts) ->
	set_cookie(Tree, "Set-Cookie2", K, V, Opts).

%% -----------------------------------------------------------------
%%@doc 删除指定键的头信息
%% @spec del_header(Tree::tuple(), K) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
del_header(Tree, K) ->
	sb_trees:delete_any(K, Tree).

%% -----------------------------------------------------------------
%% Func: mime_extension/1
%% Description: mime_extension
%% Returns: string()
%% -----------------------------------------------------------------
mime_extension(".001") -> "application/x-001";
mime_extension(".301") -> "application/x-301";
mime_extension(".323") -> "text/h323";
mime_extension(".906") -> "application/x-906";
mime_extension(".907") -> "drawing/907";
mime_extension(".a11") -> "application/x-a11";
mime_extension(".acp") -> "audio/x-mei-aac";
mime_extension(".ai") -> "application/postscript";
mime_extension(".aif") -> "audio/aiff";
mime_extension(".aifc") -> "audio/aiff";
mime_extension(".aiff") -> "audio/aiff";
mime_extension(".anv") -> "application/x-anv";
mime_extension(".asa") -> "text/asa";
mime_extension(".asf") -> "video/x-ms-asf";
mime_extension(".asp") -> "text/asp";
mime_extension(".asx") -> "video/x-ms-asf";
mime_extension(".au") -> "audio/basic";
mime_extension(".avi") -> "video/avi";
mime_extension(".awf") -> "application/vnd.adobe.workflow";
mime_extension(".biz") -> "text/xml";
mime_extension(".bmp") -> "application/x-bmp";
mime_extension(".bot") -> "application/x-bot";
mime_extension(".bz2") -> "application/x-bzip2";
mime_extension(".c") -> "text/plain";
mime_extension(".c4t") -> "application/x-c4t";
mime_extension(".c90") -> "application/x-c90";
mime_extension(".cal") -> "application/x-cals";
mime_extension(".cat") -> "application/vnd.ms-pki.seccat";
mime_extension(".cdf") -> "application/x-netcdf";
mime_extension(".cdr") -> "application/x-cdr";
mime_extension(".cel") -> "application/x-cel";
mime_extension(".cer") -> "application/x-x509-ca-cert";
mime_extension(".cg4") -> "application/x-g4";
mime_extension(".cgm") -> "application/x-cgm";
mime_extension(".cit") -> "application/x-cit";
mime_extension(".class") -> "java/*";
mime_extension(".cml") -> "text/xml";
mime_extension(".cmp") -> "application/x-cmp";
mime_extension(".cmx") -> "application/x-cmx";
mime_extension(".cot") -> "application/x-cot";
mime_extension(".cpp") -> "text/plain";
mime_extension(".crl") -> "application/pkix-crl";
mime_extension(".crt") -> "application/x-x509-ca-cert";
mime_extension(".csi") -> "application/x-csi";
mime_extension(".css") -> "text/css";
mime_extension(".csv") -> "text/csv";
mime_extension(".cut") -> "application/x-cut";
mime_extension(".dbf") -> "application/x-dbf";
mime_extension(".dbm") -> "application/x-dbm";
mime_extension(".dbx") -> "application/x-dbx";
mime_extension(".dcd") -> "text/xml";
mime_extension(".dcx") -> "application/x-dcx";
mime_extension(".der") -> "application/x-x509-ca-cert";
mime_extension(".dgn") -> "application/x-dgn";
mime_extension(".dib") -> "application/x-dib";
mime_extension(".dll") -> "application/x-msdownload";
mime_extension(".doc") -> "application/msword";
mime_extension(".docx") -> "application/msword";
mime_extension(".dot") -> "application/msword";
mime_extension(".drw") -> "application/x-drw";
mime_extension(".dtd") -> "text/xml";
mime_extension(".dwf") -> "model/vnd.dwf";
mime_extension(".dwg") -> "application/x-dwg";
mime_extension(".dxb") -> "application/x-dxb";
mime_extension(".dxf") -> "application/x-dxf";
mime_extension(".edn") -> "application/vnd.adobe.edn";
mime_extension(".emf") -> "application/x-emf";
mime_extension(".eml") -> "message/rfc822";
mime_extension(".ent") -> "text/xml";
mime_extension(".epi") -> "application/x-epi";
mime_extension(".eps") -> "application/postscript";
mime_extension(".etd") -> "application/x-ebx";
mime_extension(".exe") -> "application/x-msdownload";
mime_extension(".ez") -> "application/andrew-inset";
mime_extension(".fax") -> "image/fax";
mime_extension(".fdf") -> "application/vnd.fdf";
mime_extension(".fif") -> "application/fractals";
mime_extension(".fo") -> "text/xml";
mime_extension(".frm") -> "application/x-frm";
mime_extension(".g4") -> "application/x-g4";
mime_extension(".gbr") -> "application/x-gbr";
mime_extension(".gif") -> "image/gif";
mime_extension(".gl2") -> "application/x-gl2";
mime_extension(".gp4") -> "application/x-gp4";
mime_extension(".gz") -> "application/x-gzip";
mime_extension(".h") -> "text/plain";
mime_extension(".hgl") -> "application/x-hgl";
mime_extension(".hmr") -> "application/x-hmr";
mime_extension(".hpg") -> "application/x-hpgl";
mime_extension(".hpl") -> "application/x-hpl";
mime_extension(".hqx") -> "application/mac-binhex40";
mime_extension(".hrf") -> "application/x-hrf";
mime_extension(".hta") -> "application/hta";
mime_extension(".htc") -> "text/x-component";
mime_extension(".htm") -> "text/html";
mime_extension(".html") -> "text/html";
mime_extension(".htt") -> "text/webviewhtml";
mime_extension(".htx") -> "text/html";
mime_extension(".icb") -> "application/x-icb";
mime_extension(".ico") -> "image/x-icon";
mime_extension(".iff") -> "application/x-iff";
mime_extension(".ig4") -> "application/x-g4";
mime_extension(".igs") -> "application/x-igs";
mime_extension(".iii") -> "application/x-iphone";
mime_extension(".img") -> "application/x-img";
mime_extension(".ins") -> "application/x-internet-signup";
mime_extension(".isp") -> "application/x-internet-signup";
mime_extension(".IVF") -> "video/x-ivf";
mime_extension(".java") -> "java/*";
mime_extension(".jfif") -> "image/jpeg";
mime_extension(".jpe") -> "image/jpeg";
mime_extension(".jpeg") -> "image/jpeg";
mime_extension(".jpg") -> "image/jpeg";
mime_extension(".js") -> "text/javascript";
mime_extension(".jsp") -> "text/html";
mime_extension(".la1") -> "audio/x-liquid-file";
mime_extension(".lar") -> "application/x-laplayer-reg";
mime_extension(".latex") -> "application/x-latex";
mime_extension(".lavs") -> "audio/x-liquid-secure";
mime_extension(".lbm") -> "application/x-lbm";
mime_extension(".lmsff") -> "audio/x-la-lms";
mime_extension(".ltr") -> "application/x-ltr";
mime_extension(".m1v") -> "video/x-mpeg";
mime_extension(".m2v") -> "video/x-mpeg";
mime_extension(".m3u") -> "audio/mpegurl";
mime_extension(".m4a") -> "audio/mpeg";
mime_extension(".m4e") -> "video/mpeg4";
mime_extension(".mac") -> "application/x-mac";
mime_extension(".man") -> "application/x-troff-man";
mime_extension(".math") -> "text/xml";
mime_extension(".mdb") -> "application/msaccess";
mime_extension(".mfp") -> "application/x-shockwave-flash";
mime_extension(".mht") -> "message/rfc822";
mime_extension(".mhtml") -> "message/rfc822";
mime_extension(".mi") -> "application/x-mi";
mime_extension(".mid") -> "audio/mid";
mime_extension(".midi") -> "audio/mid";
mime_extension(".mil") -> "application/x-mil";
mime_extension(".mml") -> "text/xml";
mime_extension(".mnd") -> "audio/x-musicnet-download";
mime_extension(".mns") -> "audio/x-musicnet-stream";
mime_extension(".mov") -> "video/quicktime";
mime_extension(".movie") -> "video/x-sgi-movie";
mime_extension(".mp1") -> "audio/mp1";
mime_extension(".mp2") -> "audio/mp2";
mime_extension(".mp2v") -> "video/mpeg";
mime_extension(".mp3") -> "audio/mp3";
mime_extension(".mp4") -> "video/mpeg4";
mime_extension(".mpa") -> "video/x-mpg";
mime_extension(".mpd") -> "application/vnd.ms-project";
mime_extension(".mpe") -> "video/x-mpeg";
mime_extension(".mpeg") -> "video/mpg";
mime_extension(".mpg") -> "video/mpg";
mime_extension(".mpga") -> "audio/rn-mpeg";
mime_extension(".mpp") -> "application/vnd.ms-project";
mime_extension(".mps") -> "video/x-mpeg";
mime_extension(".mpt") -> "application/vnd.ms-project";
mime_extension(".mpv") -> "video/mpg";
mime_extension(".mpv2") -> "video/mpeg";
mime_extension(".mpw") -> "application/vnd.ms-project";
mime_extension(".mpx") -> "application/vnd.ms-project";
mime_extension(".mtx") -> "text/xml";
mime_extension(".mxp") -> "application/x-mmxp";
mime_extension(".net") -> "image/pnetvue";
mime_extension(".nrf") -> "application/x-nrf";
mime_extension(".nws") -> "message/rfc822";
mime_extension(".odc") -> "text/x-ms-odc";
mime_extension(".out") -> "application/x-out";
mime_extension(".p10") -> "application/pkcs10";
mime_extension(".p12") -> "application/x-pkcs12";
mime_extension(".p7b") -> "application/x-pkcs7-certificates";
mime_extension(".p7c") -> "application/pkcs7-mime";
mime_extension(".p7m") -> "application/pkcs7-mime";
mime_extension(".p7r") -> "application/x-pkcs7-certreqresp";
mime_extension(".p7s") -> "application/pkcs7-signature";
mime_extension(".pc5") -> "application/x-pc5";
mime_extension(".pci") -> "application/x-pci";
mime_extension(".pcl") -> "application/x-pcl";
mime_extension(".pcx") -> "application/x-pcx";
mime_extension(".pdf") -> "application/pdf";
mime_extension(".pdx") -> "application/vnd.adobe.pdx";
mime_extension(".pfx") -> "application/x-pkcs12";
mime_extension(".pgl") -> "application/x-pgl";
mime_extension(".php") -> "text/php";
mime_extension(".php3") -> "application/x-httpd-php";
mime_extension(".php4") -> "application/x-httpd-php";
mime_extension(".php5") -> "application/x-httpd-php";
mime_extension(".pic") -> "application/x-pic";
mime_extension(".pko") -> "application/vnd.ms-pki.pko";
mime_extension(".pl") -> "application/x-perl";
mime_extension(".plg") -> "text/html";
mime_extension(".pls") -> "audio/scpls";
mime_extension(".plt") -> "application/x-plt";
mime_extension(".png") -> "image/png";
mime_extension(".pot") -> "application/vnd.ms-powerpoint";
mime_extension(".ppa") -> "application/vnd.ms-powerpoint";
mime_extension(".ppm") -> "application/x-ppm";
mime_extension(".pps") -> "application/vnd.ms-powerpoint";
mime_extension(".ppt") -> "application/vnd.ms-powerpoint";
mime_extension(".pr") -> "application/x-pr";
mime_extension(".prf") -> "application/pics-rules";
mime_extension(".prn") -> "application/x-prn";
mime_extension(".prt") -> "application/x-prt";
mime_extension(".ps") -> "application/postscript";
mime_extension(".ptn") -> "application/x-ptn";
mime_extension(".pwz") -> "application/vnd.ms-powerpoint";
mime_extension(".r3t") -> "text/vnd.rn-realtext3d";
mime_extension(".ra") -> "audio/vnd.rn-realaudio";
mime_extension(".ram") -> "audio/x-pn-realaudio";
mime_extension(".ras") -> "application/x-ras";
mime_extension(".rat") -> "application/rat-file";
mime_extension(".rdf") -> "text/xml";
mime_extension(".rec") -> "application/vnd.rn-recording";
mime_extension(".red") -> "application/x-red";
mime_extension(".rgb") -> "application/x-rgb";
mime_extension(".rjs") -> "application/vnd.rn-realsystem-rjs";
mime_extension(".rjt") -> "application/vnd.rn-realsystem-rjt";
mime_extension(".rlc") -> "application/x-rlc";
mime_extension(".rle") -> "application/x-rle";
mime_extension(".rm") -> "application/vnd.rn-realmedia";
mime_extension(".rmf") -> "application/vnd.adobe.rmf";
mime_extension(".rmi") -> "audio/mid";
mime_extension(".rmj") -> "application/vnd.rn-realsystem-rmj";
mime_extension(".rmm") -> "audio/x-pn-realaudio";
mime_extension(".rmp") -> "application/vnd.rn-rn_music_package";
mime_extension(".rms") -> "application/vnd.rn-realmedia-secure";
mime_extension(".rmvb") -> "application/vnd.rn-realmedia-vbr";
mime_extension(".rmx") -> "application/vnd.rn-realsystem-rmx";
mime_extension(".rnx") -> "application/vnd.rn-realplayer";
mime_extension(".rp") -> "image/vnd.rn-realpix";
mime_extension(".rpm") -> "audio/x-pn-realaudio-plugin";
mime_extension(".rsml") -> "application/vnd.rn-rsml";
mime_extension(".rt") -> "text/vnd.rn-realtext";
mime_extension(".rtf") -> "application/msword";
mime_extension(".rv") -> "video/vnd.rn-realvideo";
mime_extension(".sam") -> "application/x-sam";
mime_extension(".sat") -> "application/x-sat";
mime_extension(".sdp") -> "application/sdp";
mime_extension(".sdw") -> "application/x-sdw";
mime_extension(".sgm") -> "text/sgml";
mime_extension(".sgml") -> "text/sgml";
mime_extension(".sh") -> "application/x-sh";
mime_extension(".sit") -> "application/x-stuffit";
mime_extension(".slb") -> "application/x-slb";
mime_extension(".sld") -> "application/x-sld";
mime_extension(".slk") -> "drawing/x-slk";
mime_extension(".smi") -> "application/smil";
mime_extension(".smil") -> "application/smil";
mime_extension(".smk") -> "application/x-smk";
mime_extension(".snd") -> "audio/basic";
mime_extension(".sol") -> "text/plain";
mime_extension(".sor") -> "text/plain";
mime_extension(".spc") -> "application/x-pkcs7-certificates";
mime_extension(".spl") -> "application/futuresplash";
mime_extension(".spp") -> "text/xml";
mime_extension(".ssm") -> "application/streamingmedia";
mime_extension(".sst") -> "application/vnd.ms-pki.certstore";
mime_extension(".stl") -> "application/vnd.ms-pki.stl";
mime_extension(".stm") -> "text/html";
mime_extension(".sty") -> "application/x-sty";
mime_extension(".svg") -> "text/xml";
mime_extension(".swf") -> "application/x-shockwave-flash";
mime_extension(".tar") -> "application/x-tar";
mime_extension(".tdf") -> "application/x-tdf";
mime_extension(".tg4") -> "application/x-tg4";
mime_extension(".tga") -> "application/x-tga";
mime_extension(".tgz") -> "application/x-gzip";
mime_extension(".tif") -> "image/tiff";
mime_extension(".tiff") -> "image/tiff";
mime_extension(".tld") -> "text/xml";
mime_extension(".top") -> "drawing/x-top";
mime_extension(".torrent") -> "application/x-bittorrent";
mime_extension(".tsd") -> "text/xml";
mime_extension(".txt") -> "text/plain";
mime_extension(".uin") -> "application/x-icq";
mime_extension(".uls") -> "text/iuls";
mime_extension(".vcf") -> "text/x-vcard";
mime_extension(".vda") -> "application/x-vda";
mime_extension(".vdx") -> "application/vnd.visio";
mime_extension(".vml") -> "text/xml";
mime_extension(".vpg") -> "application/x-vpeg005";
mime_extension(".vsd") -> "application/vnd.visio";
mime_extension(".vss") -> "application/vnd.visio";
mime_extension(".vst") -> "application/vnd.visio";
mime_extension(".vsw") -> "application/vnd.visio";
mime_extension(".vsx") -> "application/vnd.visio";
mime_extension(".vtx") -> "application/vnd.visio";
mime_extension(".vxml") -> "text/xml";
mime_extension(".wav") -> "audio/wav";
mime_extension(".wax") -> "audio/x-ms-wax";
mime_extension(".wb1") -> "application/x-wb1";
mime_extension(".wb2") -> "application/x-wb2";
mime_extension(".wb3") -> "application/x-wb3";
mime_extension(".wbm") -> "image/wbm";
mime_extension(".wbmp") -> "image/vnd.wap.wbmp";
mime_extension(".webp") -> "image/webp";
mime_extension(".wiz") -> "application/msword";
mime_extension(".wk3") -> "application/x-wk3";
mime_extension(".wk4") -> "application/x-wk4";
mime_extension(".wkq") -> "application/x-wkq";
mime_extension(".wks") -> "application/x-wks";
mime_extension(".wm") -> "video/x-ms-wm";
mime_extension(".wma") -> "audio/x-ms-wma";
mime_extension(".wmd") -> "application/x-ms-wmd";
mime_extension(".wmf") -> "application/x-wmf";
mime_extension(".wml") -> "text/vnd.wap.wml";
mime_extension(".wmv") -> "video/x-ms-wmv";
mime_extension(".wmx") -> "video/x-ms-wmx";
mime_extension(".wmz") -> "application/x-ms-wmz";
mime_extension(".wp6") -> "application/x-wp6";
mime_extension(".wpd") -> "application/x-wpd";
mime_extension(".wpg") -> "application/x-wpg";
mime_extension(".wpl") -> "application/vnd.ms-wpl";
mime_extension(".wq1") -> "application/x-wq1";
mime_extension(".wr1") -> "application/x-wr1";
mime_extension(".wri") -> "application/x-wri";
mime_extension(".wrk") -> "application/x-wrk";
mime_extension(".ws") -> "application/x-ws";
mime_extension(".ws2") -> "application/x-ws";
mime_extension(".wsc") -> "text/scriptlet";
mime_extension(".wsdl") -> "text/xml";
mime_extension(".wvx") -> "video/x-ms-wvx";
mime_extension(".xdp") -> "application/vnd.adobe.xdp";
mime_extension(".xdr") -> "text/xml";
mime_extension(".xfd") -> "application/vnd.adobe.xfd";
mime_extension(".xfdf") -> "application/vnd.adobe.xfdf";
mime_extension(".xhtml") -> "application/xhtml+xml";
mime_extension(".xls") -> "application/vnd.ms-excel";
mime_extension(".xlsx") -> "application/vnd.ms-excel";
mime_extension("..appcache") -> "text/cache-manifest";
mime_extension(".xlw") -> "application/x-xlw";
mime_extension(".xml") -> "text/xml";
mime_extension(".xpl") -> "audio/scpls";
mime_extension(".xq") -> "text/xml";
mime_extension(".xql") -> "text/xml";
mime_extension(".xquery") -> "text/xml";
mime_extension(".xsd") -> "text/xml";
mime_extension(".xsl") -> "text/xml";
mime_extension(".xslt") -> "text/xml";
mime_extension(".xwd") -> "application/x-xwd";
mime_extension(".x_b") -> "application/x-x_b";
mime_extension(".x_t") -> "application/x-x_t";
mime_extension(".z") -> "application/x-compress";
mime_extension(".zip") -> "application/zip";
mime_extension(".*") -> "application/octet-stream";
mime_extension([$. | T]) -> "application/x-" ++ T;
mime_extension(_) -> "application/octet-stream".

%% -----------------------------------------------------------------
%% Func: reason_phrase/1
%% Description: RFC 2616, HTTP 1.1 Status codes
%% Returns: string()
%% -----------------------------------------------------------------
reason_phrase(100) -> "Continue";
reason_phrase(101) -> "Switching Protocols";
reason_phrase(200) -> "OK";
reason_phrase(201) -> "Created";
reason_phrase(202) -> "Accepted";
reason_phrase(203) -> "Non-Authoritative Information";
reason_phrase(204) -> "No Content";
reason_phrase(205) -> "Reset Content";
reason_phrase(206) -> "Partial Content";
reason_phrase(300) -> "Multiple Choices";
reason_phrase(301) -> "Moved Permanently";
reason_phrase(302) -> "Moved Temporarily";
reason_phrase(303) -> "See Other";
reason_phrase(304) -> "Not Modified";
reason_phrase(305) -> "Use Proxy";
reason_phrase(306) -> "(unused)";
reason_phrase(307) -> "Temporary Redirect";
reason_phrase(400) -> "Bad Request";
reason_phrase(401) -> "Unauthorized";
reason_phrase(402) -> "Payment Required";
reason_phrase(403) -> "Forbidden";
reason_phrase(404) -> "Object Not Found";
reason_phrase(405) -> "Method Not Allowed";
reason_phrase(406) -> "Not Acceptable";
reason_phrase(407) -> "Proxy Authentication Required";
reason_phrase(408) -> "Request Time-out";
reason_phrase(409) -> "Conflict";
reason_phrase(410) -> "Gone";
reason_phrase(411) -> "Length Required";
reason_phrase(412) -> "Precondition Failed";
reason_phrase(413) -> "Request Entity Too Large";
reason_phrase(414) -> "Request-URI Too Large";
reason_phrase(415) -> "Unsupported Media Type";
reason_phrase(416) -> "Requested Range Not Satisfiable";
reason_phrase(417) -> "Expectation Failed";
reason_phrase(500) -> "Internal Server Error";
reason_phrase(501) -> "Not Implemented";
reason_phrase(502) -> "Bad Gateway";
reason_phrase(503) -> "Service Unavailable";
reason_phrase(504) -> "Gateway Time-out";
reason_phrase(505) -> "HTTP Version not supported";

% RFC 2518, HTTP Extensions for Distributed Authoring -- WEBDAV
reason_phrase(102) -> "Processing";
reason_phrase(207) -> "Multi-Status";
reason_phrase(422) -> "Unprocessable Entity";
reason_phrase(423) -> "Locked";
reason_phrase(424) -> "Failed Dependency";
reason_phrase(507) -> "Insufficient Storage";

% (Work in Progress) WebDAV Advanced Collections
reason_phrase(425) -> "";

% RFC 2817, HTTP Upgrade to TLS
reason_phrase(426) -> "Upgrade Required";

% RFC 3229, Delta encoding in HTTP
reason_phrase(226) -> "IM Used";

reason_phrase(_) -> "Internal Server Error".

%% -----------------------------------------------------------------
%% Func: week/1
%% Description: week
%% Returns: string()
%% -----------------------------------------------------------------
week(1) -> "Mon";
week(2) -> "Tue";
week(3) -> "Wed";
week(4) -> "Thu";
week(5) -> "Fri";
week(6) -> "Sat";
week(7) -> "Sun".

%% -----------------------------------------------------------------
%% Func: month/1
%% Description: month
%% Returns: string()
%% -----------------------------------------------------------------
month(1) -> "Jan";
month(2) -> "Feb";
month(3) -> "Mar";
month(4) -> "Apr";
month(5) -> "May";
month(6) -> "Jun";
month(7) -> "Jul";
month(8) -> "Aug";
month(9) -> "Sep";
month(10) -> "Oct";
month(11) -> "Nov";
month(12) -> "Dec".

%% -----------------------------------------------------------------
%% Func: convert_month/1
%% Description: convert_month
%% Returns: integer()
%% -----------------------------------------------------------------
convert_month("Jan") -> 1;
convert_month("Feb") -> 2;
convert_month("Mar") -> 3;
convert_month("Apr") -> 4;
convert_month("May") -> 5;
convert_month("Jun") -> 6;
convert_month("Jul") -> 7;
convert_month("Aug") -> 8;
convert_month("Sep") -> 9;
convert_month("Oct") -> 10;
convert_month("Nov") -> 11;
convert_month("Dec") -> 12.

%% -----------------------------------------------------------------
%% Func: rfc1123_date/1
%% Description: rfc1123_date
%% Returns: string()
%% -----------------------------------------------------------------
rfc1123_date({{YYYY, MM, DD}, {Hour, Min, Sec}}) ->
	DayNumber = calendar:day_of_the_week({YYYY, MM, DD}),
	lists:flatten(io_lib:format("~s, ~2.2.0w ~3.s ~4.4.0w ~2.2.0w:~2.2.0w:~2.2.0w GMT",
		[week(DayNumber), DD, month(MM), YYYY, Hour, Min, Sec]));
rfc1123_date(_) ->
	undefined.

%% -----------------------------------------------------------------
%% Func: convert_rfc1123_date/1
%% Description: convert_rfc1123_date
%% Returns: {{Year, Month, Day}, {Hour, Min, Sec}}}
%% -----------------------------------------------------------------
convert_rfc1123_date(
	[_D, _A, _Y, _C, _SP,
	D1, D2, _SP,
	M, O, N, _SP,
	Y1, Y2, Y3, Y4, _SP,
	H1, H2, _Col,
	M1, M2, _Col,
	S1, S2 | _]) ->
	Year = list_to_integer([Y1, Y2, Y3, Y4]),
	Day = list_to_integer([D1, D2]),
	Month = convert_month([M, O, N]),
	Hour = list_to_integer([H1, H2]),
	Min = list_to_integer([M1, M2]),
	Sec = list_to_integer([S1, S2]),
	{{Year, Month, Day}, {Hour, Min, Sec}}.

%%%===================LOCAL FUNCTIONS==================
% 路由消息
route(Con, Src, Info, Headers, Body) ->
	path_route(Con, [{host, z_lib:get_value(Headers, "host", "")} | Info],
		Headers, parse_body(Info, Headers, Body),
		Src, z_lib:get_value(Info, path, "/")).

% 路径路由消息
path_route(Con, Info, Headers, Body, Src, Path) when Path =:= "/" ->
	case zm_config:get(?MODULE, {Src, Path}) of
		{_, [{_, _, _} | _] = MFAList, ErrorHandler} ->
			route_apply(Con, [{cur_path, Path} | Info],
				Headers, Body, MFAList, ErrorHandler, Src, "/");
		none ->
			zm_log:warn(?MODULE, route_miss, Src, Path,
				[{info, Info}, {headers, Headers}, {body, Body}]),
			% 没有对应的处理模块，502 网关失败
			send(Con, [{code, 502} | Info], sb_trees:empty(), [])
	end;
path_route(_Con, Info, Headers, Body, Src, Path) when Path =:= "." ->
	zm_log:warn(?MODULE, route_error, Src, Path,
		[{info, Info}, {headers, Headers}, {body, Body}]);
path_route(Con, Info, Headers, Body, Src, Path) ->
	case zm_config:get(?MODULE, {Src, Path}) of
		{_, [{_, _, _} | _] = MFAList, ErrorHandler} ->
			route_apply(Con, [{cur_path, Path} | Info],
				Headers, Body, MFAList, ErrorHandler, Src, Path);
		none ->
			path_route(Con, Info, Headers, Body, Src, filename:dirname(Path))
	end.

% 路由执行函数
route_apply(Con, Info, Headers, Body, MFAList, ErrorHandler, Src, Path) ->
	put('$log', {Src, Path}),
	try
		{Info1, Headers1, Body1} = apply_route(
			Con, Info, Headers, Body, MFAList, Src, Path),
		send(Con, Info1, Headers1, Body1)
	catch
		Error:Reason ->
			error_handle(ErrorHandler, Con, Info, Headers, Body,
				{Error, Reason, erlang:get_stacktrace()}, Src, Path)
	end.

apply_route(Con, Info, Headers, Body, [{M, F, A} = MFA | T], Src, Path) ->
	zm_log:debug(?MODULE, apply_route, Src, Path,
		[{mfa, MFA}, {info, Info}, {headers, Headers}, {body, Body}]),
	case M:F(A, Con, Info, Headers, Body) of
		{ok, Info1, Headers1, Body1} ->
			apply_route(Con, Info1, Headers1, Body1, T, Src, Path);
		{break, Info1, Headers1, Body1} ->
			zm_log:debug(?MODULE, apply_route_break, Src, Path,
				[{info, Info1}, {headers, Headers1}, {body, Body1}]),
			{Info1, Headers1, Body1};
		R ->
			throw({"route error, invalid result ", MFA, R})
	end;
apply_route(_Con, Info, Headers, Body, [], Src, Path) ->
	zm_log:debug(?MODULE, apply_route_over, Src, Path,
		[{info, Info}, {headers, Headers}, {body, Body}]),
	{Info, Headers, Body}.

% 错误处理
error_handle({M, F, A} = MFA, Con, Info, Headers, Body, Error, Src, Path) ->
	zm_log:debug(?MODULE, error_handle, Src, Path,
		[{mfa, MFA}, {info, Info}, {headers, Headers},
		{body, Body}, {error, Error}]),
	try
		M:F(A, Con, Info, Headers, Body, Error)
	catch
		Err:Reason ->
			zm_log:warn(?MODULE, {error_handle_error, Err}, Src, Path,
				[{mfa, MFA}, {info, Info},
				{headers, Headers}, {body, Body}, {error_log, Error},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}])
	end;
error_handle(Handler, _Con, Info, Headers, Body, Error, Src, Path) ->
	zm_log:warn(?MODULE, error_handle, Src, Path,
		[{handler, Handler}, {info, Info}, {headers, Headers},
		{body, Body}, {error, Error}]),
	ok.

% 路由消息
route_sse(Con, Src, Info, Headers, Body) ->
	path_route_sse(Con, [{host, z_lib:get_value(Headers, "host", "")} | Info],
		Headers, parse_body(Info, Headers, Body),
		Src, z_lib:get_value(Info, path, "/")).

% 路径路由消息
path_route_sse(Con, Info, Headers, Body, Src, Path) when Path =:= "/" ->
	case zm_config:get(?MODULE, {Src, Path}) of
		{_, [{_, _, _} | _] = MFAList, ErrorHandler} ->
			route_apply_sse(Con, [{cur_path, Path} | Info],
				Headers, Body, MFAList, ErrorHandler, Src, "/");
		none ->
			zm_log:warn(?MODULE, route_miss, Src, Path,
				[{info, Info}, {headers, Headers}, {body, Body}]),
			% 没有对应的处理模块，502 网关失败
			send(Con, [{code, 502} | Info], sb_trees:empty(), [])
	end;
path_route_sse(_Con, Info, Headers, Body, Src, Path) when Path =:= "." ->
	zm_log:warn(?MODULE, route_error, Src, Path,
		[{info, Info}, {headers, Headers}, {body, Body}]);
path_route_sse(Con, Info, Headers, Body, Src, Path) ->
	case zm_config:get(?MODULE, {Src, Path}) of
		{_, [{_, _, _} | _] = MFAList, ErrorHandler} ->
			route_apply_sse(Con, [{cur_path, Path} | Info],
				Headers, Body, MFAList, ErrorHandler, Src, Path);
		none ->
			path_route_sse(Con, Info, Headers, Body, Src, filename:dirname(Path))
	end.

% 路由执行函数
route_apply_sse(Con, Info, Headers, Body, MFAList, ErrorHandler, Src, Path) ->
	put('$log', {Src, Path}),
	try
		{Info1, Headers1, Body1} = apply_route_sse(
			Con, Info, Headers, Body, MFAList, Src, Path),
		send_event(Con, Info1, Headers1, Body1)
	catch
		Error:Reason ->
			error_handle(ErrorHandler, Con, Info, Headers, Body,
				{Error, Reason, erlang:get_stacktrace()}, Src, Path)
	end.

apply_route_sse(Con, Info, Headers, Body, [{M, F, A} = MFA | T], Src, Path) ->
	zm_log:debug(?MODULE, apply_route, Src, Path,
		[{mfa, MFA}, {info, Info}, {headers, Headers}, {body, Body}]),
	case M:F(A, Con, Info, Headers, Body) of
		{ok, Info1, Origin, Body1} ->
			apply_route_sse(Con, Info1, ?SSE_HEADER(Origin), Body1, T, Src, Path);
		{break, Info1, Origin, Body1} ->
			zm_log:debug(?MODULE, apply_route_sse_break, Src, Path,
				[{info, Info1}, {headers, ?SSE_HEADER(Origin)}, {body, Body1}]),
			{Info1, ?SSE_HEADER(Origin), Body1};
		R ->
			throw({"route sse request error, invalid result ", MFA, R})
	end;
apply_route_sse(_Con, Info, Headers, Body, [], Src, Path) ->
	zm_log:debug(?MODULE, apply_route_sse_over, Src, Path,
		[{info, Info}, {headers, Headers}, {body, Body}]),
	{Info, Headers, Body}.

%根据Info, Headers, Body分析出新的Body
parse_body(Info, Headers, Body) ->
	case z_lib:get_value(Info, method, 'GET') of
		'GET' ->
			parse_get_body(Info, Headers, Body);
		'HEAD' ->
			parse_get_body(Info, Headers, Body);
		'POST' ->
			parse_post_body(Info, Headers, Body);
		_ ->
			Body
	end.

%GET方法下，根据Info, Headers, Body分析出新的Body
parse_get_body(Info, _Headers, _Body) ->
	lists:reverse(parse_qs(z_lib:get_value(Info, qs, ""), [])).

%POST方法下，根据Info, Headers, Body分析出新的Body
parse_post_body(Info, Headers, Body) ->
	case z_lib:get_value(Headers, "content-type", "") of
		"application/x-www-form-urlencoded" ++ _ ->
			lists:reverse(parse_qs(binary_to_list(Body),
				parse_qs(z_lib:get_value(Info, qs, ""), [])));
		"multipart/form-data; boundary=" ++ Boundary ->
			Boundary1 = list_to_binary([$-, $- | Boundary]),
			case binary:split(Body, Boundary1) of
				[_, Bin] ->
					lists:reverse(parse_multipart(Bin, Boundary1,
						parse_qs(z_lib:get_value(Info, qs, ""), [])));
				[_] ->
					lists:reverse(parse_qs(z_lib:get_value(Info, qs, ""), []))
			end;
		_ ->
			Body
	end.

%分析multipart格式下新的Body
parse_multipart(<<"--">>, _Boundary, L) ->
	L;
parse_multipart(Bin, Boundary, L) ->
	case binary:split(Bin, <<"\r\n\r\n">>) of
		[Header, Bin1] ->
			case binary:split(Bin1, <<"\r\n", Boundary/binary>>) of
				[Body, Bin2] ->
					K = binary_to_str(z_lib:binary_get_part(
						Header, <<"name=\"">>, <<"\"">>, <<>>)),
					case z_lib:binary_get_part(
						Header, <<"filename=\"">>, <<"\"">>, <<>>) of
						<<>> ->
							parse_multipart(Bin2, Boundary,
								[{K, binary_to_str(Body)} | L]);
						FN ->
							CT = binary_to_list(z_lib:binary_get_part(
								Header, <<"Content-Type:">>, <<"\r\n">>, <<>>)),
							parse_multipart(Bin2, Boundary, [{K, {binary_to_str(FN),
								skip_whitespace(CT), Body}} | L])
					end;
				[_] ->
					L
			end;
		[_] ->
			L
	end.

%将二进制数据变成字符串，试图使用utf8编码
binary_to_str(Bin) ->
	case unicode:characters_to_list(Bin, utf8) of
		Str when is_list(Str) ->
			Str;
		_ ->
			binary_to_list(Bin)
	end.

%分析单个的cookie字符串
parse_cookie(String) ->
	S = skip_whitespace(String),
	{K, R} = parse_token(S, S, []),
	{V, R1} = parse_value(skip_whitespace(R)),
	{K, V, parse_next(R1)}.

%跳过起始的空白字符
skip_whitespace([H | T]) when ?IS_WHITESPACE(H) ->
	skip_whitespace(T);
skip_whitespace(L) ->
	L.

%分析第一个分隔符
parse_token(_S, [H | _] = String, L) when ?IS_SEPARATOR(H) ->
	{lists:reverse(L), String};
parse_token(S, [H | T], L) ->
	parse_token(S, T, [H | L]);
parse_token(S, [], _L) ->
	{S, []}.

%分析第一个可用的值，在""范围内或没有""
parse_value([$= | T]) ->
	case skip_whitespace(T) of
		[?QUOTE | String] ->
			parse_quoted(String, String, []);
		String ->
			parse_token(String, String, [])
	end;
parse_value(String) ->
	{"", String}.

%分析""范围内的文字，处理转义字符
parse_quoted(_S, [?QUOTE | T], L) ->
	{lists:reverse(L), T};
parse_quoted(S, [$\\, H | T], L) ->
	parse_quoted(S, T, [H | L]);
parse_quoted(S, [H | T], L) ->
	parse_quoted(S, T, [H | L]);
parse_quoted(S, [], _L) ->
	{S, []}.

%分析cookie的分隔符，找到下一个可用cookie
parse_next([$; | T]) ->
	T;
parse_next([$, | T]) ->
	T;
parse_next([_ | T]) ->
	parse_next(T);
parse_next([]) ->
	[].

%设置cookie
set_cookie(Tree, CK, K, V, Opts) when is_list(K), is_list(Opts) ->
	case sb_trees:get(CK, Tree, []) of
		[_ | _] = L ->
			sb_trees:enter(CK,
				[put_cookie(K, V, Opts, [<<"\r\nSet-Cookie: ">>]) | L], Tree);
		_ ->
			sb_trees:insert(CK,
				[put_cookie(K, V, Opts, [])], Tree)
	end.

%将键值和选项放入到列表中
put_cookie(K, V, ["Discard" | T], L) ->
	put_cookie(K, V, T, [<<"; Discard">> | L]);
put_cookie(K, V, ["HttpOnly" | T], L) ->
	put_cookie(K, V, T, [<<"; HttpOnly">> | L]);
put_cookie(K, V, ["Secure" | T], L) ->
	put_cookie(K, V, T, [<<"; Secure">> | L]);
put_cookie(K, V, [{"Port", V1} | T], L) ->
	put_cookie(K, V, T, [<<"; Port=\"">>, z_lib:any_to_iolist(V1), <<"\"">> | L]);
put_cookie(K, V, [{"CommentURL", V1} | T], L) ->
	put_cookie(K, V, T, [<<"; CommentURL=\"">>, z_lib:any_to_iolist(V1), <<"\"">> | L]);
put_cookie(K, V, [{K1, V1} | T], L) when is_list(K1) ->
	put_cookie(K, V, T, [<<"; ">>, K1, <<"=">>, z_lib:any_to_iolist(V1) | L]);
put_cookie(K, V, [_ | T], L) ->
	put_cookie(K, V, T, L);
put_cookie(K, V, [], L) ->
	[K, <<"=">>, z_lib:any_to_iolist(V) | L].

%构建http事件消息
new_http_event([{Key, Value}|T], L) ->
	new_http_event(T, ["\n\n", lists:concat([Key, ": ", Value])|L]);
new_http_event([], L) ->
	lists:flatten(lists:reverse(L)).