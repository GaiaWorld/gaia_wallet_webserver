%%@doc HTTP连接
%%@end

-module(zm_http_con).

-description("http connection").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start/4, exist/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HEADERS_RECV_TIMEOUT, 30*1000).
-define(REQUEST_RECV_TIMEOUT, 60*1000).
-define(KEEP_ALIVE_TIMEOUT, 60*1000).

-define(HIBERNATE_TIMEOUT, 3000).

-define(HEADER_AMOUNT, 999).

-define(MAX_CONTENT_LENGTH, 4*1024*1024).
-define(MAX_CHUNKED_LENGTH, 10*1024*1024).
-define(MAX_FILE_LENGTH, 400*1024*1024).

-define(HTTP_REQUEST_SIZE, 16).
-define(HTTP_HEADER_SIZE, 4).
-define(HTTP_EOH_SIZE, 2).

-define(MAX_WBITS, 15).

-define(DEFAULT_MAX_HEAP_SIZE, 64 * 1024 * 1024).

-define(SSE_HEADER(Origin), sb_trees:from_dict([{"Content-Type", "text/event-stream"}, 
												{"Cache-Control", "no-cache"}, 
												{"Connection", "keep-alive"}, 
												{"Access-Control-Allow-Origin", Origin}])).

%%%=======================RECORD=======================
-record(zm_http, {pid}).
-record(state, {parent, src, socket, addr, timeout, limit, status, info, headers, body, time_ref, statistics, error}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 启动一个http连接控制进程
%% @spec start(Args::list(), Parent::pid(), Src::atom(), Socket::port()) -> return()
%% where
%%      return() = {ok, pid()} | {error, term()}
%%@end
%% -----------------------------------------------------------------
-type start_return() :: {ok, pid()} | {error, term()}.
-spec start(list(), pid(), atom(), port()) ->
	start_return().
%% -----------------------------------------------------------------
start(Args, Parent, Src, Socket)
	when is_list(Args), is_pid(Parent), is_atom(Src) ->
	gen_server:start(?MODULE, {Args, Parent, Src, Socket}, []).

%% -----------------------------------------------------------------
%%@doc check process type
%%```
%%  Pid:进程号
%%'''
%% @spec exist(Pid::pid()) -> return()
%% where
%%      return() = true | false
%%@end
%% -----------------------------------------------------------------
exist(Pid) when is_pid(Pid) ->
	case z_lib:pid_dictionary(Pid, '$initial_call') of
		 {_, {?MODULE, _, _}} ->
			 true;
		 _ ->
			 false
	 end.

%% -----------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, State} |
%%		{ok, StateName, State, Timeout} |
%%		ignore|
%%		{stop, StopReason}
%% -----------------------------------------------------------------
init({Args, Parent, Src, Socket}) ->
	z_process:local_process(?MODULE, ?DEFAULT_MAX_HEAP_SIZE, false, true),
	{ok, {IP, Port}} = zm_socket:peername(Socket),
	Timeout = z_lib:get_value(Args, timeout, {?HEADERS_RECV_TIMEOUT,
		?REQUEST_RECV_TIMEOUT, ?KEEP_ALIVE_TIMEOUT}),
	Addr = [{src, Src}, {ip, IP}, {port, Port}, {portocol, http}],
	zm_event:notify(Src, {?MODULE, init}, Addr),
	{ok, #state{parent = Parent, src = Src,
		socket = Socket, addr = Addr, timeout = Timeout,
		limit = {z_lib:get_value(Args, max_content_length, ?MAX_CONTENT_LENGTH),
		z_lib:get_value(Args, max_content_length, ?MAX_CHUNKED_LENGTH),
		z_lib:get_value(Args, max_file_length, ?MAX_FILE_LENGTH)},
		status = http_request, info = wait}, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State} |
%%		{reply, Reply, State, Timeout} |
%%		{noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, Reply, State} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_call(Msg, _From, State) ->
	{stop, normal, State#state{error={undefined_msg, Msg}}}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast(Msg, State) ->
	{stop, normal, State#state{error={undefined_msg, Msg}}}.

%% -----------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {noreply, NextStateName, NextState} |
%%		{noreply, NextStateName, NextState, Timeout} |
%%		{stop, Reason, NewState}
%% -----------------------------------------------------------------
% http请求的首行
handle_info({Type, _Socket, {http_request, Method, {abs_path, Uri}, Version}},
	#state{socket = Socket, addr = Addr, status = http_request, time_ref = Ref} = State)
	when Type =:= http; Type =:= ssl ->
	zm_socket:setopts(Socket, [{active, once}]),
	{Path, Other} = zm_http:parse_url_path(Uri),
	{Query, _} = zm_http:parse_url_query(Other),
	cancel_timeout(Ref),
	{noreply, State#state{status = http_header,
		info = [{path, Path}, {qs, Query},
		{method, Method}, {uri, Uri}, {version, Version} | Addr],
		headers = sb_trees:empty(), time_ref = none, statistics = {length(Uri) +
		?HTTP_REQUEST_SIZE, 0}}, ?HIBERNATE_TIMEOUT};

% http头信息
handle_info({Type, _Socket, {http_header, _, Name, _, Value}},
	#state{socket = Socket, status = http_header, headers = Headers,
	time_ref = Ref, statistics = {Size, _}} = State) when Type =:= http; Type =:= ssl ->
	case sb_trees:size(Headers) of
		N when N >= ?HEADER_AMOUNT ->
			{stop, normal, State#state{error = header_overflow}};
		_ ->
			zm_socket:setopts(Socket, [{active, once}]),
			Key = normalize(Name),
			cancel_timeout(Ref),
			{noreply, State#state{
				headers = sb_trees:enter(Key, Value, Headers),
				time_ref = none, statistics = {Size + length(Key) +
				length(Value) + ?HTTP_REQUEST_SIZE, 0}},
				?HIBERNATE_TIMEOUT}
	end;

% http头信息结束
handle_info({Type, _Socket, http_eoh}, #state{socket = Socket,
	status = http_header, info = Info, headers = Headers,
	statistics = {Size, _}} = State) when Type =:= http; Type =:= ssl ->
	%状态响应码100 (Continue) 状态代码的使用，
	%允许客户端在发request消息body之前先用request header试探一下server，
	%看server要不要接收request body
	State1 = State#state{statistics = {Size + ?HTTP_EOH_SIZE, 0}},
	case z_lib:get_value(Headers, "accept", undefined) of
		"text/event-stream" ->
			recv_sse(State1, Headers, <<>>);
		_ ->
			case z_lib:get_value(Headers, "expect", undefined) of
				undefined ->
					case z_lib:get_value(Headers, "transfer-encoding", undefined) of
						undefined ->
							case z_lib:get_value(Headers, "content-length", undefined) of
								undefined ->
									recv(State1, Headers, <<>>);
								"0" ->
									recv(State1, Headers, <<>>);
								Length ->
									recv_content(Length, State1, Headers)
							end;
						"chunked" ->
								recv_chunked(State1);
						Unknown ->
							{stop, normal, State1#state{error = Unknown}}
					end;
				"100-" ++ _ ->
					% "100-continue"
					SendSize = send_info_headers(Socket, [{code, 100} | Info],
						zm_http:set_header(sb_trees:empty(), "Content-Length", 0)),
					recv_continue(State#state{statistics = {
						Size + ?HTTP_EOH_SIZE, SendSize}});
				_ ->
					Length = iolist_size("invalid expect"),
					SendSize = send_info_headers(Socket, [{code, 400} | Info],
						zm_http:set_header(sb_trees:empty(),
						"Content-Length", Length)),
					zm_socket:send(Socket, "invalid expect"),
					keep_alive(Headers, State#state{statistics = {
						Size + ?HTTP_EOH_SIZE, SendSize + Length}})
			end
	end;

% 接收内容数据
handle_info({Type, _Socket, Bin}, #state{status = http_content,
	headers = Headers, body = {Len, Body}, time_ref = Ref, statistics = {Size, _}} = State)
	when Type =:= tcp; Type =:= ssl ->
	S = byte_size(Bin),
	case Bin of
		<<Bin1:Len/binary, _/binary>> ->
			recv(State#state{statistics = {Size + S, 0}},
				Headers, iolist_to_binary(lists:reverse([Bin1 | Body])));
		_ ->
			cancel_timeout(Ref),
			{noreply, State#state{body = {Len - S, [Bin | Body]},
				time_ref = none, statistics = {Size + S, 0}},
				?HIBERNATE_TIMEOUT}
	end;

% 接收chunked数据
handle_info({Type, _Socket, Bin}, #state{limit = {_, Limit, _},
	status = http_chunked, headers = Headers, body = Body, time_ref = Ref} = State)
	when Type =:= tcp; Type =:= ssl ->
	case chunked(Body, Bin) of
		{ok, Data, Footer} ->
			Headers1 = footer_headers(Headers, Footer),
			recv(State, Headers1, Data);
		{Size, _, _} when Limit =< Size ->
			{stop, normal, State#state{error={chunked_body_too_large, Size}}};
		Chunked ->
			cancel_timeout(Ref),
			{noreply, State#state{body = Chunked, time_ref = none},
				?HIBERNATE_TIMEOUT}
	end;

handle_info({tcp_closed, _Socket}, State) ->
	{stop, normal, State#state{error={shutdown, tcp_closed}}};
handle_info({ssl_closed, _Socket}, State) ->
	{stop, normal, State#state{error={shutdown, tcp_closed}}};

% 发送空数据
handle_info({http_response, Info, Headers, []}, #state{socket = Socket,
	status = http_response, statistics = {Size, _}} = State) ->
	SendSize = send_info_headers(Socket, Info,
		zm_http:set_header(Headers, "Content-Length", 0)),
	keep_alive(Headers, State#state{statistics = {Size, SendSize}});

handle_info({http_response, Info, Headers, Body}, #state{socket = Socket,
	status = http_response, statistics = {Size, _}} = State)
	when is_binary(Body); is_list(Body) ->
	SendSize = case z_lib:get_value(Info, method, 'GET') of
		'HEAD' ->
			send_info_headers(Socket, Info, Headers);
		_ ->
			send_info_headers_body(Socket, Info, Headers, Body)
	end,
	keep_alive(Headers, State#state{statistics = {Size, SendSize}});

% 发送头信息数据
handle_info({http_response, Info, Headers, MFA}, #state{socket = Socket,
	status = http_response, time_ref = Ref, statistics = {Size, _}} = State) ->
	SendSize = send_info_headers(Socket, Info, Headers),
	Gzip = case z_lib:get_value(Headers, "Content-Encoding", none) of
		"gzip" ->
			Z = zlib:open(),
			zlib:deflateInit(Z, default, deflated, 16 + ?MAX_WBITS, 8, default),
			Z;
		"deflate" ->
			Z = zlib:open(),
			zlib:deflateInit(Z, default, deflated, -?MAX_WBITS, 8, default),
			Z;
		_ ->
			none
	end,
	cancel_timeout(Ref),
	case MFA of
		{M, F, A} ->
			apply(M, F, A),
			{noreply, State#state{status = http_response_body,
				info = {Gzip, z_lib:get_value(Headers, "Connection", "close")},
				time_ref = none, statistics = {Size, SendSize}},
				?HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State#state{status = http_response_body,
				info = {Gzip, z_lib:get_value(Headers, "Connection", "close")},
				time_ref = none, statistics = {Size, SendSize}},
				?HIBERNATE_TIMEOUT}
	end;

% 发送内容数据结束
handle_info({http_response_body, Body, _MFA}, #state{socket = Socket,
	status = http_response_body, info = {Gzip, Close}} = State)
	when Body =:= <<>>; Body =:= [] ->
	send_body(Socket, <<>>, Gzip),
	close(Close, State);

% 发送内容数据
handle_info({http_response_body, Body, MFA}, #state{socket = Socket,
	status = http_response_body, info = {Gzip, _}, time_ref = Ref, statistics = {Size1, Size2}} = State) ->
	SendSize = send_body(Socket, Body, Gzip),
	case MFA of
		{M, F, A} ->
			apply(M, F, A);
		_ ->
			none
	end,
	cancel_timeout(Ref),
	{noreply, State#state{time_ref = none,
		statistics = {Size1, Size2 + SendSize}}, ?HIBERNATE_TIMEOUT};

% 发送内容数据结束
handle_info({http_response_body_chunk, Body, _MFA}, #state{socket = Socket,
	status = http_response_body, info = {_Gzip, Close}, statistics = {Size1, Size2}} = State)
	when Body =:= <<>>; Body =:= [] ->
	SendSize = send_body(Socket, <<"0\r\n\r\n">>, none),
	close(Close, State#state{statistics = {Size1, Size2 + SendSize}});

% 发送内容数据
handle_info({http_response_body_chunk, Body, MFA}, #state{socket = Socket,
	status = http_response_body, info = {Gzip, _}, time_ref = Ref, statistics = {Size1, Size2}} = State) ->
	Data = if
		is_port(Gzip) ->
			Bin = zlib:deflate(Gzip, Body, sync),
			[erlang:integer_to_list(iolist_size(Bin), 16), <<"\r\n">>, Bin, <<"\r\n">>];
		true ->
			[erlang:integer_to_list(iolist_size(Body), 16), <<"\r\n">>, Body, <<"\r\n">>]
	end,
	SendSize = send_body(Socket, Data, none),
	case MFA of
		{M, F, A} ->
			apply(M, F, A);
		_ ->
			none
	end,
	cancel_timeout(Ref),
	{noreply, State#state{time_ref = none,
		statistics = {Size1, Size2 + SendSize}}, ?HIBERNATE_TIMEOUT};

% 发送事件流
handle_info({http_event, [], Origin, Body}, #state{socket = Socket, info = Info} = State) ->
	send_info_headers_body(Socket, Info, ?SSE_HEADER(Origin), Body),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({http_event, Info, Headers, Body}, #state{socket = Socket} = State) ->
	send_info_headers_body(Socket, Info, Headers, Body),
	{noreply, State#state{status = http_event_response}, ?HIBERNATE_TIMEOUT};

handle_info({close, Reason}, State) ->
	{stop, normal, State#state{error=Reason}};

handle_info({timeout, Ref, Reason}, #state{time_ref = Ref} = State) ->
	{stop, normal, State#state{error={shutdown, {timeout, Reason}}}};
handle_info({timeout, _Ref, _}, #state{time_ref = Ref} = State) when is_reference(Ref) ->
	{noreply, State, hibernate};
handle_info({timeout, _Ref, _}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(timeout, #state{status = http_request, info = wait} = State) ->
	% 为了防止攻击，如果连接建立后，没有接受或发送任何消息，则断掉连接
	{stop, normal, State#state{error={shutdown, socket_init_wait_timeout}}};
handle_info(timeout, #state{timeout = {_, _, Timeout}, status = http_request} = State) ->
	{noreply, State#state{
		time_ref = erlang:start_timer(Timeout, self(), http_request)}, hibernate};
handle_info(timeout, #state{timeout = {Timeout, _, _}, status = http_header} = State) ->
	{noreply, State#state{
		time_ref = erlang:start_timer(Timeout, self(), http_header)}, hibernate};
handle_info(timeout, #state{timeout = {_, Timeout, _}, status = http_content} = State) ->
	{noreply, State#state{
		time_ref = erlang:start_timer(Timeout, self(), http_content)}, hibernate};
handle_info(timeout, #state{timeout = {_, Timeout, _}, status = http_chunked} = State) ->
	{noreply, State#state{
		time_ref = erlang:start_timer(Timeout, self(), http_chunked)}, hibernate};
handle_info(timeout, State) ->
	{noreply, State, hibernate};

handle_info(Info, State) ->
	{stop, normal, State#state{error={undefined_info, Info}}}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(Reason, #state{parent = Parent, src = Src, socket = Socket,
	addr = Addr, info = Info, statistics = Statistics, error = Error} = State) ->
	[zm_socket:close(Socket) || Error =/= {shutdown, tcp_closed}],
	send_parent(Parent, closed, Info, Statistics),
	Reason1 = if
		Reason =:= normal -> Reason;
		true -> Error
	end,
	zm_event:notify(Src, {?MODULE, closed}, {Addr, Reason1}),
	State.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
% 取消超时的定时器
cancel_timeout(Ref) when is_atom(Ref) ->
	Ref;
cancel_timeout(Ref) ->
	erlang:cancel_timer(Ref).

%将键标准化
normalize(K) when is_list(K) ->
	string:to_lower(K);
normalize(K) when is_atom(K) ->
	normalize(string:to_lower(atom_to_list(K)));
normalize(K) when is_binary(K) ->
	normalize(string:to_lower(binary_to_list(K))).

% 接收到全部的请求信息
recv(#state{parent = Parent, src = Src, socket = Socket, addr = Addr,
	timeout = Timeout, limit = Limit, info = Info, statistics = Statistics}, Headers, Body) ->
	zm_http:recv(#zm_http{pid = self()}, Src, Info, Headers, Body),
	{noreply, #state{parent = Parent, src = Src, socket = Socket, addr = Addr,
		timeout = Timeout, limit = Limit, status = http_response,
		info = Info, statistics = Statistics}, ?HIBERNATE_TIMEOUT}.

% 接收内容信息
recv_content(Length, #state{socket = Socket,
	limit = {Limit, _, FileLimit}, time_ref = Ref} = State, Headers) ->
	Limit1 = case z_lib:get_value(Headers, "content-type", "") of
		"multipart/form-data; boundary" ++ _ ->
			FileLimit;
		_ ->
			Limit
	end,
	try list_to_integer(Length) of
		Len when Len =< Limit1 ->
			zm_socket:setopts(Socket, [{active, true}, {packet, raw}]),
			cancel_timeout(Ref),
			{noreply, State#state{
				status = http_content, body = {Len, []}, time_ref = none},
				?HIBERNATE_TIMEOUT};
		_ ->
			{stop, normal, State#state{error={body_too_large, Length}}}
	catch
		_ : _ ->
			{stop, normal, State#state{error={invalid_content_length, Length}}}
	end.

% 接收chunked信息
recv_chunked(#state{socket = Socket, time_ref = Ref} = State) ->
	zm_socket:setopts(Socket, [{active, true}, {packet, raw}]),
	cancel_timeout(Ref),
	{noreply, State#state{status = http_chunked,
		body = {0, {0, []}, []}, time_ref = none}, ?HIBERNATE_TIMEOUT}.

% 继续接收信息
recv_continue(#state{parent = Parent, src = Src, socket = Socket, addr = Addr,
	timeout = Timeout, limit = Limit, info = Info, time_ref = Ref, statistics = Statistics}) ->
	zm_socket:setopts(Socket, [{active, once}, {packet, http}]),
	send_parent(Parent, statistics, Info, Statistics),
	cancel_timeout(Ref),
	{noreply, #state{parent = Parent, src = Src, socket = Socket, addr = Addr,
		timeout = Timeout, limit = Limit, status = http_request},
		?HIBERNATE_TIMEOUT}.

% 接收sse请求
recv_sse(#state{src = Src, socket = Socket, info = Info} = State1, Headers, Body) ->
	send_info_headers(Socket, [{code, 200} | Info], ?SSE_HEADER("*")),
	zm_http:recv_sse(#zm_http{pid = self()}, Src, Info, Headers, Body),
	keep_alive(Headers, State1).

% 读取chunked数据
chunked({Size, {0, Header}, Body}, Bin) ->
	case binary:match(Bin, <<$\n>>) of
		nomatch ->
			{Size, {0, Header ++ binary_to_list(Bin)}, Body};
		{2, _} ->
			<<_:2/binary, _:8, Bin1/binary>> = Bin,
			case chunked_length(Header) of
				0 ->
					chunked({Size, [], Body}, Bin1);
				Len ->
					chunked({Size + Len, {Len, []}, Body}, Bin1)
			end;
		{Offset, _} ->
			<<_:Offset/binary, _:8, Bin1/binary>> = Bin,
			case chunked_length(Header ++ binary_to_list(Bin, 1, Offset - 2)) of
				0 ->
					chunked({Size, [], Body}, Bin1);
				Len ->
					chunked({Size + Len, {Len, []}, Body}, Bin1)
			end
	end;
chunked({Amount, {-2, []}, Body} = Chunked, Bin) ->
	case Bin of
		<<_:16, Bin2/binary>> ->
			chunked({Amount, {0, []}, Body}, Bin2);
		<<_:8, _/binary>> ->
			{Amount, {-1, []}, Body};
		_ ->
			Chunked
	end;
chunked({Amount, {-1, []}, Body} = Chunked, Bin) ->
	case Bin of
		<<_:8, Bin1/binary>> ->
			chunked({Amount, {0, []}, Body}, Bin1);
		_ ->
			Chunked
	end;
chunked({Amount, {Size, Content}, Body}, Bin) when Size > 0 ->
	case Bin of
		<<>> ->
			{Amount, {Size, Content}, Body};
		<<Bin1:Size/binary, Bin2/binary>> ->
			chunked({Amount, {-2, []}, [lists:reverse([Bin1 | Content]) | Body]}, Bin2);
		_ ->
			{Amount, {Size - byte_size(Bin), [Bin | Content]}, Body}
	end;
chunked({Size, Footer, Body}, Bin) ->
	case binary:match(Bin, <<$\n>>) of
		nomatch ->
			{Size, Footer ++ binary_to_list(Bin), Body};
		{2, _} ->
			{ok, iolist_to_binary(lists:reverse(Body)), []};
		{Offset, _} ->
			{ok, iolist_to_binary(lists:reverse(Body)),
				[z_lib:split(Footer ++ binary_to_list(Bin, 1, Offset - 2), $:)]}
	end.

% 读取chunked数据的长度
chunked_length(L) ->
	[H | _] = L1 = zm_http:trim_hex(L, []),
	if
		H =:= $0 ->
			0;
		true ->
			erlang:list_to_integer(L1, 16)
	end.

% 将footer添加到Headers中
footer_headers(Headers, [{K, V} |T]) ->
	footer_headers(sb_trees:enter(normalize(K), V, Headers), T);
footer_headers(Headers, [_ |T]) ->
	footer_headers(Headers, T);
footer_headers(Headers, []) ->
	Headers.

% 发送版本、状态和头信息
send_info_headers(Socket, Info, Headers) ->
	L = version_code(Info, headers_iolist(
		sb_trees:to_list(Headers), [<<"\r\n">>])),
	Size = iolist_size(L),
	zm_socket:send(Socket, L),
	Size.

% 将头信息转成iolist
headers_iolist([{K, V} | T], L) ->
	headers_iolist(T, [K, <<": ">>, V, <<"\r\n">> | L]);
headers_iolist([_ | T], L) ->
	headers_iolist(T, L);
headers_iolist([], L) ->
	L.

% 添加版本和状态码
version_code(Info, L) ->
	X = z_lib:get_value(Info, code, 200),
	L1 = [integer_to_list(X), $\s, zm_http:reason_phrase(X), <<"\r\n">> | L],
	case z_lib:get_value(Info, version, {1, 0}) of
		{1, 1} ->
			[<<"HTTP/1.1 ">> | L1];
		{1, 0} ->
			[<<"HTTP/1.0 ">> | L1]
	end.

% 发送版本、状态、头信息和内容
send_info_headers_body(Socket, Info, Headers, Body) ->
	case z_lib:get_value(Headers, "Content-Encoding", none) of
		"gzip" ->
			Data = z_lib:gzip(Body),
			Length = iolist_size(Data),
			Size1 = send_info_headers(Socket, Info, zm_http:set_header(
				Headers, "Content-Length", Length)),
			zm_socket:send(Socket, Data),
			Size1 + Length;
		"deflate" ->
			Data = z_lib:zip(Body),
			Length = iolist_size(Data),
			Size1 = send_info_headers(Socket, Info, zm_http:set_header(
				Headers, "Content-Length", Length)),
			zm_socket:send(Socket, Data),
			Size1 + Length;
		_ ->
			Length = iolist_size(Body),
			Size1 = case z_lib:get_value(Headers, "Content-Length", none) of
				[N | _] when N >=$1, N =<$9 ->
					send_info_headers(Socket, Info, Headers);
				_ ->
					send_info_headers(Socket, Info, zm_http:set_header(
						Headers, "Content-Length", Length))
			end,
			zm_socket:send(Socket, Body),
			Size1 + Length
	end.

% 发送消息体，根据参数决定是否压缩
send_body(Socket, <<>>, Z) when is_port(Z) ->
	Data = zlib:deflate(Z, <<>>, finish),
	zlib:deflateEnd(Z),
	zlib:close(Z),
	Size = iolist_size(Data),
	zm_socket:send(Socket, Data),
	Size;
send_body(_Socket, <<>>, _) ->
	0;
send_body(Socket, Body, Z) when is_port(Z) ->
	Data = zlib:deflate(Z, Body, none),
	Size = iolist_size(Data),
	zm_socket:send(Socket, Data),
	Size;
send_body(Socket, Body, _) ->
	Size = iolist_size(Body),
	zm_socket:send(Socket, Body),
	Size.

% 保持或关闭连接
keep_alive(Headers, State) ->
	close(z_lib:get_value(Headers, "connection", "close"), State).

% 根据连接状态保持或关闭连接
close("keep-alive", #state{parent = Parent, src = Src, socket = Socket, addr = Addr,
	timeout = Timeout, limit = Limit, info = Info, time_ref = Ref, statistics = Statistics}) ->
	zm_socket:setopts(Socket, [{active, once}, {packet, http}]),
	send_parent(Parent, statistics, Info, Statistics),
	cancel_timeout(Ref),
	{noreply, #state{parent = Parent, src = Src, socket = Socket, addr = Addr,
		timeout = Timeout, limit = Limit, status = http_request},
		?HIBERNATE_TIMEOUT};
close(_, #state{socket = Socket} = State) ->
	zm_socket:close(Socket),
	{stop, normal, State}.

% 向父进程发送退出信息和统计信息
send_parent(Parent, Type, Info, {Size1, Size2}) ->
	Path = z_lib:get_value(Info, path, ""),
	gen_server:cast(Parent, {Type, self(), [{{Path, req}, Size1}, {{Path, resp}, Size2}]});
send_parent(Parent, closed, _Info, _) ->
	gen_server:cast(Parent, {closed, self(), []});
send_parent(_, _, _, _) ->
	none.
