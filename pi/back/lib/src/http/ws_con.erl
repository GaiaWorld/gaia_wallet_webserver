%% @doc WebSocket的连接

-module(ws_con).


%%%=======================EXPORT=======================
-export([start/4, exist/1, get_pid/1, call/2, p_send/2, send/2, r_send/2, recv/2, send_pk/3, close/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
%%参数模块的执行方法
-define(MF(MA, F), (element(1, MA)):F(MA).

-define(HEADERS_RECV_TIMEOUT, 30 * 1000).
-define(WS_CON_TIMEOUT, 86400000).
-define(REQUEST_RECV_TIMEOUT, 60 * 1000).
-define(HIBERNATE_TIMEOUT, 3000).
-define(HEADER_AMOUNT, 999).
-define(HTTP_REQUEST_SIZE, 16).

-define(UPGRADE_KEY, "upgrade").
-define(CONNECTION_KEY, "connection").
-define(WS_CON_ERR_CODE, 400).
-define(DEFAULT_UPGRADE, "websocket").
-define(DEFAULT_CONNECTION, "upgrade").
-define(DEFAULT_UUID, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").
-define(DEFAULT_RESPONSE_CODE, 101).
-define(HANDED, handed).

-define(BASE64_BINARY, 0).
-define(BINARY, 1).
-define(TEXT, 2).
-define(CMD_BASE64_PARARM, "data").
-define(CMD_TEXT_PARARM, "info").

-define(HEARTBEAT, heartbeat).
-define(HEARTBEAT_TIME, 10000).
-define(HEARTBEAT_CMD, "$H").
-define(HEARTBEAT_COUNT, "C").
-define(OPEN, open).
-define(DEFAULT_HEARTBEAT_CFG, {close, 3}).

-define(DEFAULT_MAX_HEAP_SIZE, 64 * 1024 * 1024).

%%%=======================RECORD=======================
-record(?MODULE, {pid}).
-record(state, {parent, src, socket, addr, con, session, mod, args, timeout, ws_timeout, http_timeout, ws_timer_ref, timer_ref, statistics, status, info, headers, dtype, ismask, buffer, cache, queue,
				 send_count, receive_count, heartbeat_miss, timer_heartbeat, heartbeat_cfg, error}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start/4
%% Description: Starts a web socket connection.
%% Returns: {ok, pid()} | {error, term()}.
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
exist(#?MODULE{pid = Pid}) ->
	exist(Pid);
exist(Pid) when is_pid(Pid) ->
	case z_lib:pid_dictionary(Pid, '$initial_call') of
		 {_, {?MODULE, _, _}} ->
			 true;
		 _ ->
			 false
	 end.

%% -----------------------------------------------------------------
%% Function: get_pid
%% Description: 获得连接对应的进程
%% Returns: pid
%% -----------------------------------------------------------------
get_pid(#?MODULE{pid = Pid}) when is_pid(Pid) ->
	Pid.

%% -----------------------------------------------------------------
%% Function: call
%% Description: 向连接进程发送call消息
%% Returns: any()
call(#?MODULE{pid = Pid}, Msg)->
	gen_server:call(Pid, Msg).

%% -----------------------------------------------------------------
%% Function: p_send
%% Description: 向连接进程发送消息
%% Returns: ok
%% -----------------------------------------------------------------
p_send(#?MODULE{pid = Pid}, Msg) when is_pid(Pid) ->
	Pid ! Msg,
	ok.

%% -----------------------------------------------------------------
%% Function: send
%% Description: 通过连接向对端发送消息
%% Returns: ok
%% -----------------------------------------------------------------
send(#?MODULE{pid = Pid}, Msg) when is_pid(Pid) ->
	Pid ! {send, Msg},
	ok.

%% -----------------------------------------------------------------
%% Function: r_send
%% Description: 请求回应消息
%% Returns: ok
%% -----------------------------------------------------------------
r_send(#?MODULE{pid = Pid}, Msg) when is_pid(Pid) ->
	Pid ! {r_send, Msg},
	ok.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息接收命令
%% @spec recv(Con::tuple(),Term::term()) -> ok
%%@end
%% -----------------------------------------------------------------
recv(#?MODULE{pid = Pid}, {_Cmd, _Msg} = Term) ->
	Pid ! {recv, Term},
	ok.

%% -----------------------------------------------------------------
%% Func: send_pk/3
%% Description: 发送密钥数据
%% Returns: ok
%% -----------------------------------------------------------------
send_pk(_, 0, 0) ->
	ok;
send_pk(Con, RH, SH) ->
	#?MODULE{pid = Pid} = Con,
	Pid ! {send_pk, RH, SH}.

%% -----------------------------------------------------------------
%% Function: close
%% Description: 向连接进程发送关闭消息
%% Returns: ok
%% -----------------------------------------------------------------
close(#?MODULE{pid = Pid}, Reason) when is_pid(Pid) ->
	Pid ! {close, Reason},
	ok.

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
	{HttpTimeout, WSConTimeout, Timeout}=case z_lib:get_value(Args, timeout, {?HEADERS_RECV_TIMEOUT, ?WS_CON_TIMEOUT, ?REQUEST_RECV_TIMEOUT}) of
		{X, Y} ->
			{X, ?WS_CON_TIMEOUT, Y};
		Val ->
			Val
	end,
	DataType=z_lib:get_value(Args, type, ?BASE64_BINARY),
	Addr = [{src, Src}, {ip, IP}, {port, Port}, {portocol, http}],
	Con=#?MODULE{pid = self()},
	Session = zm_session:new(Con),
	zm_event:notify(Src, {?MODULE, init}, {Session, Addr}),
	{ok, #state{parent = Parent, src = Src,
		socket = Socket, addr = Addr, con = Con, session = Session, 
				args = Args, timeout = Timeout, ws_timeout = WSConTimeout, http_timeout = HttpTimeout, 
				status = http_request, info = wait, dtype = DataType, 
				send_count = 0, receive_count = 0, heartbeat_miss = 0, heartbeat_cfg = read_cfg(Args)}, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State} |
%%		{reply, Reply, State, Timeout} |
%%		{noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, Reply, State} | (terminate/2 is called)
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_call({session, {F, A}}, _From, #state{session = Session} = State) ->
	{reply, z_lib:mapply(Session, F, A), State, ?HIBERNATE_TIMEOUT};
handle_call(Msg, _From, State) ->
	{stop, normal, State#state{error={undefined_msg, Msg}}}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_cast(Msg, State) ->
	{stop, normal, State#state{error={undefined_msg, Msg}}}.

%% -----------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {noreply, NextStateName, NextState} |
%%		{noreply, NextStateName, NextState, Timeout} |
%%		{stop, Reason, NewState}
%% -----------------------------------------------------------------
handle_info({Type, _Socket, {http_request, Method, {abs_path, Uri}, Version}},
	#state{socket = Socket, addr = Addr, status = http_request, timer_ref = Ref} = State)
	when Type =:= http; Type =:= ssl ->
	zm_socket:setopts(Socket, [{active, once}]),
	{Path, Other} = zm_http:parse_url_path(Uri),
	{Query, _} = zm_http:parse_url_query(Other),
	cancel_timeout(Ref),
	{noreply, State#state{status = http_header,
		info = [{path, Path}, {qs, Query},
		{method, Method}, {uri, Uri}, {version, Version} | Addr],
		headers = sb_trees:empty(), timer_ref = none, statistics = {length(Uri) +
		?HTTP_REQUEST_SIZE, 0}}, ?HIBERNATE_TIMEOUT};

handle_info({Type, _Socket, {http_header, _, Name, _, Value}},
	#state{socket = Socket, status = http_header, headers = Headers,
	timer_ref = Ref, statistics = {Size, _}} = State) when Type =:= http; Type =:= ssl ->
	case sb_trees:size(Headers) of
		N when N >= ?HEADER_AMOUNT ->
			{stop, normal, State#state{error=header_overflow}};
		_ ->
			zm_socket:setopts(Socket, [{active, once}]),
			Key = normalize(Name),
			cancel_timeout(Ref),
			{noreply, State#state{
				headers = sb_trees:enter(Key, Value, Headers),
				timer_ref = none, statistics = {Size + length(Key) +
				length(Value) + ?HTTP_REQUEST_SIZE, 0}},
				?HIBERNATE_TIMEOUT}
	end;

handle_info({Type, _, http_eoh}, #state{socket = Socket, args = Args, con = Con,
	status = http_header, info = Info, headers = Headers,
	statistics = {_Size, _}, heartbeat_cfg = {HeartbeatState, _}} = State) when Type =:= http; Type =:= ssl ->
	{M, MA, IsMask} = z_lib:get_value(Args, protocol, []),
	case accept(M, Headers) of
		{ok, Code, Tree} ->
			send_info_headers(Socket, [{code, Code} | Info], zm_http:set_header(Tree, "Content-Length", 0)),
			zm_socket:setopts(Socket, [{active, true}, {packet, 0}]), 
			A=M:init(MA, {?MODULE, send_pk, Con}),
			{noreply, State#state{mod = M, args = A, status = ?HANDED, ismask = IsMask, buffer = <<>>, cache = {0, []}, 
								  queue = queue:new(), timer_heartbeat = heartbeat_start(HeartbeatState)}, ?HIBERNATE_TIMEOUT};
		{error, Code, Reason} ->
			Length = iolist_size(Reason),
			send_info_headers(Socket, [{code, Code} | Info],
				zm_http:set_header(sb_trees:empty(), "Content-Length", Length)),
			zm_socket:send(Socket, Reason),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

% 接收数据
handle_info({tcp, _Socket, Bin}, #state{ws_timer_ref = WSRef} = State) ->
	cancel_timeout(WSRef),
	case con_receive(Bin, State) of
		{client_closed, NewState} ->
			{stop, normal, NewState#state{error=client_closed}};
		NewState ->
			{noreply, NewState#state{ws_timer_ref = none}, ?HIBERNATE_TIMEOUT}
	end;
handle_info({ssl, _Socket, Bin}, #state{ws_timer_ref = WSRef} = State) ->
	cancel_timeout(WSRef),
	case con_receive(Bin, State) of
		{client_closed, NewState} ->
			{stop, normal, NewState#state{error=client_closed}};
		NewState ->
			{noreply, NewState#state{ws_timer_ref = none}, ?HIBERNATE_TIMEOUT}
	end;
handle_info(queue, #state{src = Src, addr = Addr, session = Session, timeout = Timeout, statistics = Tree, buffer = Buffer, queue = Queue} = State) ->
	{noreply, handle_queue(Queue, Buffer, Src, Addr, Session, Tree, Timeout, State), ?HIBERNATE_TIMEOUT};


% 关闭连接
handle_info({tcp_closed, _Socket}, State) ->
	{stop, normal, State#state{error=tcp_closed}};
handle_info({ssl_closed, _Socket}, State) ->
	{stop, normal, State#state{error=tcp_closed}};
handle_info({send, {Cmd, _} = Data}, State) ->
	{noreply, con_send({Cmd, send}, Data, State), ?HIBERNATE_TIMEOUT};
handle_info({r_send, {ReqCmd, Data}}, State) ->
	{noreply, con_send({ReqCmd, resp}, Data, State), ?HIBERNATE_TIMEOUT};
handle_info({send_pk, RH, SH}, #state{socket = Socket, mod = Mod, args = {M, _}, ismask = IsMask} = State) ->
	{ok, _, D}=Mod:encode({M, {{true, false, true, false}, {true, false, true, false}, [], []}}, IsMask, {"send_pk", [{"", "0"}, {"R", RH}, {"S", SH}]}),
	case zm_socket:send(Socket, D) of
		ok ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		{error, Reason} ->
			{stop, normal, State#state{error={socket_send_pk, Reason}}}
	end;
handle_info({close, Reason}, State) ->
	{stop, normal, State#state{error=Reason}};

% 处理请求
handle_info({session, Info}, #state{addr = Addr, session = Session, ws_timer_ref = WSRef, timer_ref = Ref} = State) ->
	%处理请求消息，则移除连接超时定时器
	cancel_timeout(WSRef),
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, handle),
		Addr, Info), ws_timer_ref = none, timer_ref = none}, ?HIBERNATE_TIMEOUT};

% 定时或超时事件处理
handle_info({timeout, WSRef, ws_timeout}, #state{ws_timeout = WSConTimeout, ws_timer_ref = WSRef} = State) ->
	%当前已握手连接超时，则关闭当前连接
	{stop, normal, State#state{error={tcp_closed, {timeout, WSConTimeout}}}};
handle_info({timeout, Ref, ?HEARTBEAT}, #state{timer_heartbeat = Ref} = State) ->
	heartbeat_check(State);
handle_info({timeout, Ref, Reason}, #state{timer_ref = Ref} = State) ->
	{stop, normal, State#state{error={shutdown, {timeout, Reason}}}};
handle_info({timeout, _Ref, _}, #state{timer_ref = Ref} = State) when is_reference(Ref) ->
	{noreply, State, hibernate};
handle_info({timeout, _Ref, _}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info(timeout, #state{status = http_request, info = wait} = State) ->
	{stop, normal, State#state{error={shutdown, socket_init_wait_timeout}}};
handle_info(timeout, #state{http_timeout = HttpTimeout, status = http_header} = State) ->
	%当前进程正在解决http头超时，则设置连接超时定时器
	{noreply, State#state{
		timer_ref = erlang:start_timer(HttpTimeout, self(), http_header)}, hibernate};
handle_info(timeout, #state{ws_timeout = WSConTimeout, ws_timer_ref = WSRef, status = ?HANDED} = State) ->
	%当前已握手连接开始休眠，则设置连接超时定时器
	case WSRef of
		none ->
			{noreply, State#state{
				ws_timer_ref = erlang:start_timer(WSConTimeout, self(), ws_timeout)}, hibernate};
		_ ->
			{noreply, State, hibernate}
	end;
handle_info(timeout, State) ->
	{noreply, State, hibernate};

handle_info({recv, Term}, #state{src = Src, addr = Addr, session = Session, ws_timer_ref = WSRef, timer_ref = Ref} = State) ->
	%接收到消息，则移除连接超时定时器
	cancel_timeout(WSRef),
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, recv),
		Src, Addr, Term), ws_timer_ref = none, timer_ref = none}, ?HIBERNATE_TIMEOUT};
handle_info({'ETS-TRANSFER', _Tab, _FromPid, _GiftData}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(Info, State) ->
	{stop, normal, State#state{error={undefined_msg, Info}}}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(Reason, #state{parent = Parent, src = Src, socket = Socket, 
	session = Session, info = Info, statistics = Statistics, error = Error} = State) ->
	[zm_socket:close(Socket) || Reason =/= {shutdown, tcp_closed}],
	send_parent(Parent, closed, Info, Statistics),
	zm_event:notify(Src, {?MODULE, closed}, {Session, Info, Reason, Error}),
	State.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
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

send_info_headers(Socket, Info, Headers) ->
	L = version_code(Info, headers_iolist(
		sb_trees:to_list(Headers), [<<"\r\n">>])),
	Size = iolist_size(L),
	zm_socket:send(Socket, L),
	Size.

headers_iolist([{K, V} | T], L) ->
	headers_iolist(T, [K, <<": ">>, V, <<"\r\n">> | L]);
headers_iolist([_ | T], L) ->
	headers_iolist(T, L);
headers_iolist([], L) ->
	L.

version_code(Info, L) ->
	X = z_lib:get_value(Info, code, 200),
	L1 = [integer_to_list(X), $\s, zm_http:reason_phrase(X), <<"\r\n">> | L],
	case z_lib:get_value(Info, version, {1, 0}) of
		{1, 1} ->
			[<<"HTTP/1.1 ">> | L1];
		{1, 0} ->
			[<<"HTTP/1.0 ">> | L1]
	end.

% 向父进程发送退出信息和统计信息
send_parent(Parent, Type, Info, {Size1, Size2}) ->
	Path = z_lib:get_value(Info, path, ""),
	gen_server:cast(Parent, {Type, self(), [{{Path, req}, Size1}, {{Path, resp}, Size2}]});
send_parent(Parent, closed, _Info, _) ->
	gen_server:cast(Parent, {closed, self(), []});
send_parent(_, _, _, _) ->
	none.

%% 接受连接
accept(M, Headers) ->
	case M:supported(z_lib:get_value(Headers, "sec-websocket-version", none)) of
		true ->
			case {z_lib:get_value(Headers, "host", []), z_lib:get_value(Headers, "sec-websocket-key", [])} of
				{[], _} ->
					{error, ?WS_CON_ERR_CODE, "invalid host"};
				{_, []} ->
					{error, ?WS_CON_ERR_CODE, "invalid key"};
				{_, Key} ->
					case (string:to_lower(z_lib:get_value(Headers, ?UPGRADE_KEY, [])) =:= ?DEFAULT_UPGRADE) and 
							 (string:str(string:to_lower(z_lib:get_value(Headers, ?CONNECTION_KEY, [])), ?DEFAULT_CONNECTION) =/= 0) of
						true ->
							handshake(Key);
						false ->
							{error, ?WS_CON_ERR_CODE, "invalid header"}
					end
			end;
		false ->
			{error, ?WS_CON_ERR_CODE, "invalid version"}
	end.

handshake(Key) ->
	{ok, ?DEFAULT_RESPONSE_CODE, sb_trees:from_dict([
		{"Upgrade", ?DEFAULT_UPGRADE},
		{"Connection", ?DEFAULT_CONNECTION},
		% 如果使用erl5.9以上版本，替换为crypto:hash(sha, [Key, ?DEFAULT_UUID])
		% {"Sec-WebSocket-Accept", base64:encode(crypto:sha([Key, ?DEFAULT_UUID]))}])}.
		{"Sec-WebSocket-Accept", base64:encode(crypto:hash(sha, [Key, ?DEFAULT_UUID]))}])}.

% 发送消息
con_send(CmdType, Data, #state{socket = Socket,
	mod = M, args = A, timer_ref = Ref, statistics = Tree, dtype = DataType, ismask = IsMask, send_count = OldSendCount} = State) ->
	{ok, A1, D} = case DataType of
		?BASE64_BINARY ->
			M:encode_base64(A, IsMask, Data);
		_ ->
			M:encode(A, IsMask, Data)
	end,
	case zm_socket:send(Socket, D) of
		ok ->
			cancel_timeout(Ref),
			State#state{args = A1, timer_ref = none,
				statistics = zm_con:statistics(
				Tree, CmdType, byte_size(D)), send_count = send_count(CmdType, OldSendCount)};
		{error, Reason} ->
			exit({socket_send, Reason})
	end.

send_count(CmdType, OldSendCount) ->
	case CmdType of
		{_, send} ->
			OldSendCount + 1;
		_ ->
			OldSendCount
	end.

% 接收数据
con_receive(Bin, #state{src = Src, addr = Addr,
	session = Session, mod = M, args = A, timeout = Timeout, timer_ref = Ref, statistics = Tree, buffer = Buffer, cache = Cache, queue = Queue} = State) ->
	cancel_timeout(Ref),
	Bin1=case Buffer of
			<<>> -> Bin;
			_ -> <<Buffer/binary, Bin/binary>>
	end,
	{A1, NewQueue}=M:decode(A, Cache, Bin1, Queue),
	handle_queue(NewQueue, Bin1, Src, Addr, Session, Tree, Timeout, State#state{args = A1}).

parse_text([$?, $?|Info], Cmd) ->
	{lists:reverse(Cmd), [{?CMD_TEXT_PARARM, Info}]};
parse_text([$?|PL], Cmd) ->
	{lists:reverse(Cmd), lists:reverse(zm_http:parse_qs(PL, []))};
parse_text([Char|PL], Cmd) ->
	parse_text(PL, [Char|Cmd]);
parse_text([], _) ->
	invalid_request_text.

handle_queue(Queue, Bin, Src, Addr, Session, Tree, Timeout, #state{dtype = DataType} = State) ->
	case queue:out(Queue) of
		{{value, {continue, 0, NewCache}}, _} ->
			State#state{timer_ref = erlang:start_timer(Timeout, self(), continue_recv), buffer = <<>>, cache = NewCache};
		{{value, {continue, _, NewCache}}, NewQueue} ->
			self() ! queue,
			State#state{timer_ref = erlang:start_timer(Timeout, self(), continue_recv), buffer = <<>>, cache = NewCache, queue = NewQueue};
		{{value, {wait, 0}}, _} ->
			State#state{timer_ref = erlang:start_timer(Timeout, self(), wait_recv), buffer = Bin};
		{{value, {wait, _}}, NewQueue} ->
			self() ! queue,
			State#state{timer_ref = erlang:start_timer(Timeout, self(), wait_recv), buffer = Bin, queue = NewQueue};
		{{value, {NewCache, {Cmd, _} = Term}}, NewQueue} ->
			self() ! queue,
			State2 = heartbeat_handle(Cmd, Queue, State),
			State2#state{session = ?MF(Session, recv), Src, Addr, Term), timer_ref = none,
				statistics = zm_con:statistics(Tree, {Cmd, req}, byte_size(Bin)), dtype = ?BINARY, 
				buffer = <<>>, cache = NewCache, queue = NewQueue};
		{{value, {NewCache, IOList}}, NewQueue} ->
			case parse_text(binary_to_list(IOList), []) of
				{TextCmd, KVList} = TextTerm ->
					self() ! queue,
					case DataType of
						?TEXT ->
							State#state{session = ?MF(Session, recv), Src, Addr, TextTerm), timer_ref = none, 
								statistics = zm_con:statistics(Tree, {TextCmd, req}, byte_size(Bin)),
								buffer = <<>>, cache = NewCache, queue = NewQueue};
						_ ->
							case (State#state.mod):decode_base64(State#state.args, proplists:get_value(?CMD_BASE64_PARARM, KVList)) of
								{ok, A1, {BinaryCmd, _} = BinaryTerm} ->
									State2 = heartbeat_handle(BinaryCmd, Queue, State),
									State2#state{session = ?MF(Session, recv), Src, Addr, BinaryTerm), args = A1, timer_ref = none,
										statistics = zm_con:statistics(Tree, {BinaryCmd, req}, byte_size(Bin)), dtype = ?BASE64_BINARY,
										buffer = <<>>, cache = NewCache, queue = NewQueue};
								{error, Reason} ->
									erlang:error(Reason)
							end
					end;
				Reason ->
					erlang:error(Reason)
			end;
		{{value, closed}, _} ->
			{client_closed, State};
		{empty, _NewQueue} ->
			State#state{cache = {0, []}}
	end.

%%处理前台返回的心跳包 
heartbeat_handle(Cmd, Queue, State) ->
	case Cmd of
		?HEARTBEAT_CMD ->
			case Queue of
				{[{_, {Cmd, KVList}} | []], _} ->
					State#state{receive_count = z_lib:get_value(KVList, ?HEARTBEAT_COUNT, 0), heartbeat_miss = 0};
				_ ->
					State#state{heartbeat_miss = 0}
			end;
		_ ->
			State#state{heartbeat_miss = 0}
	end.
			
%%检测心跳 
heartbeat_check(#state{src = Src, session = Session, send_count = SendCount, receive_count = RCount, heartbeat_miss = HeartbeatMiss, heartbeat_cfg = {_, HeartbeatMissCfg}} = State) ->
	case HeartbeatMiss of
		HeartbeatMissCfg ->
			{stop, normal, State#state{error={packet_loss_limit, HeartbeatMiss}}};
		_ ->
			zm_event:notify(Src, {?MODULE, packet_difference}, {Session, SendCount, RCount}),
			{noreply, State#state{heartbeat_miss = HeartbeatMiss + 1, timer_heartbeat = erlang:start_timer(?HEARTBEAT_TIME, self(), ?HEARTBEAT)}, ?HIBERNATE_TIMEOUT}
	end.
			
%%根据配置启动心跳检查
heartbeat_start(HeartbeatState) ->
	case HeartbeatState of
		?OPEN ->
			erlang:start_timer(?HEARTBEAT_TIME, self(), ?HEARTBEAT);
		_ ->
			normal
	end.

%%读取配置
read_cfg(Args) ->
	case z_lib:get_value(Args, ?HEARTBEAT, ?DEFAULT_HEARTBEAT_CFG) of
		{?OPEN, _} = V->
 			V;
		_ ->
			{close, 3}
	end.
			
			