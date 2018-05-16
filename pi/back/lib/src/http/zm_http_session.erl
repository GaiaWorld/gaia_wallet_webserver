%%@doc HTTP的会话空连接，负责维护会话
%%  @type record() = {zm_http_session,Pid::pid()}
%%@end


-module(zm_http_session).

-description("http session connection").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/2]).
-export([service/5, get_msgs/1, set_scid/2, get_session/2, bind/3]).
-export([get_pid/1, p_send/2, send/2, r_send/2, recv/2, close/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
%%参数模块的执行方法
-define(MF(MA, F), (element(1, MA)):F(MA).

-define(SESSION_COOKIE_ID, "ZMSCID").
-define(HTTP_HEADERS, http_headers).
-define(HTTP_BODY, http_body).
-define(ACCESS_TIMEOUT, 30*1000).
-define(BIND_TIMEOUT, 5000).
-define(MSG_NUMBER, "msg_number").
-define(MSG_COUNT, 4).

-define(SERVER_INTERNAL_ERROR, 500).

-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(?MODULE, {pid}).
-record(state, {src, addr, session, timeout, bind, msglist = [], msgnumber = 0, timer_ref}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 启动一个http会话
%%  @spec start_link(Addr::list(),Timeout::integer()) -> return()
%%  where
%%	return() = {ok, pid()} | {error, term()}
%%@end
%% -----------------------------------------------------------------
-type start_return() :: {ok, pid()} | {error, term()}.
-spec start_link(list(), integer()) ->
	start_return().
%% -----------------------------------------------------------------
start_link(Addr, Timeout) ->
	gen_server:start_link(?MODULE, {Addr, Timeout}, []).

%% -----------------------------------------------------------------
%%@doc 接受客户端的请求，将请求转发到http指定服务
%% @spec service(Args, Con, Info, Headers, Body) -> return()
%% where
%%      return() = {ok, Info, Headers, Body}
%%@end
%% -----------------------------------------------------------------
service(Args, Con, Info, Headers, Body) ->
	MT = case z_lib:get_value(Args, cmd, "") of
		"" ->
			throw({?SERVER_INTERNAL_ERROR, "undefined msg type"});
		V ->
			V
	end,
	{ID1, Pid1} = get_session(Info, Headers),
	access(ID1, Pid1, MT, Con, Info, Headers, Body,
		z_lib:get_value(Args, timeout, ?ACCESS_TIMEOUT)).

%% -----------------------------------------------------------------
%%@doc get_msgs
%% @spec get_msgs(Headers) -> return()
%% where
%%      return() = {ok, list()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
get_msgs(Headers) ->
	case zm_http:get_cookie(Headers, ?SESSION_COOKIE_ID, undefined) of
		undefined ->
			{error, "need authorized session"};
		ID ->
			case zm_http_session_table:get(ID) of
				none ->
					{error, "authorized session not found"};
				{_, Pid} ->
					try
						gen_server:call(Pid, get_msgs, ?BIND_TIMEOUT)
					catch
						_:Reason ->
							{error, Reason}
					end
			end
	end.

%% -----------------------------------------------------------------
%%@doc 设置SESSION_COOKIE_ID
%% @spec set_scid(Headers,ID) -> NewHeaders
%%@end
%% -----------------------------------------------------------------
set_scid(Headers, ID) ->
	zm_http:set_cookie(Headers, ?SESSION_COOKIE_ID, ID).

%% -----------------------------------------------------------------
%%@doc 获得会话
%% @spec get_session(Info,Headers) -> return()
%% where
%%      return() = {ID, Pid}
%%@end
%% -----------------------------------------------------------------
get_session(Info, Headers) ->
	case zm_http:get_cookie(Headers, ?SESSION_COOKIE_ID, undefined) of
		undefined ->
			zm_http_session_table:create(get_addr(Info));
		ID ->
			case zm_http_session_table:get(ID) of
				none ->
					zm_http_session_table:create(get_addr(Info));
				{_, Pid} ->
					{none, Pid}
			end
	end.

%% -----------------------------------------------------------------
%%@doc 绑定已存在的http连接
%% @spec bind(Pid::pid(),Con,MFA) -> return()
%% where
%%      return() = ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
bind(Pid, Con, {_M, _F, _A} = MFA) ->
	gen_server:call(Pid, {bind, Con, MFA}, ?BIND_TIMEOUT).

%% -----------------------------------------------------------------
%%@doc 获得连接对应的进程
%% @spec get_pid(Con::tuple()) -> pid()
%%@end
%% -----------------------------------------------------------------
get_pid(#?MODULE{pid = Pid}) when is_pid(Pid) ->
	Pid.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息
%% @spec p_send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
p_send(#?MODULE{pid = Pid}, Msg) when is_pid(Pid) ->
	Pid ! Msg,
	ok.

%% -----------------------------------------------------------------
%%@doc 通过连接向对端发送消息
%% @spec send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
send(#?MODULE{pid = Pid}, Msg) when is_pid(Pid) ->
	case Msg of
		{"r_ok", [{"", {_, ConPid}} | Msg1]} ->
			ConPid ! {r_ok, Msg1};
		{"r_ok", [{_, ConPid}, Msg1]} ->
			ConPid ! {r_ok, Msg1};
		{"r_err", [{"", {_, ConPid}} | Msg1]} ->
			ConPid ! {r_err, Msg1};
		_ ->
			Pid ! {send, Msg}
	end,
	ok.

%% -----------------------------------------------------------------
%%@doc 请求回应消息
%% @spec r_send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
r_send(Con, {_ReqCmd, Msg}) ->
	send(Con, Msg);
r_send(_, _) ->
	ok.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息接收命令
%% @spec recv(Con::tuple(),Term::term()) -> ok
%%@end
%% -----------------------------------------------------------------
recv(#?MODULE{pid = Pid}, {_Cmd, _Msg}=Term) when is_pid(Pid) ->
	Pid ! {recv, Term},
	ok.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送关闭消息
%% @spec close(Con::tuple(),Reason::term()) -> ok
%%@end
%% -----------------------------------------------------------------
close(#?MODULE{pid = Pid}, Reason) when is_pid(Pid) ->
	Pid ! {close, Reason},
	ok.

%% -----------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore|
%%		{stop, StopReason}
%% -----------------------------------------------------------------
init({Addr, Timeout}) ->
	Session = zm_session:new(#?MODULE{pid = self()}),
	Src = z_lib:get_value(Addr, src, none),
	zm_event:notify(Src, {?MODULE, init}, {Session, Addr}),
	{ok, #state{src = Src, addr = Addr, session = Session, timeout = Timeout},
		?HIBERNATE_TIMEOUT}.

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
handle_call({bind, {_, Pid} = Con, {M, F, A} = MFA}, _From,
	#state{src = Src, session = Session, bind = Bind, msglist = L} = State) ->
	case Bind of
		{{_, Pid}, _} ->
			ok;
		{{_, OldPid} = OldCon, _} ->
			?MF(OldCon, send_chunk), "", none),
			zm_event:notify(Src, {?MODULE, unbind}, {Session, OldPid, rebind}),
			zm_event:notify(Src, {?MODULE, bind}, Session),
			erlang:monitor(process, Pid);
		_ ->
			zm_event:notify(Src, {?MODULE, bind}, Session),
			erlang:monitor(process, Pid)
	end,
	case M:F(A, lists:reverse(L)) of
		[] ->
			ok;
		<<>> ->
			ok;
		Data ->
			?MF(Con, send_chunk), Data, none)
	end,
	{reply, ok, State#state{bind = {Con, MFA}, msglist = [], timer_ref = none},
		?HIBERNATE_TIMEOUT};

handle_call(get_msgs, _From, #state{msglist = L} = State) ->
	{reply, lists:reverse(L), State#state{msglist = [], timer_ref = none},
		?HIBERNATE_TIMEOUT};

handle_call(Msg, _From, State) ->
	{stop, {undefined_msg, Msg}, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast(Msg, State) ->
	{stop, {undefined_msg, Msg}, State}.

%% -----------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {noreply, NextStateName, NextState} |
%%		{noreply, NextStateName, NextState, Timeout} |
%%		{stop, Reason, NewState}
%% -----------------------------------------------------------------
handle_info({access, Info, Msg}, #state{src = Src, session = Session} = State) ->
	{noreply, State#state{session = ?MF(Session, recv), Src, Info, Msg), timer_ref = none},
		?HIBERNATE_TIMEOUT};

handle_info({send, {Type, MsgL}},
	#state{bind = {Con, {M, F, A}}, msglist = L,
	msgnumber = MsgNumber} = State) when is_list(MsgL) ->
	Msg = {Type, [{?MSG_NUMBER, MsgNumber} | MsgL]},
	case ?MF(Con, is_alive)) of
		true ->
			Data = M:F(A, [Msg]),
			?MF(Con, send_chunk), Data, none),
			{noreply, State#state{
				msglist = [Msg | lists:sublist(L, ?MSG_COUNT)],
				msgnumber = MsgNumber + 1, timer_ref = none},
				?HIBERNATE_TIMEOUT};
		false ->
			{noreply, State#state{
				bind = none, msglist = [Msg | L],
				msgnumber = MsgNumber + 1, timer_ref = none},
				?HIBERNATE_TIMEOUT}
	end;

handle_info({send, {Type, MsgL}},
	#state{msglist = L, msgnumber = MsgNumber} = State) when is_list(MsgL) ->
	{noreply, State#state{
		msglist = [{Type, [{?MSG_NUMBER, MsgNumber} | MsgL]} | L],
		msgnumber = MsgNumber + 1, timer_ref = none},
		?HIBERNATE_TIMEOUT};

handle_info({close, Reason}, State) ->
	{stop, Reason, State};

handle_info({session, Info}, #state{addr = Addr, session = Session, timer_ref = Ref} = State) ->
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, handle), Addr, Info),
		timer_ref = none}, ?HIBERNATE_TIMEOUT};

handle_info({timeout, SessionRef, {session, Info}},
	#state{addr = Addr, session = Session, timer_ref = Ref} = State) ->
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, timeout),
		Addr, SessionRef, Info), timer_ref = none}, ?HIBERNATE_TIMEOUT};
handle_info({timeout, Ref, Reason}, #state{timer_ref = Ref} = State) ->
	{stop, {shutdown, {timeout, Reason}}, State};
handle_info({timeout, _Ref, _}, #state{timer_ref = Ref} = State) when is_reference(Ref) ->
	{noreply, State, hibernate};
handle_info({timeout, _Ref, _}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};


handle_info({recv, Term}, #state{src = Src, addr = Addr, session = Session, timer_ref = Ref} = State) ->
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, recv),
		Src, Addr, Term), timer_ref = none}, ?HIBERNATE_TIMEOUT};

handle_info(timeout, #state{timeout = Timeout} = State) ->
	{noreply, State#state{
		timer_ref = erlang:start_timer(Timeout, self(), recv_timeout)}, hibernate};

handle_info({'DOWN', _, process, Pid, Info}, #state{addr = Addr, session = Session, bind = {Pid, _}} = State) ->
	zm_event:notify(z_lib:get_value(Addr, src, none), {?MODULE, unbind}, {Session, Pid, Info}),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({'DOWN', _, process, _Pid, _Info}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(Info, State) ->
	{stop, {undefined_info, Info}, State}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(Reason, #state{addr = Addr, session = Session} = State) ->
	zm_event:notify(z_lib:get_value(Addr, src, none),
		{?MODULE, closed}, {Session, Addr, Reason}),
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
cancel_timeout(Ref) when is_reference(Ref) ->
	erlang:cancel_timer(Ref);
cancel_timeout(Ref) ->
	Ref.

%从信息中获得地址
get_addr(Info) ->
	[{src, z_lib:get_value(Info, src, none)},
		{ip, z_lib:get_value(Info, ip, {0, 0, 0, 0})},
		{port, z_lib:get_value(Info, port, 0)},
		{portocol, z_lib:get_value(Info, portocol, undefined)}].

%会话通讯
access(ID, Pid, MT, Con, Info, Headers, Body, Timeout) ->
	Info1 = [{?HTTP_HEADERS, Headers} | Info],
	Msg = if
		is_binary(Body) ->
			{MT, [{"", Con}, {?HTTP_BODY, Body}]};
		is_list(Body) ->
			{MT, [{"", Con} | Body]}
	end,
	Pid ! {access, Info1, Msg},
	receive
		{r_ok, Msg1} ->
			access_ok(ID, Info1, zm_http:resp_headers(Info, Headers, ".htm", none), Msg1);
		{r_err, Msg1} ->
			access_error(Msg1)
		after Timeout ->
			throw({?SERVER_INTERNAL_ERROR, "access session timeout"})
	end.

%通讯返回成功
access_ok(none, Info, Headers, Msg) ->
	{ok, Info, Headers, Msg};
access_ok(ID, Info, Headers, Msg) ->
	{ok, Info, set_scid(Headers, ID), Msg}.

%通讯返回错误
access_error(Msg) ->
	Why = case z_lib:get_value(Msg, "why", "") of
		S when is_list(S) ->
			S;
		Any ->
			io_lib:write(Any)
	end,
	Reason = case z_lib:get_value(Msg, "vars", none) of
		none ->
			Why;
		Vars ->
			format(Why, Vars)
	end,
	Reason1 = case z_lib:get_value(Msg, "stacktrace", none) of
		none ->
			Reason;
		Stacktrace ->
			Reason ++ [$\s | io_lib:write(Stacktrace)]
	end,
	case z_lib:get_value(Msg, "type", ?SERVER_INTERNAL_ERROR) of
		Type when is_integer(Type) ->
			throw({Type, Reason1});
		_ ->
			throw({?SERVER_INTERNAL_ERROR, Reason1})
	end.

%格式化错误
format(Format, Data) ->
	try
		io_lib:format(Format, Data)
	catch
		_:_ ->
			Format ++ [$\s | io_lib:write(Data)]
	end.
