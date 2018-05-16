%%%@doc TCP连接
%%```
%%% 协议部分交由协议模块处理，通过TCP连接参数和协议模块配置，可支持块协议和流协议
%%'''
%%@end


-module(zm_tcp_con).

-description("tcp connection").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start/4]).
-export([exist/1, get_pid/1, call/2, p_send/2, send/2, r_send/2, recv/2, close/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HIBERNATE_TIMEOUT, 3000).
%%参数模块的执行方法
-define(MF(MA, F), (element(1, MA)):F(MA).

%%%=======================RECORD=======================
-record(?MODULE, {pid}).
-record(state, {parent, src, socket, addr, session, mod, args, timeout, timer_ref, statistics, error}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 启动一个tcp连接控制进程
%% @spec start(Argus::tuple(),Parent::pid(),Src::atom(),Socket::port()) -> return()
%% where
%% return() = {ok, pid()} | {error, term()}
%%@end
%% -----------------------------------------------------------------
-type start_return() :: {ok, pid()} | {error, term()}.
-spec start(list(), pid(), atom(), port()) ->
	start_return().
%% -----------------------------------------------------------------
start({_, _, _} = Args, Parent, Src, Socket)
	when is_pid(Parent), is_atom(Src), is_port(Socket) ->
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
%%@doc 获得连接对应的进程
%% @spec get_pid(Con::tuple()) -> pid()
%%@end
%% -----------------------------------------------------------------
get_pid(#?MODULE{pid = Pid}) when is_pid(Pid) ->
	Pid.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送call消息
%% @spec call(Con::tuple()) -> any()
%%@end
%% -----------------------------------------------------------------
call(#?MODULE{pid = Pid}, Msg)->
	gen_server:call(Pid, Msg).

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息
%% @spec p_send(Con::tuple(), Msg) -> ok
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
	Pid ! {send, Msg},
	ok.

%% -----------------------------------------------------------------
%%@doc 请求回应消息
%% @spec r_send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
r_send(#?MODULE{pid = Pid}, Msg) when is_pid(Pid) ->
	Pid ! {r_send, Msg},
	ok.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息接收命令
%% @spec recv(Con::tuple(),Term::term()) -> ok
%%@end
%% -----------------------------------------------------------------
recv(#?MODULE{pid = Pid}, {_Cmd, _Msg} = Term) when is_pid(Pid) ->
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
init({{M, Args, Timeout}, Parent, Src, Socket}) ->
	%process_flag(scheduler, 1),
	A = M:init(Args, Socket),
	{ok, {IP, Port}} = zm_socket:peername(Socket),
	Addr = [{src, Src}, {ip, IP}, {port, Port}, {portocol, tcp}],
	Session = zm_session:new(#?MODULE{pid = self()}),
	zm_event:notify(Src, {?MODULE, init}, {Session, Addr}),
	{ok, #state{parent = Parent, src = Src, socket = Socket,
		addr = Addr, session = Session, mod = M, args = A, timeout = Timeout,
		statistics = sb_trees:empty()}, ?HIBERNATE_TIMEOUT}.

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
handle_call({session, {F, A}}, _From, #state{session=Session} = State) ->
	{reply, z_lib:mapply(Session, F, A), State};
handle_call(Msg, _From, State) ->
	{stop, normal, State#state{error = {undefined_msg, Msg}}}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast(Msg, State) ->
	{stop, normal, State#state{error = {undefined_msg, Msg}}}.

%% -----------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {noreply, NextStateName, NextState} |
%%		{noreply, NextStateName, NextState, Timeout} |
%%		{stop, Reason, NewState}
%% -----------------------------------------------------------------
handle_info({tcp, _Socket, Bin}, State) ->
	{noreply, con_receive(Bin, State), ?HIBERNATE_TIMEOUT};

handle_info({ssl, _Socket, Bin}, State) ->
	{noreply, con_receive(Bin, State), ?HIBERNATE_TIMEOUT};

handle_info({send, {Cmd, _} = Data}, State) ->
	con_send({Cmd, send}, Data, State);

handle_info({r_send, {ReqCmd, Data}}, State) ->
	con_send({ReqCmd, resp}, Data, State);

handle_info({close, Reason}, State) ->
	{stop, normal, State#state{error = Reason}};

handle_info({session, Info}, #state{addr = Addr, session = Session, timer_ref = Ref} = State) ->
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, handle),
		Addr, Info), timer_ref = none}, ?HIBERNATE_TIMEOUT};

handle_info({timeout, SessionRef, {session, Info}},
	#state{addr = Addr, session = Session, timer_ref = Ref} = State) ->
	cancel_timeout(Ref),
	{noreply, State#state{session =?MF(Session, timeout),
		Addr, SessionRef, Info), timer_ref = none}, ?HIBERNATE_TIMEOUT};
handle_info({timeout, Ref, Reason}, #state{timer_ref = Ref} = State) ->
	{stop, normal, State#state{error = {shutdown, {timeout, Reason}}}};
handle_info({timeout, _Ref, _}, #state{timer_ref = Ref} = State) when is_reference(Ref) ->
	{noreply, State, hibernate};
handle_info({timeout, _Ref, _}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(timeout, #state{parent = Parent, timeout = Timeout, statistics = Statistics} = State) ->
	send_parent(Parent, statistics, Statistics),
	{noreply, State#state{timer_ref = erlang:start_timer(Timeout, self(), recv_timeout),
		statistics = sb_trees:empty()}, hibernate};

handle_info({recv, Term}, #state{src = Src, addr = Addr, session = Session, timer_ref = Ref} = State) ->
	cancel_timeout(Ref),
	{noreply, State#state{session = ?MF(Session, recv),
		Src, Addr, Term), timer_ref = none}, ?HIBERNATE_TIMEOUT};

handle_info({tcp_closed, _Socket}, State) ->
	{stop, normal, State#state{error = {shutdown, tcp_closed}}};
handle_info({ssl_closed, _Socket}, State) ->
	{stop, normal, State#state{error = {shutdown, tcp_closed}}};

handle_info({tcp_error, _Socket, Reason}, State) ->
	{stop, normal, State#state{error = {shutdown, {tcp_error, Reason}}}};
handle_info({ssl_error, _Socket, Reason}, State) ->
	{stop, normal, State#state{error = {shutdown, {ssl_error, Reason}}}};

handle_info(Info, State) ->
	{stop, normal, State#state{error = {undefined_info, Info}}}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(Reason, #state{parent = Parent, src = Src, socket = Socket,
	addr = Addr, session = Session, statistics = Statistics, error = Error} = State) ->
	[zm_socket:close(Socket) || Error =/= {shutdown, tcp_closed}],
	send_parent(Parent, closed, Statistics),
	Reason1 = if
		Reason =:= normal -> Reason;
		true -> Error
	end,
	zm_event:notify(Src, {?MODULE, closed}, {Session, Addr, Reason1}),
	[zm_log:warn(?MODULE, terminate, Src, "tcp closed error", 
							   [{session, Session},
								{reason, Reason},
								{error, Error}]) || Error =/= {shutdown, tcp_closed}],
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

% 发送消息，调用数据转换模块
con_send(CmdType, Data, #state{src = Src, socket = Socket,
	mod = M, args = A, timer_ref = Ref, statistics = Tree} = State) ->
	{ok, A1, D} = try
		M:encode(A, Data)
	catch
		E:R ->
			zm_log:warn(?MODULE, con_send, Src, CmdType, [{E, R}]),
			erlang:error(R)
	end,
	case zm_socket:send(Socket, D) of
		ok ->
			cancel_timeout(Ref),
			{noreply, State#state{args = A1, timer_ref = none,
				statistics = zm_con:statistics(
				Tree, CmdType, iolist_size(D))}, ?HIBERNATE_TIMEOUT};
		{error, Reason} ->
			{stop, normal, State#state{error = {socket_send, Reason}}}
	end.

% 接收数据，并调用数据转换模块
con_receive(Bin, #state{src = Src, addr = Addr,
	session = Session, mod = M, args = A, timer_ref = Ref, statistics = Tree} = State) ->
	cancel_timeout(Ref),
	case M:decode(A, Bin) of
		{ok, A1, {Cmd, _Msg} = Term} ->
			State#state{session = ?MF(Session, recv),
				Src, Addr, Term), args = A1, timer_ref = none,
				statistics = zm_con:statistics(
				Tree, {Cmd, req}, byte_size(Bin))};
		{ok, A1} ->
			State#state{args = A1, timer_ref = none}
	end.

% 向父进程发送退出信息和统计信息
send_parent(Parent, Type, Tree) when is_tuple(Tree) ->
	gen_server:cast(Parent, {Type, self(), sb_trees:to_list(Tree)});
send_parent(Parent, closed, _) ->
	gen_server:cast(Parent, {closed, self(), []});
send_parent(_, _, _) ->
	none.
