%%@doc 场景进程
%%```
%%% 场景维护用户定义的参数
%%% 接收场景消息，负责根据配置的命令表执行消息处理
%%% 命令表的格式为：[消息类型, MFA, 类型(0为立即执行，1为高等级, -1为低等级, 同一等级的命令按接收的先后次序处理, frame命令在高等级命令后执行)]
%%% MFA调用格式为 M:F(A, {Type, Name}, Args, Msg, From), 如果是cast调用，则From为none，
%%% 如果在call调用中，用新进程计算结果，应该返回noreply，然后新进程中用z_lib:reply(From, R)来返回结果。
%%% 命令表上必须设置burden处理命令，应该设置frame_start, frame, frame_end的处理命令，
%%'''
%%@end

-module(zm_scene).

-description("scene pid").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start/2]).
-export([get/2, set/2, unset/2, get_args/1, cast/2, call/2, call_send/3]).
-export([cast_apply/4, call_apply/4, delay_cast/3, delay_cast/5, frame/1, frame/2, close/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
%%帧时间差为50毫秒
-define(FRAME_TIME, 50).
%%帧命令
-define(FRAME, frame).
-define(FRAME_START, frame_start).
-define(FRAME_END, frame_end).

%%%=======================RECORD=======================
-record(state, {type_name, src, cmd_map, frame_time,
	start_time, close, args, next_time, high_order, frame_order, low_order}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start/2
%% Description: Starts a scene pid
%% -----------------------------------------------------------------
start({Src, CmdMap, FrameTime, MFA}, TypeName) ->
	gen_server:start(?MODULE, {TypeName, Src, CmdMap, FrameTime, MFA}, []);
start(Table, {Type, _} = TypeName) ->
	case zm_config:get(Table, TypeName) of
		{_, Src, CmdMap, FrameTime, MFA} ->
			gen_server:start(?MODULE,
				{TypeName, Src, CmdMap, FrameTime, MFA}, []);
		_ ->
			case zm_config:get(Table, Type) of
				{_, Src, CmdMap, FrameTime, MFA} ->
					gen_server:start(?MODULE,
						{TypeName, Src, CmdMap, FrameTime, MFA}, []);
				_ ->
					{error, invalid_type_name}
			end
	end.

%% -----------------------------------------------------------------
%%@doc 获得指定类型名称的场景参数
%% @spec get(Table,TypeName::{Type, Name}) -> return()
%% where
%%  return() = {any(), Src, CmdMap, FrameTime, InitMFA} | none
%%@end
%% -----------------------------------------------------------------
get(Table, TypeName) ->
	zm_config:get(Table, TypeName).

%% -----------------------------------------------------------------
%%@doc 设置指定类型名称的命令表，
%%```
%%  Src为源，CmdMap为命令表，FrameTime为每帧间隔时间，InitMFA为初始化MFA，
%%'''
%% @spec set(Table, {TypeName, Src, CmdMap, FrameTime, InitMFA} | L) -> return()
%% where
%%  return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Table, [_ | _] = L) ->
	[I || I <- L, begin {_, _, _, FrameTime, {_, _, _}} = I, not is_integer(FrameTime) end],
	zm_config:set(Table, L, []);
set(Table, {_, _, _, FrameTime, {_, _, _}} = El) when is_integer(FrameTime) ->
	zm_config:set(Table, El).

%% -----------------------------------------------------------------
%%@doc 取消指定类型名称的命令表
%% @spec unset(Table, {TypeName, Src, CmdMap, FrameTime, InitMFA} | L) -> return()
%% where
%%  return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Table, [_ | _] = L) ->
	zm_config:unset(Table, L, []);
unset(Table, El) ->
	zm_config:unset(Table, El).

%% -----------------------------------------------------------------
%%@doc 获得进程参数
%% @spec get_args(ScenePid) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
get_args(ScenePid) ->
	gen_server:call(ScenePid, get_args).

%% -----------------------------------------------------------------
%%@doc 向场景发送通知命令消息
%% @spec cast(ScenePid, Msg) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
cast(ScenePid, Msg) when is_tuple(Msg) ->
	gen_server:cast(ScenePid, Msg).

%% -----------------------------------------------------------------
%%@doc 向场景发送调用命令消息
%% @spec call(ScenePid, Msg) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
call(ScenePid, Msg) when is_tuple(Msg) ->
	gen_server:call(ScenePid, Msg).

%% -----------------------------------------------------------------
%%@doc 发送Call数据
%% @spec call_send(ScenePid, From, Msg) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
call_send(ScenePid, From, Msg) ->
	z_lib:send(ScenePid, {'$gen_call', From, Msg}).

%% -----------------------------------------------------------------
%%@doc 向场景发送通知执行指令
%% @spec cast_apply(ScenePid, {M, F, A}, Msg, Type) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
cast_apply(ScenePid, {_M, _F, _A} = MFA, Msg, Type) ->
	gen_server:cast(ScenePid, {MFA, Msg, Type}).

%% -----------------------------------------------------------------
%%@doc 向场景发送通知执行指令
%% @spec call_apply(ScenePid, {M, F, A}, Msg, Type) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
call_apply(ScenePid, {_M, _F, _A} = MFA, Msg, Type) ->
	gen_server:call(ScenePid, {MFA, Msg, Type}).

%% -----------------------------------------------------------------
%%@doc 向场景发送延迟通知命令消息
%% @spec delay_cast(ScenePid, Msg, DelayTime) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
delay_cast(ScenePid, Msg, DelayTime) when is_tuple(Msg) ->
	if
		node() =:= node(ScenePid) ->
			erlang:start_timer(DelayTime, ScenePid, Msg);
		true -> 
			ScenePid ! {delay, Msg, DelayTime}
	end.

%% -----------------------------------------------------------------
%%@doc 向场景发送延迟调用命令消息
%% @spec delay_cast(ScenePid, MFA, Msg, Type, DelayTime) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
delay_cast(ScenePid, {_, _, _} = MFA, Msg, Type, DelayTime) ->
	if
		node() =:= node(ScenePid) ->
			erlang:start_timer(DelayTime, ScenePid, {MFA, Msg, Type});
		true -> 
			ScenePid ! {delay, {MFA, Msg, Type}, DelayTime}
	end.

%% -----------------------------------------------------------------
%%@doc 向场景发送定时消息
%% @spec frame(ScenePid) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
frame(ScenePid) ->
	ScenePid ! {timeout, none, ?FRAME}.

%% -----------------------------------------------------------------
%%@doc 向场景发送定时消息
%% @spec frame(ScenePid, Now) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
frame(ScenePid, Now) ->
	ScenePid ! {timeout, none, {?FRAME, Now}}.

%% -----------------------------------------------------------------
%%@doc 向场景发送关闭消息
%% @spec close(Con::tuple(),Reason::term()) -> ok
%%@end
%% -----------------------------------------------------------------
close(ScenePid, Reason) when is_pid(ScenePid) ->
	ScenePid ! {close, Reason},
	ok.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({TypeName, Src, CmdMap, FrameTime, {M, F, A}}) ->
	Now = zm_time:now_millisecond(),
	{Close, Args} = M:F(A, TypeName, Src, FrameTime, Now),
	[erlang:start_timer(FrameTime, self(), frame) || FrameTime > 0],
	{ok, #state{type_name = TypeName, src = Src,
		cmd_map = CmdMap, frame_time = FrameTime,
		start_time = Now, close = Close, args = Args,
		next_time = Now + FrameTime, high_order = [], frame_order = [], low_order = []}}.

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
handle_call({{_, _, _} = MFA, Msg, Type}, From, State) ->
	{noreply, route(call_apply, MFA, Msg, Type, From, State)};
handle_call(Msg, From, State) when is_tuple(Msg) ->
	{noreply, route(Msg, From, State)};
handle_call(get_args, _From, #state{args = Args} = State) ->
	{reply, Args, State};

handle_call(_Request, _From, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({{_, _, _} = MFA, Msg, Type}, State) ->
	{noreply, route(cast_apply, MFA, Msg, Type, none, State)};
handle_cast(Msg, State) when is_tuple(Msg) ->
	{noreply, route(Msg, none, State)};

handle_cast(_Msg, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State} 
%% -----------------------------------------------------------------
handle_info({timeout, _Ref, ?FRAME}, #state{frame_time = FrameTime,
	next_time = Time} = State) ->
	frame_(State, Time),
	Now = zm_time:now_millisecond(),
	if
		Now - Time < FrameTime ->
			erlang:start_timer(FrameTime - (Now - Time), self(), ?FRAME),
			{noreply, State#state{next_time = Time + FrameTime,
				high_order = [], frame_order = [], low_order = []}};
		true ->
			erlang:start_timer(0, self(), ?FRAME),
			{noreply, State#state{next_time = Now,
				high_order = [], frame_order = [], low_order = []}}
	end;
handle_info({timeout, _Ref, {?FRAME, Now}}, State) ->
	frame_(State, Now),
	{noreply, State#state{next_time = Now, high_order = [], frame_order = [], low_order = []}};

handle_info({timeout, _Ref, {MFA, Msg, Type}}, State) when is_tuple(Msg) ->
	{noreply, route(cast_apply, MFA, Msg, Type, none, State)};
handle_info({timeout, _Ref, Msg}, State) when is_tuple(Msg) ->
	{noreply, route(Msg, none, State)};

handle_info(timeout, State) ->
	{noreply, State, hibernate};

handle_info({delay, Msg, DelayTime}, State) when is_tuple(Msg) ->
	erlang:start_timer(DelayTime, self(), Msg),
	{noreply, State};

handle_info({close, Reason}, State) ->
	{stop, Reason, State};

handle_info(_Info, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(Reason, #state{src = Src, close = Close, args = Args}) ->
	try
		Close(Args, Reason)
	catch
		Error:Why ->
			zm_log:warn(?MODULE, {terminate_close, Error}, Src, none,
				[{reason, Reason}, {close, Close}, {args, Args},
				{error, Why}, {stacktrace, erlang:get_stacktrace()}])
	end.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
% 获得消息对应的命令MFA
get_cmd(CmdMap, Cmd) when is_tuple(CmdMap) ->
	sb_sets:get(Cmd, CmdMap, none);
get_cmd(CmdMap, Cmd) ->
	zm_config:get(CmdMap, Cmd).

% 消息路由
route(Cmd, MFA, Msg, Type, From, #state{type_name = TypeName, src = Src,
	args = Args, high_order = HighOrder, low_order = LowOrder} = State) ->
	if
		Type < 0 ->
			State#state{low_order = [{MFA, Msg, From} | LowOrder]};
		Type > 0 ->
			State#state{high_order = [{MFA, Msg, From} | HighOrder]};
		true ->
			handle(Src, Cmd, MFA, TypeName, Args, Msg, From),
			State
	end.

% 消息路由
route(Msg, From, #state{type_name = TypeName, src = Src, cmd_map = CmdMap,
	args = Args, high_order = HighOrder, frame_order = FrameOrder, low_order = LowOrder} = State) ->
	Cmd = element(1, Msg),
	case get_cmd(CmdMap, Cmd) of
		{_, MFA, 0} ->
			handle(Src, Cmd, MFA, TypeName, Args, Msg, From),
			State;
		{_, MFA, Type} when Type < 0 ->
			State#state{low_order = [{MFA, Msg, From} | LowOrder]};
		{_, MFA, _} ->
			State#state{high_order = [{MFA, Msg, From} | HighOrder]};
		_ ->
			State#state{frame_order = [{Msg, From} | FrameOrder]}
	end.

% 消息执行函数
handle(Src, Cmd, {M, F, A} = MFA, TypeName, Args, Msg, none) ->
	try
		M:F(A, TypeName, Args, Msg, none)
	catch
		Error:Reason ->
			zm_log:warn(?MODULE, {handle_cast, Error}, Src, Cmd,
				[{mfa, MFA}, {msg, Msg},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}])
	end;
handle(Src, Cmd, {M, F, A} = MFA, TypeName, Args, Msg, From) ->
	try M:F(A, TypeName, Args, Msg, From) of
		noreply ->
			noreply;
		R ->
			z_lib:reply(From, R)
	catch
		Error:Reason ->
			ST = erlang:get_stacktrace(),
			zm_log:warn(?MODULE, {handle_call, Error}, Src, Cmd,
				[{mfa, MFA}, {msg, Msg},
				{error, Reason}, {stacktrace, ST}])
	end.

% 每帧执行
frame_(#state{type_name = TypeName, src = Src, cmd_map = CmdMap,
	args = Args, high_order = HighOrder, frame_order = FrameOrder, low_order = LowOrder}, Now) ->
	% frame_start命令在开始执行
	frame_handle(TypeName, Src, CmdMap, Args, Now, ?FRAME_START, HighOrder),
	list_handle(TypeName, Src, Args, lists:reverse(HighOrder)),
	% frame命令在中间执行
	frame_handle(TypeName, Src, CmdMap, Args, Now, ?FRAME, FrameOrder),
	list_handle(TypeName, Src, Args, lists:reverse(LowOrder)),
	% frame_end命令在结束执行
	frame_handle(TypeName, Src, CmdMap, Args, Now, ?FRAME_END, LowOrder).

% 执行帧指令
frame_handle(TypeName, Src, CmdMap, Args, Now, Cmd, OrderList) ->
	case get_cmd(CmdMap, Cmd) of
		{_, MFA, _} ->
			handle(Src, Cmd, MFA, TypeName, Args, {Cmd, OrderList, Now}, none);
		none ->
			none
	end.

% 执行指令列表
list_handle(TypeName, Src, Args, [{MFA, Msg, From} | T]) ->
	handle(Src, list_handle, MFA, TypeName, Args, Msg, From),
	list_handle(TypeName, Src, Args, T);
list_handle(_TypeName, _Src, _Args, []) ->
	ok.
