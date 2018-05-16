%%%@doc会话
%%```
%%%由连接进程调用接收消息，负责根据配置表执行消息处理，
%%%配置表的格式为：[{源类型, 消息类型}, MFA列表, 错误处理MFA, 请求(0为通知, 1为请求), 超时(毫秒，小于0为并发, 大于0为顺序)]
%%%如果是顺序化消息，则负责顺序化执行消息，
%%%如果是请求消息，则负责根据请求ID返回响应消息，
%%%提供本地存储的属性，
%%%可以通过发送消息，消息会被转到连接进程中
%%'''
%%@end


-module(zm_session).

-description("session").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([exist/1, get/1, set/5, unset/5, new/1, new/3, recv/4, handle/3, timeout/4]).
-export([get_attr/1, set_attr/2, send/2, r_send/2, close/2, route/8, sequence_route/9]).

%%%=======================DEFINE=======================
%%参数模块的执行方法
-define(MF(MA, F), (element(1, MA)):F(MA).

-define(SERVER_INTERNAL_ERROR, 500).
-define(COLLATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(?MODULE, {con, time, attr, sequence, concurrent, collate_time}).

%%%=================EXPORTED FUNCTIONS=================
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
	 case erlang:process_info(Pid, initial_call) of
		 {_, {?MODULE, _, _}} ->
			 true;
		 _ ->
			 false
	 end.

%% -----------------------------------------------------------------
%%@doc 获得会话指定消息类型的配置
%%```
%% Src项目，即配置文件里的Project，由底层生成 例如 app
%% Cmd前台向后台通讯的指令，例："app/user_login"
%%'''
%% @spec get(Key::{Src::atom(),Cmd::list()}) -> return()
%% where
%% return() =  none | tuple()
%%@end
%% -----------------------------------------------------------------
get(Key) ->
	zm_config:get(?MODULE, Key).

%% -----------------------------------------------------------------
%%@doc 设置会话指定消息类型的配置
%%```
%% Old即同type的以前的值{Type, MFAList, ErrorHandler, ReqType, Timeout}
%% ReqType为请求的类型  0为通知（不需要返回），1为请求（需返回）
%%'''
%% @spec set(Type::{Src::atom(),Cmd::list()},MFAList::[{M::atom(),F::atom(),A::term()}],ErrorHandler::{M::atom(),F::atom(),A::term()},ReqType::integer(),Timeout::integer()) -> return()
%% where
%% return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set({Src, Cmd} = Type, [{_M, _F, _A} | _] = MFAList,
	{_M1, _F1, _A1} = ErrorHandler, ReqType, Timeout) when is_atom(Src), is_list(Cmd) ->
	zm_config:set(?MODULE, {Type, MFAList, ErrorHandler, ReqType, Timeout}).

%% -----------------------------------------------------------------
%%@doc 取消设置会话指定消息类型的配置
%% @spec unset(Type::{Src::atom(),Cmd::list()},MFAList::[{M::atom(),F::atom(),A::term()}],ErrorHandler::[{M::atom(),F::atom(),A::term()}],ReqType::integer(),Timeout::integer()) -> return()
%% where
%% return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Type, MFAList, ErrorHandler, ReqType, Timeout) ->
	zm_config:unset(?MODULE, {Type, MFAList, ErrorHandler, ReqType, Timeout}).

%% -----------------------------------------------------------------
%% new session
%% new(Con::tuple()) -> Session
%% -----------------------------------------------------------------
new(Con) when is_tuple(Con) ->
	Time = zm_time:now_millisecond(),
	#?MODULE{con = Con, time = Time,
		attr = sb_trees:empty(), concurrent = [], collate_time = Time}.

%% -----------------------------------------------------------------
%% new session
%% new(Con::tuple(), Time::integer(), Attr::sb_trees()) -> Session
%% -----------------------------------------------------------------
new(Con, Time, Attr) when is_tuple(Con) ->
	#?MODULE{con = Con, time = Time,
		attr = Attr, concurrent = [], collate_time = Time}.

%% -----------------------------------------------------------------
%%@doc 接收消息，消息的格式为{命令字, 消息正文}，根据路由信息对消息进行处理
%%```
%% Timeout=0本进程处理消息;
%% Timeout<0新开进程并行处理;
%% Timeout>0在一个伴随进程中顺序执行
%%'''
%% @spec recv(Session, Src, Info, Msg) -> return()
%% where
%%	return() =NewSession
%%@end
%% ----------------------------------------------------------------
recv(#?MODULE{con = Con} = Session, Src, Info, {Cmd, Msg}) ->
	Now = zm_time:now_millisecond(),
	S = Session#?MODULE{time = Now},
	SS = #?MODULE{con = Con, time = Now},
	zm_log:debug(?MODULE, route, Src, Cmd,
		[{session, SS}, {info, Info}, {msg, Msg}]),
	case zm_config:get(?MODULE, {Src, Cmd}) of
		{_, [{_, _, _} | _] = MFAList, ErrorHandler, Req, 0} ->
			route(collate_session(S), [{cmd, Cmd} | Info],
				Msg, MFAList, ErrorHandler, Req, Src, Cmd);
		{_, [{_, _, _} | _] = MFAList, ErrorHandler, Req, Timeout} when Timeout < 0 ->
			route_spawn(S, [{cmd, Cmd} | Info],
				Msg, MFAList, ErrorHandler, Req, -Timeout, Src, Cmd);
		{_, [{_, _, _} | _], _, _, _} = R ->
			route_sequence(S, [{cmd, Cmd} | Info], Msg, R, Src, Cmd);
		none ->
			zm_event:notify(Src, {?MODULE, route}, {SS, Info, Cmd, Msg}),
			S
	end.

%% -----------------------------------------------------------------
%% Function: handle/2
%% Description: handle msg
%% Returns: Session
%% -----------------------------------------------------------------
handle(#?MODULE{attr = Attr} = Session, Info, {set_attr, L}) ->
	case attr_update(Info, Attr, L) of
		false ->
			Session;
		Attr1 ->
			zm_event:notify(z_lib:get_value(Info, src, ""), {?MODULE, attr_update}, Attr1),
			Session#?MODULE{attr = Attr1}
	end;

handle(#?MODULE{sequence = {Queue, Ref, Pid, _MFA, _, _}} = Session,
	_Info, {sequence, Pid, Ref}) ->
	erlang:cancel_timer(Ref),
	case queue:out(Queue) of
		{empty, _} ->
			Session#?MODULE{sequence = none};
		{{value, {Info, Msg, {_, MFAList, ErrorHandler, Req, Timeout}, Src, Cmd}}, Q} ->
			route_sequence(Session, Info, Msg, MFAList,
				ErrorHandler, Req, Timeout, Src, Cmd, Q)
	end;
handle(Session, _, _) ->
	Session.

%% -----------------------------------------------------------------
%% Function: timeout/3
%% Description: timeout handle
%% Returns: Session
%% -----------------------------------------------------------------
timeout(#?MODULE{sequence = {Queue, Ref, Pid, MFA, OldSrc, OldCmd}} = Session,
	_Info, Ref, sequence_timeout) ->
	kill_pid(Pid, MFA, OldSrc, OldCmd),
	case queue:out(Queue) of
		{{value, {Info, Msg, {_, MFAList, ErrorHandler, Req, Timeout}, Src, Cmd}}, Q} ->
			route_sequence(Session, Info, Msg, MFAList,
				ErrorHandler, Req, Timeout, Src, Cmd, Q);
		{empty, _} ->
			Session#?MODULE{sequence = none}
	end;
timeout(Session, _, _, _) ->
	Session.

%% -----------------------------------------------------------------
%%@doc 获得会话的属性
%% @spec get_attr(Session::tuple()) -> tuple()
%%@end
%% -----------------------------------------------------------------
get_attr(#?MODULE{attr = Attr}) ->
	Attr;
get_attr(Con)->
	?MF(Con, call), {session, {get_attr, []}}).

%% -----------------------------------------------------------------
%%@doc 属性更新，根据每个属性的版本号来决定是否更新
%%```
%% List=[{Key,{{Value, ExpireTime, Vsn}, SrcInfo}}]
%% ExpireTime为该属性的过期时间,ExpireTime=:=0时删除该条属性,ExpireTime=:=infinity永久存在
%% Vsn为新的版本号,版本号相同才能修改属性(Vsn=:=0时不检查版本号)
%% SrcInfo为设置来源
%%'''
%% @spec set_attr(Con::tuple(),List::list) -> return()
%% where
%%	return() = term()
%%@end
%% ----------------------------------------------------------------
set_attr(Con, [_ | _] = List) ->
	?MF(Con, p_send), {session, {set_attr, List}});
set_attr(_, _) ->
	ok.

%% -----------------------------------------------------------------
%%@doc 消息发送方法
%% @spec send(Con::tuple(),Msg::tuple()) -> ok
%%@end
%% -----------------------------------------------------------------
send(Con, {_, _} = Msg) ->
	?MF(Con, send), Msg);
send(_, _) ->
	ok.

%% -----------------------------------------------------------------
%%@doc 请求消息发送方法
%% @spec r_send(Con::tuple(),Msg::tuple()) -> ok
%%@end
%% -----------------------------------------------------------------
r_send(Con, {_ReqCmd, {_, _}} = Msg) ->
	?MF(Con, r_send), Msg);
r_send(_, _) ->
	ok.

%% -----------------------------------------------------------------
%%@doc 会话关闭
%% @spec close(Con::tuple(),Reason::term()) -> ok
%%@end
%% -----------------------------------------------------------------
close(Con, Reason) ->
	?MF(Con, close), Reason).

%% -----------------------------------------------------------------
%%@doc 路由方法，新处理进程的入口方法调用
%% @spec route(Session::tuple(),Info,Msg,MFAList::[mfa()],Errorhandler::mfa(),Req::integer(),Src::atom(),Cmd::list()) -> Session
%% where
%%	mfa() = {M::atom(),F::atom(),A::term()}
%%	mfas() = [mfa()]
%%@end
%% -----------------------------------------------------------------
route(#?MODULE{con = Con, time = Time, attr = Attr} = Session,
	Info, Msg, MFAList, ErrorHandler, Req, Src, Cmd) ->
	put('$log', {Src, Cmd}),
	route_apply(Con, Time, Attr, Info, Msg, MFAList, Req, Src, Cmd, ErrorHandler),
	Session.

%% -----------------------------------------------------------------
%% Function: sequence_route
%% Description: 顺序路由方法，仅供内部调用
%% Returns: any()
%% -----------------------------------------------------------------
sequence_route(#?MODULE{con = Con} = Session,
	Info, Msg, MFAList, ErrorHandler, Req, Src, Cmd, Ref) ->
	route(Session, Info, Msg, MFAList, ErrorHandler, Req, Src, Cmd),
	?MF(Con, p_send), {session, {sequence, self(), Ref}}).

%%%===================LOCAL FUNCTIONS==================
% 整理会话
collate_session(#?MODULE{attr = Attr, time = Time, concurrent = L, collate_time = CT} = Session)
	when Time > CT + ?COLLATE_TIMEOUT->
	Session#?MODULE{attr = attr_expire(Attr, Time),
		concurrent = [E || E <- L, pid_expire(E, Time)], collate_time = Time};
collate_session(Session) ->
	Session.

% 路由到新进程中
route_spawn(Session, Info, Msg, MFAList, ErrorHandler, Req, Timeout, Src, Cmd) ->
	#?MODULE{con = Con, time = Time, attr = Attr, concurrent = L} = Session1 = collate_session(Session),
	Args = [#?MODULE{con = Con, time = Time, attr = Attr},
		Info, Msg, MFAList, ErrorHandler, Req, Src, Cmd],
	Pid = spawn(?MODULE, route, Args),
	Session1#?MODULE{concurrent = [{Pid, Time + Timeout, {?MODULE, route, Args}, Src, Cmd} | L]}.

%% -----------------------------------------------------------------
%%@doc 路由顺序化消息
%%```
%% 如果伴随进程（sequence）存在则在队列中增加消息；不存在则新开伴随进程
%%'''
%% @spec route_sequence() -> return()
%% where
%%	return() =
%%@end
%% ----------------------------------------------------------------
route_sequence(#?MODULE{sequence = {Queue, Ref, Pid, MFA, OldSrc, OldCmd}} = Session, Info, Msg, R, Src, Cmd) ->
	Session#?MODULE{sequence = {queue:in({Info, Msg, R, Src, Cmd}, Queue), Ref, Pid, MFA, OldSrc, OldCmd}};
route_sequence(Session, Info, Msg, {_, MFAList, ErrorHandler, Req, Timeout}, Src, Cmd) ->
	route_sequence(Session, Info, Msg, MFAList, ErrorHandler, Req, Timeout, Src, Cmd, queue:new()).

% 顺序路由到新开伴随进程中
route_sequence(Session, Info, Msg, MFAList, ErrorHandler, Req, Timeout, Src, Cmd, Queue) ->
	#?MODULE{con = Con, time = Time, attr = Attr} = Session1 = collate_session(Session),
	Ref = erlang:start_timer(Timeout, self(), {session, sequence_timeout}),
	Args = [#?MODULE{con = Con, time = Time, attr = Attr},
		Info, Msg, MFAList, ErrorHandler, Req, Src, Cmd, Ref],
	Pid = spawn(?MODULE, sequence_route, Args),
	Session1#?MODULE{sequence = {Queue, Ref, Pid, {?MODULE, sequence_route, Args}, Src, Cmd}}.

% 新进程的路由请求函数
route_apply(Con, Time, Attr, Info, Msg, MFAList, 1, Src, Cmd, ErrorHandler) ->
	{Seq, Msg1} = case Msg of
		{Bin, Offset} ->
		<<_:Offset/binary, I:32, _/binary>> = Bin,
			{I, {Bin, Offset + 4}};
		KVList ->
			{_, I} = lists:keyfind("", 1, KVList),
			{I, Msg}
	end,
	try
		{AttrOrder, NewMsg} = apply_route(Con,
			Time, Attr, [{time, Time} | Info], Msg1,
			MFAList, sb_trees:empty(), Src, Cmd),
		set_attr(Con, sb_trees:to_list(AttrOrder)),
		case NewMsg of
			[{_, _} | _] ->
				r_send(Con, {Cmd, {"r_ok", [{"", Seq} | NewMsg]}});
			[] ->
				r_send(Con, {Cmd, {"r_ok", [{"", Seq}]}});
			_ ->
				r_send(Con, {Cmd, {"r_ok", [Seq, NewMsg]}})
		end
	catch
		throw:{Type, Why, Vars} = Reason ->
			S = case Why of
				[H | _] when H > 0, H < 16#10ffff ->
					Why;
				_ ->
					io_lib:write(Why)
			end,
			r_send(Con, {Cmd, {"r_err", [
				{"", Seq}, {"type", Type}, {"why", S}, {"vars", Vars}]}}),
			{throw, Reason, erlang:get_stacktrace()};
		Error:Reason ->
			try
				error_handle(ErrorHandler, Con, Attr, Info, Msg, {Error, Reason, erlang:get_stacktrace()}, Src, Cmd)
			catch
				_:_ ->
					S = case Reason of
						[H | _] when H > 0, H < 16#10ffff ->
							Reason;
						_ ->
							io_lib:write(Reason)
					end,
					ST = erlang:get_stacktrace(),
					r_send(Con, {Cmd, {"r_err", [
						{"", Seq},
						{"type", ?SERVER_INTERNAL_ERROR},
						{"why", S},
						{"stacktrace", lists:flatten(z_lib:string(ST))}]}}),
					{Error, Reason, ST}
			end
	end;
% 新进程的路由执行函数
route_apply(Con, Time, Attr, Info, Msg, MFAList, _, Src, Cmd, _) ->
	{AttrOrder, NewMsg} = apply_route(Con,
		Time, Attr, [{time, Time} | Info], Msg,
		MFAList, sb_trees:empty(), Src, Cmd),
	set_attr(Con, sb_trees:to_list(AttrOrder)),
	send(Con, NewMsg).

apply_route(Con, Time, Attr, Info, Msg, [{M, F, A} = MFA | T], AttrOrder, Src, Cmd) ->
	zm_log:debug(?MODULE, apply_route, Src, Cmd,
		[{mfa, MFA}, {attr, Attr}, {info, Info}, {msg, Msg}]),
	case M:F(A, Con, Attr, Info, Msg) of
		{ok, R, Info1, Msg1} ->
			{Attr1, AttrOrder1} = attr_modify(
				Attr, AttrOrder, R, Time, {MFA, Info, Msg, Src, Cmd}),
			apply_route(Con, Time, Attr1, Info1, Msg1, T, AttrOrder1, Src, Cmd);
		{break, R, Info1, Msg1} ->
			{Attr1, AttrOrder1} = attr_modify(
				Attr, AttrOrder, R, Time, {MFA, Info, Msg, Src, Cmd}),
			zm_log:debug(?MODULE, apply_route_break, Src, Cmd,
				[{attr, Attr1}, {info, Info1}, {msg, Msg1}, {attr_order, AttrOrder1}]),
			{AttrOrder1, Msg1};
		R ->
			throw({"route error, invalid result ", MFA, R})
	end;
apply_route(_Con, _Time, Attr, Info, Msg, [], AttrOrder, Src, Cmd) ->
	zm_log:debug(?MODULE, apply_route_over,Src, Cmd,
		[{attr, Attr}, {info, Info}, {msg, Msg}, {attr_order, AttrOrder}]),
	{AttrOrder, Msg}.

%% -----------------------------------------------------------------
%%@doc 删除过期的属性
%% @spec attr_expire(Attr::sb_trees(), Time::integer()) -> return()
%% where
%%	return() =sb_trees()
%%@end
%% ----------------------------------------------------------------
attr_expire(Attr, Time) ->
	attr_expire(Attr, Time, sb_trees:next(sb_trees:iterator(Attr))).

attr_expire(Attr, Time, {K, {_, _, E, _}, I}) when E < Time ->
	attr_expire(sb_trees:delete(K, Attr), Time, sb_trees:next(I));
attr_expire(Attr, Time, {_, _, I}) ->
	attr_expire(Attr, Time, sb_trees:next(I));
attr_expire(Attr, _Time, none) ->
	Attr.

% 属性修改，并记录修改指令
attr_modify(Attr, AttrOrder, [{K, V, E} | T], Time, Info) ->
	{Attr1, AttrOrder1} = attr_modify(Attr, AttrOrder, K, V, E, Time, Info),
	attr_modify(Attr1, AttrOrder1, T, Time, Info);
attr_modify(Attr, AttrOrder, [{K, V} | T], Time, Info) ->
	{Attr1, AttrOrder1} = attr_modify(Attr, AttrOrder, K, V, infinity, Time, Info),
	attr_modify(Attr1, AttrOrder1, T, Time, Info);
attr_modify(Attr, AttrOrder, [_ | T], Time, Info) ->
	attr_modify(Attr, AttrOrder, T, Time, Info);
attr_modify(Attr, AttrOrder, [], _Time, _Info) ->
	{Attr, AttrOrder}.

% 属性修改，并记录修改指令
attr_modify(Attr, AttrOrder, K, V, E, _Time, Info) when E =< 0 ->
	case sb_trees:get(K, Attr, none) of
		{_, _, Vsn} ->
			{sb_trees:delete(K, Attr),
				sb_trees:enter(K, {{V, 0, Vsn}, Info}, AttrOrder)};
		none ->
			{Attr, AttrOrder}
	end;
attr_modify(Attr, AttrOrder, K, V, E, Time, Info) ->
	E1 = if
		is_integer(E) -> E + Time;
		true -> E
	end,
	case sb_trees:get(K, Attr, none) of
		{_, _, Vsn} ->
			R = {V, E1, Vsn},
			{sb_trees:update(K, R, Attr), sb_trees:enter(K, {R, Info}, AttrOrder)};
		none ->
			R = {V, E1, 1},
			{sb_trees:insert(K, R, Attr), sb_trees:enter(K, {R, Info}, AttrOrder)}
	end.

%% -----------------------------------------------------------------
%%@doc 属性更新，根据每个属性的版本号来决定是否更新
%%```
%% KvList=[{Key,{{Value, ExpireTime, Vsn}, SrcInfo}}]
%% ExpireTime为该属性的过期时间,ExpireTime=:=0时删除该条属性
%% Vsn为新的版本号,Vsn=:=0时不检查版本号
%% SrcInfo为设置来源
%%'''
%% @spec attr_update(Info::term(),Attr::sb_trees(),KvList::list) -> return()
%% where
%%	return() = sb_trees()
%%@end
%% ----------------------------------------------------------------
attr_update(Info, Attr, [{K, {{V, E, 0} = Value, _SrcInfo}} | T]) ->
	case sb_trees:get(K, Attr, none) of
		{_, _, Vsn} ->
			if
				E =:= 0 ->
					attr_update(Info, sb_trees:delete(K, Attr), T);
				true ->
					attr_update(Info, sb_trees:update(
						K, {V, E, Vsn + 1}, Attr), T)
			end;
		none ->
			if
				E =:= 0 ->
					attr_update(Info, Attr, T);
				true ->
					attr_update(Info, sb_trees:insert(K, Value, Attr), T)
			end
	end;
attr_update(Info, Attr, [{K, {{V, E, Vsn} = Value, SrcInfo}} | T]) ->
	case sb_trees:get(K, Attr, none) of
		{_, _, Vsn} ->
			if
				E =:= 0 ->
					attr_update(Info, sb_trees:delete(K, Attr), T);
				true ->
					attr_update(Info, sb_trees:update(
						K, {V, E, Vsn + 1}, Attr), T)
			end;
		{_, _, _} = Cur ->
			% 报告错误
			case SrcInfo of
				{MFA, _Info, Msg, Src, Cmd} ->
					zm_log:warn(?MODULE, attr_conflict, Src, Cmd,
						[{mfa, MFA}, {info, Info}, {msg, Msg},
						{kv, {K, Value}}, {cur_kv, Cur}]);
				_ ->
					zm_log:warn(?MODULE, attr_conflict, ?MODULE, none,
						[{src_info, SrcInfo}, {info, Info},
						{kv, {K, Value}}, {cur_kv, Cur}])
			end,
			false;
		none ->
			if
				E =:= 0 ->
					attr_update(Info, Attr, T);
				true ->
					attr_update(Info, sb_trees:insert(K, Value, Attr), T)
			end
	end;
attr_update(Info, Attr, [_ | T]) ->
	attr_update(Info, Attr, T);
attr_update(_Info, Attr, []) ->
	Attr.

% 并发进程超期
pid_expire({Pid, Expire, MFA, Src, Cmd}, Time) ->
	case is_process_alive(Pid) of
		true when Time > Expire ->
			kill_pid(Pid, MFA, Src, Cmd),
			false;
		R ->
			R
	end.

% 杀掉指定进程
kill_pid(Pid, MFA, Src, Cmd) ->
	CurFun = process_info(Pid, current_function),
	exit(Pid, kill),
	zm_log:warn(?MODULE, kill_pid, Src, Cmd,
		[{pid, Pid}, {mfa, MFA}, {cur_fun, CurFun}]).

% 错误处理
error_handle({M, F, A} = MFA, Con, Attr, Info, Msg, Error, Src, Cmd) ->
	zm_log:debug(?MODULE, error_handle, Src, Cmd,
		[{mfa, MFA}, {con, Con}, {attr, Attr},
		{info, Info}, {msg, Msg}, {error, Error}]),
		M:F(A, Con, Attr, Info, {Src, Cmd, Msg}, Error);
error_handle(Handler, Con, Attr, Info, Msg, Error, Src, Cmd) ->
	zm_log:warn(?MODULE, error_handle, Src, Cmd,
		[{handler, Handler}, {con, Con}, {attr, Attr},
		{info, Info}, {msg, Msg}, {error, Error}]),
	ok.
