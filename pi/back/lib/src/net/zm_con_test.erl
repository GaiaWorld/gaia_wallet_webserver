%%%@doc连接测试
%%```
%%% 供后台调试使用，发送消息、设置属性都是打印到控制台
%%% S = zm_session:new(zm_con_test:new("001"), zm_time:now_millisecond(), sb_trees:from_dict([{'app/user', {10001, 999999999, 0}}, {'app/role', {10001, 999999999, 0}}], nil)).
%%% zm_session:recv(S, app, [], {"app/hero/shortcut@write", [{"", 1}, {"data", "1,2,3,4,5,6,7,8"}]}).
%%'''
%%@end


-module(zm_con_test).

-description("zm_con_test").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([test/4, new/1, get_pid/1, p_send/2, send/2, r_send/2, recv/2, close/2]).
-export([new/0, recv/3, recv/4]).

-define(MF(MA, F), (element(1, MA)):F(MA).
-define(DEF_TIMEOUT, 30000).

%%%=======================RECORD=======================
-record(?MODULE, {pid}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% new con test
%% test(S, Src, Info, Msg) -> ok
%% -----------------------------------------------------------------
test(S, Src, Info, Msg) ->
	zm_session:recv(S, Src, Info, Msg),
	ok.

%% -----------------------------------------------------------------
%% new con test
%% new(Pid::pid()) -> tuple()
%% -----------------------------------------------------------------
new(Pid) ->
	#?MODULE{pid = Pid}.

new() ->
	Session = zm_session:new(#?MODULE{pid = self()}),
	case ets:info(?MODULE) of
		undefined ->
			ets:new(?MODULE,[named_table, public]),
			ets:insert(?MODULE, {?MODULE, Session});
		_ ->
			ets:insert(?MODULE, {?MODULE, Session})
	end,
	ok.

%% -----------------------------------------------------------------
%%@doc 获得连接对应的进程
%% @spec get_pid(Con::tuple()) -> pid()
%%@end
%% -----------------------------------------------------------------
get_pid(#?MODULE{pid = Pid}) ->
	Pid.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息
%% @spec p_send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
p_send(#?MODULE{pid = Pid}, {session, Msg}) ->
	case ets:lookup(?MODULE, ?MODULE) of
		[] ->
			format("p_send, ~s:~s:~s,~s, ~p, msg:~s ~n", Pid, io_lib:write(Msg, 32));
		[{_, Session}] ->
			
			NewSession = ?MF(Session, handle), [], Msg),
			ets:insert(?MODULE, {?MODULE, NewSession})
	end.

%% -----------------------------------------------------------------
%%@doc 通过连接向对端发送消息
%% @spec send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
send(#?MODULE{pid = Pid}, Msg) ->
	format("send, ~s:~s:~s,~s, ~p, msg:~p ~n", Pid, Msg).

%% -----------------------------------------------------------------
%%@doc 请求回应消息
%% @spec r_send(Con::tuple(),Msg) -> ok
%%@end
%% -----------------------------------------------------------------
r_send(#?MODULE{pid = Pid}, Msg) ->
	case ets:lookup(?MODULE, ?MODULE) of
		[] ->
			format("r_send, ~s:~s:~s,~s, ~p, msg:~p ~n", Pid, Msg);
		[{_, {_, {_, SPid}, _, _, _, _, _}}] ->
			SPid ! {r_send, Msg}
	end.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送消息接收命令
%% @spec recv(Con::tuple(),Term::term()) -> ok
%%@end
%% -----------------------------------------------------------------
recv(#?MODULE{pid = Pid}, {_Cmd, _Msg} = Term) ->
	format("recv, ~s:~s:~s,~s, ~p, msg:~p ~n", Pid, Term).

recv(Src, Info, {Cmd, Msg}) ->
	[{_, Session}] = ets:lookup(?MODULE, ?MODULE),
	zm_session:recv(Session, Src, Info, {Cmd, Msg}),
	receive
		{r_send, R} -> 
			R
	after ?DEF_TIMEOUT ->
			  zm_log:warn(?MODULE, recv, recv, "timeout!!!!!!!!!!!!!", [
																					  {cmd, Cmd},
																					  {msg, Msg},
																					  {timeout, ?DEF_TIMEOUT}]),
			  timeout
	end.

recv(Src, Info, {Cmd, Msg}, Filter) ->
	[{_, Session}] = ets:lookup(?MODULE, ?MODULE),
	zm_session:recv(Session, Src, Info, {Cmd, Msg}),
	receive
		{r_send, {_, {_, Values}} = R} -> 
			case if_value(Filter, Values) of
				true ->
					R;
				_ ->
					zm_log:warn(?MODULE, recv, recv, "Return value error!!!!!!", [
																					  {cmd, Cmd},
																					  {msg, Msg},
																					  {filter, Filter},
																					  {value, R}]),
					{error, R}
			end
	after ?DEF_TIMEOUT ->
			  zm_log:warn(?MODULE, recv, recv, "timeout!!!!!!!!!!!!!", [
																					  {cmd, Cmd},
																					  {msg, Msg},
																					  {timeout, ?DEF_TIMEOUT}]),
			  timeout
	end.

%% -----------------------------------------------------------------
%%@doc 向连接进程发送关闭消息
%% @spec close(Con::tuple(),Reason::term()) -> ok
%%@end
%% -----------------------------------------------------------------
close(#?MODULE{pid = Pid}, Reason) ->
	format("close, ~s:~s:~s,~s, ~p, reason:~p ~n", Pid, Reason).

% 当前时间
format(Text, Pid, Data) ->
	MS = z_lib:now_millisecond(),
	{_, {H, Mi, S}} = z_lib:second_to_localtime(MS div 1000),
	io:format(Text, [z_lib:integer_to_list(H, 2), z_lib:integer_to_list(Mi, 2),
		z_lib:integer_to_list(S, 2), z_lib:integer_to_list(MS rem 1000, 3), Pid, Data]),
	ok.

%%判断返回值
if_value([{K, V} | T], Value) ->
	case z_lib:get_value(Value, K, "") =:= V of
		true ->
			if_value(T, Value);
		_ ->
			false
	end;
if_value([], _) ->
	true.