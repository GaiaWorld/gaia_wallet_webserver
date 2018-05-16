%%@doc 分布式定时执行器
%%```
%%%两类时间：基于秒的相对时间，比如每多少秒干什么。
%%%	基于日期的绝对时间，比如晚上八点干什么。
%%%相对时间：{{源, 名称（源内唯一）}, {M, F, A}, time, 循环时间（毫秒）, 超时（毫秒）}
%%%绝对时间：{{源, 名称（源内唯一）}, {M, F, A}, date, 日期, {时, 分, 秒}, 冲突（{类型, 等级（整数，最小为0）} | none）, 超时（毫秒）}
%%%冲突按照日来计算，如果当日中有同类型的定时事件，则只执行等级高（包括并列）。
%%%
%%%日期字段：
%%% year + month + day_of_month
%%% year + month + week_of_month + day_of_week
%%% year + day_of_year
%%% year + week_of_year + day_of_week
%%% 简写：year简写为y， month简写为m， day_of_month简写为d， week_of_month简写为w，
%%%	day_of_week简写为dw， day_of_year简写为dy， week_of_year简写为wy，
%%% 日期的列表表示为交集，日期字段的列表表示为并集
%%%
%%%例如：
%%% {d, 8}
%%% [{m, 8}, {d, [8, 12]}]
%%% [{m, 8}, {w, 1}, {dw, [6, 7]}]
%%% [{m, [8, 10]}, {w, [1, 2]}, {dw, [1, 2, 6, 7]}]
%%% [{m, 8}, {dw, [6, 7]}]
%%% [{y, 2010}, {dy, 108}]
%%% [{wy, [42, 43]}, {dw, 1}]
%%%
%%%时分秒字段：
%%%{any | integer() | [integer()...], any | integer() | [integer()...], any | integer() | [integer()...]}
%%%'''
%%%配置样例：
%% 注意：定时器调用的mfa中的方法必须是两参数的，一个是mfa中配置的a；另一个是定时器模块处理时传出的参数，详见handle方法
%%	{[], zm_dtimer, [
%%		{Project, CurProject},
%%		{app_announce, send, AnnounceTab},
%%		{time, 10000 , 10000}
%%	]}.
%%	{[calc], zm_dtimer, [
%%	{Project, notice},
%%	{app_announce, send, {AnnounceTab, RoleOnlineTab}},
%%	{date, any, {16,25, 0}, {area, 1}, 30000}
%%	]}.
%%@end

-module(zm_dtimer).

-description("distributed timer").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1]).
-export([day_of_year/1, day_of_year/3, week_of_year/1, week_of_year/3, week_of_month/1, week_of_month/3]).
-export([match_date/2, match_hms/2, match_number/2]).
-export([exist/1, get/2, set/3, test/3, unset/3, delete/2, handle/5]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(LOCAL, local).
-define(GLOBAL, global).

-define(TIMEOUT, 100).

%%%=======================RECORD=======================
-record(state, {type, ets, pids, second}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts a dist timer.
%% -----------------------------------------------------------------
start_link({?LOCAL, InitDelayTime} = Args) when is_integer(InitDelayTime) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []);
start_link({?GLOBAL, InitDelayTime} = Args) when is_integer(InitDelayTime) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% -----------------------------------------------------------------
%%@doc  获得指定的年月日所在年的日
%% @spec  day_of_year({Y, M, D}) -> return()
%% where
%%  return() =  1 | to | 366
%%@end
%% -----------------------------------------------------------------
day_of_year({Y, M, D}) ->
	day_of_year(Y, M, D).

%% -----------------------------------------------------------------
%%@doc  获得指定的年月日所在年的日
%% @spec  day_of_year(Y, M, D) -> return()
%% where
%%  return() =  1 | to | 366
%%@end
%% -----------------------------------------------------------------
day_of_year(Y, M, D) when M > 1 ->
	day_of_year(Y, M - 1, calendar:last_day_of_the_month(Y, M - 1) + D);
day_of_year(_Y, _, D) ->
	D.

%% -----------------------------------------------------------------
%%@doc  获得指定的年月日所在年的星期
%% @spec  week_of_year({Y, M, D}) -> return()
%% where
%%  return() =  1 | to | 54
%%@end
%% -----------------------------------------------------------------
week_of_year({Y, M, D}) ->
	week_of_year(Y, M, D).

%% -----------------------------------------------------------------
%%@doc  获得指定的年月日所在年的星期
%% @spec  week_of_year(Y, M, D) -> return()
%% where
%%  return() =  1 | to | 54
%%@end
%% -----------------------------------------------------------------
week_of_year(Y, M, D) ->
	FD = calendar:day_of_the_week(Y, 1, 1),
	((day_of_year(Y, M, D) - 1 + FD - 1) div 7) + 1.

%% -----------------------------------------------------------------
%%@doc  获得指定的年月日所在月的星期
%% @spec  week_of_month({Y, M, D}) -> return()
%% where
%%  return() =  1 | 2 | 3 | 4 | 5 | 6
%%@end
%% -----------------------------------------------------------------
week_of_month({Y, M, D}) ->
	week_of_month(Y, M, D).

%% -----------------------------------------------------------------
%%@doc  获得指定的年月日所在月的星期
%% @spec  week_of_month(Y, M, D) -> return()
%% where
%%  return() =  1 | 2 | 3 | 4 | 5 | 6
%%@end
%% -----------------------------------------------------------------
week_of_month(Y, M, D) ->
	FD = calendar:day_of_the_week(Y, M, 1),
	((D - 1 + FD - 1) div 7) + 1.

%% -----------------------------------------------------------------
%%@doc  检查指定的年月日是否匹配日期
%% @spec  match_date(Data::{Y, M, D}, TN::{Type, Number}) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
match_date({_Y, _M, _D} = Date, {_Type, _Number} = TN) ->
	match_date1(Date, TN);
match_date({_Y, _M, _D} = Date, L) when is_list(L) ->
	match_date2(Date, L);
match_date(_YMD, any) ->
	true;
match_date(_YMD, _Date) ->
	false.

%% -----------------------------------------------------------------
%%@doc  检查指定的时分秒是否匹配时间
%% @spec  match_hms({H, M, S}, {Hour, Minute, Second}) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
match_hms({H, M, S}, {Hour, Minute, Second}) ->
	case match_number(H, Hour) of
		true ->
			case match_number(M, Minute) of
				true ->
					match_number(S, Second);
				false ->
					false
			end;
		false ->
			false
	end;
match_hms({_H, _M, _S}, _Time) ->
	false.

%% -----------------------------------------------------------------
%%@doc  匹配指定的数字
%% @spec  match_number(N, L) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
match_number(N, L) when is_list(L) ->
	lists:member(N, L);
match_number(_N1, any) ->
	true;
match_number(N1, N2) ->
	N1 =:= N2.

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
%%@doc get handler
%%```
%%  Src:源
%%  Type:类型
%%'''
%% @spec get(Src::atom(),Type::atom()) -> return()
%% where
%%      return() = none | {Type::{Src::atom(),Type::atom()}, {M::atom() ,F::atom(), A::term(), Opt::tuple()}}
%%@end
%% -----------------------------------------------------------------
get(Src, Type) when is_atom(Src) ->
	case ets:lookup(?MODULE, {Src, Type}) of
		[T] -> T;
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc 设置定时事件处理器
%% @spec set(Type::{Src::atom(),Type::atom()},MFA::{M::atom(),F::atom(),A::term()},Date::date()) -> ok
%% where
%%      date() = {time, Cycle::integer(), Timeout::integer()}|
%%              {date, Date::tuple()|list(), {H::integer(), M::integer(), S::integer()}, Conflict::tuple(), Timeout::integer()}
%%@end
%% -----------------------------------------------------------------
set({Src, _Type} = Type, {M, F, _A} = MFA, {time, Cycle, Timeout}) when is_atom(Src),
	is_atom(M), is_atom(F), is_integer(Cycle), is_integer(Timeout) ->
	gen_server:call(?MODULE, {set, Type, MFA,  {time, Cycle, Timeout, 0, 0}});
set({Src, _Type} = Type, {M, F, _A} = MFA, {date, _Date, {_H, _M, _S}, _Conflict, Timeout} = Date)
	when is_atom(Src), is_atom(M),  is_atom(F), is_integer(Timeout) ->
	gen_server:call(?MODULE, {set, Type, MFA, Date}).

%% -----------------------------------------------------------------
%%@doc get handler
%%```
%%  Src:源
%%  Type:类型
%%'''
%% @spec test(Src::atom(),Type::atom(), TimeOrDate::integer() | {{integer(), integer(), integer()}, {integer(), integer(), integer()}}) -> return()
%% where
%%      return() = none | {Type::{Src::atom(),Type::atom()}, {M::atom() ,F::atom(), A::term(), Opt::tuple()}}
%%@end
%% -----------------------------------------------------------------
test(Src, Type, TimeOrDate) when is_atom(Src) ->
	case ets:lookup(?MODULE, {Src, Type}) of
		[{_SrcType, {M, F, A}, _TimeDate}] ->
			M:F(A, TimeOrDate);
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc 删除定时事件处理器
%% @spec unset(Type::{Src::atom(),Type::atom()},any(),any()) -> ok
%%@end
%% -----------------------------------------------------------------
unset(Type, _, _) ->
	gen_server:call(?MODULE, {delete, Type}).

%% -----------------------------------------------------------------
%%@doc 删除定时事件处理器
%% @spec delete(Src::atom(),Type::atom()) -> return()
%% where
%%      return() = none | ok
%%@end
%% -----------------------------------------------------------------
delete(Src, Type) when is_atom(Src) ->
	gen_server:call(?MODULE, {delete, {Src, Type}}).

%% -----------------------------------------------------------------
%%@doc 事件处理函数
%% @spec  handle(SrcType::{Src, Type}, MFA, TimeDate, Expire, Parent) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
handle({Src, Type} = SrcType, {M, F, A} = MFA, TimeDate, Expire, Parent) ->
	put('$log', {Src, Type}),
	try
		R = M:F(A, TimeDate),
		zm_log:debug(?MODULE, handle, Src, Type,
			[{mfa, MFA}, {timedate, TimeDate}, {result, R}])
	catch
		Error:Why ->
			zm_log:warn(?MODULE, {handle_error, Error}, Src, Type,
				[{mfa, MFA}, {timedate, TimeDate},
				{error, Why}, {stacktrace, erlang:get_stacktrace()}])
	end,
	if
		is_pid(Parent) ->
			gen_server:cast(Parent, {handled, SrcType, Expire});
		true ->
			ok
	end.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Type, InitDelayTime}) ->
	[zm_service:set(?MODULE, running) || Type =/= ?LOCAL],
	if
		InitDelayTime > ?TIMEOUT ->
			erlang:start_timer(InitDelayTime, self(), none);
		true ->
			erlang:start_timer(?TIMEOUT, self(), none)
	end,
	{ok, #state{type = Type, ets = ets:new(?MODULE, [set, protected, named_table]),
		pids = [], second = zm_time:now_second()}}.

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
handle_call({set, SrcType, MFA, TimeDate}, _From, #state{ets = Ets} = State) ->
	ets:insert(Ets, {SrcType, MFA, TimeDate}),
	{reply, ok, State};

handle_call({delete, SrcType}, _From, #state{ets = Ets} = State) ->
	ets:delete(Ets, SrcType),
	{reply, ok, State};

handle_call(_Request, _From, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({handle, SrcType, MFA,
	TimeDate, Timeout, Expire, Parent}, #state{pids = L} = State) ->
	{noreply, State#state{pids = remote_handle(
		SrcType, MFA, TimeDate, Timeout, Expire, Parent, L)}};
handle_cast({handled, SrcType, Expire}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, SrcType) of
		[{_, MFA, {time, Cycle, Timeout, NextTime, Expire}}] ->
			ets:insert(Ets, {SrcType, MFA, {time, Cycle, Timeout, NextTime, 0}}),
			{noreply, State};
		_ ->
			{noreply, State}
	end;
handle_cast(_Msg, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({timeout, _Ref, none}, #state{type = Type,
	ets = Ets, pids = L, second = Second} = State) ->
	Now = zm_time:now_millisecond(),
	LL = [E || E <- L, pid_expire(E, Now)],
	N = node(),
	case list_node(Type) of
		[{N, _} | _] = NAL ->
			NL = z_lib:shuffle([Node || {Node, _} <- NAL], Now),
			case Now div 1000 of
				Sec when Sec > Second ->
					TL = get_time_list(Second + 1, Sec, []),
					{_, NP, _, Tree} = z_lib:ets_select(Ets,
						fun run_timedate/2, {Now, {N, NL, NL, LL},
						TL, sb_trees:empty()}),
					{_, _, _, List} = handle_conflict(
						Now, NP, TL, sb_trees:iterator(Tree)),
					erlang:start_timer(?TIMEOUT, self(), none),
					{noreply, #state{type = Type,
						ets = Ets, pids = List, second = Sec}};
				_ ->
					{_, {_, _, _, List}, _, _} = z_lib:ets_select(Ets,
						fun run_time/2, {Now, {N, NL, NL, LL}, [], none}),
					erlang:start_timer(?TIMEOUT, self(), none),
					{noreply, State#state{pids = List}}
			end;
		_ ->
			erlang:start_timer(?TIMEOUT, self(), none),
			{noreply, State#state{pids = LL}}
	end;

handle_info(_Info, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(_Reason, State) ->
	State.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
% 匹配指定类型的日期
match_date1({Y, _M, _D}, {y, List}) when is_list(List) ->
	lists:member(Y, List);
match_date1({Y, _M, _D}, {y, Year}) ->
	Y =:= Year;

match_date1({_Y, M, _D}, {m, List}) when is_list(List) ->
	lists:member(M, List);
match_date1({_Y, M, _D}, {m, Month}) ->
	M =:= Month;

match_date1({_Y, _M, D}, {d, List}) when is_list(List) ->
	lists:member(D, List);
match_date1({_Y, _M, D}, {d, Day}) ->
	D =:= Day;

match_date1({Y, M, D}, {w, List}) when is_list(List) ->
	lists:member(week_of_month(Y, M, D), List);
match_date1({Y, M, D}, {w, W}) ->
	week_of_month(Y, M, D) =:= W;

match_date1({Y, M, D}, {dw, List}) when is_list(List) ->
	lists:member(calendar:day_of_the_week(Y, M, D), List);
match_date1({Y, M, D}, {dw, Day}) ->
	calendar:day_of_the_week(Y, M, D) =:= Day;

match_date1({Y, M, D}, {wy, List}) when is_list(List) ->
	lists:member(week_of_year(Y, M, D), List);
match_date1({Y, M, D}, {wy, W}) ->
	week_of_year(Y, M, D) =:= W;

match_date1({Y, M, D}, {dy, List}) when is_list(List) ->
	lists:member(day_of_year(Y, M, D), List);
match_date1({Y, M, D}, {dy, Day}) ->
	day_of_year(Y, M, D) =:= Day.

% 匹配指定的日期列表
match_date2({_Y, _M, _D} = Date, [H | T]) ->
	case match_date1(Date, H) of
		true ->
			match_date2(Date, T);
		false ->
			false
	end;
match_date2({_Y, _M, _D}, []) ->
	true.

% 执行进程
remote_handle({Src, Type} = SrcType, MFA, TimeDate, Timeout, Expire, Parent, L) ->
	[{spawn(?MODULE, handle, [SrcType, MFA, TimeDate, Expire, Parent]),
		zm_time:now_millisecond() + Timeout - 1, MFA, TimeDate, Src, Type} | L].

% 本地执行进程
local_handle({Src, Type} = SrcType, MFA, TimeDate, Expire, Parent, L) ->
	[{spawn(?MODULE, handle, [SrcType, MFA, TimeDate, Expire, Parent]),
		Expire, MFA, TimeDate, Src, Type} | L].

% 执行进程超期
pid_expire({Pid, Expire, MFA, TimeDate, Src, Type}, Time) ->
	case is_process_alive(Pid) of
		true when Time > Expire ->
			kill_pid(Pid, MFA, TimeDate, Src, Type),
			false;
		R ->
			R
	end.

% 杀掉指定进程
kill_pid(Pid, MFA, TimeDate, Src, Type) ->
	CurFun = process_info(Pid, current_function),
	exit(Pid, kill),
	zm_log:warn(?MODULE, kill_pid, Src, Type,
		[{pid, Pid}, {mfa, MFA}, {timedate, TimeDate}, {cur_fun, CurFun}]).

% 列出可用节点
list_node(?LOCAL) ->
	[{node(), local_running}];
list_node(_) ->
	% TODO 增加zm_node的过滤
	zm_service:get(?MODULE).

% 获得前后时间差对应的本地时间列表
get_time_list(Start, End, L) when Start =< End ->
	get_time_list(Start + 1, End, [calendar:now_to_local_time(
		{Start div 1000000, Start rem 1000000, 0}) | L]);
get_time_list(_Start, _End, L) ->
	L.

% 执行定时动作
run_timedate(Args, {_SrcType, _MFA, {time, _Cycle, _Timeout, _NextTime, _Expire}} = A) ->
	run_time(Args, A);
run_timedate(Args, {_SrcType, _MFA, {date, _Date, _Time, _Conflict, _Timeout}} = A) ->
	run_date(Args, A);
run_timedate(Args, _) ->
	{ok, Args}.

% 执行相对定时动作
run_time({Now, _, _, _} = Args, {SrcType, MFA, {time, Cycle, Timeout, 0, _Expire}}) ->
	{update, Args, {SrcType, MFA, {time, Cycle, Timeout, Now + Cycle, 0}}};
run_time({Now, _, _, _} = Args, {_, _, {time, _, _, NextTime, 0}}) when Now < NextTime ->
	{ok, Args};
run_time({Now, NP, TL, Tree}, {SrcType, MFA, {time, Cycle, Timeout, _, 0}}) ->
	{update, {Now, random_node(Now, NP, SrcType, MFA, Timeout), TL, Tree},
		{SrcType, MFA, {time, Cycle, Timeout, Now + Cycle, Now + Timeout}}};
run_time({Now, NP, TL, Tree}, {SrcType, MFA,
	{time, Cycle, Timeout, _NextTime, Expire}}) when Now > Expire ->
	{update, {Now, random_node(Now, NP, SrcType, MFA, Timeout), TL, Tree},
		{SrcType, MFA, {time, Cycle, Timeout, Now + Cycle, Now + Timeout}}};
run_time(Args, _) ->
	{ok, Args}.

% 执行相对定时动作
run_date({Now, NP, TL, Tree}, {{Src, _} = SrcType, MFA,
	{date, Date, Time, {ConflictType, Level}, Timeout}}) ->
		K = {Src, ConflictType},
		V = {Level, SrcType, MFA, Date, Time, Timeout},
		L = sb_trees:get(K, Tree, []),
		{ok, {Now, NP, TL, sb_trees:enter(K, [V | L], Tree)}};
run_date({Now, NP, TL, Tree} = Args, {SrcType, MFA,
	{date, Date, Time, _Conflict, Timeout}}) ->
	case match_time(Date, Time, TL) of
		none ->
			{ok, Args};
		DateTime ->
			{ok, {Now, random_node(Now, NP,
				SrcType, MFA, Timeout, DateTime), TL, Tree}}
	end;
run_date(Args, _) ->
	{ok, Args}.

% 匹配定时事件
match_time(Date, Time, [{YMD, HMS} = H | T]) ->
	case match_date(YMD, Date) of
		true ->
			case match_hms(HMS, Time) of
				true ->
					H;
				false ->
					match_time(Date, Time, T)
			end;
		false ->
			match_time(Date, Time, T)
	end;
match_time(_Date, _Time, []) ->
	none.

% 执行冲突类型的定时动作
handle_conflict(Now, NP, TL, Iter) ->
	case sb_trees:next(Iter) of
		{_, L, I} ->
			handle_conflict(Now, time_conflict(
				Now, NP, TL, lists:reverse(lists:sort(L))), TL, I);
		none ->
			NP
	end.

% 指定时间列表和指定冲突类型的定时动作
time_conflict(Now, NP, [{YMD, HMS} = H | T], L) ->
	time_conflict(Now, run_conflict(Now, H, NP,
		match_conflict(YMD, HMS, L, 0, [])), T, L);
time_conflict(_Now, NP, [], _L) ->
	NP.

% 执行冲突类型的定时动作
run_conflict(Now, DateTime, NP, [{_, SrcType, MFA, _, _, Timeout} | T]) ->
	run_conflict(Now, DateTime, random_node(
		Now, NP, SrcType, MFA, Timeout, DateTime), T);
run_conflict(_Now, _DateTime, NP, []) ->
	NP.

% 匹配指定时间的冲突类型的定时事件列表
match_conflict(YMD, HMS, [{Level, _, _, Date, Time, _} = H | T],
	Last, L) when Level >= Last ->
	case match_date(YMD, Date) of
		true ->
			case match_hms(HMS, Time) of
				true ->
					match_conflict(YMD, HMS, T, Level, [H | L]);
				false ->
					match_conflict(YMD, HMS, T, Level, L)
			end;
		false ->
			match_conflict(YMD, HMS, T, Last, L)
	end;
match_conflict(_YMD, _HMS, _, _, L) ->
	L.

% 在随机节点上执行相对定时动作
random_node(Now, {N, [N | T] = NL, [], L}, SrcType, MFA, Timeout) ->
	{N, NL, T, local_handle(SrcType, MFA, Now, Now + Timeout, self(), L)};
random_node(Now, {N, NL, [N | T], L}, SrcType, MFA, Timeout) ->
	{N, NL, T, local_handle(SrcType, MFA, Now, Now + Timeout, self(), L)};
random_node(Now, {N, [H | T] = NL, [], L}, SrcType, MFA, Timeout) ->
	gen_server:cast({?MODULE, H},
		{handle, SrcType, MFA, Now, Timeout, Now + Timeout, self()}),
	{N, NL, T, L};
random_node(Now, {N, NL, [H | T], L}, SrcType, MFA, Timeout) ->
	gen_server:cast({?MODULE, H},
		{handle, SrcType, MFA, Now, Timeout, Now + Timeout, self()}),
	{N, NL, T, L}.

% 在随机节点上执行绝对定时动作
random_node(Now, {N, [N | T] = NL, [], L}, SrcType, MFA, Timeout, Date) ->
	{N, NL, T, local_handle(SrcType, MFA, Date, Now + Timeout, none, L)};
random_node(Now, {N, NL, [N | T], L}, SrcType, MFA, Timeout, Date) ->
	{N, NL, T, local_handle(SrcType, MFA, Date, Now + Timeout, none, L)};
random_node(Now, {N, [H | T] = NL, [], L}, SrcType, MFA, Timeout, Date) ->
	gen_server:cast({?MODULE, H},
		{handle, SrcType, MFA, Date, Timeout, Now + Timeout, none}),
	{N, NL, T, L};
random_node(Now, {N, NL, [H | T], L}, SrcType, MFA, Timeout, Date) ->
	gen_server:cast({?MODULE, H},
		{handle, SrcType, MFA, Date, Timeout, Now + Timeout, none}),
	{N, NL, T, L}.
