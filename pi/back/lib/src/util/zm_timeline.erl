%%@doc 时间线模块
%%```
%%% 提供对数值进行线性插值或Hermite Curve插值，提供对单个或多个域的求值
%%'''
%%@end


-module(zm_timeline).

-description("timeline").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([calc_time/3, inter_linear/3, inter_hermite/8, linear/3, curve/3]).

%%%=======================INLINE=======================
-compile({inline, [calc_time/8, inter_catmull_rom/10, index1/2, index2/2, index_/4]}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  DataList长度必须大于1，根据数据计算数据时间及总时间
%%```
%%  Cycle表示是否循环
%%'''
%% @spec  calc_time(DataList, Fun, Cycle) -> return()
%% where
%%  return() =  {DataTimeList, ListLength, Last, TotalTime}
%%@end
%% -----------------------------------------------------------------
calc_time([H | [_ | _] = DataList], Fun, Cycle) ->
	calc_time(DataList, H, H, Fun, Cycle, 1, 0, []).

%% -----------------------------------------------------------------
%%@doc  线性计算插值，假设一条过v1、v2点的直线线，求其在t上的点，t范围是0到1
%% @spec  inter_linear(pos1, pos2, T) -> return()
%% where
%%  return() =  number() | {number(), number()} | {number(), number(), number()}
%%@end
%% -----------------------------------------------------------------
inter_linear({V11, V12, V13}, {V21, V22, V23}, T) ->
	{V11 + (V21 - V11) * T, V12 + (V22 - V12) * T, V13 + (V23 - V13) * T};
inter_linear({V11, V12}, {V21, V22}, T) ->
	{V11 + (V21 - V11) * T, V12 + (V22 - V12) * T};
inter_linear(V1, V2, T) ->
	V1 + (V2 - V1) * T.

%% -----------------------------------------------------------------
%%@doc  插值三次厄尔密样条，假设一条过v1、v2、v3、v4点的平滑曲线，T1表示V1-V2的时间，T2表示V2-V3的时间，T3表示V3-V4的时间，求其在T上的点，T为在V2-V3间的时间权重（范围是0到1）
%% @spec  inter_hermite(pos1, pos2, pos3, pos4, T1, T2, T3, T) -> return()
%% where
%%  return() =  number() | {number(), number()} | {number(), number(), number()}
%%@end
%% -----------------------------------------------------------------
inter_hermite({V11, V12, V13}, {V21, V22, V23}, {V31, V32, V33}, {V41, V42, V43}, T1, T2, T3, T) ->
	TT = T * T,
	TTT = T * TT,
	{inter_catmull_rom(V11, V21, V31, V41, T1, T2, T3, T, TT, TTT),
		inter_catmull_rom(V12, V22, V32, V42, T1, T2, T3, T, TT, TTT),
		inter_catmull_rom(V13, V23, V33, V43, T1, T2, T3, T, TT, TTT)};
inter_hermite({V11, V12}, {V21, V22}, {V31, V32}, {V41, V42}, T1, T2, T3, T) ->
	TT = T * T,
	TTT = T * TT,
	{inter_catmull_rom(V11, V21, V31, V41, T1, T2, T3, T, TT, TTT),
		inter_catmull_rom(V12, V22, V32, V42, T1, T2, T3, T, TT, TTT)};
inter_hermite(V1, V2, V3, V4, T1, T2, T3, T) ->
	inter_catmull_rom(V1, V2, V3, V4, T1, T2, T3, T, T * T, T * T * T).

%% -----------------------------------------------------------------
%%@doc  线性求值
%%```
%%  TimeLine为{DataTimeList, Length, Last, TotalTime}
%%'''
%% @spec  linear(TimeLine, CurrentTime, One::boolean()) -> return()
%% where
%%  return() = {number(), list(),integer()} | {{number(), number()}, list(), integer()} | {{number(), number(), number()}, list(), integer()} | {over, Data}
%%@end
%% -----------------------------------------------------------------
linear({_, _, _, _} = TimeLine, CurrentTime, true) when CurrentTime > 0 ->
	case index1(TimeLine, CurrentTime) of
		{_, {Data1, Time1} = H, [{Data2, _Time2} | _] = T, Time, UserTime} ->
			{inter_linear(Data1, Data2, Time / Time1), [H | T], UserTime};
		R ->
			R
	end;
linear({DataTimeList, _, _, _} = TimeLine, CurrentTime, _One) when CurrentTime > 0 ->
	case index2(TimeLine, CurrentTime) of
		{_, {Data1, Time1} = H, [{Data2, _Time2} | _] = T, Time, UserTime} ->
			{inter_linear(Data1, Data2, Time / Time1), [H | T], UserTime};
		{_, {Data1, Time1} = H, T, Time, UserTime} ->
			[{Data2, _Time2} | _] = DataTimeList,
			{inter_linear(Data1, Data2, Time / Time1), [H | T], UserTime}
	end;
linear({[{Data, _Time} | T], _, _, _}, _, _) ->
	{Data, T, 0}.

%% -----------------------------------------------------------------
%%@doc  曲线平滑求值
%%```
%%  TimeLine为{DataTimeList, Length, Last, TotalTime}
%%'''
%% @spec  curve(TimeLine, CurrentTime, One) -> return()
%% where
%%  return() = {number(), list(),integer()} | {{number(), number()}, list(), integer()} | {{number(), number(), number()}, list(), integer()} | {over, Data}
%%@end
%% -----------------------------------------------------------------
curve({_, 2, _, _} = TimeLine, CurrentTime, One) ->
	linear(TimeLine, CurrentTime, One);
curve({_, _, _, _} = TimeLine, CurrentTime, true) when CurrentTime > 0 ->
	case index1(TimeLine, CurrentTime) of
		{Last, Cur, L, Time, UserTime} ->
			{curve_point1(Last, Cur, L, Time), [Cur | L], UserTime};
		R ->
			R
	end;
curve({DataTimeList, _, _, _} = TimeLine, CurrentTime, _One) when CurrentTime > 0 ->
	{Last, Cur, L, Time, UserTime} = index2(TimeLine, CurrentTime),
	{curve_point2(Last, Cur, L, DataTimeList, Time), [Cur | L], UserTime};
curve({[{Data, _Time} | T], _, _, _}, _, _) ->
	{Data, T, 0}.

%%%===================LOCAL FUNCTIONS==================
% 计算总时间
calc_time([H2 | T], H1, First, Fun, Cycle, N, Total, L) ->
	Time = Fun(H1, H2),
	calc_time(T, H2, First, Fun, Cycle, N + 1, Total + Time, [{H1, Time} | L]);
calc_time([], Last, _First, _Fun, false, N, Total, L) ->
	H = {Last, 0},
	{lists:reverse([H | L]), N, H, Total};
calc_time([], Last, First, Fun, _Cycle, N, Total, L) ->
	Time = Fun(Last, First),
	H = {Last, Time},
	{lists:reverse([H | L]), N, H, Total + Time}.

% 插值Catmull-Rom样条
inter_catmull_rom(V1, V2, V3, V4, T1, T2, T3, T, TT, TTT) ->
	M1 = T2 * (V3 - V1) / (T1 + T2),
	M2 = T2 * (V4 - V2) / (T2 + T3),
	V2 + M1 * T + (-3 * V2 - 2 * M1 + 3 * V3 - M2) * TT + (2 * V2 + M1 - 2 * V3 + M2) * TTT.

% 单次计算，获得时间所在的时间数组中的位置
index1({[H | _] = DataTimeList, _Lenth, {Last, _}, TotalTime}, Time) ->
	if
		Time >= TotalTime ->
			{over, Last};
		true ->
			index_(DataTimeList, H, Time, 0)
	end.

% 循环计算，获得时间所在的时间数组中的位置
index2({DataTimeList, _Lenth, Last, TotalTime}, Time) ->
	if
		Time >= TotalTime ->
			index_(DataTimeList, Last, Time rem TotalTime, (Time div TotalTime) * TotalTime);
		true ->
			index_(DataTimeList, Last, Time, 0)
	end.

% 获得时间所在的时间数组中的位置
index_([{_Data, Time} = H | T], Last, CurTime, UserTime) ->
	if
		Time < CurTime ->
			index_(T, H, CurTime - Time, UserTime + Time);
		true ->
			{Last, H, T, CurTime, UserTime}
	end.

%单次计算，获得曲线点
curve_point1({Data1, Time1}, {Data2, Time2}, DataTimeL, Time) ->
	case DataTimeL of
		[{Data3, Time3}, {Data4, _Time4} | _] ->
			inter_hermite(Data1, Data2, Data3, Data4, Time1, Time2, Time3, Time / Time2);
		[{Data3, Time3}] ->
			inter_hermite(Data1, Data2, Data3, Data3, Time1, Time2, Time3, Time / Time2)
	end.

%循环计算，循环数据下，获得曲线点
curve_point2({Data1, Time1}, {Data2, Time2}, DataTimeL, DataTimeList, Time) ->
	case DataTimeL of
		[{Data3, Time3}, {Data4, _Time4} | _] ->
			inter_hermite(Data1, Data2, Data3, Data4, Time1, Time2, Time3, Time / Time2);
		[{Data3, Time3}] ->
			[{Data4, _Time4} | _] = DataTimeList,
			inter_hermite(Data1, Data2, Data3, Data4, Time1, Time2, Time3, Time / Time2);
		[] ->
			[{Data3, Time3}, {Data4, _Time4} | _] = DataTimeList,
			inter_hermite(Data1, Data2, Data3, Data4, Time1, Time2, Time3, Time / Time2)
	end.
