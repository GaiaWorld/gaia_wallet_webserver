%%%@doc 基于空间分割和A*算法的寻路模块
%%```
%%'''
%%@end
%%% 寻路模块


-module(zm_astar).

-description("pathfind by astar").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([find/11, reset/6, distance/2, in_door/8]).

%%%=======================INLINE=======================
-compile({inline, []}).

%%%=======================DEFINE=======================
-define(OPTIMIZATION_ANGLE, 0.09).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% @doc 寻路方法
%%```
%% Space, 为空间模块数据
%% Start, Target, 为起始点和目标点，每都是double元组{x, y, z}
%% BodyType, 为半体型（x, y, z），如果体型小于空间的宽高，则该空间可进入
%% TargetResetType, 如果目标点为无效的，搜索附近有效点，1表示直线搜索，0表示寻找最近可走点，-1表示不搜索
%% Limit, 为搜索限制，防止搜索过长，一般为100
%% LimitNear, 如果搜索限制，是否返回一条最靠近目标的路径
%% MoveFun, 为空间移动函数，如果返回true，则该空间可进入
%% JudgeCostFun, 预估两点之间移动开销的估价函数
%% MoveCostFun, 计算向指定点移动的开销
%% InDoorFun, 判断物体能否通过两个空间之间的门，如果不能返回false，如果可以返回一个节点，上面必须有过门时的点point，一般该函数使用本模块的in_door
%% 返回error表示错误，返回false表示搜索不到路径，否则返回路径列表
%%'''
%% @spec find(Space::space(), Start::{float(), float(), float()}, Target::{float(), float(), float()},BodyType::{float(), float(), float()}, TargetResetType::2 | 0 | -1, Limit::integer(), LimitNear::true | false, MoveFun::function(), JudgeCostFun::function(), MoveCostFun::function(), InDoorFun::function()) -> return()
%% where
%%  return() =  list() | false | {error, Reason}
%%@end
%% -----------------------------------------------------------------
find(Space, Start, Target, BodyType, TargetResetType,
	Limit, LimitNear, MoveFun, JudgeCostFun, MoveCostFun, InDoorFun) ->
	try
		find_path(Space, Start, Target, BodyType, TargetResetType,
			Limit, LimitNear, MoveFun, JudgeCostFun, MoveCostFun, InDoorFun)
	catch
		_:Reason ->
			{error, Reason, erlang:get_stacktrace()}
	after
		[K || {K, _V} <- erlang:get(), case K of {?MODULE, _, _} -> erlang:erase(K), false; _ -> false end]
	end.

%% -----------------------------------------------------------------
%% @doc 检查目标点是否有效，搜索附近有效点，1表示直线搜索，0表示寻找最近可走点，-1表示不搜索
%% @spec reset(Space::space(), Start::{float(), float(), float()}, Target::{float(), float(), float()},
%%	 TargetResetType::1 | 0 | -1, MoveFun::function(), MoveCostFun::function()) -> return()
%% where
%%  return() =  {Node::space_node(), Point::{float(), float(), float()}} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
reset(Space, Start, Target, TargetResetType, MoveFun, MoveCostFun) ->
	try
		reset_point(Space, Start, Target, TargetResetType, MoveFun, MoveCostFun)
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%% @doc 计算两点间的距离
%% @spec distance(Start::{float(), float(), float()}, Target::{float(), float(), float()}) -> float()
%%@end
%% -----------------------------------------------------------------
distance({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	math:sqrt((X2 - X1) * (X2 - X1) + (Y2 - Y1) * (Y2 - Y1) + (Z2 - Z1) * (Z2 - Z1)).

%% -----------------------------------------------------------------
%% @doc 进门函数
%% @spec in_door(Split::integer(), BodyType::{float(), float(), float()}, Min::{float(), float(), float()}, Max::{float(), float(), float()}, Point::{float(), float(), float()}, Target::{float(), float(), float()}, SrcNodeNode::space_node(), DestSpaceNode::space_node()) -> float()
%% where
%%  return() =  false | Point
%%@end
%% -----------------------------------------------------------------
in_door(1, {BodyTypeX, BodyTypeY, BodyTypeZ}, {MinX, MinY, MinZ},
	{MaxX, MaxY, MaxZ}, {StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}, _SrcNodeNode, _DestSpaceNode) ->
	if
		BodyTypeZ > MaxZ - MinZ ->
			%如果是x,y轴分割的门，不容许物体低头钻过
			false;
		BodyTypeY > MaxY - MinY andalso BodyTypeX > MaxY - MinY ->
			%物体侧身也无法通过
			false;
		true ->
			case zm_space:cross_x_door(BodyTypeX, BodyTypeY, BodyTypeZ,
				MinX, MinY, MinZ, MaxX, MaxY, MaxZ,
				StartX, StartY, StartZ, TargetX, TargetY, TargetZ) of
				{_, P} -> P;
				P -> P
			end
	end;
in_door(2, {BodyTypeX, BodyTypeY, BodyTypeZ}, {MinX, MinY, MinZ},
	{MaxX, MaxY, MaxZ}, {StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}, _SrcNode, _DestSpaceNode) ->
	if
		BodyTypeZ > MaxZ - MinZ ->
			%如果是x,y轴分割的门，不容许物体低头钻过
			false;
		BodyTypeX > MaxX - MinX andalso BodyTypeY > MaxX - MinX ->
			% 物体侧身也无法通过
			false;
		true ->
			case zm_space:cross_y_door(BodyTypeX, BodyTypeY, BodyTypeZ,
				MinX, MinY, MinZ, MaxX, MaxY, MaxZ,
				StartX, StartY, StartZ, TargetX, TargetY, TargetZ) of
				{_, P} -> P;
				P -> P
			end
	end;
in_door(3, {BodyTypeX, BodyTypeY, BodyTypeZ}, {MinX, MinY, MinZ},
	{MaxX, MaxY, MaxZ}, {StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}, _SrcNode, _DestSpaceNode) ->
	if
		(BodyTypeX > MaxX - MinX orelse BodyTypeY > MaxY - MinY) andalso
			(BodyTypeY > MaxX - MinX orelse BodyTypeX > MaxY - MinY) ->
			%z轴分割的门，检查能否侧身通过
			false;
		true ->
			case zm_space:cross_z_door(BodyTypeX, BodyTypeY, BodyTypeZ,
				MinX, MinY, MinZ, MaxX, MaxY, MaxZ,
				StartX, StartY, StartZ, TargetX, TargetY, TargetZ) of
				{_, P} -> P;
				P -> P
			end
	end.

%%%===================LOCAL FUNCTIONS==================
% 重置指定的点
reset_point(Space, Start, Target, TargetResetType, MoveFun, MoveCostFun) ->
	Node = zm_space:query_point(Space, Target, Start),
	case MoveFun(Node) of
		true ->
			{Node, Target};
		false when TargetResetType =:= 1 ->
			Fun = fun(_, CurNode, Point) ->
				case MoveFun(CurNode) of
					true ->
						{result, {CurNode, Point}};
					_ ->
						Point
				end
			end,
			case zm_space:query_line(Space, Target, Node, Start, Fun, Target) of
				{ok, RNode, Point} -> {RNode, Point};
				{result, R} -> R
			end;
		false when TargetResetType =:= 0 ->
			{TID, _, _, _, _} = Node,
			Heap = case target_near(Space, MoveFun, MoveCostFun, -1, 
				[{0, Node, Target}], gb_sets:empty(), []) of
				{[], Heap1} ->
					Heap1;
				{L1, Heap1} ->
					%最多寻找2层，就必然有可走区域
					{_Temp, Heap2} = target_near(Space,
						MoveFun, MoveCostFun, TID, L1, Heap1, []),
						Heap2
			end,
			case Heap of
				{Size, _} when Size > 0 ->
					{0, N, Point} = gb_sets:take_smallest(Heap),
					{N, Point};
				_ ->
					{error, near_point_not_found}
			end;
		_ ->
			{error, {invalid_target, Node}}
	end.

% 遍历不可走的空间列表，将可走空间及点放入到堆中，将不可走的空间及点放到列表中，
target_near(Space, MoveFun, MoveCostFun, PID, [{Cost, {_, _, _, Link, _}, Point} | T], Heap, L) ->
	target_near(Space, MoveFun, MoveCostFun, PID, T, Heap, Cost, Point, Link, L);
target_near(_Space, _MoveFun, _MoveCostFun, _PID, _, Heap, L) ->
	{L, Heap}.

% 遍历空间关联列表，将可走空间及点放入到堆中，将不可走的空间及点放到列表中，
target_near(Space, MoveFun, MoveCostFun, PID, NextL, Heap, Cost, Point, [{PID, _Min, _Max, _Split} | T], L) ->
	target_near(Space, MoveFun, MoveCostFun, PID, NextL, Heap, Cost, Point, T, L);
target_near(Space, MoveFun, MoveCostFun, PID, NextL, Heap, Cost, Point, [{ID, Min, Max, Split} | T], L) ->
	Near = get_near(Split, Min, Max, Point),
	SpaceNode = zm_space:get(Space, ID),
	Node = {Cost + MoveCostFun(Point, Near), SpaceNode, Near},
	case MoveFun(SpaceNode) of
		true ->
			target_near(Space, MoveFun, MoveCostFun, PID, NextL,
				gb_sets:add(Node, Heap), Cost, Point, T, L);
		_ ->
			target_near(Space, MoveFun, MoveCostFun, PID, NextL,
				Heap, Cost, Point, T, [Node | L])
	end;
target_near(Space, MoveFun, MoveCostFun, PID, NextL, Heap, _Cost, _Point, _, L) ->
	target_near(Space, MoveFun, MoveCostFun, PID, NextL, Heap, L).

%获得离指定点最近的横截面上的一个点
get_near(1, {MinX, MinY, MinZ}, {_MaxX, MaxY, MaxZ}, {_X, Y, Z}) ->
	Y1 = if
		MinY > Y -> MinY;
		MaxY < Y -> MaxY;
		true -> Y
	end,
	Z1 = if
		MinZ > Z -> MinZ;
		MaxZ < Z -> MaxZ;
		true -> Z
	end,
	{MinX, Y1, Z1};
get_near(2, {MinX, MinY, MinZ}, {MaxX, _MaxY, MaxZ}, {X, _Y, Z}) ->
	X1 = if
		MinX > X -> MinX;
		MaxX < X -> MaxX;
		true -> X
	end,
	Z1 = if
		MinZ > Z -> MinZ;
		MaxZ < Z -> MaxZ;
		true -> Z
	end,
	{X1, MinY, Z1};
get_near(3, {MinX, MinY, MinZ}, {MaxX, MaxY, _MaxZ}, {X, Y, _Z}) ->
	X1 = if
		MinX > X -> MinX;
		MaxX < X -> MaxX;
		true -> X
	end,
	Y1 = if
		MinY > Y -> MinY;
		MaxY < Y -> MaxY;
		true -> Y
	end,
	{X1, Y1, MinZ}.

% 路径搜索
find_path(Space, Start, Target, BodyType, TargetResetType,
	Limit, LimitNear, MoveFun, JudgeCostFun, MoveCostFun, InDoorFun) ->
	{SID, _, _, Link, _} = SrcNode = zm_space:query_point(Space, Start, Target),
	[erlang:error({invalid_start, SrcNode}) || MoveFun(SrcNode) =/= true],
	case reset(Space, Start, Target,
		TargetResetType, MoveFun, MoveCostFun) of
		{{SID, _, _, _, _}, Point} ->
			% 相同空间
			[Start, Point];
		{{TID, _, _, _, _}, Point} ->
			space_door(gb_sets:empty(), Space, Start,
				TID, Point, BodyType, Limit - 1, LimitNear,
				MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
				0, SID, -1, Start, SrcNode, Link);
		E ->
			E
	end.

% A*搜索
astar({Size, _} = Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
	MoveFun, JudgeCostFun, MoveCostFun, InDoorFun) when Size > 0, Limit > 0 ->
	{{_Cost, MoveCost, _, ID, PID, _PPID, Point, _Door, {_, _, _, Link, _} = SrcNode}, Heap1} = gb_sets:take_smallest(Heap),
	space_door(Heap1, Space, Start, TID, Target, BodyType, Limit - 1, LimitNear,
		MoveFun, JudgeCostFun, MoveCostFun, InDoorFun, MoveCost, ID, PID, Point, SrcNode, Link);
astar(_Heap, _Space, _Start, _TID, _Target, _BodyType, Limit, _LimitNear,
	_MoveFun, _JudgeCostFun, _MoveCostFun, _InDoorFun) when Limit > 0 ->
	{error, space_unreachable};
astar(_Heap, _Space, Start, _TID, _Target, BodyType, _Limit, true,
	_MoveFun, _JudgeCostFun, _MoveCostFun, _InDoorFun) ->
	find_near(erlang:get(), Start, BodyType, none, 16#ffffffff);
astar(_Heap, _Space, _Start, _TID, _Target, _BodyType, _Limit, _LimitNear,
	_MoveFun, _JudgeCostFun, _MoveCostFun, _InDoorFun) ->
	{error, space_not_found}.

% 搜索空间门
space_door(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
	MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
	MoveCost, ID, PID, Point, SrcNode, [{PID, _Min, _Max, _Split} | T]) ->
	space_door(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
		MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
		MoveCost, ID, PID, Point, SrcNode, T);
space_door(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
	MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
	MoveCost, ID, PID, Point, SrcNode, [{ID1, Min, Max, Split} = Door | T]) ->
	SpaceNode = zm_space:get(Space, ID1),
	case check_space_node(MoveFun, SpaceNode, ID1, ID, MoveCost) of
		true ->
			case InDoorFun(Split, BodyType, Min, Max, Point, Target, SrcNode, SpaceNode) of
				Point1 when is_tuple(Point1) ->
					if
						TID =:= ID1 ->
							backtracking(Start, Target, BodyType, ID, PID, [Door]);
						true ->
							space_door(cost_space_node(Heap, Target, JudgeCostFun, MoveCostFun,
								MoveCost, ID, PID, Point, ID1, Point1, Door, SpaceNode),
								Space, Start, TID, Target, BodyType, Limit, LimitNear,
								MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
								MoveCost, ID, PID, Point, SrcNode, T)
					end;
				false ->
					space_door(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
						MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
						MoveCost, ID, PID, Point, SrcNode, T)
			end;
		false ->
			space_door(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
				MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
				MoveCost, ID, PID, Point, SrcNode, T)
	end;
space_door(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
	MoveFun, JudgeCostFun, MoveCostFun, InDoorFun,
	_MoveCost, _ID, _PID, _Point, _SrcNode, _) ->
	astar(Heap, Space, Start, TID, Target, BodyType, Limit, LimitNear,
		MoveFun, JudgeCostFun, MoveCostFun, InDoorFun).

% 检查空间是否可走，如果这2个空间的已有的最小移动距离大于该节点已经移动的距离，则可走
check_space_node(MoveFun, Space, ID, PID, MoveCost) ->
	case MoveFun(Space) of
		true ->
			case get({?MODULE, PID, ID}) of
				{_, Cost, _, _, _, _, _, _, _} -> Cost > MoveCost;
				_ -> true
			end;
		_ ->
			false
	end.

% 计算移动距离和预判距离，如果这2个空间节点的上次进入的距离大，则替换成本次的数据
cost_space_node(Heap, Target, JudgeCostFun, MoveCostFun,
	MoveCost, PID, PPID, PPoint, ID, Point, Door, SpaceNode) ->
	Cost1 = MoveCost + MoveCostFun(PPoint, Point),
	Cost2 = JudgeCostFun(Point, Target),
	Cost = Cost1 + Cost2,
	Key = {?MODULE, ID, PID},
	case get(Key) of
		{LastCost, _, _, _, _, _, _, _, _} when LastCost < Cost ->
			Heap;
		_ ->
			N = {Cost, Cost1, Cost2, ID, PID, PPID, Point, Door, SpaceNode},
			put(Key, N),
			gb_sets:add(N, Heap)
	end.

%回溯路径
backtracking(Start, Target, BodyType, ID, PID, L) ->
	case erase({?MODULE, ID, PID}) of
		{_, _, _, _, _, PPID, _Point, Door, _SpaceNode} ->
			backtracking(Start, Target, BodyType, PID, PPID, [Door | L]);
		_ ->
			case optimization_path(Start, Target, BodyType, L, []) of
				[{P, Split} | T] ->
					[Start | optimization_angle(BodyType, Start, P, Split, T, Target, [])];
				_ ->
					[Start, Target]
			end
	end.

%优化路径
optimization_path(Start, Target, BodyType, Door, L) ->
	case optimization_path(Start, Target, BodyType, Door, Door, 1, 0.0, none, 0) of
		{Point, {_ID, _, _, Split}, Door1, Door2} ->
			optimization_path(Start, Point, BodyType, Door1,
				[{Point, Split} | optimization_path(Point, Target, BodyType, Door2, L)]);
		_ ->
			L
	end.

%计算起点、终点的连线，中间门上距离最远的点
optimization_path(Start, Target, BodyType, L,
	[{_ID, Min, Max, Split} | T], N, LastDist, LastPoint, LastN) ->
	case zm_space:cross_door(Split, BodyType, Min, Max, Start, Target) of
		{_, Point} ->
			case z_math:pl_distance(Point, Start, Target) of
				Dist when Dist > LastDist ->
					optimization_path(Start, Target, BodyType, L,
						T, N + 1, Dist, Point, N);
				_ ->
					optimization_path(Start, Target, BodyType, L,
						T, N + 1, LastDist, LastPoint, LastN)
			end;
		_ ->
			optimization_path(Start, Target, BodyType, L, T, N + 1, LastDist, LastPoint, LastN)
	end;
optimization_path(_Start, _Target, _BodyType, L, _, _, _, LastPoint, LastN) when LastN > 0 ->
	optimization_path_split(LastPoint, LastN, L, []);
optimization_path(_Start, _Target, _BodyType, _L, _, _N, _LastDist, _LastPoint, _LastN) ->
	false.

%取到计算出的最远点及两端的门列表
optimization_path_split(Point, N, [H | T], L) when N > 1 ->
	optimization_path_split(Point, N - 1, T, [H | L]);
optimization_path_split(Point, _, [H | T], L) ->
	{Point, H, lists:reverse(L), T}.

%优化夹角，计算start, target对point上门的夹角，如果有一方太小，则向小的一方靠拢
optimization_angle(BodyType, Start, Point, Split, [{P1, S1} | T], Target, L) ->
	D1 = vec3_angle_cos(Split, Point, Start),
	D2 = vec3_angle_cos(Split, Point, P1),
	P = if
		D1 >= ?OPTIMIZATION_ANGLE andalso D2 >= ?OPTIMIZATION_ANGLE ->
			Point;
		true ->
			get_deviation(Split, BodyType, D1, D2, Start, Point, P1)
	end,
	optimization_angle(BodyType, Point, P1, S1, T, Target, [P | L]);
optimization_angle(BodyType, Start, Point, Split, _, Target, L) ->
	D1 = vec3_angle_cos(Split, Point, Start),
	D2 = vec3_angle_cos(Split, Point, Target),
	P = if
		D1 >= ?OPTIMIZATION_ANGLE andalso D2 >= ?OPTIMIZATION_ANGLE ->
			Point;
		true ->
			get_deviation(Split, BodyType, D1, D2, Start, Point, Target)
	end,
	lists:reverse([Target, P | L]).

%计算2点夹角对指定分割平面的夹角的COS值的平方
vec3_angle_cos(1, {X1, Y1, Z1}, {X2, Y2, Z2}) ->
	X = (X2 - X1) * (X2 - X1), Y = Y2 - Y1, Z = Z2 - Z1,
	X / (X + Y * Y + Z * Z);
vec3_angle_cos(2, {X1, Y1, Z1}, {X2, Y2, Z2}) ->
	X = X2 - X1, Y = (Y2 - Y1) * (Y2 - Y1), Z = Z2 - Z1,
	Y / (X * X + Y + Z * Z);
vec3_angle_cos(3, {X1, Y1, Z1}, {X2, Y2, Z2}) ->
	X = X2 - X1, Y = Y2 - Y1, Z = (Z2 - Z1) * (Z2 - Z1),
	Z / (X * X + Y * Y + Z).

%获得偏向
get_deviation(1, {BodyTypeX, _BodyTypeY, _BodyTypeZ}, D1, D2, {SX, _SY, _SZ}, {X, Y, Z}, {TX, _TY, _TZ}) ->
	if
		% 向start 靠拢
		D1 < D2 andalso SX > TX -> {X + BodyTypeX, Y, Z};
		D1 < D2 -> {X - BodyTypeX, Y, Z};
		% 向target 靠拢
		TX > SX -> {X + BodyTypeX, Y, Z};
		true -> {X - BodyTypeX, Y, Z}
	end;
get_deviation(2, {_BodyTypeX, BodyTypeY, _BodyTypeZ}, D1, D2, {_SX, SY, _SZ}, {X, Y, Z}, {_TX, TY, _TZ}) ->
	if
		% 向start 靠拢
		D1 < D2 andalso SY > TY -> {X, Y + BodyTypeY, Z};
		D1 < D2 -> {X, Y - BodyTypeY, Z};
		% 向target 靠拢
		TY > SY -> {X, Y + BodyTypeY, Z};
		true -> {X, Y - BodyTypeY, Z}
	end;
get_deviation(3, {_BodyTypeX, _BodyTypeY, BodyTypeZ}, D1, D2, {_SX, _SY, SZ}, {X, Y, Z}, {_TX, _TY, TZ}) ->
	if
		% 向start 靠拢
		D1 < D2 andalso SZ > TZ -> {X, Y, Z + BodyTypeZ};
		D1 < D2 -> {X, Y, Z - BodyTypeZ};
		% 向target 靠拢
		TZ > SZ -> {X, Y, Z + BodyTypeZ};
		true -> {X, Y, Z - BodyTypeZ}
	end.

%计算最靠近的路径
find_near([{{?MODULE, _ID, _PID}, {_, _, JudgeCost, _, _, _, _, _, _} = Node} | T],
	Start, BodyType, _NearNode, NearDist) when JudgeCost < NearDist ->
	find_near(T, Start, BodyType, Node, JudgeCost);
find_near([_ | T], Start, BodyType, NearNode, NearDist) ->
	find_near(T, Start, BodyType, NearNode, NearDist);
find_near(_, Start, BodyType, {_, _, _, _ID, ID, PID, Point, _Door, _Space}, _NearDist) ->
	case erase({?MODULE, ID, PID}) of
		{_, _, _, _, _, PPID, _Point, Door, _Space} ->
			backtracking(Start, Point, BodyType, PID, PPID, [Door]);
		_ ->
			[Start, Point]
	end;
find_near(_, _Start, _BodyType, _, _NearDist) ->
	{error, not_near_space}.
