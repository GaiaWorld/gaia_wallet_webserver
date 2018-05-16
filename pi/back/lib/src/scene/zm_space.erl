%%@doc 空间模块
%%```
%%%	基于kdtree的空间划分及空间连接
%%%	@type space() = {KDTree, SBTree::sb_trees(), Box}
%%%	@type point() = {float(), float(), float()}
%%%	@type link_node() = {ID::integer(), Min::point(), Max::point(), Split::integer()}
%%%	@type space_node() = {ID::integer(), Min::point(), Max::point(), Link::[link_node()], Info::any()}
%%'''
%%@end


-module(zm_space).

-description("space").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([create/3, size/1, get_box/1, get/2]).
-export([query_point/3, query_range/3, query_line/5, query_line/6]).
-export([cross_door/5, cross_door/6, cross_x_door/15, cross_y_door/15, cross_z_door/15]).
-export([json/1]).

%%%=======================INLINE=======================
-compile({inline, [id_list/2, node_list/3, cross_x_point/11, cross_y_point/11, cross_z_point/11, cross_x_point/14, cross_y_point/14, cross_z_point/14, door_x_point/11, door_y_point/11, door_z_point/11]}).
-compile({inline_size, 32}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  create
%% @spec  create(KdtreePointList::list(), SpaceNodeTree::sb_trees(), Box::tuple()) -> return()
%% where
%%  return() = Space::space()
%%@end
%% -----------------------------------------------------------------
create(KDTreePointList, SpaceNodeTree, Box) ->
	{create_kdtree(KDTreePointList, zm_kdtree:empty()), SpaceNodeTree, Box}.

%% -----------------------------------------------------------------
%%@doc Returns the number of nodes in Tree
%% @spec  size(Tree::tree()) -> return()
%% where
%%  return() = integer()
%%@end
%% -----------------------------------------------------------------
size({_KDTree, SBTree, _Box}) ->
	sb_trees:size(SBTree).

%% -----------------------------------------------------------------
%%@doc  get_box
%% @spec  get_box(Space::space()) -> {tuple(), tuple()}
%%@end
%% -----------------------------------------------------------------
get_box({_KDTree, _SBTree, Box}) ->
	Box.

%% -----------------------------------------------------------------
%%@doc  get
%% @spec  get(Space::space(), ID) -> space_node()
%%@end
%% -----------------------------------------------------------------
get({_KDTree, SBTree, _Box}, ID) ->
	sb_trees:get(ID, SBTree).

%% -----------------------------------------------------------------
%%@doc 查询指定的点，返回包含它的空间节点，如果该点在分割轴上，则根据靠近FixPos来返回空间节点
%% @spec  query_point({}, Pos::point(),FixPos) -> return()
%% where
%%  return() = SpaceNode::space_node()
%%@end
%% -----------------------------------------------------------------
query_point({KDTree, SBTree, _Box}, Pos, FixPos) ->
	ID = case zm_kdtree:query_point(Pos, KDTree, FixPos) of
		{left, {_Pos, _Split, _, _, _, _, _, {LeftID, _RightID}}} ->
			LeftID;
		{right, {_Pos, _Split, _, _, _, _, _, {_LeftID, RightID}}} ->
			RightID
	end,
	sb_trees:get(ID, SBTree).

%% -----------------------------------------------------------------
%%@doc 查询指定的轴平行的范围，必须保证pos1所有维度的值都比pos2的小，返回包含的节点
%% @spec  query_range(Space::space(), Min::point(), Max::point()) -> return()
%% where
%%  return() = [SpaceNode::space_node()]
%%@end
%% -----------------------------------------------------------------
query_range({KDTree, SBTree, _Box}, Min, Max) ->
	node_list(lists:usort(id_list(
		zm_kdtree:query_range(Min, Max, KDTree), [])), SBTree, []).

%% -----------------------------------------------------------------
%%@doc 查询线段穿过的空间
%% @spec  query_line(Space::space(), Start::point(), Target::point(), Fun::function(), Args::any()) -> return()
%% where
%%  return() = {ok, Node::space_node(), Args1::any()} | {result, Args1::any()}
%%
%%	Fun Arguments:[Args::any(), Node::space_node(), Point::point()]
%%	Fun Returns:
%%		{result, Args1::any()} | %返回结果
%%		Args1::term() %表示继续寻找
%%@end
%% -----------------------------------------------------------------
query_line({_KDTree, SBTree, _Box} = Space, Start, Target, Fun, Args) ->
	{_, _, _, Link, _} = Node = query_point(Space, Start, Target),
	query_line(SBTree, Start, Target, Fun, Args, -1, Node, Link).

%% -----------------------------------------------------------------
%%@doc 查询线段穿过的空间
%% @spec  query_line(Space::space(), Start::point(), StartNode::space_node(), Target::point(), Fun::function(), Args::any()) -> return()
%% where
%%  return() = {ok, Node::space_node(), Args1::any()} | {result, Args1::any()}
%%
%%	Fun Arguments:[Args::any(), Node::space_node(), Point::point()]
%%	Fun Returns:
%%		{result, Args1::any()} | %返回结果
%%		Args1::term() %表示继续寻找
%%@end
%% -----------------------------------------------------------------
query_line({_KDTree, SBTree, _Box}, Start, {_, _, _, Link, _} = StartNode, Target, Fun, Args) ->
	query_line(SBTree, Start, Target, Fun, Args, -1, StartNode, Link).

%% -----------------------------------------------------------------
%%@doc 判断线段是否和门相交，返回相交类型或相交点Type | Point，0表示跨越门的分割轴但不相交，-1为小于门的分割轴，1为大于门的分割轴
%% @spec  cross_door(Split::integer(), Min::point(), Max::point(), Start::point(), Target::point()) -> return()
%% where
%%  return() = 0 | 1 | -1 | Point::point()
%%@end
%% -----------------------------------------------------------------
cross_door(1, {MinX, MinY, MinZ}, {_MaxX, MaxY, MaxZ},
	{StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}) ->
	if
		TargetX < MinX ->
			if
				StartX > MinX ->
					cross_x_point(MinX, MinY, MinZ, MaxY, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				true -> -1
			end;
		TargetX > MinX ->
			if
				StartX < MinX ->
					cross_x_point(MinX, MinY, MinZ, MaxY, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				true -> 1
			end;
		TargetX < StartX -> 1;
		true -> -1
	end;
cross_door(2, {MinX, MinY, MinZ}, {MaxX, _MaxY, MaxZ},
	{StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}) ->
	if
		TargetY < MinY ->
			if
				StartY > MinY ->
					cross_y_point(MinY, MinX, MinZ, MaxX, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				true -> -1
			end;
		TargetY > MinY ->
			if
				StartY < MinY ->
					cross_y_point(MinY, MinX, MinZ, MaxX, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				true -> 1
			end;
		TargetY < StartY -> 1;
		true -> -1
	end;
cross_door(3, {MinX, MinY, MinZ}, {MaxX, MaxY, _MaxZ},
	{StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}) ->
	if
		TargetZ < MinZ ->
			if
				StartZ > MinZ ->
					cross_z_point(MinZ, MinX, MinY, MaxX, MaxY,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				true -> -1
			end;
		TargetZ > MinZ ->
			if
				StartZ < MinZ ->
					cross_z_point(MinZ, MinX, MinY, MaxX, MaxY,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				true -> 1
			end;
		TargetZ < StartZ -> 1;
		true -> -1
	end.

%% -----------------------------------------------------------------
%%@doc 判断线段是否和门相交，返回相交类型或相交点{Type, Point} | Point，0表示跨越门的分割轴但不相交，-1为小于门的分割轴，1为大于门的分割轴
%% @spec  cross_door(Split::integer(), BodyType::point(), Min::point(), Max::point(), Start::point(), Target::point()) -> return()
%% where
%%  return() = {0 | 1 | -1, Point::point()} | Point::point()
%%@end
%% -----------------------------------------------------------------
cross_door(1, {BodyTypeX, BodyTypeY, BodyTypeZ}, {MinX, MinY, MinZ}, {MaxX, MaxY, MaxZ},
	{StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}) ->
	cross_x_door(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxX, MaxY, MaxZ,
	StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
cross_door(2, {BodyTypeX, BodyTypeY, BodyTypeZ}, {MinX, MinY, MinZ}, {MaxX, MaxY, MaxZ},
	{StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}) ->
	cross_y_door(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxX, MaxY, MaxZ,
	StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
cross_door(3, {BodyTypeX, BodyTypeY, BodyTypeZ}, {MinX, MinY, MinZ}, {MaxX, MaxY, MaxZ},
	{StartX, StartY, StartZ}, {TargetX, TargetY, TargetZ}) ->
	cross_z_door(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxX, MaxY, MaxZ,
	StartX, StartY, StartZ, TargetX, TargetY, TargetZ).

%% -----------------------------------------------------------------
%%@doc 判断线段是否和x分割轴的门相交
%%@end
%% -----------------------------------------------------------------
cross_x_door(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, _MaxX, MaxY, MaxZ,
	StartX, StartY, StartZ, TargetX, TargetY, TargetZ) ->
	if
		TargetX < MinX ->
			if
				StartX > MinX ->
					cross_x_point(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				TargetX > StartX ->
					door_x_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ, TargetY, TargetZ);
				true ->
					door_x_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ, StartY, StartZ)
			end;
		TargetX > MinX ->
			if
				StartX < MinX ->
					cross_x_point(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				TargetX < StartX ->
					door_x_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ, TargetY, TargetZ);
				true ->
					door_x_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ, StartY, StartZ)
			end;
		TargetX < StartX ->
			door_x_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ, TargetY, TargetZ);
		true ->
			door_x_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxY, MaxZ, TargetY, TargetZ)
	end.

%% -----------------------------------------------------------------
%%@doc 判断线段是否和x分割轴的门相交
%%@end
%% -----------------------------------------------------------------
cross_y_door(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxX, _MaxY, MaxZ,
	StartX, StartY, StartZ, TargetX, TargetY, TargetZ) ->
	if
		TargetY < MinY ->
			if
				StartY > MinY ->
					cross_y_point(BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				TargetY > StartY ->
					door_y_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ, TargetX, TargetZ);
				true ->
					door_y_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ, StartX, StartZ)
			end;
		TargetY > MinY ->
			if
				StartY < MinY ->
					cross_y_point(BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				TargetY < StartY ->
					door_y_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ, TargetX, TargetZ);
				true ->
					door_y_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ, StartX, StartZ)
			end;
		TargetY < StartY ->
			door_y_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ, TargetX, TargetZ);
		true ->
			door_y_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinY, MinX, MinZ, MaxX, MaxZ, TargetX, TargetZ)
	end.

%% -----------------------------------------------------------------
%%@doc 判断线段是否和x分割轴的门相交
%%@end
%% -----------------------------------------------------------------
cross_z_door(BodyTypeX, BodyTypeY, BodyTypeZ, MinX, MinY, MinZ, MaxX, MaxY, _MaxZ,
	StartX, StartY, StartZ, TargetX, TargetY, TargetZ) ->
	if
		TargetZ < MinZ ->
			if
				StartZ > MinZ ->
					cross_z_point(BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				TargetZ > StartZ ->
					door_z_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY, TargetX, TargetY);
				true ->
					door_z_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY, StartX, StartY)
			end;
		TargetZ > MinZ ->
			if
				StartZ < MinZ ->
					cross_z_point(BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY,
						StartX, StartY, StartZ, TargetX, TargetY, TargetZ);
				TargetZ < StartZ ->
					door_z_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY, TargetX, TargetY);
				true ->
					door_z_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY, StartX, StartY)
			end;
		TargetZ < StartZ ->
			door_z_point(1, BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY, TargetX, TargetY);
		true ->
			door_z_point(-1, BodyTypeX, BodyTypeY, BodyTypeZ, MinZ, MinX, MinY, MaxX, MaxY, TargetX, TargetY)
	end.

%% -----------------------------------------------------------------
%% @doc 从Json中加载
%% @spec json(JsonTerm::sb_trees()) -> return()
%% where
%%  return() = Space::space()
%%@end
%% -----------------------------------------------------------------
json(JsonTerm) ->
	{_, Array} = sb_trees:get("array", sb_trees:get("tree", JsonTerm)),
	KDTreePointList = [json_tree_node(Node) || Node <- Array],
	Map = sb_trees:get("map", JsonTerm),
	SpaceNodeList = [json_space_node(ID, Node) || {ID, Node} <- sb_trees:to_list(Map)],
	Box = sb_trees:get("box", JsonTerm),
	{3, Min} = sb_trees:get("min", Box),
	{3, Max} = sb_trees:get("max", Box),
	create(KDTreePointList, sb_trees:from_orddict(lists:sort(SpaceNodeList)), {list_to_tuple(Min), list_to_tuple(Max)}).

%从Json中转成KDTreeNode
json_tree_node(Node) ->
	{3, Pos} = sb_trees:get("pos", Node),
	Split = sb_trees:get("split", Node),
	Left = sb_trees:get("left", sb_trees:get("data", Node)),
	Right = sb_trees:get("right", sb_trees:get("data", Node)),
	{list_to_tuple(Pos), Split + 1, {Left, Right}}.

%从Json中转成SpaceNode
json_space_node(ID, Node) ->
	I = list_to_integer(ID),
	{3, Min} = sb_trees:get("min", Node),
	{3, Max} = sb_trees:get("max", Node),
	{_, Link} = sb_trees:get("link", Node),
	L = [json_link_node(N) || N <- Link],
	Info = sb_trees:get("info", Node),
	{I,{I, list_to_tuple(Min), list_to_tuple(Max), L, Info}}.

%从Json中转成LinkNode
json_link_node(Node) ->
	ID = sb_trees:get("id", Node),
	{3, Min} = sb_trees:get("min", Node),
	{3, Max} = sb_trees:get("max", Node),
	Split = sb_trees:get("split", Node),
	{ID, list_to_tuple(Min), list_to_tuple(Max), Split + 1}.

%%%===================LOCAL FUNCTIONS==================
%创建kdtree
create_kdtree([{Pos, Split, Data} | T], Tree) ->
	create_kdtree(T, zm_kdtree:insert(Pos, Split, Data, Tree));
create_kdtree(_, Tree) ->
	Tree.

%从kdtree节点列表中获取空间ID列表
id_list([Node | T], L) ->
	{ID1, ID2} = zm_kdtree:get_data(Node),
	id_list(T, [ID2, ID1 | L]);
id_list(_, L) ->
	L.

%从ID列表中获取空间节点列表
node_list([ID | T], SBTree, L) ->
	Node = sb_trees:get(ID, SBTree),
	node_list(T, SBTree, [Node | L]);
node_list(_, _SBTree, L) ->
	L.

% 查询线段穿过的空间
query_line(SBTree, Start, Target, Fun, Args, PID, Node, [{PID, _Min, _Max, _Split} | T]) ->
	query_line(SBTree, Start, Target, Fun, Args, PID, Node, T);
query_line(SBTree, Start, Target, Fun, Args, PID, Node, [{ID, Min, Max, Split} | T]) ->
	case cross_door(Split, Min, Max, Start, Target) of
		Type when is_integer(Type) ->
			query_line(SBTree, Start, Target, Fun, Args, PID, Node, T);
		Point ->
			NextNode = sb_trees:get(ID, SBTree),
			case Fun(Args, NextNode, Point) of
				{result, _} = R ->
					R;
				Args1 ->
					{SID, _, _, _, _} = Node,
					{_, _, _, Link, _} = NextNode,
					query_line(SBTree, Start, Target, Fun, Args1, SID, NextNode, Link)
			end
	end;
query_line(_SBTree, _Start, _Target, _Fun, Args, _PID, Node, _) ->
	{ok, Node, Args}.



% 计算x分割轴上的交点
cross_x_point(PK, MinM, MinN, MaxM, MaxN,
	StartK, StartM, StartN, TargetK, TargetM, TargetN) ->
	PM = (TargetM - StartM) * (PK - StartK) / (TargetK - StartK) + StartM,
	if
		PM < MinM -> 0;
		PM > MaxM -> 0;
		true ->
			PN = (TargetN - StartN) * (PK - StartK) / (TargetK - StartK) + StartN,
			if
				PN < MinN -> 0;
				PN > MaxN -> 0;
				true -> {PK, PM, PN}
			end
	end.

% 计算y分割轴上的交点
cross_y_point(PK, MinM, MinN, MaxM, MaxN,
	StartM, StartK, StartN, TargetM, TargetK, TargetN) ->
	PM = (TargetM - StartM) * (PK - StartK) / (TargetK - StartK) + StartM,
	if
		PM < MinM -> 0;
		PM > MaxM -> 0;
		true ->
			PN = (TargetN - StartN) * (PK - StartK) / (TargetK - StartK) + StartN,
			if
				PN < MinN -> 0;
				PN > MaxN -> 0;
				true -> {PM, PK, PN}
			end
	end.

% 计算z分割轴上的交点
cross_z_point(PK, MinM, MinN, MaxM, MaxN,
	StartM, StartN, StartK, TargetM, TargetN, TargetK) ->
	PM = (TargetM - StartM) * (PK - StartK) / (TargetK - StartK) + StartM,
	if
		PM < MinM -> 0;
		PM > MaxM -> 0;
		true ->
			PN = (TargetN - StartN) * (PK - StartK) / (TargetK - StartK) + StartN,
			if
				PN < MinN -> 0;
				PN > MaxN -> 0;
				true -> {PM, PN, PK}
			end
	end.



% 计算x分割轴上的交点
cross_x_point(_BodyTypeX, BodyTypeM, BodyTypeN, PK, MinM, MinN, MaxM, MaxN,
	StartK, StartM, StartN, TargetK, TargetM, TargetN) ->
	PM = (TargetM - StartM) * (PK - StartK) / (TargetK - StartK) + StartM,
	PN = (TargetN - StartN) * (PK - StartK) / (TargetK - StartK) + StartN,
	PM1 = if
		PM < MinM + BodyTypeM ->
			MinM + BodyTypeM;
		PM > MaxM - BodyTypeM ->
			MaxM - BodyTypeM;
		true ->
			PM
	end,
	Tmp=MinN + BodyTypeN,
	if
		PN < Tmp ->
			{0, {PK, PM1, Tmp}};
		PN > MaxN - BodyTypeN ->
			{0, {PK, PM1, MaxN - BodyTypeN}};
		PM =/= PM1 ->
			{0, {PK, PM1, PN}};
		true ->
			{PK, PM, PN}
	end.

% 计算y分割轴上的交点
cross_y_point(BodyTypeM, _BodyTypeY, BodyTypeN, PK, MinM, MinN, MaxM, MaxN,
	StartM, StartK, StartN, TargetM, TargetK, TargetN) ->
	PM = (TargetM - StartM) * (PK - StartK) / (TargetK - StartK) + StartM,
	PN = (TargetN - StartN) * (PK - StartK) / (TargetK - StartK) + StartN,
	Tmp=MinM + BodyTypeM,
	PM1 = if
		PM < Tmp ->
			Tmp;
		PM > MaxM - BodyTypeM ->
			MaxM - BodyTypeM;
		true ->
			PM
	end,
	if
		PN < MinN + BodyTypeN ->
			{0, {PM1, PK, MinN + BodyTypeN}};
		PN > MaxN - BodyTypeN ->
			{0, {PM1, PK, MaxN - BodyTypeN}};
		PM =/= PM1 ->
			{0, {PM1, PK, PN}};
		true ->
			{PM, PK, PN}
	end.

% 计算z分割轴上的交点
cross_z_point(BodyTypeM, BodyTypeN, _BodyTypeZ, PK, MinM, MinN, MaxM, MaxN,
	StartM, StartN, StartK, TargetM, TargetN, TargetK) ->
	PM = (TargetM - StartM) * (PK - StartK) / (TargetK - StartK) + StartM,
	PN = (TargetN - StartN) * (PK - StartK) / (TargetK - StartK) + StartN,
	Tmp=MinM + BodyTypeM,
	PM1 = if
		PM < Tmp ->
			Tmp;
		PM > MaxM - BodyTypeM ->
			MaxM - BodyTypeM;
		true ->
			PM
	end,
	if
		PN < MinN + BodyTypeN ->
			{0, {PM1, MinN + BodyTypeN, PK}};
		PN > MaxN - BodyTypeN ->
			{0, {PM1, MaxN - BodyTypeN, PK}};
		PM =/= PM1 ->
			{0, {PM1, PN, PK}};
		true ->
			{PM,PN, PK}
	end.


% 计算门上x分割轴上的最近点
door_x_point(Type, _BodyTypeX, BodyTypeM, BodyTypeN, PK, MinM, MinN, MaxM, MaxN, PM, PN) ->
	PM1 = if
		PM < MinM + BodyTypeM ->
			MinM + BodyTypeM;
		PM > MaxM - BodyTypeM ->
			MaxM - BodyTypeM;
		true ->
			PM
	end,
	if
		PN < MinN + BodyTypeN ->
			{Type, {PK, PM1, MinN + BodyTypeN}};
		PN > MaxN - BodyTypeN ->
			{Type, {PK, PM1, MaxN - BodyTypeN}};
		true ->
			{Type, {PK, PM1, PN}}
	end.

% 计算门上y分割轴上的最近点
door_y_point(Type, BodyTypeM, _BodyTypeY, BodyTypeN, PK, MinM, MinN, MaxM, MaxN, PM, PN) ->
	PM1 = if
		PM < MinM + BodyTypeM ->
			MinM + BodyTypeM;
		PM > MaxM - BodyTypeM ->
			MaxM - BodyTypeM;
		true ->
			PM
	end,
	if
		PN < MinN + BodyTypeN ->
			{Type, {PM1, PK, MinN + BodyTypeN}};
		PN > MaxN - BodyTypeN ->
			{Type, {PM1, PK, MaxN - BodyTypeN}};
		true ->
			{Type, {PM1, PK, PN}}
	end.

% 计算门上z分割轴上的最近点
door_z_point(Type, BodyTypeM, BodyTypeN, _BodyTypeZ, PK, MinM, MinN, MaxM, MaxN, PM, PN) ->
	PM1 = if
		PM < MinM + BodyTypeM ->
			MinM + BodyTypeM;
		PM > MaxM - BodyTypeM ->
			MaxM - BodyTypeM;
		true ->
			PM
	end,
	if
		PN < MinN + BodyTypeN ->
			{Type, {PM1, MinN + BodyTypeN, PK}};
		PN > MaxN - BodyTypeN ->
			{Type, {PM1, MaxN - BodyTypeN, PK}};
		true ->
			{Type, {PM1, PN, PK}}
	end.
