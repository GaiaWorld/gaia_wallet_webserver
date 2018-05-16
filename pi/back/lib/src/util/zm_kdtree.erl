%%%@doc k-dimensional树模块
%%	@type tree_node() = {Pos::tuple(), Split::integer(), Size::integer(), Left::tuple(), Right::tuple(), Min::tuple(), Max::tuple(), Data::any()}
%%@end

-module(zm_kdtree).

-description("kdtree").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([empty/0, is_empty/1, get_pos/1, get_split/1, size/1, get_min/1, get_max/1, get_data/1, member/2, insert/4]).
-export([query_point/3, query_range/3, query_distance/4]).
-export([to_list/1, to_list/2]).

%%%=======================INLINE=======================
-compile({inline, [member1/2, insert1/5, query_point1/3, query_range1/5, query_distance1/6, to_list/2, contain/4, range/6, min/4, max/4, node_insert/3]}).

%%%=======================DEFINE=======================
-define(NIL, nil).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc Returns a new empty tree
%% @spec  empty() -> return()
%% where
%%  return() =  nil
%%@end
%% -----------------------------------------------------------------
empty() ->
	?NIL.

%% -----------------------------------------------------------------
%%@doc Returns true if Tree is an empty tree, and false otherwise.
%% @spec  is_empty(Tree::tree()) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
is_empty(Tree) ->
	Tree =:= ?NIL.

%% -----------------------------------------------------------------
%%@doc Returns the pos of node
%% @spec  get_pos(Tree::tree()) -> return()
%% where
%%  return() = tuple()
%%@end
%% -----------------------------------------------------------------
get_pos({Pos, _Split, _Size, _Left, _Right, _Min, _Max, _Data}) ->
	Pos.

%% -----------------------------------------------------------------
%%@doc Returns the split of node
%% @spec  get_split(Tree::tree()) -> return()
%% where
%%  return() = integer()
%%@end
%% -----------------------------------------------------------------
get_split({_Pos, Split, _Size, _Left, _Right, _Min, _Max, _Data}) ->
	Split;
get_split(?NIL) ->
	0.

%% -----------------------------------------------------------------
%%@doc Returns the number of nodes in Tree
%% @spec  size(Tree::tree()) -> return()
%% where
%%  return() = integer()
%%@end
%% -----------------------------------------------------------------
size({_Pos, _Split, Size, _Left, _Right, _Min, _Max, _Data}) ->
	Size;
size(?NIL) ->
	0.

%% -----------------------------------------------------------------
%%@doc Returns the min of node
%% @spec  get_min(Tree::tree()) -> return()
%% where
%%  return() = tuple()
%%@end
%% -----------------------------------------------------------------
get_min({_Pos, _Split, _Size, _Left, _Right, Min, _Max, _Data}) ->
	Min.

%% -----------------------------------------------------------------
%%@doc Returns the max of node
%% @spec  get_max(Tree::tree()) -> return()
%% where
%%  return() = tuple()
%%@end
%% -----------------------------------------------------------------
get_max({_Pos, _Split, _Size, _Left, _Right, _Min, Max, _Data}) ->
	Max.

%% -----------------------------------------------------------------
%%@doc Returns the data of node
%% @spec  get_data(Tree::tree()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
get_data({_Pos, _Split, _Size, _Left, _Right, _Min, _Max, Data}) ->
	Data;
get_data(?NIL) ->
	0.

%% -----------------------------------------------------------------
%%@doc member node with Pos in Tree
%% @spec  member(Pos::tuple(), Tree::tree()) -> return()
%% where
%%  return() = Node | none
%%@end
%% -----------------------------------------------------------------
member(Pos, Tree) ->
	member1(Pos, Tree).

member1(Pos, {Pos, _Split, _Size, _Left, _Right, _Min, _Max, _Data} = Node) ->
	Node;
member1(Pos, {Pos1, Split, _Size, Left, Right, _Min, _Max, _Data}) ->
	if
		element(Split, Pos) =< element(Split, Pos1) ->
			member1(Pos, Left);
		true ->
			member1(Pos, Right)
	end;
member1(_, _) ->
	none.

%% -----------------------------------------------------------------
%%@doc insert node in Tree
%% @spec  insert(Pos::tuple(), Split::integer(), Data::any(), Tree::tree()) -> return()
%% where
%%  return() = Node
%%@end
%% -----------------------------------------------------------------
insert(Pos, Split, Data, Tree) ->
	insert1(Pos, Split, Data, Tree, []).

insert1(Pos, Split, Data, {Pos1, Split1, _Size, Left, Right, _Min, _Max, _Data} = Node, L) ->
	if
		element(Split1, Pos) =< element(Split1, Pos1) ->
			insert1(Pos, Split, Data, Left, [left, Node | L]);
		true ->
			insert1(Pos, Split, Data, Right, [right, Node | L])
	end;
insert1(Pos, Split, Data, _, L) ->
	node_insert(tuple_size(Pos), L, {Pos, Split, 1, ?NIL, ?NIL, Pos, Pos, Data}).

%% -----------------------------------------------------------------
%%@doc 查询指定的点，返回包含它的节点，如果该点在分割轴上，则根据靠近FixPos来返回空间节点
%% @spec  query_point(Pos::tuple(), Tree::tree_node(),FixPos) -> return()
%% where
%%  return() = {left | right, tree_node()}
%%@end
%% -----------------------------------------------------------------
query_point(Pos, Tree, FixPos) ->
	query_point1(Pos, Tree, FixPos).

query_point1(Pos, {Pos1, Split, _Size, Left, Right, _Min, _Max, _Data} = Node, FixPos) ->
	K = element(Split, Pos),
	K1 = element(Split, Pos1),
	if
		K < K1 ->
			if
				Left =/= ?NIL -> query_point1(Pos, Left, FixPos);
				true -> {left, Node}
			end;
		K > K1 ->
			if
				Right =/= ?NIL -> query_point1(Pos, Right, FixPos);
				true -> {right, Node}
			end;
		element(Split, FixPos) =< K1 ->
			if
				Left =/= ?NIL -> query_point1(Pos, Left, FixPos);
				true -> {left, Node}
			end;
		true ->
			if
				Right =/= ?NIL -> query_point1(Pos, Right, FixPos);
				true -> {right, Node}
			end
	end.

%% -----------------------------------------------------------------
%%@doc 查询指定的轴平行的范围，必须保证pos1所有维度的值都比pos2的小，返回包含的节点
%% @spec  query_range(Min::tuple(), Max::tuple(), Tree::tree()) -> return()
%% where
%%  return() = list()
%%@end
%% -----------------------------------------------------------------
query_range(Min, Max, Tree) ->
	query_range1(tuple_size(Min), Min, Max, Tree, []).

query_range1(KD, Min1, Max1, 
	{Pos, _Split, Size, Left, Right, Min2, Max2, _Data} = Node, L) when Size > 1 ->
	case range(KD, Min1, Max1, Min2, Max2, 2) of
		2 ->
			to_list(Left, [Node | to_list(Right, L)]);
		1 ->
			L1 = case contain(KD, Min1, Max1, Pos) of
				true -> [Node | L];
				_ -> L
			end,
			query_range1(KD, Min1, Max1, Left,
				query_range1(KD, Min1, Max1, Right, L1));
		0 ->
			L
	end;
query_range1(KD, Min1, Max1, {Pos, _Split, _Size, _Left, _Right, _Min, _Max, _Data} = Node, L) ->
	case contain(KD, Min1, Max1, Pos) of
		true -> [Node | L];
		_ -> L
	end;
query_range1(_KD, _Min1, _Max1, _, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 查询指定的距离范围，返回包含的节点，RangeFun为计算点到各面的距离，用于快速判断范围，DistFun为计算两点距离的函数
%% @spec  query_distance(Pos::tuple(), RangeFun::function(), DistFun::function(), Tree::tree()) -> return()
%% where
%%  return() = list()
%%@end
%% -----------------------------------------------------------------
query_distance(Pos, RangeFun, DistFun, Tree) ->
	query_distance1(tuple_size(Pos), Pos, RangeFun, DistFun, Tree, []).

query_distance1(KD, Pos1, RangeFun, DistFun,
	{Pos2, _Split, Size, Left, Right, Min, Max, _Data} = Node, L) when Size > 1 ->
	case RangeFun(KD, Pos1, Min, Max) of
		2 ->
			to_list(Left, [Node | to_list(Right, L)]);
		1 ->
			L1 = case DistFun(KD, Pos1, Pos2) of
				true -> [Node | L];
				_ -> L
			end,
			query_distance1(KD, Pos1, RangeFun, DistFun, Left,
				query_distance1(KD, Pos1, RangeFun, DistFun, Right, L1));
		0 ->
			L
	end;
query_distance1(KD, Pos1, _RangeFun, DistFun,
	{Pos2, _Split, _Size, _Left, _Right, _Min, _Max, _Data} = Node, L) ->
	case DistFun(KD, Pos1, Pos2) of
		true -> [Node | L];
		_ -> L
	end;
query_distance1(_KD, _Pos1, _RangeFun, _DistFun, _, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc a tree into an list
%% @spec  to_list(Tree::tree()) -> return()
%% where
%%  return() = list()
%%@end
%% -----------------------------------------------------------------
to_list({_Pos, _Split, _Size, Left, Right, _Min, _Max, _Data} = Node) ->
	to_list(Left, [Node | to_list(Right, [])]);
to_list(_) ->
	[].

%% -----------------------------------------------------------------
%%@doc a tree into an list
%% @spec  to_list(Tree::tree(), list()) -> return()
%% where
%%  return() = list()
%%@end
%% -----------------------------------------------------------------
to_list({_Pos, _Split, _Size, Left, Right, _Min, _Max, _Data} = Node, L) ->
	to_list(Left, [Node | to_list(Right, L)]);
to_list(_, L) ->
	L.

%%%===================LOCAL FUNCTIONS==================
% 判断指定的点是否在范围内
contain(KD, Min, Max, P) when KD > 0 ->
	E = element(KD, P),
	case element(KD, Min) of
		E1 when E1 > E ->
			false;
		_ ->
			case element(KD, Max) of
				E2 when E2 < E ->
					false;
				_ ->
					contain(KD - 1, Min, Max, P)
			end
	end;
contain(_KD, _P1, _P2, _P) ->
	true.

% 判断2个范围是否不相交0，相交1，包含2
range(KD, Min1, Max1, Min2, Max2, Type) when KD > 0 ->
	EMin1 = element(KD, Min1),
	case element(KD, Max2) of
		E when EMin1 > E ->
			0;
		EMax2 ->
			EMax1 = element(KD, Max1),
			case element(KD, Min2) of
				E when EMax1 < E ->
					0;
				EMin2 ->
					if
						EMin1 =< EMin2 andalso EMax1 >= EMax2 ->
							range(KD - 1, Min1, Max1, Min2, Max2, Type);
						true ->
							range(KD - 1, Min1, Max1, Min2, Max2, 1)
					end
			end
	end;
range(_KD, _Min1, _Max1, _Min2, _Max2, Type) ->
	Type.

% 获得两个点中最小值集合的点
min(KD, P1, P2, L) when KD > 0 ->
	E1 = element(KD, P1),
	case element(KD, P2) of
		E2 when E2 < E1 ->
			min(KD - 1, P1, P2, [E2 | L]);
		_ ->
			min(KD - 1, P1, P2, [E1 | L])
	end;
min(_, _P1, _P2, L) ->
	list_to_tuple(L).

% 获得两个点中最大值集合的点
max(KD, P1, P2, L) when KD > 0 ->
	E1 = element(KD, P1),
	case element(KD, P2) of
		E2 when E2 > E1 ->
			max(KD - 1, P1, P2, [E2 | L]);
		_ ->
			max(KD - 1, P1, P2, [E1 | L])
	end;
max(_, _P1, _P2, L) ->
	list_to_tuple(L).

% 节点插入操作
node_insert(KD, [left, {Pos, Split, Size, _Left1, Right, Min1, Max1, Data} | T],
	{_Pos, _Split, _Size, _Left2, _Right, Min2, Max2, _Data} = Node) ->
	node_insert(KD, T, {Pos, Split, Size + 1, Node, Right,
		min(KD, Min1, Min2, []), max(KD, Max1, Max2, []), Data});
node_insert(KD, [right, {Pos, Split, Size, Left, _Right1, Min1, Max1, Data} | T],
	{_Pos, _Split, _Size, _Left, _Right2, Min2, Max2, _Data} = Node) ->
	node_insert(KD, T, {Pos, Split, Size + 1, Left, Node,
		min(KD, Min1, Min2, []), max(KD, Max1, Max2, []), Data});
node_insert(_KD, _, Node) ->
	Node.
