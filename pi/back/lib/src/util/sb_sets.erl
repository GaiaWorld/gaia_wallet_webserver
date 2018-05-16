%%@doc Size Balanced Tree（SBT）平衡二叉树
%% @type sb_sets() = {Key::term(), El::term(), Size::integer(), Left::sb_sets(), Right::sb_sets()} | nil
%%@end


-module(sb_sets).

-description("size balanced tree").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([empty/0, new/1, is_empty/1, size/1, is_defined/2, get/2, get/3, smallest/1, largest/1, rank/2, by_rank/2]).
-export([insert/2, update/2, enter/2, action/4, delete/2, delete_any/2, remove/2, take_smallest/1, take_largest/1]).
-export([from_list/1, from_list/2, from_order/1, to_list/1, to_list/2]).
-export([iterator/1, iterator/2, next/1, map/2]).

%%%=======================INLINE=======================
-compile({inline, [key/1, rank/3, by_rank1/2, insert1/4, update1/5, enter1/5, action1/6, delete1/3, delete_any1/4, remove1/4, take_smallest1/1, take_largest1/1, from_order/2, iterator1/2, iterator1/3, node_insert/2, maintain_left/5, maintain_right/5, left_ratote/5, right_ratote/5, node_update/2, node_delete/2, node_delete/3]}).
-compile({inline_size, 32}).

%%%=======================DEFINE=======================
-define(NIL, nil).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 构建一棵空树
%% @spec empty()    -> nil
%%@end
%% -----------------------------------------------------------------
empty() ->
	?NIL.

%% -----------------------------------------------------------------
%%@doc 构建一棵指定了初始值的树
%% @spec new(El::term()) -> {El::term(), 1, nil, nil}
%%@end
%% -----------------------------------------------------------------
new(El) ->
	{key(El), El, 1, ?NIL, ?NIL}.

%% -----------------------------------------------------------------
%%@doc 判断是否为空树
%% @spec is_empty(Tree::sb_sets()) -> boolean()
%%@end
%% -----------------------------------------------------------------
is_empty(Tree) ->
	Tree =:= ?NIL.

%% -----------------------------------------------------------------
%%@doc 获取指定树的大小
%% @spec size(Tree::sb_sets()) -> integer()
%%@end
%% -----------------------------------------------------------------
size({_, _, Size, _, _}) ->
	Size;
size(?NIL) ->
	0.

%% -----------------------------------------------------------------
%%@doc 检查指定的Key在树中是否存在
%% @spec is_defined(Key::term(), Tree::sb_sets()) -> boolean()
%%@end
%% -----------------------------------------------------------------
is_defined(Key, {Key1, _, _, Left, _}) when Key < Key1 ->
	is_defined(Key, Left);
is_defined(Key, {Key1, _, _, _, Right}) when Key > Key1 ->
	is_defined(Key, Right);
is_defined(_, N) ->
	N =/= ?NIL.

%% -----------------------------------------------------------------
%%@doc 遍历树，从树中获取指定Key的值
%% @spec get(Key::term(), Tree::sb_sets()) -> return()
%% where
%% return() = El::term()
%%@end
%% -----------------------------------------------------------------
get(Key, {Key1, _, _, Left, _}) when Key < Key1 ->
	get(Key, Left);
get(Key, {Key1, _, _, _, Right}) when Key > Key1 ->
	get(Key, Right);
get(_, {_, El, _, _, _}) ->
	El.

%% -----------------------------------------------------------------
%%@doc 遍历树，从树中获取指定Key的值
%% @spec get(Key::term(), Tree::sb_sets(), Default:: term()) -> return()
%% where
%% return() = El::term()
%%@end
%% -----------------------------------------------------------------
get(Key, {Key1, _, _, Left, _}, Default) when Key < Key1 ->
	get(Key, Left, Default);
get(Key, {Key1, _, _, _, Right}, Default) when Key > Key1 ->
	get(Key, Right, Default);
get(_, {_, El, _, _, _}, _Default) ->
	El;
get(_, _, Default) ->
	Default.

%% -----------------------------------------------------------------
%%@doc 获取树中最小的元素
%% @spec smallest(Tree::sb_sets()) -> El::term()
%%@end
%% -----------------------------------------------------------------
smallest({_, _, _, Left, _}) when is_tuple(Left) ->
	smallest(Left);
smallest({_Key, El, _, _, _}) ->
	El.

%% -----------------------------------------------------------------
%%@doc 获取树中最大的元素
%% @spec largest(Tree::sb_sets()) -> El::term()
%%@end
%% -----------------------------------------------------------------
largest({_, _, _, _, Right}) when is_tuple(Right) ->
	largest(Right);
largest({_Key, El, _, _, _}) ->
	El.

%% -----------------------------------------------------------------
%%@doc 获取指定Key在树中的排名
%% @spec rank(Key::term(), Tree::sb_sets()) -> return()
%% where
%% return() = integer() | {less, integer()}
%%@end
%% -----------------------------------------------------------------
rank(Key, Tree) ->
	rank(Key, Tree, 1).

rank(Key, {Key1, _, _, Left, _}, C) when Key < Key1 ->
	rank(Key, Left, C);
rank(Key, {Key1, _, _, Left, Right}, C) when Key > Key1 ->
	case Left of
		{_, _, Size, _, _} ->
			rank(Key, Right, C + Size + 1);
		_ ->
			rank(Key, Right, C + 1)
	end;
rank(Key, {Key, _, _, Left, _}, C) ->
	case Left of
		{_, _, Size, _, _} ->
			C + Size;
		_ ->
			C
	end;
rank(_, _, C) ->
	{less, C}.

%% -----------------------------------------------------------------
%%@doc 获取指定排名在树中的Key
%% @spec by_rank(Rank::integer() | float(), Tree::sb_sets()) -> El::term()
%%@end
%% -----------------------------------------------------------------
by_rank(Rank, Tree) ->
	by_rank1(Rank - 1, Tree).

by_rank1(Rank, {_K, El, _, Left, Right}) ->
	case Left of
		{_, _, LS, _, _} when Rank < LS ->
			by_rank1(Rank, Left);
		{_, _, LS, _, _} when Rank > LS ->
			by_rank1(Rank - LS - 1, Right);
		_ ->
			El
	end.

%% -----------------------------------------------------------------
%%@doc 插入一个新的元素(不允许插入存在的key)
%% @spec insert(El::term(), Tree::sb_sets()) -> NewTree::sb_sets()
%%@end
%% -----------------------------------------------------------------
insert(El, Tree) ->
	insert1(key(El), El, Tree, []).

insert1(Key, El, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	insert1(Key, El, Left, [left, Node | L]);
insert1(Key, El, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	insert1(Key, El, Right, [right, Node | L]);
insert1(Key, _El, {Key, _, _, _, _}, _L) ->
	erlang:error({key_exists, Key});
insert1(Key, El, _, L) ->
	node_insert(L, {Key, El, 1, ?NIL, ?NIL}).

%% -----------------------------------------------------------------
%%@doc 更新一个已存在元素(更新值)
%% @spec update(El::term(), Tree::sb_sets()) -> NewTree::sb_sets()
%%@end
%% -----------------------------------------------------------------
update(El, Tree) ->
	update1(Tree, key(El), El, Tree, []).

update1(Tree, Key, El, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	update1(Tree, Key, El, Left, [left, Node | L]);
update1(Tree, Key, El, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	update1(Tree, Key, El, Right, [right, Node | L]);
update1(Tree, Key, El, {Key, El1, Size, Left, Right}, L) ->
	if
		El =:= El1 ->
			Tree;
		true ->
			node_update(L, {Key, El, Size, Left, Right})
	end;
update1(_Tree, Key, _El, _, _L) ->
	erlang:error({key_not_found, Key}).

%% -----------------------------------------------------------------
%%@doc 写入一个元素(如果存在相同key,则更新值)
%% @spec enter(El::term(), Tree::sb_sets()) -> NewTree::sb_sets()
%%@end
%% -----------------------------------------------------------------
enter(El, Tree) ->
	enter1(Tree, key(El), El, Tree, []).

enter1(Tree, Key, El, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	enter1(Tree, Key, El, Left, [left, Node | L]);
enter1(Tree, Key, El, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	enter1(Tree, Key, El, Right, [right, Node | L]);
enter1(Tree, Key, El, {Key, El1, Size, Left, Right}, L) ->
	if
		El =:= El1 ->
			Tree;
		true ->
			node_update(L, {Key, El, Size, Left, Right})
	end;
enter1(_Tree, Key, El, _, L) ->
	node_insert(L, {Key, El, 1, ?NIL, ?NIL}).

%% -----------------------------------------------------------------
%%@doc 遍历树，找到指定的元素，并进行指定的操作
%% @spec action(Key::term(), F::function(), A::any(), Tree::sb_sets()) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
action(Key, F, A, Tree) when is_function(F) ->
	action1(Tree, Key, F, A, Tree, []).

action1(Tree, Key, F, A, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	action1(Tree, Key, F, A, Left, [left, Node | L]);
action1(Tree, Key, F, A, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	action1(Tree, Key, F, A, Right, [right, Node | L]);
action1(Tree, Key, F, A, {Key, El, Size, Left, Right}, L) ->
	case F(A, El) of
		break ->
			Tree;
		delete ->
			node_delete(L, node_delete(Size - 1, Left, Right));
		{ok, El} ->
			Tree;
		{ok, V} ->
			node_update(L, {Key, V, Size, Left, Right});
		El ->
			Tree;
		V ->
			node_update(L, {Key, V, Size, Left, Right})
	end;
action1(Tree, Key, F, A, _, L) ->
	case F(A, none) of
		break ->
			Tree;
		delete ->
			Tree;
		{ok, El} ->
			node_insert(L, {Key, El, 1, ?NIL, ?NIL});
		V ->
			node_insert(L, {Key, V, 1, ?NIL, ?NIL})
	end.

%% -----------------------------------------------------------------
%%@doc 删除一个已存在的元素(必须存在指定key)
%% @spec delete(Key::term(), Tree::sb_sets()) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
delete(Key, Tree) ->
	delete1(Key, Tree, []).

delete1(Key, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	delete1(Key, Left, [left, Node | L]);
delete1(Key, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	delete1(Key, Right, [right, Node | L]);
delete1(Key, {Key, _, Size, Left, Right}, L) ->
	node_delete(L, node_delete(Size - 1, Left, Right));
delete1(Key, _, _L) ->
	erlang:error({key_not_found, Key}).

%% -----------------------------------------------------------------
%%@doc 删除一个元素(有指定key则删除)
%% @spec delete_any(Key::term(), Tree::sb_sets()) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
delete_any(Key, Tree) ->
	delete_any1(Tree, Key, Tree, []).

delete_any1(Tree, Key, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	delete_any1(Tree, Key, Left, [left, Node | L]);
delete_any1(Tree, Key, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	delete_any1(Tree, Key, Right, [right, Node | L]);
delete_any1(_Tree, Key, {Key, _, Size, Left, Right}, L) ->
	node_delete(L, node_delete(Size - 1, Left, Right));
delete_any1(Tree, _, _, _L) ->
	Tree.

%% -----------------------------------------------------------------
%%@doc 移除一个元素，返回移除的元素
%% @spec remove(Key::term(), Tree::sb_sets()) -> return()
%% where
%% return() = none | {El::term(), Tree::sb_sets()}
%%@end
%% -----------------------------------------------------------------
remove(Key, Tree) ->
	remove1(Tree, Key, Tree, []).

remove1(Tree, Key, {Key1, _, _, Left, _} = Node, L) when Key < Key1 ->
	remove1(Tree, Key, Left, [left, Node | L]);
remove1(Tree, Key, {Key1, _, _, _, Right} = Node, L) when Key > Key1 ->
	remove1(Tree, Key, Right, [right, Node | L]);
remove1(_Tree, Key, {Key, El, Size, Left, Right}, L) ->
	{{Key, El}, node_delete(L, node_delete(Size - 1, Left, Right))};
remove1(_Tree, _, _, _L) ->
	none.

%% -----------------------------------------------------------------
%%@doc 从树中取出最小的元素
%% @spec take_smallest(Tree::sb_sets()) -> return()
%% where
%%	return() = {El::term(), Tree2::sb_sets()}
%%@end
%% -----------------------------------------------------------------
take_smallest(Tree) ->
	El = smallest(Tree),
	{El, take_smallest1(Tree)}.

take_smallest1({K, El, S, Left, Right}) when is_tuple(Left) ->
	maintain_right(K, El, S - 1, take_smallest1(Left), Right);
take_smallest1(_) ->
	?NIL.

%% -----------------------------------------------------------------
%%@doc 从树中取出最大的元素
%% @spec take_largest(Tree::sb_sets()) -> return()
%% where
%%	return() = {El::term(), Tree2::sb_sets()}
%%@end
%% -----------------------------------------------------------------
take_largest(Tree) ->
	El = largest(Tree),
	{El, take_largest1(Tree)}.

take_largest1({K, El, S, Left, Right}) when is_tuple(Right) ->
	maintain_left(K, El, S - 1, Left, take_largest1(Right));
take_largest1(_) ->
	?NIL.

%% -----------------------------------------------------------------
%%@doc 从元素的列表填充指定的树
%% @spec from_list(L) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
from_list(L) ->
	from_order(lists:sort(L)).

%% -----------------------------------------------------------------
%%@doc 从元素的列表填充指定的树
%% @spec from_list(ElList::[El::term()],Tree::sb_sets()) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
from_list([El | T], Tree) ->
	from_list(T, enter1(Tree, key(El), El, Tree, []));
from_list([], Tree) ->
	Tree.

%% -----------------------------------------------------------------
%%@doc 从有序的元素的列表构建一棵树
%% @spec from_order(ElList::[El::term()]) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
from_order(L) ->
	{Tree, _} = from_order(L, length(L)),
	Tree.

from_order(L, Size) when Size > 1 ->
	S = Size - 1,
	S2 = S div 2,
	S1 = S - S2,
	{T1, [El | L1]} = from_order(L, S1),
	{T2, L2} = from_order(L1, S2),
	{{key(El), El, Size, T1, T2}, L2};
from_order([El | L], 1) ->
	{{key(El), El, 1, ?NIL, ?NIL}, L};
from_order(L, 0) ->
	{?NIL, L}.

%% -----------------------------------------------------------------
%%@doc 将指定的树转换为有序列表
%% @spec to_list(Tree::sb_sets()) -> [El::term()]
%%@end
%% -----------------------------------------------------------------
to_list({_, El, _, Left, Right}) ->
	to_list(Left, [El | to_list(Right, [])]);
to_list(_) ->
	[].

%% -----------------------------------------------------------------
%%@doc 将指定的树填充到指定列表
%%```
%% 返回结果为[El::term()] ++ L
%%'''
%% @spec to_list(Tree::sb_sets(),L::list()) -> list()
%%@end
%% -----------------------------------------------------------------
to_list({_, El, _, Left, Right}, L) ->
	to_list(Left, [El | to_list(Right, L)]);
to_list(_, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 获取指定树的迭代器
%% @spec iterator(Tree::sb_sets()) -> iter()
%%@end
%% -----------------------------------------------------------------
iterator(Tree) ->
	iterator1(Tree, []).

iterator1({_, _, _, Left, _} = T, L) ->
	iterator1(Left, [T | L]);
iterator1(_, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 获取指定树从指定关键字开始的迭代器
%% @spec iterator(Key::term(), Tree::sb_sets()) -> iter()
%%@end
%% -----------------------------------------------------------------
iterator(Key, Tree) ->
	iterator1(Key, Tree, []).

iterator1(Key, {Key1, _, _, Left, _} = T, L) when Key < Key1 ->
	iterator1(Key, Left, [T | L]);
iterator1(Key, {Key1, _, _, _, Right}, L) when Key > Key1 ->
	iterator1(Key, Right, L);
iterator1(Key, {Key, _, _, _, _} = T, L) ->
	[T | L];
iterator1(_, _, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 使用迭代器向下遍历
%% @spec next(iter()) -> return()
%% where
%% return() = none | {Val::term(), Iter2::iter()}
%%@end
%% -----------------------------------------------------------------
next([{_, El, _, _, Right} | T]) ->
	{El, iterator1(Right, T)};
next(_) ->
	none.

%% -----------------------------------------------------------------
%%@doc 对所有的元素执行一个相同的操作
%%```
%% F函数的参数分别为(Key,El)
%%'''
%% @spec map(F::function(),Tree::sb_sets()) -> Tree::sb_sets()
%%@end
%% -----------------------------------------------------------------
map(F, {K, El, Size, Left, Right}) ->
	{K, F(El), Size, map(F, Left), map(F, Right)};
map(_, _) ->
	?NIL.

%%%===================LOCAL FUNCTIONS==================
% 获得元素的键
key(El) when is_tuple(El) andalso tuple_size(El) > 0 ->
	element(1, El);
key(El) ->
	El.

% 节点插入操作
node_insert([left, {K, V, S, _, R} | T], Node) ->
	node_insert(T, maintain_left(K, V, S + 1, Node, R));
node_insert([right, {K, V, S, L, _} | T], Node) ->
	node_insert(T, maintain_right(K, V, S + 1, L, Node));
node_insert([], Node) ->
	Node.

% Maintain操作，Maintain(T)用于修复以T为根的 SBT。调用Maintain(T)的前提条件是T的子树都已经是SBT。
% 左节点增加大小，Maintain操作
maintain_left(Key, El, Size, {_, _, _, {_, _, LLS, _, _}, _} = Left, {_, _, RS, _, _} = Right) when LLS > RS ->
	right_ratote(Key, El, Size, Left, Right);
maintain_left(Key, El, Size, {LK, LV, LS, LL, {_, _, LRS, _, _} = LR}, {_, _, RS, _, _} = Right) when LRS > RS ->
	right_ratote(Key, El, Size, left_ratote(LK, LV, LS, LL, LR), Right);
maintain_left(Key, El, Size, {_, _, LS, _, _} = Left, ?NIL) when LS > 1 ->
	right_ratote(Key, El, Size, Left, ?NIL);
maintain_left(Key, El, Size, Left, Right) ->
	{Key, El, Size, Left, Right}.

% 右节点增加大小，Maintain操作
maintain_right(Key, El, Size, {_, _, LS, _, _} = Left, {_, _, _, _, {_, _, RRS, _, _}} = Right) when RRS > LS ->
	left_ratote(Key, El, Size, Left, Right);
maintain_right(Key, El, Size, {_, _, LS, _, _} = Left, {RK, RV, RS, {_, _, RLS, _, _} = RL, RR}) when RLS > LS ->
	left_ratote(Key, El, Size, Left, right_ratote(RK, RV, RS, RL, RR));
maintain_right(Key, El, Size, ?NIL, {_, _, RS, _, _} = Right) when RS > 1 ->
	left_ratote(Key, El, Size, ?NIL, Right);
maintain_right(Key, El, Size, Left, Right) ->
	{Key, El, Size, Left, Right}.

% 节点左旋
left_ratote(Key, El, Size, Left, {RK, RV, _, RL, RR}) ->
	LSize = case Left of
		{_, _, LS, _, _} -> LS;
		_ -> 0
	end,
	RSize = case RL of
		{_, _, RLS, _, _} -> RLS;
		_ -> 0
	end,
	{RK, RV, Size, {Key, El, LSize + RSize + 1, Left, RL}, RR}.

% 节点右旋
right_ratote(Key, El, Size, {LK, LV, _, LL, LR}, Right) ->
	LSize = case LR of
		{_, _, LRS, _, _} -> LRS;
		_ -> 0
	end,
	RSize = case Right of
		{_, _, RS, _, _} -> RS;
		_ -> 0
	end,
	{LK, LV, Size, LL, {Key, El, LSize + RSize + 1, LR, Right}}.

% 节点更新操作
node_update([left, {K, V, S, _, R} | T], Node) ->
	node_update(T, {K, V, S, Node, R});
node_update([right, {K, V, S, L, _} | T], Node) ->
	node_update(T, {K, V, S, L, Node});
node_update([], Node) ->
	Node.

% 节点删除操作，选Size大的子树旋转，旋转到叶子节点，然后删除
node_delete(Size, {LK, LV, LS, LL, LR}, {_, _, RS, _, _} = Right) when LS > RS ->
	LSize = case LR of
		{_, _, LRS, _, _} -> LRS;
		_ -> 0
	end,
	maintain_right(LK, LV, Size, LL, node_delete(LSize + RS, LR, Right));
node_delete(Size, {_, _, LS, _, _} = Left, {RK, RV, _, RL, RR}) ->
	RSize = case RL of
		{_, _, RLS, _, _} -> RLS;
		_ -> 0
	end,
	maintain_left(RK, RV, Size, node_delete(LS + RSize, Left, RL), RR);
node_delete(_,  Left, _) when is_tuple(Left) ->
	Left;
node_delete(_, _, Right) ->
	Right.

% 节点删除操作
node_delete([left, {K, V, S, _, R} | T], Node) ->
	node_delete(T, maintain_right(K, V, S - 1, Node, R));
node_delete([right, {K, V, S, L, _} | T], Node) ->
	node_delete(T, maintain_left(K, V, S - 1, L, Node));
node_delete([], Node) ->
	Node.
