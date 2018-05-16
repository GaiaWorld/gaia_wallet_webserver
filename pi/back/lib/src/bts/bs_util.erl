%% Author: Administrator
%% Created: 2012-12-4
%% Description: TODO: Add description to bs_util
-module(bs_util).

%%
%% Include files
%%
-include("idxlib.hrl").
-include("bs_trees.hrl").
-include("new_btsm.hrl").

%%
%% Exported Functions
%%
-export([mapping_frees/2, recover_frees/2, 
		 ext/2, compute_t/1, 
		 lower_bound/1, upper_bound/1, 
		 node_size/2, node_size1/2, 
		 tree_count/1, tree_count2/2, child_count/2, modify_count/3, add_count/2, deduct_count/2,
		 copy/7,
		 node_alloc/3, node_alloc1/3, 
		 write_ok/2, free_point/4,
		 is_split/2, split_tuple/2, split_tuple1/2,
		 is_merge/1, is_merge/2, left/2, right/2, delete_tuple/2, merge_tuple/2, merge_tuple/3,
		 get_child/8, prev_node/5, batch_prev_node/5, next_node/5, batch_next_node/5, left_node/8, right_node/8, move_to_parent/3, prev_element/2, next_element/2]).

%%
%% API Functions
%%

%%填充分配器的记录
mapping_frees(FreeBlocks, Alloter) ->
	case bs_alloter:from_free(Alloter, FreeBlocks) of
		ok ->
			ok;
		error ->
			erlang:error({mapping_frees_failed, FreeBlocks})
	end.

%%恢复空闲列表
recover_frees([{Cmd, Size} = Opcode|T], Alloter) when Cmd =:= ?IDX_ALLOC_CODE; Cmd =:= ?DAT_ALLOC_CODE ->
	case bs_alloter:alloc(Alloter, Size) of
		{_, _} ->
			recover_frees(T, Alloter);
		?EMPTY ->
			erlang:error({alloc_opcode_failed, Opcode})
	end;
recover_frees([{_, {_, _} = Point} = Opcode|T], Alloter) ->
	case bs_alloter:free(Alloter, Point) of
		{ok, _} ->
			recover_frees(T, Alloter);
		?EMPTY ->
			erlang:error({free_opcode_failed, Opcode})
	end;
recover_frees([], _) ->
	ok.

%%简单牛顿法开n次方
ext(2, N) when is_integer(N), N > 0 ->
	math:sqrt(N);
ext(P, N) when is_integer(P), is_integer(N), P > 1, N > 0 ->
	ext(P, N, ext(P - 1, N)). 

%%计算B树的因子
compute_t(Size) ->
	case ?BS_BASE_NODE_SIZE div (Size + ?ENTRY_STRUCT_SIZE) of
		T when T < ?BS_MIN_T; T >= ?BS_MAX_T ->
			?BS_MAX_T;
		T when (T rem 2) =/= 0 ->
			T + 1;
		T ->
			T
	end.

%%获取项数下界
lower_bound(T) ->
	T - 1.

%%获取项数上界
upper_bound(T) ->
	T bsl 1 - 1.

%%计算节点大小
node_size(Node, IsCheckSum) ->
	case IsCheckSum of
		true ->
			erlang:external_size(Node) + 8;
		false ->
			erlang:external_size(Node) + 4
	end.

%%计算节点大小
node_size1(Node, IsCheckSum) ->
	case IsCheckSum of
		true ->
			Bin=term_to_binary(Node),
			{size(Bin) + 8, Bin};
		false ->
			Bin=term_to_binary(Node),
			{size(Bin) + 4, Bin}
	end.

%%获取项数
tree_count({?NON_LEAF_NODE, KVS, Childs}) ->
	tuple_size(KVS) + tree_count(tuple_to_list(Childs), 0);
tree_count({?LEAF_NODE, KVS}) ->
	tuple_size(KVS).

%%获取项数
tree_count2(LNode, RNode) ->
	tree_count(LNode) + tree_count(RNode).

%%获取子节点的项数
child_count({?NON_LEAF_NODE, _, Childs}, Index) ->
	{_, Size}=element(Index, Childs),
	Size;
child_count({?LEAF_NODE, _}, _Index) ->
	0.

%%修改项数
modify_count({Type, KVS, Points}, Index, Count) ->
	{Point, Size}=element(Index, Points),
	{Type, KVS, setelement(Index, Points, {Point, Size + Count})};
modify_count({Type, KVS}, _Index, _Count) ->
	{Type, KVS}.

%%增加项数
add_count(Node, Index) ->
	modify_count(Node, Index, 1).

%%减少项数
deduct_count(Node, Index) ->
	modify_count(Node, Index, -1).

%%获取指定节点的拷贝
copy(Src, {Cache, _, _, _, _, _, _} = Handle, Reader, IntTab, {M, F, A}, Point, IntState) ->
	case zm_cache:get(Cache, Point) of
		{ok, _} = N ->
			N;
		none when IntState =/= [] ->
			ets:insert(IntTab, {Src, IntState}),
			M:F(A, Src, Reader, Handle, Point);
		none ->
			erlang:error({error, root_not_exist})
	end.

%%分配存储空间
node_alloc(Alloter, Node, IsCheckSum) ->
	case bs_alloter:alloc(Alloter, node_size(Node, IsCheckSum)) of
		{{_, Offset} = Point, _} ->
			{{bs_alloter:size(Point), Offset}, Node};
		?EMPTY ->
			erlang:error({error, not_enough})
	end.

%%分配存储空间
node_alloc1(Alloter, Node, IsCheckSum) ->
	{Size, Bin}=node_size1(Node, IsCheckSum),
	case bs_alloter:alloc(Alloter, Size) of
		{{_, Offset} = Point, _} ->
			{{{bs_alloter:size(Point), Offset}, Node}, {?IDX_ALLOC_CODE, Size}, Bin};
		?EMPTY ->
			erlang:error({error, not_enough})
	end.

%%写成功后的处理
write_ok({Cache, _, _, _, _, _, _}, Nodes) ->
	add_copys(Nodes, Cache);
write_ok(Handle, [{{NewRoot, _}, _}|_]) ->
	erlang:error({badarg, Handle, NewRoot}).

%%释放存储空间
free_point([{_, Offset} = Point|T], Alloter, Cache, Codes) ->
	RealPoint={bs_alloter:pos(Point), Offset},
	case bs_alloter:free(Alloter, RealPoint) of
		{ok, _} ->
			free_point(T, Alloter, Cache, [{?IDX_FREE_CODE, RealPoint}|Codes]);
		?EMPTY ->
			erlang:error({free_node_failed, Alloter, Point})
	end;
free_point([?EMPTY|T], Alloter, Cache, Codes) ->
	free_point(T, Alloter, Cache, Codes);
free_point([], _Alloter, _Cache, Codes) ->
	Codes.

%%判断是否需要分裂
is_split(T, {?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) >= upper_bound(T);
is_split(T, {?LEAF_NODE, KVS}) ->
	tuple_size(KVS) >= upper_bound(T).

%%从指定位置分裂
split_tuple(Tuple, Index) when size(Tuple) > Index ->
	{L, [H|T]}=lists:split(Index, tuple_to_list(Tuple)),
	{list_to_tuple(L), H, list_to_tuple(T)}.

%%从指定位置分裂
split_tuple1(Tuple, Index) when size(Tuple) > Index ->
	{L, R}=lists:split(Index, tuple_to_list(Tuple)),
	{list_to_tuple(L), list_to_tuple(R)}.

%%判断根节点是否需要合并
is_merge({?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) =< 1;
is_merge({?LEAF_NODE, _KVS}) ->
	false.

%%判断子节点是否需要合并
is_merge(T, {?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) =< lower_bound(T);
is_merge(T, {?LEAF_NODE, KVS}) ->
	tuple_size(KVS) =< lower_bound(T).

%%获取左节点
left({?NON_LEAF_NODE, _, Childs}, ChildIndex) when ChildIndex > 1 ->
	{Child, _}=element(ChildIndex - 1, Childs),
	{ok, Child};
left({?NON_LEAF_NODE, _, _}, _ChildIndex) ->
	out_of_index;
left({?LEAF_NODE, _}, _ChildIndex) ->
	?EMPTY.

%%获取右节点
right({?NON_LEAF_NODE, _, Childs}, ChildIndex) when ChildIndex < tuple_size(Childs) ->
	{Child, _}=element(ChildIndex + 1, Childs),
	{ok, Child};
right({?NON_LEAF_NODE, _, _}, _ChildIndex) ->
	out_of_index;
right({?LEAF_NODE, _}, _ChildIndex) ->
	?EMPTY.

%%删除项
delete_tuple(Tuple, Index) ->
	{Left, [H|Right]}=lists:split(Index - 1, tuple_to_list(Tuple)),
	{H, list_to_tuple(Left ++ Right)}.

%%合并
merge_tuple(Left, KV, Right) ->
	list_to_tuple(tuple_to_list(Left) ++ [KV|tuple_to_list(Right)]).

%%合并
merge_tuple(Left, Right) ->
	list_to_tuple(tuple_to_list(Left) ++ tuple_to_list(Right)).

%%获取子节点
get_child(Src, Handle, Reader, IntTab, ReadInt, Parent, Index, IntState) ->
	case Parent of
		{?NON_LEAF_NODE, _, Childs} ->
			{ChildPoint, _}=element(Index, Childs),
			case copy(Src, Handle, Reader, IntTab, ReadInt, ChildPoint, IntState) of
				{ok, Child}  ->
					{ok, ChildPoint, Child};
				E ->
					E
			end;
		{?LEAF_NODE, _} ->
			?EMPTY
	end.

%%获取前趋项
prev_node(Src, {Cache, IntTab, _, {M, F, A}, _, _, _} = Handle, Reader, Point, IntState) ->
	case zm_cache:get(Cache, Point) of
		{ok, {?NON_LEAF_NODE, _, Childs}} ->
			{ChildPoint, _}=element(tuple_size(Childs), Childs),
			prev_node(Src, Handle, Reader, ChildPoint, IntState);
		{ok, {?LEAF_NODE, KVS}} ->
			element(tuple_size(KVS), KVS);
		none when IntState =/= [] ->
			ets:insert(IntTab, {Src, IntState}),
			M:F(A, Src, Reader, Handle, Point);
		none ->
			erlang:error({error, root_not_exist})
	end.

%%批量获取前趋项
batch_prev_node(Src, {Cache, IntTab, _, {M, F, A}, _, _, _} = Handle, Reader, Point, IntState) ->
	case zm_cache:get(Cache, Point) of
		{ok, {?NON_LEAF_NODE, _, Childs}} ->
			{ChildPoint, _}=element(tuple_size(Childs), Childs),
			batch_prev_node(Src, Handle, Reader, ChildPoint, IntState);
		{ok, {?LEAF_NODE, KVS}} ->
			tuple_to_list(KVS);
		none when IntState =/= [] ->
			ets:insert(IntTab, {Src, IntState}),
			M:F(A, Src, Reader, Handle, Point);
		none ->
			erlang:error({error, root_not_exist})
	end.

%%获取后继项
next_node(Src, {Cache, IntTab, _, {M, F, A}, _, _, _} = Handle, Reader, Point, IntState) ->
	case zm_cache:get(Cache, Point) of
		{ok, {?NON_LEAF_NODE, _, Childs}} ->
			{ChildPoint, _}=element(1, Childs),
			next_node(Src, Handle, Reader, ChildPoint, IntState);
		{ok, {?LEAF_NODE, KVS}} ->
			element(1, KVS);
		none when IntState =/= [] ->
			ets:insert(IntTab, {Src, IntState}),
			M:F(A, Src, Reader, Handle, Point);
		none ->
			erlang:error({error, root_not_exist})
	end.

%%批量获取后继项
batch_next_node(Src, {Cache, IntTab, _, {M, F, A}, _, _, _} = Handle, Reader, Point, IntState) ->
	case zm_cache:get(Cache, Point) of
		{ok, {?NON_LEAF_NODE, _, Childs}} ->
			{ChildPoint, _}=element(1, Childs),
			batch_next_node(Src, Handle, Reader, ChildPoint, IntState);
		{ok, {?LEAF_NODE, KVS}} ->
			tuple_to_list(KVS);
		none when IntState =/= [] ->
			ets:insert(IntTab, {Src, IntState}),
			M:F(A, Src, Reader, Handle, Point);
		none ->
			erlang:error({error, root_not_exist})
	end.

%%获取左节点
left_node(Src, Handle, Reader, IntTab, ReadInt, Node, ChildIndex, IntState) ->
	case left(Node, ChildIndex) of
		{ok, LPoint} ->
			case copy(Src, Handle, Reader, IntTab, ReadInt, LPoint, IntState) of
				{ok, LCopy} ->
					{ok, LPoint, LCopy};
				E ->
					E
			end;
		E ->
			E
	end.

%%获取右节点
right_node(Src, Handle, Reader, IntTab, ReadInt, Node, ChildIndex, IntState) ->
	case right(Node, ChildIndex) of
		{ok, RPoint} ->
			case copy(Src, Handle, Reader, IntTab, ReadInt, RPoint, IntState) of
				{ok, RCopy} ->
					{ok, RPoint, RCopy};
				E ->
					E
			end;
		E ->
			E
	end.

%%移动到上级节点
move_to_parent(KeyIndex, KVS, KV) ->
	{element(KeyIndex, KVS), 
	setelement(KeyIndex, KVS, KV)}.

%%获取以前的所有项
prev_element(Index, Tuple) ->
	element(1, lists:split(Index - 1, tuple_to_list(Tuple))).

%%获取以后的所有项
next_element(Index, Tuple) ->
	element(2, lists:split(Index, tuple_to_list(Tuple))).

%%
%% Local Functions
%%

ext(P, N, Guess) ->
	K=P - 1,
	case (Guess * K + N / math:pow(Guess, K)) / P of
		NewGuess when Guess - NewGuess >= 1 ->
			ext(P, N, (Guess * K + N / math:pow(Guess, K)) / P);
		NewGuess ->
			NewGuess
	end.

add_copys(Nodes, Cache) ->
	zm_cache:sets(Cache, Nodes, 1).

tree_count([{_, Size}|T], Total) ->
	tree_count(T, Total + Size);
tree_count([], Total) ->
	Total.