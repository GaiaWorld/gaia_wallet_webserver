%% @author Administrator
%% @doc 有缓存的B-tree，节点写时复制，对数据提供统一的查询，插入和删除接口，支持定长和不定长的节点
%% KVS的结构:[{Key, Value, Vsn, Time}|T]
%%

-module(b_trees).

%%
%% Include files
%%
-include("bs_trees.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([new/6, get_root/1, set_root/2, get_cache/1, get_alloter/1, get_free_blocks/1, set_commited/2, collate/1]).
-export([continue/4, count/1, first/1, last/1, prev/2, next/2, lookup/2, save/2, insert/4, update/4, delete/3, rank/2, by_rank/2]).
-compile({inline, [
				   compute_t/1, node_size/2, node_alloc/3, lower_bound/1, upper_bound/1, kvs_count/2, kvs_count/1, get_node/4, 
				   min_key/2,
				   max_key/2,
				   filter_key/2, search_max/2, search_prev_node/4, search_prev/4,
				   search_min/2, search_next_node/4, search_next/4,
				   lookup_key/2, lookup_node/3, lookup_key_/2,
				   is_split/2, split_kvs/2, split_child/2, split_node/4, insert_node/6, leaf_alloc_insert/4, non_leaf_alloc_insert/6, new_kv/2,
				   update_entry/6, update_node/5, alloc_update/5,
				   is_remove_root/2, is_merge/2, is_move/2, merge/2, merge/3, merge_node/3,  
				   search_max_/2, search_min_/2, delete_prev/14, delete_next/15, delete_non_leaf/7, left_buddy_handle/17, right_buddy_handle/17, delete_continue/9,
				   left_move/4, right_move/4, childs_merged/18, left_buddy_merge/18, right_buddy_merge/18, delete_node/6, leaf_alloc_delete/5, non_leaf_alloc_delete/6, 
				   childs_count/2, childs_count1/3, rank_continue/6, rank_node/4,
				   childs_rank/1, childs_rank1/3, by_rank_continue/3, by_rank_node/3
				  ]}).
-compile({inline_size, 32}).
-compile(export_all). %临时使用，调试完成后删除

%%
%%构建一个指定缓存大小的B树
%%
new({Mod, Fun, Args}, {M, Type}, Size, Capacity, Sizeout, Root, FreeBlocks, Timeout) ->
	T=compute_t(Size),
	Cache=M:new(Type, Capacity, Sizeout, Timeout),
	Alloter=case Size of
				0 ->
					bs_alloter:new(?BS_MAX_STORAGE_SIZE, Size, FreeBlocks);
				_ ->
					bs_alloter:new(?BS_MAX_STORAGE_SIZE, ?NODE_STRUCT_SIZE(upper_bound(T), Size), FreeBlocks)
	end,
	Reader=new_reader(Mod, Fun, Args),
	io:format("!!!!!!Root:~p, T:~p, Node_Size:~p~n", [Root, T, ?NODE_STRUCT_SIZE(upper_bound(T), Size)]),
	case init(Cache, M, Root) of
		{ok, RootPoint} ->
			{T, RootPoint, Cache, M, Alloter, Reader};
		E ->
			E
	end.

%%
%%构建一个指定缓存大小的B树
%%
new(MFA, MT, Size, Capacity, Sizeout, Timeout) ->
	new(MFA, MT, Size, Capacity, Sizeout, ?EMPTY, [], Timeout).


continue(From, Idx, Handle, Point) ->
	QueueKey={?ASYN_WAIT, Idx, Point},
	{NextFrom, NextType}=case queue:out(get(QueueKey)) of
		{empty, _} ->
			erase(QueueKey),
			{?EMPTY, ?EMPTY};
		{{value, Item}, NewWaitLoadQueue} ->
			put(QueueKey, NewWaitLoadQueue),
			Item
	end,
	case erase({?INTERRUPT_STATE, From}) of
		{{lookup, _}, lookup_node, [Point, Key]} ->
			{lookup_node(Handle, Point, Key), {lookup, Key}, NextFrom, NextType};
		{{prev, _}, search_prev, [Point, Key, Log]} ->
			{search_prev(Handle, Point, Key, Log), {prev, Key}, NextFrom, NextType};
		{{prev, Key}, search_prev_node, [Log, Node, Index]} ->
			{search_prev_node(Handle, Log, Node, Index), {prev, Key}, NextFrom, NextType};
		{{prev, Key}, search_max, [Node]} ->
			{search_max(Handle, Node), {prev, Key}, NextFrom, NextType};
		{{next, _}, search_next, [Point, Key, Log]} ->
			{search_next(Handle, Point, Key, Log), {next, Key}, NextFrom, NextType};
		{{next, Key}, search_next_node, [Log, Node, Index]} ->
			{search_next_node(Handle, Log, Node, Index), {next, Key}, NextFrom, NextType};
		{{next, Key}, search_min, [Node]} ->
			{search_min(Handle, Node), {next, Key}, NextFrom, NextType};
		{first, min_key, [Node]} ->
			{min_key(Handle, Node), first, NextFrom, NextType};
		{last, max_key, [Node]} ->
			{max_key(Handle, Node), last, NextFrom, NextType};
		{{rank, _}, rank_continue, [Childs, KeyIndex, Child, Key, Count]} ->
			{rank_continue(Handle, Childs, KeyIndex, Child, Key, Count), {rank, Key}, NextFrom, NextType};
		{{by_rank, _}, by_rank_continue, [Child, Rank]} ->
			{by_rank_continue(Handle, Child, Rank), {by_rank, Rank}, NextFrom, NextType};
		{{insert, _, _, _, _} = Req, insert_node, [NodeLine, Splits, Key, Value, IsCheckSum]} ->
			{insert_node(Handle, NodeLine, Splits, Key, Value, IsCheckSum), Req, NextFrom, NextType};
		{{update, _, _, _, _} = Req, update_node, [NodeLine, Key, Value, IsCheckSum]} ->
			{update_node(Handle, NodeLine, Key, Value, IsCheckSum), Req, NextFrom, NextType};
		{{delete, _, _, _, _} = Req, delete_non_leaf, [NodeLine, Moves, MergeFrees, KeyIndex, Key, IsCheckSum]} ->
			{delete_non_leaf(Handle, NodeLine, Moves, MergeFrees, KeyIndex, Key, IsCheckSum), Req, NextFrom, NextType};
		{{delete, _, _, _, _} = Req, search_max_, [Node]} ->
			{search_max_(Handle, Node), Req, NextFrom, NextType};
		{{delete, _, _, _, _} = Req, search_min_, [Node]} ->
			{search_min_(Handle, Node), Req, NextFrom, NextType};
		{{delete, _, _, _, _} = Req, delete_continue, [NodeLine, Moves, MergeFrees, Index, Point_, Count, Key, IsCheckSum]} ->
			{delete_continue(Handle, NodeLine, Moves, MergeFrees, Index, Point_, Count, Key, IsCheckSum), Req, NextFrom, NextType};
		{{delete, _, _, _, _} = Req, left_buddy_handle, [LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point_, Node, Count, LIndex, LPoint, LCount, Key, IsCheckSum]} ->
			{left_buddy_handle(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point_, Node, Count, LIndex, LPoint, LCount, Key, IsCheckSum), Req, NextFrom, NextType};
		{{delete, _, _, _, _} = Req, right_buddy_handle, [LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point_, Node, Count, RIndex, RPoint, RCount, Key, IsCheckSum]} ->
			{right_buddy_handle(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point_, Node, Count, RIndex, RPoint, RCount, Key, IsCheckSum), Req, NextFrom, NextType};
		undefined ->
			erlang:error({continue_error, {invalid_interrupt_state, Handle, From}})
	end.

%%
%%查询当前项的数量
%%
count({_, {_, _} = RootPoint, Cache, M, _, _}) ->
	case M:get(Cache, RootPoint) of
		{ok, _, {?NON_LEAF_NODE, KVS, Childs}} ->
			tuple_size(KVS) + lists:sum([ChildCount || {_, ChildCount} <- tuple_to_list(Childs)]);
		{ok, _, {?LEAF_NODE, KVS}} ->
			tuple_size(KVS);
		false ->
			{count_error, {root_node_not_exist, RootPoint}}
	end;
count({_, ?EMPTY, _, _, _, _}) ->
	0.

%%
%%检索首关键字
%%
first({_, {_, _} = RootPoint, Cache, M, _, _} = Handle) ->
	case M:get(Cache, RootPoint) of
		{ok, _, RootNode} ->
			min_key(Handle, RootNode);
		false ->
			{first_error, {root_node_not_exist, RootPoint}}
	end;
first({_, ?EMPTY, _, _, _, _}) ->
	{first_error, root_not_exist}.

%%
%%检索尾关键字
%%
last({_, {_, _} = RootPoint, Cache, M, _, _} = Handle) ->
	case M:get(Cache, RootPoint) of
		{ok, _, RootNode} ->
			max_key(Handle, RootNode);
		false ->
			{last_error, {root_node_not_exist, RootPoint}}
	end;
last({_, ?EMPTY, _, _, _, _}) ->
	{last_error, root_not_exist}.

%%
%%查询指定关键字的前一个关键字
%%
prev({_, {_, _} = RootPoint, _, _, _, _} = Handle, Key) ->
	search_prev(Handle, RootPoint, Key, []);
prev({_, ?EMPTY, _, _, _, _}, _) ->
	{prev_error, root_not_exist}.

%%
%%查询指定关键字的后一个关键字
%%
next({_, {_, _} = RootPoint, _, _, _, _} = Handle, Key) ->
	search_next(Handle, RootPoint, Key, []);
next({_, ?EMPTY, _, _, _, _}, _) ->
	{next_error, root_not_exist}.

%%
%%检索指定关键字的值
%%
lookup({_, {_, _} = RootPoint, _, _, _, _} = Handle, Key) ->
	lookup_node(Handle, RootPoint, Key);
lookup({_, ?EMPTY, _, _, _, _}, _) ->
	{lookup_error, root_not_exist}.

%%
%%保存节点链，用于将写操作产生的新节点链加入缓存
%%
save({_, _, Cache, M, _, _}, Nodes) ->
	M:inserts(Cache, Nodes).
	
%%
%%插入指定关键字的值
%%
insert({T, {_, _} = RootPoint, Cache, M, _, _} = Handle, Key, Value, IsCheckSum) ->
	case M:copy(Cache, RootPoint) of
		{ok, _, RootNode} ->
			case is_split(T, RootNode) of
				false ->
					insert_node(Handle, [{?EMPTY, RootPoint, RootNode, 0}], [?VOID], Key, Value, IsCheckSum);
				true ->
					case split_node(T, ?EMPTY_ROOT_NODE, 1, RootNode) of
						{NewRootNode, {K, _, _, _}, LChildIndex, LNode, LCount, RChildIndex, RNode, RCount} when Key < K ->
							insert_node(Handle, [{LChildIndex, RootPoint, LNode, LCount}, {?EMPTY, ?EMPTY, NewRootNode, 0}], [{RChildIndex, RNode, RCount}, ?VOID], Key, Value, IsCheckSum);
						{NewRootNode, {K, _, _, _}, LChildIndex, LNode, LCount, RChildIndex, RNode, RCount} when Key > K ->
							insert_node(Handle, [{RChildIndex, RootPoint, RNode, RCount}, {?EMPTY, ?EMPTY, NewRootNode, 0}], [{LChildIndex, LNode, LCount}, ?VOID], Key, Value, IsCheckSum)
					end
			end;
		false ->
			{insert_error, {root_node_not_exist, RootPoint}}
	end;
insert({_, ?EMPTY, _, _, _, _} = Handle, Key, Value, IsCheckSum) ->
	insert_node(Handle, [{?EMPTY, ?EMPTY, {?LEAF_NODE, {}}, 0}], [?VOID], Key, Value, IsCheckSum).

%%
%%更新指定关键字的值
%%
update({_, {_, _} = RootPoint, Cache, M, _, _} = Handle, Key, Value, IsCheckSum) ->
	case M:copy(Cache, RootPoint) of
		{ok, _, RootNode} ->
			update_node(Handle, [{?EMPTY, RootPoint, RootNode, 0}], Key, Value, IsCheckSum);
		false ->
			{update_error, {root_node_not_exist, RootPoint}}
	end;
update({_, ?EMPTY, _, _, _, _}, _, _, _) ->
	{update_error, root_not_exist}.

%%
%%删除指定关键字的值
%%
delete({_, {_, _} = RootPoint, Cache, M, _, _} = Handle, Key, IsCheckSum) ->
	case M:copy(Cache, RootPoint) of
		{ok, _, RootNode} ->
			delete_node(Handle, [{?EMPTY, RootPoint, RootNode, 0}], [?VOID], [], Key, IsCheckSum);
		false ->
			{delete_error, {root_node_not_exist, RootPoint}}
	end;
delete({_, ?EMPTY, _, _, _, _}, _, _) ->
	{delete_error, root_not_exist}.

%%
%%根据关键字获取排名
%%
rank({_, {_, _} = RootPoint, Cache, M, _, _} = Handle, Key) ->
	case M:get(Cache, RootPoint) of
		{ok, _, RootNode} ->
			rank_node(Handle, RootNode, Key, 0);
		false ->
			{rank_error, {root_node_not_exist, RootPoint}}
	end;
rank({_, ?EMPTY, _, _, _, _}, _) ->
	{rank_error, root_not_exist}.

%%
%%根据排名获取关键字
%%
by_rank({_, {_, _} = RootPoint, Cache, M, _, _} = Handle, Rank) ->
	case M:get(Cache, RootPoint) of
		{ok, _, RootNode} ->
			by_rank_node(Handle, RootNode, Rank);
		false ->
			{by_rank_error, {root_node_not_exist, RootPoint}}
	end;
by_rank({_, ?EMPTY, _, _, _, _}, _) ->
	{by_rank_error, root_not_exist}.

%%
%%获取句柄中根节点指针
%%
get_root({_, RootPoint, _, _, _, _}) ->
	RootPoint.

%%
%%设置句柄中根节点指针
%%
set_root({T, _, Cache, M, Alloter, Reader}, NewRootPoint) ->
	{T, NewRootPoint, Cache, M, Alloter, Reader}.

%%
%%获取句柄中的缓存模块
%%
get_cache({_, _, Cache, M, _, _}) ->
	{M, Cache};
get_cache(_) ->
	?EMPTY.

%%
%%获取句柄中的空间分配器
%%
get_alloter({_, _, _, _, Alloter, _}) ->
	Alloter;
get_alloter(_) ->
	?EMPTY.

%%
%%获取当前伙伴系统的空闲列表
%%
get_free_blocks({_, _, _, _, Alloter, _}) ->
	bs_alloter:to_free(Alloter);
get_free_blocks(_) ->
	[].

%%
%%将指定节点设置为已提交状态
%%
set_commited({_, _, Cache, M, _, _}, Points) ->
	[?EMPTY || Point <- lists:flatten(Points), M:modify(Cache, Point)],
	ok.
	
%%
%%整理缓存
%%
collate({_, _, Cache, M, _, _}) ->
	M:collate(Cache, z_lib:now_millisecond()).

%% ====================================================================
%% Internal functions
%% ====================================================================

init(Cache, M, {{Pos, Offset} = Point, Node})
  when is_integer(Pos), is_integer(Offset), Pos >= 0, Offset >= 0 ->
	case M:load(Cache, Point, Node) of
		true ->
			{ok, Point};
		E ->
			E
	end;
init(_, _, ?EMPTY) ->
	{ok, ?EMPTY}.

compute_t(Size) ->
	case ?BS_BASE_NODE_SIZE div (Size + ?ENTRY_STRUCT_SIZE) of
		T when T < ?BS_MIN_T; T >= ?BS_MAX_T ->
			?BS_MAX_T; %保证最大的分裂因子
		T when (T rem 2) =/= 0 ->
			T + 1; %保证了偶数，并且不会使项数超过默认节点大小
		T ->
			T
	end.

%%构建一个读取器
new_reader(M, F, A) ->
	fun(_Handle, Point, ContinueFun, ContinueArg) ->
		fun(Pid, Src, From, Type, Idx, Req) ->
				case get({?ASYN_WAIT, Idx, Point}) of
					undefined ->
						put({?ASYN_WAIT, Idx, Point}, queue:new()),
						put({?INTERRUPT_STATE, From}, {Req, ContinueFun, ContinueArg}),
						M:F(Pid, A, Src, From, Type, Idx, Point);
					WaitLoadQueue ->
						put({?ASYN_WAIT, Idx, Point}, queue:in({From, Type}, WaitLoadQueue)),
						put({?INTERRUPT_STATE, From}, {Req, ContinueFun, ContinueArg})
				end
		end
	end.

%%计算节点大小，返回大小
node_size(Node, IsCheckSum) ->
	case IsCheckSum of
		true ->
			erlang:external_size(Node) + 8;
		false ->
			erlang:external_size(Node) + 4
	end.

%%分配节点存储空间
node_alloc({_, _, 0} = Alloter, Node, IsCheckSum) ->
	case bs_alloter:alloc(Alloter, node_size(Node, IsCheckSum)) of
		{{_, Offset} = Point, _} ->
			{{bs_alloter:size(Point), Offset}, Node};
		?EMPTY ->
			erlang:error({error, not_enough})
	end;
node_alloc(Alloter, Node, _) ->
	case bs_alloter:alloc(Alloter, 0) of
		{{_, Offset} = Point, _} ->
			{{bs_alloter:size(Point), Offset}, Node};
		?EMPTY ->
			erlang:error({error, not_enough})
	end.

lower_bound(T) ->
	T - 1.

upper_bound(T) ->
	(T bsl 1) - 1.

kvs_count([{_, Size}|T], Total) ->
	kvs_count(T, Total + Size);
kvs_count([], Total) ->
	Total.

kvs_count({?NON_LEAF_NODE, KVS, Childs}) ->
	tuple_size(KVS) + kvs_count(tuple_to_list(Childs), 0);
kvs_count({?LEAF_NODE, KVS}) ->
	tuple_size(KVS).

get_node({_, _, Cache, M, _, Reader} = Handle, Point, F, A) ->
	case M:get(Cache, Point) of
		{ok, _, Node} ->
			{ok, Node};
		false ->
			{?ASYN_WAIT, Reader(Handle, Point, F, A)}
	end.

copy_node({_, _, Cache, M, _, Reader} = Handle, Point, F, A) ->
	case M:copy(Cache, Point) of
		{ok, _, Node} ->
			{ok, Node};
		false ->
			{?ASYN_WAIT, Reader(Handle, Point, F, A)}
	end.

lookup_key({?NON_LEAF_NODE, KVS, Childs}, Key) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{-1, Index} ->
			{Child, _}=element(Index, Childs),
			{-1, Index, Child};
		{1, Index} ->
			Right=Index + 1,
			{Child, _}=element(Right, Childs),
			{1, Right, Child};
		{0, Index} ->
			{Key, Value, Vsn, Time} = element(Index, KVS),
			{found, Index, {Value, Vsn, Time}}
	end;
lookup_key({?LEAF_NODE, KVS}, Key) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{0, Index} ->
			{Key, Value, Vsn, Time} = element(Index, KVS),
			{found, Index, {Value, Vsn, Time}};
		{_, _} ->
			?EMPTY
	end.

lookup_key_({?NON_LEAF_NODE, KVS, Childs}, Key) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{-1, Index} ->
			{-1, Index, element(Index, Childs)};
		{1, Index} ->
			Right=Index + 1,
			{1, Right, element(Right, Childs)};
		{0, Index} ->
			{Key, Value, Vsn, Time} = element(Index, KVS),
			{found, Index, {Value, Vsn, Time}}
	end;
lookup_key_({?LEAF_NODE, KVS}, Key) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{0, Index} ->
			{Key, Value, Vsn, Time} = element(Index, KVS),
			{found, Index, {Value, Vsn, Time}};
		{_, _} ->
			?EMPTY
	end.

to_kvt({Key, _, Vsn, Time}) ->
	{Key, Vsn, Time}.

min_key(Handle, {?NON_LEAF_NODE, _, Childs} = Node) ->
	case get_node(Handle, element(1, element(1, Childs)), min_key, [Node]) of
		{ok, ChildNode} ->
			min_key(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
min_key(_, {?LEAF_NODE, KVS}) ->
	{ok, to_kvt(element(1, KVS))}.

max_key(Handle, {?NON_LEAF_NODE, _, Childs} = Node) ->
	case get_node(Handle, element(1, element(tuple_size(Childs), Childs)), max_key, [Node]) of
		{ok, ChildNode} ->
			max_key(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
max_key(_, {?LEAF_NODE, KVS}) ->
	{ok, to_kvt(element(tuple_size(KVS), KVS))}.

filter_key(KVS) -> filter_key(KVS, []).

filter_key([{Key, _, Vsn, Time}|T], Keys) ->
    filter_key(T, [{Key, Vsn, Time}|Keys]);
filter_key([], Keys) ->
    Keys.

search_max(Handle, {?NON_LEAF_NODE, _, Childs} = Node) ->
	case get_node(Handle, element(1, element(tuple_size(Childs), Childs)), search_max, [Node]) of
		{ok, ChildNode} ->
			search_max(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
search_max(_, {?LEAF_NODE, KVS}) ->
	{ok, filter_key(tuple_to_list(KVS))}.

search_prev_node(Handle, Log, {?NON_LEAF_NODE, _, Childs} = Node, Index) ->
	case get_node(Handle, element(1, element(Index, Childs)), search_prev_node, [Log, Node, Index]) of
		{ok, ChildNode} ->
			search_max(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
search_prev_node(Handle, Log, {?LEAF_NODE, KVS} = Node, Index) ->
	if
		(Index > 1) and (Index =< tuple_size(KVS)) ->
			{ok, filter_key(element(1, lists:split(Index - 1, tuple_to_list(KVS))))};
		true ->
			case Log of
				{Point, ChildIndex} ->
					case get_node(Handle, Point, search_prev_node, [Log, Node, Index]) of
						{ok, {_, KVS_P, _}} ->
							{ok, [to_kvt(element(ChildIndex - 1, KVS_P))]};
						AsynReader ->
							AsynReader
					end;
				_ ->
					{search_prev_node_error, ?BS_BOF}
			end
	end.

search_prev(Handle, Point, Key, Log) ->
	case get_node(Handle, Point, search_prev, [Point, Key, Log]) of
		{ok, Node} ->
			case lookup_key(Node, Key) of
				{found, Index, _} ->
					search_prev_node(Handle, Log, Node, Index);
				{_, 1, ChildPoint} ->
					search_prev(Handle, ChildPoint, Key, Log);
				{_, ChildIndex, ChildPoint} ->
					search_prev(Handle, ChildPoint, Key, {Point, ChildIndex});
				?EMPTY ->
					{search_prev_error, key_not_exist}
			end;
		AsynReader ->
			AsynReader
	end.

search_min(Handle, {?NON_LEAF_NODE, _, Childs} = Node) ->
	case get_node(Handle, element(1, element(1, Childs)), search_min, [Node]) of
		{ok, ChildNode} ->
			search_min(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
search_min(_, {?LEAF_NODE, KVS}) ->
	{ok, lists:reverse(filter_key(tuple_to_list(KVS)))}.

search_next_node(Handle, Log, {?NON_LEAF_NODE, _, Childs} = Node, Index) ->
	case get_node(Handle, element(1, element(Index + 1, Childs)), search_next_node, [Log, Node, Index]) of
		{ok, ChildNode} ->
			search_min(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
search_next_node(Handle, Log, {?LEAF_NODE, KVS} = Node, Index) ->
	if
		(Index >= 1) and (Index < tuple_size(KVS)) ->
			{ok, lists:reverse(filter_key(element(2, lists:split(Index, tuple_to_list(KVS)))))};
		true ->
			case Log of
				{Point, ChildIndex} ->
					case get_node(Handle, Point, search_next_node, [Log, Node, Index]) of
						{ok, {_, KVS_P, _}} ->
							{ok, [to_kvt(element(ChildIndex, KVS_P))]};
						AsynReader ->
							AsynReader
					end;
				_ ->
					{search_next_node_error, ?BS_EOF}
			end
	end.

search_next(Handle, Point, Key, Log) ->
	case get_node(Handle, Point, search_next, [Point, Key, Log]) of
		{ok, Node} ->
			case lookup_key(Node, Key) of
				{found, Index, _} ->
					search_next_node(Handle, Log, Node, Index);
				{_, ChildIndex, ChildPoint} ->
					{_, _, Childs}=Node,
					case tuple_size(Childs) of
						ChildIndex ->
							search_next(Handle, ChildPoint, Key, Log);
						_ ->
							search_next(Handle, ChildPoint, Key, {Point, ChildIndex})
					end;
				?EMPTY ->
					{search_next_error, key_not_exist}
			end;
		AsynReader ->
			AsynReader
	end.


lookup_node(Handle, Point, Key) ->
	case get_node(Handle, Point, lookup_node, [Point, Key]) of
		{ok, Node} ->
			case lookup_key(Node, Key) of
				{found, _, Value} ->
					{ok, Value};
				{_, _, ChildPoint}  ->
					lookup_node(Handle, ChildPoint, Key);
				?EMPTY ->
					{lookup_node_error, key_not_exist}
			end;
		AsynReader ->
			AsynReader
	end.

is_split(T, {?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) >= upper_bound(T);
is_split(T, {?LEAF_NODE, KVS}) ->
	tuple_size(KVS) >= upper_bound(T).

split_kvs(Tuple, Index) when tuple_size(Tuple) > Index ->
	{L, [H|T]}=lists:split(Index, tuple_to_list(Tuple)),
	{list_to_tuple(L), H, list_to_tuple(T)}.

split_child(Tuple, Index) when tuple_size(Tuple) > Index ->
	{L, R}=lists:split(Index, tuple_to_list(Tuple)),
	{list_to_tuple(L), list_to_tuple(R)}.

split_node(T, {?NON_LEAF_NODE, PKVS, PChilds}, Index, {?NON_LEAF_NODE, KVS, Childs}) ->
	NewIndex=Index + 1,
	{LKVS, KV, RKVS}=split_kvs(KVS, lower_bound(T)),
	{LChilds, RChilds}=split_child(Childs, T),
	LNode={?NON_LEAF_NODE, LKVS, LChilds},
	RNode={?NON_LEAF_NODE, RKVS, RChilds},
	{{?NON_LEAF_NODE, erlang:insert_element(Index, PKVS, KV), erlang:insert_element(NewIndex, PChilds, ?PLACE_CHILD)}, 
	 KV, Index, LNode, kvs_count(LNode), NewIndex, RNode, kvs_count(RNode)};
split_node(T, {?NON_LEAF_NODE, PKVS, PChilds}, Index, {?LEAF_NODE, KVS}) ->
	NewIndex=Index + 1,
	{LKVS, KV, RKVS}=split_kvs(KVS, lower_bound(T)),
	{{?NON_LEAF_NODE, erlang:insert_element(Index, PKVS, KV), erlang:insert_element(NewIndex, PChilds, ?PLACE_CHILD)}, 
	 KV, Index, {?LEAF_NODE, LKVS}, tuple_size(LKVS), NewIndex, {?LEAF_NODE, RKVS}, tuple_size(RKVS)}.

new_kv(Key, Value) ->
	{Key, Value, ?INSERT_VERSION, z_lib:now_millisecond()}.

insert_node({T, _, _, _, _, _} = Handle, [{Index, Point, {?NON_LEAF_NODE, _, _} = Node, Count}|LT] = NodeLine, Splits, Key, Value, IsCheckSum) ->
	case lookup_key_(Node, Key) of
		{found, _, _} ->
			{insert_node_error, {key_exist, Key}};
		{_, ChildIndex, {Child, ChildCount}} ->
			case copy_node(Handle, Child, insert_node, [NodeLine, Splits, Key, Value, IsCheckSum]) of
				{ok, ChildNode} ->
					case is_split(T, ChildNode) of
						false ->
							insert_node(Handle, [{ChildIndex, Child, ChildNode, ChildCount}|NodeLine], [?VOID|Splits], Key, Value, IsCheckSum);
						true ->
							case split_node(T, Node, ChildIndex, ChildNode) of
								{NewNode, {K, _, _, _}, LChildIndex, LNode, LCount, RChildIndex, RNode, RCount} when Key < K ->
									insert_node(Handle,	[{LChildIndex, Child, LNode, LCount}, {Index, Point, NewNode, Count}|LT], [{RChildIndex, RNode, RCount}|Splits], Key, Value, IsCheckSum);
								{NewNode, {K, _, _, _}, LChildIndex, LNode, LCount, RChildIndex, RNode, RCount} when Key > K ->
									insert_node(Handle,	[{RChildIndex, Child, RNode, RCount}, {Index, Point, NewNode, Count}|LT], [{LChildIndex, LNode, LCount}|Splits], Key, Value, IsCheckSum)
							end
					end;
				AsynReader ->
					AsynReader
			end
	end;
insert_node({_, _, _, _, Alloter, _}, [{Index, Point, {?LEAF_NODE, KVS}, Count}|LT], Splits, Key, Value, IsCheckSum) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{0, _} ->
			{insert_node_error, {key_exist, Key}};
		{-1, KVIndex} ->
			{ok, leaf_alloc_insert([{Index, Point, {?LEAF_NODE, erlang:insert_element(KVIndex, KVS, new_kv(Key, Value))}, Count}|LT], Splits, Alloter, IsCheckSum)};
		{1, KVIndex} ->
			{ok, leaf_alloc_insert([{Index, Point, {?LEAF_NODE, erlang:insert_element(KVIndex + 1, KVS, new_kv(Key, Value))}, Count}|LT], Splits, Alloter, IsCheckSum)}
	end.

leaf_alloc_insert([{Index, Point, Node, Count}|LT], [?VOID|LT1], Alloter, IsCheckSum) ->
	N=node_alloc(Alloter, Node, IsCheckSum),
	non_leaf_alloc_insert(LT, LT1, Alloter, IsCheckSum, [Point], [{Index, N, Count + 1}]);
leaf_alloc_insert([{Index, Point, Node, Count}|LT], [{BuddyIndex, BuddyNode, BuddyCount}|LT1], Alloter, IsCheckSum) ->
	N=node_alloc(Alloter, Node, IsCheckSum),
	BN=node_alloc(Alloter, BuddyNode, IsCheckSum),
	non_leaf_alloc_insert(LT, LT1, Alloter, IsCheckSum, [Point], [{Index, N, Count + 1, BuddyIndex, BN, BuddyCount}]).

non_leaf_alloc_insert([{Index, Point, {?NON_LEAF_NODE, KVS, Childs}, Count}|LT], [?VOID|LT1], Alloter, IsCheckSum, Frees, Writes) ->
	case Index of
		?EMPTY ->
			case Writes of
				[{ChildIndex, {Child, _}, ChildCount}|_] ->
					{NewRootPoint, _}=N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
					{NewRootPoint, [Point|Frees], [{Index, N, Count}|Writes]};
				[{ChildIndex, {Child, _} = CN, ChildCount, BuddyChildIndex, {BuddyChild, _} = BCN, BuddyChildCount}|LT3] ->
					{NewRootPoint, _}=N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(BuddyChildIndex, setelement(ChildIndex, Childs, {Child, ChildCount}), {BuddyChild, BuddyChildCount})}, IsCheckSum),
					{NewRootPoint, [Point|Frees], [{Index, N, Count + 1}, {ChildIndex, CN, ChildCount}, {BuddyChildIndex, BCN, BuddyChildCount}|LT3]}
			end;
		_ ->
			case Writes of
				[{ChildIndex, {Child, _}, ChildCount}|_] ->
					N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
					non_leaf_alloc_insert(LT, LT1, Alloter, IsCheckSum, [Point|Frees], [{Index, N, Count + 1}|Writes]);
				[{ChildIndex, {Child, _} = CN, ChildCount, BuddyChildIndex, {BuddyChild, _} = BCN, BuddyChildCount}|LT3] ->
					N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(BuddyChildIndex, setelement(ChildIndex, Childs, {Child, ChildCount}), {BuddyChild, BuddyChildCount})}, IsCheckSum),
					non_leaf_alloc_insert(LT, LT1, Alloter, IsCheckSum, [Point|Frees], [{Index, N, Count + 1}, {ChildIndex, CN, ChildCount}, {BuddyChildIndex, BCN, BuddyChildCount}|LT3])
			end
	end;
non_leaf_alloc_insert([{Index, Point, {?NON_LEAF_NODE, KVS, Childs}, Count}|LT], [{BuddyIndex, BuddyNode, BuddyCount}|LT1], Alloter, IsCheckSum, Frees, Writes) ->
	case Writes of
		[{ChildIndex, {Child, _}, ChildCount}|_] ->
			N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
			BN=node_alloc(Alloter, BuddyNode, IsCheckSum),
			non_leaf_alloc_insert(LT, LT1, Alloter, IsCheckSum, [Point|Frees], [{Index, N, Count + 1, BuddyIndex, BN, BuddyCount}|Writes]);
		[{ChildIndex, {Child, _} = CN, ChildCount, BuddyChildIndex, {BuddyChild, _} = BCN, BuddyChildCount}|LT3] ->
			N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(BuddyChildIndex, setelement(ChildIndex, Childs, {Child, ChildCount}), {BuddyChild, BuddyChildCount})}, IsCheckSum),
			BN=node_alloc(Alloter, BuddyNode, IsCheckSum),
			non_leaf_alloc_insert(LT, LT1, Alloter, IsCheckSum, [Point|Frees], [{Index, N, Count + 1, BuddyIndex, BN, BuddyCount}, {ChildIndex, CN, ChildCount}, {BuddyChildIndex, BCN, BuddyChildCount}|LT3])
	end;
non_leaf_alloc_insert([], _, _, _, Frees, [{_, {NewRootPoint, _}, _}|_] = Writes) ->
	{NewRootPoint, Frees, Writes}.

update_entry({Type, KVS, Childs}, Index, Key, Value, Vsn, Time) ->
	{Type, setelement(Index, KVS, {Key, Value, Vsn, Time}), Childs};
update_entry({Type, KVS}, Index, Key, Value, Vsn, Time) ->
	{Type, setelement(Index, KVS, {Key, Value, Vsn, Time})}.

update_node({_, RootPoint, _, _, Alloter, _} = Handle, [{Index, Point, {?NON_LEAF_NODE, _, _} = Node, Count}|LT] = NodeLine, Key, Value, IsCheckSum) ->
	case lookup_key_(Node, Key) of
		{found, _, {Value, _, _}} ->
			{ok, {RootPoint, [], []}};
		{found, KeyIndex, {_, Vsn, _}} ->
			{ok, alloc_update([{Index, Point, update_entry(Node, KeyIndex, Key, Value, Vsn + 1, z_lib:now_millisecond()), Count}|LT], Alloter, IsCheckSum, [], [])};
		{_, ChildIndex, {Child, ChildCount}} ->
			case copy_node(Handle, Child, update_node, [NodeLine, Key, Value, IsCheckSum]) of
				{ok, ChildNode} ->
					update_node(Handle, [{ChildIndex, Child, ChildNode, ChildCount}|NodeLine], Key, Value, IsCheckSum);
				AsynReader ->
					AsynReader
			end
	end;
update_node({_, RootPoint, _, _, Alloter, _}, [{Index, Point, {?LEAF_NODE, KVS} = Node, ChildCount}|LT], Key, Value, IsCheckSum) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{0, KeyIndex} ->
			case element(KeyIndex, KVS) of
				{_, Value, _, _} ->
					{ok, {RootPoint, [], []}};
				{_, _, Vsn, _} ->
					{ok, alloc_update([{Index, Point, update_entry(Node, KeyIndex, Key, Value, Vsn + 1, z_lib:now_millisecond()), ChildCount}|LT], Alloter, IsCheckSum, [], [])}
			end;
		_ ->
			{update_node_error, {key_not_exist, Key}}
	end.

alloc_update([{?EMPTY, OldRootPoint, {Type, KVS, Childs}, Count}], Alloter, IsCheckSum, Frees, [{ChildIndex, {Child, _}, ChildCount}|_] = Writes) ->
	{NewRootPoint, _}=N=node_alloc(Alloter, {Type, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
	{NewRootPoint, [OldRootPoint|Frees], [{?EMPTY, N, Count}|Writes]};
alloc_update([{Index, OldPoint, {Type, KVS, Childs}, Count}|T], Alloter, IsCheckSum, Frees, [{ChildIndex, {Child, _}, ChildCount}|_] = Writes) ->
	N=node_alloc(Alloter, {Type, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
	alloc_update(T, Alloter, IsCheckSum, [OldPoint|Frees], [{Index, N, Count}|Writes]);
alloc_update([{Index, OldPoint, Node, Count}|T], Alloter, IsCheckSum, [], []) ->
	N=node_alloc(Alloter, Node, IsCheckSum),
	alloc_update(T, Alloter, IsCheckSum, [OldPoint], [{Index, N, Count}]);
alloc_update([], _, _, Frees, [{_, {NewRootPoint, _}, _}|_] = Writes) ->
	{NewRootPoint, Frees, Writes}.

is_remove_root(?EMPTY, {?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) =< 1;
is_remove_root(?EMPTY, {?LEAF_NODE, KVS}) ->
	tuple_size(KVS) =< 1;
is_remove_root(_, _) ->
	false.

is_merge(T, {?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) =< lower_bound(T);
is_merge(T, {?LEAF_NODE, KVS}) ->
	tuple_size(KVS) =< lower_bound(T).

is_move(T, {?NON_LEAF_NODE, KVS, _}) ->
	tuple_size(KVS) > lower_bound(T);
is_move(T, {?LEAF_NODE, KVS}) ->
	tuple_size(KVS) > lower_bound(T).

search_max_(Handle, {?NON_LEAF_NODE, _, Childs} = Node) ->
	case get_node(Handle, element(1, element(tuple_size(Childs), Childs)), search_max_, [Node]) of
		{ok, ChildNode} ->
			search_max_(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
search_max_(_, {?LEAF_NODE, KVS}) ->
	element(tuple_size(KVS), KVS).

search_min_(Handle, {?NON_LEAF_NODE, _, Childs} = Node) ->
	case get_node(Handle, element(1, element(1, Childs)), search_min, [Node]) of
		{ok, ChildNode} ->
			search_min_(Handle, ChildNode);
		AsynReader ->
			AsynReader
	end;
search_min_(_, {?LEAF_NODE, KVS}) ->
	element(1, KVS).

merge(L, R) ->
	list_to_tuple(tuple_to_list(L)++tuple_to_list(R)).

merge(L, C, R) ->
	list_to_tuple(tuple_to_list(L)++[C]++tuple_to_list(R)).

merge_node({?NON_LEAF_NODE, LKVS, LChilds}, KV, {?NON_LEAF_NODE, RKVS, RChilds}) ->
	{?NON_LEAF_NODE, merge(LKVS, KV, RKVS), merge(LChilds, RChilds)};
merge_node({?LEAF_NODE, LKVS}, KV, {?LEAF_NODE, RKVS}) ->
	{?LEAF_NODE, merge(LKVS, KV, RKVS)}.

left_move({?NON_LEAF_NODE, LKVS, LChilds}, {?NON_LEAF_NODE, PKVS, PChilds}, {?NON_LEAF_NODE, KVS, Childs}, Index) ->
	PKeyIndex=Index - 1,
	PKV=element(PKeyIndex, PKVS),
	LKeyIndex=tuple_size(LKVS),
	LKV=element(LKeyIndex, LKVS),
	LChildIndex=LKeyIndex + 1,
	LChild=element(LChildIndex, LChilds),
	{{?NON_LEAF_NODE, 
	  erlang:delete_element(LKeyIndex, LKVS), 
	  erlang:delete_element(LChildIndex, LChilds)}, 
	 {?NON_LEAF_NODE, 
	  erlang:setelement(PKeyIndex, PKVS, LKV), 
	  erlang:setelement(Index, erlang:setelement(PKeyIndex, PChilds, ?PLACE_CHILD), ?PLACE_CHILD)}, 
	 {?NON_LEAF_NODE, 
	  erlang:insert_element(1, KVS, PKV), 
	  erlang:insert_element(1, Childs, LChild)}};
left_move({?LEAF_NODE, LKVS}, {?NON_LEAF_NODE, PKVS, PChilds}, {?LEAF_NODE, KVS}, Index) ->
	PKeyIndex=Index - 1,
	PKV=element(PKeyIndex, PKVS),
	LKeyIndex=tuple_size(LKVS),
	LKV=element(LKeyIndex, LKVS),
	{{?LEAF_NODE, 
	  erlang:delete_element(LKeyIndex, LKVS)}, 
	 {?NON_LEAF_NODE, 
	  erlang:setelement(PKeyIndex, PKVS, LKV), 
	  erlang:setelement(Index, erlang:setelement(PKeyIndex, PChilds, ?PLACE_CHILD), ?PLACE_CHILD)}, 
	 {?LEAF_NODE, 
	  erlang:insert_element(1, KVS, PKV)}}.

right_move({?NON_LEAF_NODE, RKVS, RChilds}, {?NON_LEAF_NODE, PKVS, PChilds}, {?NON_LEAF_NODE, KVS, Childs}, Index) ->
	PKeyIndex=Index,
	PKV=element(PKeyIndex, PKVS),
	RKV=element(1, RKVS),
	RChild=element(1, RChilds),
	KeyIndex=tuple_size(KVS) + 1,
	{{?NON_LEAF_NODE, 
	  erlang:delete_element(1, RKVS), 
	  erlang:delete_element(1, RChilds)}, 
	 {?NON_LEAF_NODE, 
	  erlang:setelement(PKeyIndex, PKVS, RKV), 
	  erlang:setelement(Index, erlang:setelement(PKeyIndex, PChilds, ?PLACE_CHILD), ?PLACE_CHILD)}, %将上级节点中对应的Child设置为待分配 
	 {?NON_LEAF_NODE, 
	  erlang:insert_element(KeyIndex, KVS, PKV),
	  erlang:insert_element(KeyIndex + 1, Childs, RChild)}};
right_move({?LEAF_NODE, RKVS}, {?NON_LEAF_NODE, PKVS, PChilds}, {?LEAF_NODE, KVS}, Index) ->
	PKeyIndex=Index,
	PKV=element(PKeyIndex, PKVS),
	RKV=element(1, RKVS),
	{{?LEAF_NODE, 
	  erlang:delete_element(1, RKVS)}, 
	 {?NON_LEAF_NODE, 
	  erlang:setelement(PKeyIndex, PKVS, RKV), 
	  erlang:setelement(Index, erlang:setelement(PKeyIndex, PChilds, ?PLACE_CHILD), ?PLACE_CHILD)}, 
	 {?LEAF_NODE, 
	  erlang:insert_element(tuple_size(KVS) + 1, KVS, PKV)}}.

delete_prev(Handle, LT, Moves, MergeFrees, Index, Point, KVS, Childs, Count, LChild, LChildNode, LChildCount, KeyIndex, IsCheckSum) ->
	{PrevKey, _, _, _}=PrevKV=search_max_(Handle, LChildNode),
	delete_node(Handle, [{KeyIndex, LChild, LChildNode, LChildCount}, 
						 {Index, Point, {?NON_LEAF_NODE, setelement(KeyIndex, KVS, PrevKV), Childs}, Count}|LT], [?VOID|Moves], MergeFrees, PrevKey, IsCheckSum).

delete_next(Handle, LT, Moves, MergeFrees, Index, Point, KVS, Childs, Count, RChildIndex, RChild, RChildNode, RChildCount, KeyIndex, IsCheckSum) ->
	{NextKey, _, _, _}=NextKV=search_min_(Handle, RChildNode),
	delete_node(Handle, [{RChildIndex, RChild, RChildNode, RChildCount}, 
						 {Index, Point, {?NON_LEAF_NODE, setelement(KeyIndex, KVS, NextKV), Childs}, Count}|LT], [?VOID|Moves], MergeFrees, NextKey, IsCheckSum).

childs_merged(Handle, LT, Moves, MergeFrees, Index, Point, {?NON_LEAF_NODE, KVS, Childs} = Node, Count, 
			  LChild, LChildNode, LChildCount, RChildIndex, RChild, RChildNode, RChildCount, KeyIndex, Key, IsCheckSum) ->
	KV=element(KeyIndex, KVS),
	MergedNode=merge_node(LChildNode, KV, RChildNode),
	NewKVS=erlang:delete_element(KeyIndex, KVS),
	NewChilds=erlang:delete_element(RChildIndex, setelement(KeyIndex, Childs, ?PLACE_CHILD)), 
	case is_remove_root(Index, Node) of
		false ->
			delete_node(Handle, [{KeyIndex, LChild, MergedNode, LChildCount + RChildCount + 1}, 
						 {Index, Point, {?NON_LEAF_NODE, NewKVS, NewChilds}, Count}|LT], [?VOID|Moves], [RChild|MergeFrees], Key, IsCheckSum);
		true ->
			delete_node(Handle, [{?EMPTY, LChild, MergedNode, LChildCount + RChildCount + 1}|LT], [?VOID|Moves], [Point, RChild|MergeFrees], Key, IsCheckSum)
	end.

delete_non_leaf({T, _, Cache, M, _, _} = Handle, [{Index, Point, {?NON_LEAF_NODE, KVS, Childs} = Node, Count}|LT] = NodeLine, Moves, MergeFrees, KeyIndex, Key, IsCheckSum) ->
	{LChild, LChildCount}=element(KeyIndex, Childs),
	case copy_node(Handle, LChild, delete_non_leaf, [NodeLine, Moves, MergeFrees, KeyIndex, Key, IsCheckSum]) of
		{ok, LChildNode} ->
			case is_merge(T, LChildNode) of
				false ->
					delete_prev(Handle, LT, Moves, MergeFrees, Index, Point, KVS, Childs, Count, LChild, LChildNode, LChildCount, KeyIndex, IsCheckSum);
				true ->
					RChildIndex=KeyIndex + 1,
					{RChild, RChildCount}=element(RChildIndex, Childs),
					case copy_node(Handle, RChild, delete_non_leaf, [NodeLine, Moves, MergeFrees, KeyIndex, Key, IsCheckSum]) of
						{ok, RChildNode} ->
							case is_merge(T, RChildNode) of
								false ->
									M:uncopy(Cache, LChild),
									delete_next(Handle, LT, Moves, MergeFrees, Index, Point, KVS, Childs, Count, RChildIndex, RChild, RChildNode, RChildCount, KeyIndex, IsCheckSum);					
								true ->
									childs_merged(Handle, LT, Moves, MergeFrees, Index, Point, Node, Count, 
												  LChild, LChildNode, LChildCount, RChildIndex, RChild, RChildNode, RChildCount, KeyIndex, Key, IsCheckSum)
							end;
						AsynReader ->
							AsynReader
					end
			end;
		AsynReader ->
			AsynReader
	end.

left_buddy_merge(Handle, LT, Moves, MergeFrees, PIndex, PPoint, {_, PKVS, PChilds} = PNode, PCount, Index, Point, Node, Count, LIndex, LPoint, LNode, LCount, Key, IsCheckSum) ->
	PKV=element(LIndex, PKVS),
	MergedNode=merge_node(LNode, PKV, Node),
	NewPKVS=erlang:delete_element(LIndex, PKVS),
	NewPChilds=erlang:delete_element(Index, setelement(LIndex, PChilds, ?PLACE_CHILD)),
	case is_remove_root(PIndex, PNode) of
		false ->
			delete_node(Handle, [{LIndex, Point, MergedNode, LCount + Count + 1},	
								 {PIndex, PPoint, {?NON_LEAF_NODE, NewPKVS, NewPChilds}, PCount}|LT], [?VOID|Moves], [LPoint|MergeFrees], Key, IsCheckSum);
		true ->
			delete_node(Handle, [{?EMPTY, LPoint, MergedNode, LCount + Count + 1}|LT], [?VOID|Moves], [PPoint, Point|MergeFrees], Key, IsCheckSum)
	end.

right_buddy_merge(Handle, LT, Moves, MergeFrees, PIndex, PPoint, {_, PKVS, PChilds} = PNode, PCount, Index, Point, Node, Count, RIndex, RPoint, RNode, RCount, Key, IsCheckSum) ->
	{_, PKVS, PChilds}=PNode,
	PKV=element(Index, PKVS),
	MergedNode=merge_node(Node, PKV, RNode),
	NewPKVS=erlang:delete_element(Index, PKVS),
	NewPChilds=erlang:delete_element(RIndex, setelement(Index, PChilds, ?PLACE_CHILD)),
	case is_remove_root(PIndex, PNode) of
		false ->
			delete_node(Handle, [{Index, Point, MergedNode, Count + RCount + 1}, 
								 {PIndex, PPoint, {?NON_LEAF_NODE, NewPKVS, NewPChilds}, PCount}|LT], [?VOID|Moves], [RPoint|MergeFrees], Key, IsCheckSum);
		true ->
			delete_node(Handle, [{?EMPTY, Point, MergedNode, Count + RCount + 1}|LT], [?VOID|Moves], [PPoint, RPoint|MergeFrees], Key, IsCheckSum)
	end.

left_buddy_handle({T, _, _, _, _, _} = Handle, LT, Moves, MergeFrees, PIndex, PPoint, {_, _, PChilds} = PNode, PCount, Index, Point, Node, Count, LIndex, LPoint, LCount, Key, IsCheckSum) ->
	case get_node(Handle, LPoint, left_buddy_handle, [LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point, Node, Count, LIndex, LPoint, LCount, Key, IsCheckSum]) of
		{ok, LNode} ->
			case is_move(T, LNode) of
				true ->
					{NewLNode, NewPNode, NewNode}=left_move(LNode, PNode, Node, Index),
					delete_node(Handle, [{Index, Point, NewNode, Count + 1}, {PIndex, PPoint, NewPNode, PCount}|LT], 
								[{LIndex, LPoint, NewLNode, LCount - 1}|Moves], MergeFrees, Key, IsCheckSum);
				false ->
					RIndex=Index + 1,
					if
						RIndex =< tuple_size(PChilds) ->
							{RPoint, RCount}=element(RIndex, PChilds),
							right_buddy_handle(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point, Node, Count, RIndex, RPoint, RCount, Key, IsCheckSum);
						true ->
							left_buddy_merge(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, 
											 Index, Point, Node, Count, LIndex, LPoint, LNode, LCount, Key, IsCheckSum)
					end
			end;
		AsynReader ->
			AsynReader
	end.

right_buddy_handle({T, _, _, _, _, _} = Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point, Node, Count, RIndex, RPoint, RCount, Key, IsCheckSum) ->
	case get_node(Handle, RPoint, right_buddy_handle, [LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point, Node, Count, RIndex, RPoint, RCount, Key, IsCheckSum]) of
		{ok, RNode} ->
			case is_move(T, RNode) of
				true ->
					{NewRNode, NewPNode, NewNode}=right_move(RNode, PNode, Node, Index),
					delete_node(Handle, [{Index, Point, NewNode, Count + 1}, {PIndex, PPoint, NewPNode, PCount}|LT], 
								[{RIndex, RPoint, NewRNode, RCount - 1}|Moves], MergeFrees, Key, IsCheckSum);
				false ->
					right_buddy_merge(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, 
									  Index, Point, Node, Count, RIndex, RPoint, RNode, RCount, Key, IsCheckSum)
			end;
		AsynReader ->
			AsynReader
	end.

delete_continue({T, _, _, _, _, _} = Handle, [{PIndex, PPoint, {?NON_LEAF_NODE, _, PChilds} = PNode, PCount}|LT] = NodeLine, Moves, MergeFrees, Index, Point, Count, Key, IsCheckSum) ->
	case copy_node(Handle, Point, delete_continue, [NodeLine, Moves, MergeFrees, Index, Point, Count, Key, IsCheckSum]) of
		{ok, Node} ->
			case is_move(T, Node) of
				true ->
					delete_node(Handle, [{Index, Point, Node, Count}|NodeLine], [?VOID|Moves], MergeFrees, Key, IsCheckSum);
				false ->
					LIndex=Index - 1,
					if
						LIndex > 0 ->
							{LPoint, LCount}=element(LIndex, PChilds),
							left_buddy_handle(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point, Node, Count, LIndex, LPoint, LCount, Key, IsCheckSum);
						true ->
							RIndex=Index + 1,
							{RPoint, RCount}=element(RIndex, PChilds),
							right_buddy_handle(Handle, LT, Moves, MergeFrees, PIndex, PPoint, PNode, PCount, Index, Point, Node, Count, RIndex, RPoint, RCount, Key, IsCheckSum)
					end
			end;
		AsynReader ->
			AsynReader
	end.

delete_node(Handle, [{_, _, {?NON_LEAF_NODE, _, _} = Node, _}|_] = NodeLine, Moves, MergeFrees, Key, IsCheckSum) ->
	case lookup_key_(Node, Key) of
		{found, KeyIndex, _} ->
			delete_non_leaf(Handle, NodeLine, Moves, MergeFrees, KeyIndex, Key, IsCheckSum);
		{_, ChildIndex, {Child, ChildCount}} ->
			delete_continue(Handle, NodeLine, Moves, MergeFrees, ChildIndex, Child, ChildCount, Key, IsCheckSum)
	end;
delete_node({_, _, _, _, Alloter, _}, [{Index, Point, {?LEAF_NODE, KVS}, Count}|LT], Moves, MergeFrees, Key, IsCheckSum) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{0, KeyIndex} ->
			case leaf_alloc_delete([{Index, Point, {?LEAF_NODE, erlang:delete_element(KeyIndex, KVS)}, Count}|LT], Moves, MergeFrees, Alloter, IsCheckSum) of
				{NewRootPoint, Frees, [{?EMPTY, {_, {?LEAF_NODE, {}}}, _}]} ->
					{ok, {?EMPTY, [NewRootPoint|Frees], []}};
				R ->
					{ok, R}
			end;
		{_, _} ->
			{delete_node_error, {key_not_exist, Key}}
	end.

leaf_alloc_delete([{Index, Point, Node, Count}|LT], [?VOID|LT1], MergeFrees, Alloter, IsCheckSum) ->
	N=node_alloc(Alloter, Node, IsCheckSum),
	non_leaf_alloc_delete(LT, LT1, Alloter, IsCheckSum, [Point|MergeFrees], [{Index, N, Count - 1}]);
leaf_alloc_delete([{Index, Point, Node, Count}|LT], [{BuddyIndex, BuddyPoint, BuddyNode, BuddyCount}|LT1], MergeFrees, Alloter, IsCheckSum) ->
	N=node_alloc(Alloter, Node, IsCheckSum),
	BN=node_alloc(Alloter, BuddyNode, IsCheckSum),
	non_leaf_alloc_delete(LT, LT1, Alloter, IsCheckSum, [Point, BuddyPoint|MergeFrees], [{Index, N, Count - 1, BuddyIndex, BN, BuddyCount}]).
	
non_leaf_alloc_delete([{Index, Point, {?NON_LEAF_NODE, KVS, Childs}, Count}|LT], [?VOID|LT1], Alloter, IsCheckSum, Frees, Writes) ->
	case Index of
		?EMPTY ->
			case Writes of
				[{ChildIndex, {Child, _}, ChildCount}|_] ->
					{NewRootPoint, _}=N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
					{NewRootPoint, [Point|Frees], [{Index, N, Count}|Writes]};
				[{ChildIndex, {Child, _} = CN, ChildCount, BuddyChildIndex, {BuddyChild, _} = BCN, BuddyChildCount}|LT3] ->
					{NewRootPoint, _}=N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(BuddyChildIndex, setelement(ChildIndex, Childs, {Child, ChildCount}), {BuddyChild, BuddyChildCount})}, IsCheckSum),
					{NewRootPoint, [Point|Frees], [{Index, N, Count}, {ChildIndex, CN, ChildCount}, {BuddyChildIndex, BCN, BuddyChildCount}|LT3]}
			end;
		_ ->
			case Writes of
				[{ChildIndex, {Child, _}, ChildCount}|_] ->
					N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
					non_leaf_alloc_delete(LT, LT1, Alloter, IsCheckSum, [Point|Frees], [{Index, N, Count - 1}|Writes]);
				[{ChildIndex, {Child, _} = CN, ChildCount, BuddyChildIndex, {BuddyChild, _} = BCN, BuddyChildCount}|LT3] ->
					N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(BuddyChildIndex, setelement(ChildIndex, Childs, {Child, ChildCount}), {BuddyChild, BuddyChildCount})}, IsCheckSum),
					non_leaf_alloc_delete(LT, LT1, Alloter, IsCheckSum, [Point|Frees], [{Index, N, Count - 1}, {ChildIndex, CN, ChildCount}, 
																				   {BuddyChildIndex, BCN, BuddyChildCount}|LT3])
			end
	end;
non_leaf_alloc_delete([{Index, Point, {?NON_LEAF_NODE, KVS, Childs}, Count}|LT], [{BuddyIndex, BuddyPoint, BuddyNode, BuddyCount}|LT1], Alloter, IsCheckSum, Frees, Writes) ->
	case Writes of 
		[{ChildIndex, {Child, _}, ChildCount}|_] ->
			N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(ChildIndex, Childs, {Child, ChildCount})}, IsCheckSum),
			BN=node_alloc(Alloter, BuddyNode, IsCheckSum),
			non_leaf_alloc_delete(LT, LT1, Alloter, IsCheckSum, [Point, BuddyPoint|Frees], [{Index, N, Count - 1, BuddyIndex, BN, BuddyCount}|Writes]);
		[{ChildIndex, {Child, _} = CN, ChildCount, BuddyChildIndex, {BuddyChild, _} = BCN, BuddyChildCount}|LT3] ->
			N=node_alloc(Alloter, {?NON_LEAF_NODE, KVS, setelement(BuddyChildIndex, setelement(ChildIndex, Childs, {Child, ChildCount}), {BuddyChild, BuddyChildCount})}, IsCheckSum),
			BN=node_alloc(Alloter, BuddyNode, IsCheckSum),
			non_leaf_alloc_delete(LT, LT1, Alloter, IsCheckSum, [Point, BuddyPoint|Frees], [{Index, N, Count - 1, BuddyIndex, BN, BuddyCount}, {ChildIndex, CN, ChildCount}, 
																					   {BuddyChildIndex, BCN, BuddyChildCount}|LT3])
	end;
non_leaf_alloc_delete([], _, _, _, Frees, [{_, {NewRootPoint, _}, _}|_] = Writes) ->
	{NewRootPoint, Frees, Writes}.
	
childs_count(Childs, Index) ->
	childs_count1(Index, tuple_to_list(Childs), 0).

childs_count1(0, _, Count) ->
	Count;
childs_count1(Index, [{_, ChildCount}|T], Count) ->
	childs_count1(Index - 1, T, Count + ChildCount).

rank_continue(Handle, Childs, KeyIndex, Child, Key, Count) ->
	case get_node(Handle, Child, rank_continue, [Childs, KeyIndex, Child, Key, Count]) of
		{ok, ChildNode} ->
			rank_node(Handle, ChildNode, Key, Count + KeyIndex + childs_count(Childs, KeyIndex));
		AsynReader ->
			AsynReader
	end.

rank_node(Handle, {?NON_LEAF_NODE, _, Childs} = Node, Key, Count) ->
	case lookup_key(Node, Key) of
		{found, KeyIndex, _} ->
			{ok, Count + KeyIndex + childs_count(Childs, KeyIndex)};
		{_, ChildIndex, Child} ->
			rank_continue(Handle, Childs, ChildIndex - 1, Child, Key, Count)
	end;
rank_node(_, {?LEAF_NODE, KVS}, Key, Count) ->
	case z_lib:half_keyfind(KVS, 1, Key, 1) of
		{0, KeyIndex} ->
			{ok, Count + KeyIndex};
		{_, _} ->
			{rank_node_error, {key_not_exist, Key}}
	end.

childs_rank(Childs) ->
	[{Child, ChildCount}|T]=tuple_to_list(Childs),
	childs_rank1(T, ChildCount, [{Child, ChildCount}]).

childs_rank1([{Child, ChildCount}|T], Rank, L) ->
	NewRank = Rank + ChildCount + 1,
	childs_rank1(T, NewRank, [{Child, NewRank}|L]);
childs_rank1([], _, L) ->
	list_to_tuple(lists:reverse(L)).
	
by_rank_continue(Handle, Child, Rank) ->
	case get_node(Handle, Child, by_rank_continue, [Child, Rank]) of
		{ok, ChildNode} ->
			by_rank_node(Handle, ChildNode, Rank);
		AsynReader ->
			AsynReader
	end.

by_rank_node(_, {?NON_LEAF_NODE, _, _}, 0) ->
	{by_rank_node_error, key_not_exist};
by_rank_node(Handle, {?NON_LEAF_NODE, KVS, C}, Rank) ->
	Childs=childs_rank(C),
	case z_lib:half_keyfind(Childs, 1, Rank, 2) of
		{-1, 1} ->
			{Child, _} = element(1, Childs),
			by_rank_continue(Handle, Child, Rank);
		{-1, ChildIndex} ->
			Index=ChildIndex - 1,
			{Child, ChildCount}=element(Index, Childs),
			Count=ChildCount + 1,
			if
				Rank > Count ->
					{RChild, _}=element(ChildIndex, Childs),
					by_rank_continue(Handle, RChild, Rank - Count);
				Rank < Count ->
					by_rank_continue(Handle, Child, Rank - (ChildCount + 1));
				true ->
					{Key, _, Vsn, Time}=element(Index, KVS),
					{ok, {Key, Vsn, Time}}
			end;
		{1, ChildIndex} ->
			{_, ChildCount}=element(ChildIndex, Childs),
			Count=ChildCount + 1,
			Size=tuple_size(Childs),
			if
				(Rank > Count) and (Size > ChildIndex) ->
					{RChild, _}=element(ChildIndex + 1, Childs),
					by_rank_continue(Handle, RChild, Rank - Count);
				(Rank =:= Count) and (Size > ChildIndex) ->
					{Key, _, Vsn, Time}=element(ChildIndex, KVS),
					{ok, {Key, Vsn, Time}};
				true ->
					{error, key_not_exist}
			end;
		{0, ChildIndex} ->
			if
				ChildIndex > 1 ->
					{Child, _}=element(ChildIndex, Childs),
					{_, ChildCount}=element(ChildIndex - 1, Childs),
					by_rank_continue(Handle, Child, Rank - (ChildCount + 1));
				true ->
					{Child, _}=element(ChildIndex, Childs),
					by_rank_continue(Handle, Child, Rank)
			end
	end;
by_rank_node(_, {?LEAF_NODE, KVS}, Rank) ->
	if
		(Rank >= 1) and (Rank =< tuple_size(KVS)) ->
			{Key, _, Vsn, Time}=element(Rank, KVS),
			{ok, {Key, Vsn, Time}};
		true ->
			{error, key_not_exist}
	end.
