%%@doc 逻辑模块
%%```
%%% 负责存储数据库
%%% 提供匹配查找和修改插入和删除
%%% 初始化时需要设置索引，支持多组合键的索引
%%% 索引的优先级遵循设置的次序，前面的优先级高
%%% 如果一个查询可以有多个可索引的，则按照最大联合索引加索引优先级来决定查询那个索引
%%% 一个查询只能有最多一个范围查询，只有定值查询和范围查询可以利用索引加速
%%% 范围查询：{K, order, V1, V2, Limit}, V1,V2是闭区间，V1 > V2 表示降序，否则为升序，Limit表示最多取多少
%%% 范围查询只能用单索引或该键为联合索引的最后一个
%%'''
%%@end


-module(zm_logic).

-description("logic").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([create/2, create/3, list/1, get_max/1, new_id/1, destroy/1]).
-export([member/2, get_key/2, is_attr/3, get_attr/2, set_attr/4, set_attr/3, del_attr/3, get_value/4, get_values/3]).
-export([select/2, insert/3, update/3, delete/2]).
-export([select/3, update/4, delete/3, match/3]).

%%%=======================INLINE=======================
-compile({inline, [insert/3, create_index/1, pre_index/3, parse_index/4, add_union_index/3, set_attr_/5, select_list/6, select_/6, parse_query/3, parse_query/7, match_query_index/3, match_nkey/4, match_keys/2, match_1key/4, member_order/2, match_0key/2, handle_query_index/4, handle_query_index2/5, handle_query_index2/7, handle_query_index1/4, handle_query_index0/4, match_query_index/4, match_nkey/6, match_1key/6, handle_query_order/9, handle_query_order/8, handle_query_order/10, select_table/4, select_index/4, select_index_query/6, match_query/3, match_query_list/3, select_order_prev/4, select_order_next/4, select_order_prev_query/6, select_order_next_query/6, order_limit/5, order_sort/5, insert_/6, insert_/5, insert_index/3, insert_key_index/3, insert_union_index/3, insert_union_index/5, modify/4, update_/5, update_list/6, get_index_key/3, get_index_key/4, replace_diff/6, list_delete/2, delete_/5, delete_index/3, delete_key_index/3, delete_union_index/3, delete_union_index/5, del_attr_/3, matchs_list/4, match_list/3]}).
-compile({inline_size, 32}).

%%%=======================DEFINE=======================
-define(MAX, max).
-define(MIN, -16#7fffffff).

%%%=======================TYPE=======================
-type key_info() :: any().
-type record_info() :: [{key_info(), any()}].

-type query_info() ::
{key_info(), any()} |
{'$key', 'has', key_info()} |
{'$key', 'not', key_info()} |
{'$id', 'in' | 'out', list()} |
{key_info(), 'in' | 'out' | '>' | '>=' | '<' | '=<' | '!' | 'member' | 'notmember', any()} |
{key_info(), '-' | '!', any(), any()} |
{key_info(), 'order', any(), any(), integer()} |
{fun(), any()} |
{key_info(), fun(), any()}.

-type modify_info() ::
{key_info(), any()} |
{'$key', 'del', key_info()} |
{key_info(), '-' | '+' | '*' | '/' | 'div', any()} |
{key_info(), fun(), any()}.

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 创建逻辑模块，需要设置数据索引
%% @spec  create(Max, IndexList) -> return()
%% where
%% return() =  {IDEts, AttrEts, IndexTree, IndexList, MCache, QCache, true}
%%@end
%% -----------------------------------------------------------------
create(Max, IndexList) ->
	create(Max, IndexList, true).

%% -----------------------------------------------------------------
%%@doc 创建逻辑模块，需要设置数据索引
%% @spec  create(Max, IndexList, Strict) -> return()
%% where
%% return() =  {IDEts, AttrEts, IndexTree, IndexList, MCache, QCache, Strict}
%%@end
%% -----------------------------------------------------------------
create(Max, IndexList, Strict) when is_integer(Max), is_list(IndexList) ->
	MCache = ets:new(?MODULE, []),
	ets:insert(MCache, {?MAX, Max}),
	{Tree, List} = create_index(IndexList),
	{ets:new(?MODULE, []), ets:new(?MODULE, []), Tree, List,
	 MCache, ets:new(?MODULE, [public]), Strict}.

%% -----------------------------------------------------------------
%%@doc 获得所有的ID
%% @spec  list(Logic::logic()) -> return()
%% where
%%  return() =  [] | [ID, ...]
%%@end
%% -----------------------------------------------------------------
-spec list(tuple()) ->
	[] | [integer()].
%% -----------------------------------------------------------------
list({IDEts, _AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}) ->
	ets:select(IDEts, [{{'$1','_'},[],['$1']}]).

%% -----------------------------------------------------------------
%%@doc 获得当前的最大ID
%% @spec  get_max(Logic::logic()) -> return()
%% where
%%  return() =  MaxID
%%@end
%% -----------------------------------------------------------------
-spec get_max(tuple()) ->
	integer().
%% -----------------------------------------------------------------
get_max({_IDEts, _AttrEts, _IndexTree, _IndexList, MCache, _QCache, _Strict}) ->
	ets:lookup_element(MCache, ?MAX, 2).

%% -----------------------------------------------------------------
%%@doc 获得一个新的ID
%% @spec  new_id(Logic::logic()) -> return()
%% where
%%  return() =  ID
%%@end
%% -----------------------------------------------------------------
-spec new_id(tuple()) ->
	integer().
%% -----------------------------------------------------------------
new_id({_IDEts, _AttrEts, _IndexTree, _IndexList, MCache, _QCache, _Strict}) ->
	Max = ets:lookup_element(MCache, ?MAX, 2),
	ets:insert(MCache, {?MAX, Max + 1}),
	Max.

%% -----------------------------------------------------------------
%%@doc 销毁逻辑模块
%% @spec  destroy(Logic::logic()) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
destroy({IDEts, AttrEts, _, {KeysTableList, KeyTableList}, MCache, QCache, _Strict}) ->
	ets:delete(QCache),
	ets:delete(MCache),
	[ets:delete(Table) || {_, Table, _, _} <- KeysTableList],
	[ets:delete(Table) || {_, Table} <- KeyTableList],
	ets:delete(IDEts),
	ets:delete(AttrEts).

%% -----------------------------------------------------------------
%%@doc 判断对象是否存在
%% @spec  member(Logic::logic(), ID) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
member({IDEts, _AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID) ->
	ets:member(IDEts, ID).

%% -----------------------------------------------------------------
%%@doc 获得对象的全部属性键
%% @spec get_key(Logic::logic(), ID) -> return()
%% where
%%  return() =  [Key] | []
%%@end
%% -----------------------------------------------------------------
get_key({IDEts, _AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID) ->
	try
		ets:lookup_element(IDEts, ID, 2)
	catch
		_:_ ->
			[]
	end.

%% -----------------------------------------------------------------
%%@doc 判断对象是否有指定的属性
%% @spec  is_attr(Logic::logic(), ID, Key) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
is_attr({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, Key) ->
	ets:member(AttrEts, {ID, Key}).

%% -----------------------------------------------------------------
%%@doc 获得对象的全部属性键值
%% @spec get_attr(Logic::logic(), ID) -> return()
%% where
%%  return() =  [{Key, Value}] | []
%%@end
%% -----------------------------------------------------------------
get_attr({IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID) ->
	try ets:lookup_element(IDEts, ID, 2) of
		Keys ->
			[{K, V} || K <- Keys, (V = try ets:lookup_element(AttrEts, {ID, K}, 2) catch _:_ -> none end) =/= none]
	catch
		_:_ ->
			[]
	end.

%% -----------------------------------------------------------------
%%@doc 直接设置对象的属性值，必须保证对象一定存在，该属性没有被索引
%% @spec  set_attr(Logic::logic(), ID, Key, Value) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
set_attr({IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, false}, ID, Key, Value) ->
	set_attr_(IDEts, AttrEts, ID, Key, Value);
set_attr({IDEts, AttrEts, IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, Key, Value) ->
	[erlang:error({strict_module, invalid_set_attr, {ID, Key, Value}}) || sb_trees:get(Key, IndexTree, none) =/= none],
	set_attr_(IDEts, AttrEts, ID, Key, Value).

%% -----------------------------------------------------------------
%%@doc 直接设置对象的属性值列表，必须保证对象一定存在，所有属性没有被索引
%% @spec  set_attr(Logic::logic(), ID, KVList) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
set_attr({IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, false}, ID, KVList) ->
	L = [R || {Key, Value} <- KVList, (R = set_attr_(IDEts, AttrEts, ID, Key, Value)) =/= true],
	L =:= [];
set_attr({IDEts, AttrEts, IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, KVList) ->
	[erlang:error({strict_module, invalid_set_attrs, {ID, K, KVList}}) ||
		{K, _} <- KVList, sb_trees:get(K, IndexTree, none) =/= none],
	L = [R || {Key, Value} <- KVList, (R = set_attr_(IDEts, AttrEts, ID, Key, Value)) =/= true],
	L =:= [].

%% -----------------------------------------------------------------
%%@doc 删除对象的属性，必须保证该属性没有被索引
%% @spec  del_attr(Logic::logic(), ID, Key) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
del_attr({IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, false}, ID, Key) ->
	try ets:lookup_element(IDEts, ID, 2) of
		Keys ->
			ets:insert(IDEts, {ID, lists:delete(Key, Keys)}),
			ets:delete(AttrEts, {ID, Key})
	catch
		_:_ ->
			false
	end;
del_attr({IDEts, AttrEts, IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, Key) ->
	[erlang:error({strict_module, invalid_del_attr, {ID, Key}}) || sb_trees:get(Key, IndexTree, none) =/= none],
	try ets:lookup_element(IDEts, ID, 2) of
		Keys ->
			ets:insert(IDEts, {ID, lists:delete(Key, Keys)}),
			ets:delete(AttrEts, {ID, Key})
	catch
		_:_ ->
			false
	end.

%% -----------------------------------------------------------------
%%@doc 获得对象指定属性的值，如果没有则使用默认值
%% @spec  get_value(Logic::logic(), ID, Key, Default) -> return()
%% where
%%  return() =  Value
%%@end
%% -----------------------------------------------------------------
get_value({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, Key, Default) ->
	case ets:lookup(AttrEts, {ID, Key}) of
		[{_, Value}] -> Value;
		_ -> Default
	end.

%% -----------------------------------------------------------------
%%@doc 获得对象指定属性列表的值，如果没有则使用默认值
%% @spec  get_values(Logic::logic(), ID, KeyDefaultList) -> return()
%% where
%%  return() =  [Value]
%%@end
%% -----------------------------------------------------------------
get_values({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, KeyDefaultList) ->
	[case ets:lookup(AttrEts, {ID, Key}) of
		[{_, Value}] -> Value;
		_ -> Default
	end || {Key, Default} <- KeyDefaultList].

%% -----------------------------------------------------------------
%%@doc 获得指定id的记录，或指定查询条件的记录
%%```
%% query_info = Key | {Key, Value} | {Key, Comparison, A} | {Key, Range, A1, A2} | {Fun, A} | {Key, Fun, A}
%% Query = query_info | [query_info]
%%'''
%% @spec select(Logic::logic(), query_info()) -> return()
%% where
%%  return() =  [] | [integer()]
%%@end
%% -----------------------------------------------------------------
-spec select(tuple(), query_info() | [query_info()]) ->
	[] | [integer()].
%% -----------------------------------------------------------------
select({IDEts, AttrEts, _, IndexList, _MCache, QCache, Strict}, [_ | _] = Query) ->
	select_list(IDEts, AttrEts, IndexList, QCache, Query, Strict);
select({IDEts, AttrEts, IndexTree, _, _MCache, QCache, Strict}, Query) ->
	select_(IDEts, AttrEts, IndexTree, QCache, Query, Strict).

%% -----------------------------------------------------------------
%%@doc 插入记录，如果ID为0，表示用系统分配的ID，否则使用该ID，该ID一般应由该模块分配出来的
%% @spec  insert(Logic::logic(), ID, KVList) -> return()
%% where
%%  return() =  integer() | false
%%@end
%% -----------------------------------------------------------------
-spec insert(tuple(), integer(), record_info()) ->
	integer() | false.
%% -----------------------------------------------------------------
insert({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, 0, KVList) ->
	Max = ets:lookup_element(MCache, ?MAX, 2),
	ets:insert(MCache, {?MAX, Max + 1}),
	insert_(IDEts, AttrEts, Max, IndexTree, MCache, KVList);
insert({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, ID, KVList) ->
	case ets:lookup_element(MCache, ?MAX, 2) of
		Max when ID >= Max ->
			ets:insert(MCache, {?MAX, ID + 1}),
			insert_(IDEts, AttrEts, ID, IndexTree, MCache, KVList);
		_ ->
			case ets:member(IDEts, ID) of
				true ->
					false;
				_ ->
					insert_(IDEts, AttrEts, ID, IndexTree, MCache, KVList)
			end
	end.

%% -----------------------------------------------------------------
%%@doc  更新指定id的记录，或指定查询条件的记录
%%```
%%  Query = ID | Key | {Key, Value} | {Key, Fun, A} | [Key, {Key, Value}, {Key, Fun, A}]
%%  Modify = Key | {Key, Value} | {Key, Fun, A} | [Key, {Key, Value}, {Key, Fun, A}]
%%'''
%% @spec  update(Logic::logic(), ID, Modify::list()) -> return()
%% where
%%  return() =  none | list() | [{integer(), list()}]
%%@end
%% -----------------------------------------------------------------
-spec update(tuple(), query_info() | [query_info()], modify_info() | [modify_info()]) ->
	none | list() | [{integer(), list()}].
%% -----------------------------------------------------------------
update({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, ID, Modify) when is_integer(ID) ->
	case ets:member(IDEts, ID) of
		true ->
			update_(IDEts, AttrEts, IndexTree, MCache, Modify, ID);
		_ ->
			none
	end;
update({IDEts, AttrEts, IndexTree, IndexList, MCache, QCache, Strict}, [_ | _] = Query, Modify) ->
	update_list(IDEts, AttrEts, IndexTree, MCache, Modify, select_list(IDEts, AttrEts, IndexList, QCache, Query, Strict));
update({IDEts, AttrEts, IndexTree, _, MCache, QCache, Strict}, Query, Modify) ->
	update_list(IDEts, AttrEts, IndexTree, MCache, Modify, select_(IDEts, AttrEts, IndexTree, QCache, Query, Strict)).

%% -----------------------------------------------------------------
%%@doc  删除指定id的记录，或指定查询条件的记录
%%```
%%  query_info = ID | Key | {Key, Value} | {Key, Comparison, A} | {Key, Range, A1, A2} | {Fun, A} | {Key, Fun, A}
%%  Query = query_info | [query_info]
%%'''
%% @spec  delete(Logic::logic(), ID) -> return()
%% where
%%  return() =  [] | [tuple()]
%%@end
%% -----------------------------------------------------------------
-spec delete(tuple(), query_info() | [query_info()]) ->
	[] | list() | [{integer(), list()}].
%% -----------------------------------------------------------------
delete({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, ID) when is_integer(ID) ->
	delete_(IDEts, AttrEts, IndexTree, MCache, ID);
delete({IDEts, AttrEts, IndexTree, IndexList, MCache, QCache, Strict}, [_ | _] = Query) ->
	[{ID, delete_(IDEts, AttrEts, IndexTree, MCache, ID)} || ID <- select_list(IDEts, AttrEts, IndexList, QCache, Query, Strict)];
delete({IDEts, AttrEts, IndexTree, _, MCache, QCache, Strict}, Query) ->
	[{ID, delete_(IDEts, AttrEts, IndexTree, MCache, ID)} || ID <- select_(IDEts, AttrEts, IndexTree, QCache, Query, Strict)].

%% -----------------------------------------------------------------
%%@doc  在ID列表中获得指定查询条件的记录
%%```
%%  query_info = ID | Key | {Key, Value} | {Key, Comparison, A} | {Key, Range, A1, A2} | {Fun, A} | {Key, Fun, A}
%%  Query = query_info | [query_info]
%%'''
%% @spec  select(Logic::logic(), IDList, Query) -> return()
%% where
%%  return() =  [] | [integer()]
%%@end
%% -----------------------------------------------------------------
-spec select(tuple(), [integer()], query_info() | [query_info()]) ->
	[] | [integer()].
%% -----------------------------------------------------------------
select({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, IDList, Query) when is_list(Query) ->
	matchs_list(AttrEts, IDList, Query, none);
select({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, IDList, Query) ->
	match_list(AttrEts, IDList, Query).

%% -----------------------------------------------------------------
%%@doc  在ID列表中更新指定查询条件的记录
%%```
%%  Query = ID | Key | {Key, Value} | {Key, Fun, A} | [Key, {Key, Value}, {Key, Fun, A}]
%%  Modify = Key | {Key, Value} | {Key, Fun, A} | [Key, {Key, Value}, {Key, Fun, A}]
%%'''
%% @spec  update(Logic::logic(), IDList, Query, Modify) -> return()
%% where
%%  return() =  [] | [integer()]
%%@end
%% -----------------------------------------------------------------
-spec update(tuple(), [integer()], query_info() | [query_info()], modify_info() | [modify_info()]) ->
	[] | [{integer(), list()}].
%% -----------------------------------------------------------------
update({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, IDList, Query, Modify) when is_list(Query) ->
	update_list(IDEts, AttrEts, IndexTree, MCache, Modify, matchs_list(AttrEts, IDList, Query, none));
update({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, IDList, Query, Modify) ->
	update_list(IDEts, AttrEts, IndexTree, MCache, Modify, match_list(AttrEts, IDList, Query)).

%% -----------------------------------------------------------------
%%@doc  在ID列表中删除指定查询条件的记录
%%```
%%  query_info = ID | Key | {Key, Value} | {Key, Comparison, A} | {Key, Range, A1, A2} | {Fun, A} | {Key, Fun, A}
%%  Query = query_info | [query_info]
%%'''
%% @spec  delete(Logic::logic(), IDList, Query) -> return()
%% where
%%  return() =  [] | [{K, A}]
%%@end
%% -----------------------------------------------------------------
-spec delete(tuple(), [integer()], query_info() | [query_info()]) ->
	[] | [{integer(), list()}].
%% -----------------------------------------------------------------
delete({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, IDList, Query) when is_list(Query) ->
	[{ID, delete_(IDEts, AttrEts, IndexTree, MCache, ID)} || ID <- matchs_list(AttrEts, IDList, Query, none)];
delete({IDEts, AttrEts, IndexTree, _, MCache, _QCache, _Strict}, IDList, Query) ->
	[{ID, delete_(IDEts, AttrEts, IndexTree, MCache, ID)} || ID <- match_list(AttrEts, IDList, Query)].

%% -----------------------------------------------------------------
%%@doc  用指定查询条件匹配ID
%%```
%%  query_info = ID | Key | {Key, Value} | {Key, Comparison, A} | {Key, Range, A1, A2} | {Fun, A} | {Key, Fun, A}
%%  Query = query_info | [query_info]
%%'''
%% @spec  match(Logic::logic(), ID, Query) -> return()
%% where
%%  return() = true | false
%%@end
%% -----------------------------------------------------------------
-spec match(tuple(), integer(), query_info() | [query_info()]) ->
	true | false.
%% -----------------------------------------------------------------
match({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, [_ | _] = Query) ->
	match_query_list(AttrEts, ID, Query);
match({_IDEts, AttrEts, _IndexTree, _IndexList, _MCache, _QCache, _Strict}, ID, Query) ->
	match_query(AttrEts, ID, Query).

%%%===================LOCAL FUNCTIONS==================
% 建立索引树
create_index(L) ->
	parse_index(pre_index(L, [], length(L) + 1), sb_trees:empty(), [], []).

% 预处理索引,返回有序[{count, index, keys:list()}]
pre_index([[H] | T], L, N) ->
	pre_index(T, [{1, N, H} | L], N - 1);
pre_index([H | T], L, N) when is_list(H) ->
	Keys = lists:reverse(H),
	pre_index(T, [{length(Keys), N, Keys} | L], N - 1);
pre_index([H | T], L, N) ->
	pre_index(T, [{1, N, H} | L], N - 1);
pre_index([], L, _N) ->
	lists:sort(L).

% 分析索引树，建立单键索引和联合索引
parse_index([{1, _, Key} | T], Tree, L1, L2) ->
	case sb_trees:get(Key, Tree, none) of
		none ->
			Table = ets:new(?MODULE, [ordered_set]),
			Tree1 = sb_trees:insert(Key, {Table, []}, Tree),
			parse_index(T, Tree1, [{Key, Table} | L1], L2);
		_ ->
			parse_index(T, Tree, L1, L2)
	end;
parse_index([{Len, _, [_ | Keys1] = Keys} | T], Tree, L1, L2) ->
	case sb_trees:get(Keys, Tree, none) of
		none ->
			Table = ets:new(?MODULE, [ordered_set]),
			Tree1 = add_union_index(Keys, {Table, Keys}, sb_trees:insert(Keys, {Table, Len}, Tree)),
			parse_index(T, Tree1, L1, [{Keys, Table, lists:sort(Keys), lists:sort(Keys1)} | L2]);
		_ ->
			parse_index(T, Tree, L1, L2)
	end;
parse_index(_, Tree, L1, L2) ->
	{Tree, {L2, L1}}.

% 添加联合索引
add_union_index([K | T], TableUnion, Tree) ->
	Tree1 = case sb_trees:get(K, Tree, none) of
				{Table, Union} ->
					sb_trees:update(K, {Table, [TableUnion | Union]}, Tree);
				none ->
					sb_trees:insert(K, {0, [TableUnion]}, Tree)
			end,
	add_union_index(T, TableUnion, Tree1);
add_union_index([], _TableUnion, Tree) ->
	Tree.

% 设置指定ID的KV
set_attr_(IDEts, AttrEts, ID, Key, Value) ->
	IKV = {{ID, Key}, Value},
	case ets:insert_new(AttrEts, IKV) of
		true ->
			try
				Keys = ets:lookup_element(IDEts, ID, 2),
				ets:insert(IDEts, {ID, [Key | Keys]})
			catch
				_:_ ->
					ets:delete(AttrEts, {ID, Key}),
					false
			end;
		false ->
			ets:insert(AttrEts, IKV)
	end.

% 选出符合查询条件列表的记录
select_list(IDEts, AttrEts, Index, Cache, Query, Strict) ->
	case parse_query(Index, Cache, Query) of
		{Table, Key, []} ->
			select_index(Table, Key, {Key, <<>>}, []);
		{Table, Key, Q} ->
			select_index_query(AttrEts, Table, Key, {Key, <<>>}, Q, []);
		{Table, []} ->
			% ets:select(Table, [{{{'_','$1'}},[],['$1']}]);
			RL = ets:tab2list(Table),
			[ID || {{_, ID}} <- RL];
		{Table, Q} ->
			% ets:select(Table, [{{{'_','$1'}},[],['$1']}]);
			RL = ets:tab2list(Table),
			[ID || {{_, ID}} <- RL, match_query_list(AttrEts, ID, Q)];
		{Table, prev, Limit, Key1, Key2, []} ->
			select_order_prev(Table, {Key1, <<>>}, {Key2, -1}, Limit);
		{Table, _Action, Limit, Key1, Key2, []} ->
			select_order_next(Table, {Key1, -1}, {Key2, <<>>}, Limit);
		{Table, prev, Limit, Key1, Key2, Q} ->
			select_order_prev_query(AttrEts, Table, {Key1, <<>>}, {Key2, -1}, Limit, Q);
		{Table, _Action, Limit, Key1, Key2, Q} ->
			select_order_next_query(AttrEts, Table, {Key1, -1}, {Key2, <<>>}, Limit, Q);
		{OrderKey, Action, Limit, {Table, Key, Q}} ->
			order_limit(AttrEts, OrderKey, Action, Limit,
						select_index_query(AttrEts, Table, Key, {Key, <<>>}, Q, []));
		{OrderKey, Action, Limit, {Table, Q}} ->
			% ets:select(Table, [{{{'_','$1'}},[],['$1']}]);
			RL = ets:tab2list(Table),
			order_limit(AttrEts, OrderKey, Action, Limit,
						[ID || {{_, ID}} <- RL, match_query_list(AttrEts, ID, Q)]);
		{OrderKey, Action, Limit, Q} ->
			order_limit(AttrEts, OrderKey, Action, Limit,
						select_table(IDEts, AttrEts, Q, Strict));
		Q ->
			select_table(IDEts, AttrEts, Q, Strict)
	end.

% 选出符合单个查询条件的记录
select_(IDEts, AttrEts, Index, _Cache, {K, V} = Query, Strict) ->
	case sb_trees:get(K, Index, none) of
		{Table, _Union} when Table =/= 0 ->
			select_index(Table, V, {V, <<>>}, []);
		_ ->
			select_table(IDEts, AttrEts, Query, Strict)
	end;
select_(IDEts, AttrEts, Index, _Cache, Query, Strict) when is_atom(Query) ->
	case sb_trees:get(Query, Index, none) of
		{Table, _Union} when Table =/= 0 ->
			% ets:select(Table, [{{{'_','$1'}},[],['$1']}]);
			[ID || {{_, ID}} <- ets:tab2list(Table)];
		_ ->
			select_table(IDEts, AttrEts, Query, Strict)
	end;
select_(IDEts, AttrEts, Index, _Cache, {K, 'order', V1, V2, Limit}, Strict) when V1 > V2 ->
	case sb_trees:get(K, Index, none) of
		{Table, _Union} when Table =/= 0 ->
			select_order_prev(Table, {V1, <<>>}, {V2, -1}, Limit);
		_ ->
			order_limit(AttrEts, K, prev, Limit, select_table(
				IDEts, AttrEts, {K, '-', V1, V2}, Strict))
	end;
select_(IDEts, AttrEts, Index, _Cache, {K, 'order', V1, V2, Limit}, Strict) ->
	case sb_trees:get(K, Index, none) of
		{Table, _Union} when Table =/= 0 ->
			select_order_next(Table, {V1, -1}, {V2, <<>>}, Limit);
		_ ->
			order_limit(AttrEts, K, next, Limit, select_table(
				IDEts, AttrEts, {K, '-', V1, V2}, Strict))
	end;
select_(IDEts, AttrEts, _Index, _Cache, Query, Strict) when is_tuple(Query) ->
	select_table(IDEts, AttrEts, Query, Strict);
select_(_IDEts, _AttrEts, _Index, _Cache, _, _Strict) ->
	[].

% 获得查询的键
parse_query(Index, Cache, Query) ->
	parse_query(Index, Cache, Query, none, [], [], []).

% 获得查询的键
parse_query(Index, Cache, [{_K, _V} = H | T], Order, L1, L2, L3) ->
	parse_query(Index, Cache, T, Order, [H | L1], L2, L3);
parse_query(Index, Cache, [{_K, 'order', _, _, _} = H | T], _Order, L1, L2, L3) ->
	parse_query(Index, Cache, T, H, L1, L2, L3);
parse_query(Index, Cache, [H | T], Order, L1, L2, L3) when is_atom(H) ->
	parse_query(Index, Cache, T, Order, L1, [H | L2], L3);
parse_query(Index, Cache, [{_K, _, _} = H | T], Order, L1, L2, L3) ->
	parse_query(Index, Cache, T, Order, L1, L2, [H | L3]);
parse_query(Index, Cache, [{_K, _, _, _} = H | T], Order, L1, L2, L3) ->
	parse_query(Index, Cache, T, Order, L1, L2, [H | L3]);
parse_query(Index, Cache, [_ | T], Order, L1, L2, L3) ->
	parse_query(Index, Cache, T, Order, L1, L2, L3);
parse_query(Index, Cache, _, none, L1, L2, L3) ->
	L = lists:sort(L1),
	KL = [K || {K, _} <- L],
	KQ = {KL, L2},
	case ets:lookup(Cache, KQ) of
		[{_, QueryIndex}] ->
			handle_query_index(QueryIndex, L, L2, L3);
		_ ->
			QueryIndex = match_query_index(Index, KL, L2),
			ets:insert(Cache, {KQ, QueryIndex}),
			handle_query_index(QueryIndex, L, L2, L3)
	end;
parse_query(Index, Cache, _, {Key, _, V1, V2, Limit}, L1, L2, L3) ->
	L = lists:sort(L1),
	KL = [K || {K, _} <- L],
	KQ = {Key, KL, L2},
	Action = if
		V1 > V2 -> prev;
		true -> next
	end,
	case ets:lookup(Cache, KQ) of
		[{_, QueryIndex}] ->
			handle_query_order(QueryIndex, Key, Action, V1, V2, Limit, L, L2, L3);
		_ ->
			QueryIndex = match_query_index(Index, Key, KL, L2),
			ets:insert(Cache, {KQ, QueryIndex}),
			handle_query_order(QueryIndex, Key, Action, V1, V2, Limit, L, L2, L3)
	end.

% 匹配最佳的查询索引
match_query_index({UnionIndex, Index}, KeyList, KList) ->
	match_nkey(UnionIndex, Index, KeyList, KList).

% 匹配联合查询的键
match_nkey([{Keys, Table, SortKeys, _SortKeys1} | T], Index, KeyList, KList) ->
	case match_keys(SortKeys, KeyList) of
		true ->
			{Table, 2, Keys};
		false ->
			match_nkey(T, Index, KeyList, KList)
	end;
match_nkey(_, Index, KeyList, KList) ->
	match_1key(Index, Index, KeyList, KList).

% 索引匹配查询
match_keys([Key | _] = Keys, [K | T]) when Key > K ->
	match_keys(Keys, T);
match_keys([Key | Keys], [Key | T]) ->
	match_keys(Keys, T);
match_keys(Keys, _) ->
	Keys =:= [].

% 获得单查询的键
match_1key(Index, [{Key, Table} | T], KeyList, KList) ->
	case member_order(Key, KeyList) of
		true ->
			{Table, 1, Key};
		false ->
			match_1key(Index, T, KeyList, KList)
	end;
match_1key(Index, _, _KeyList, KList) ->
	match_0key(Index, lists:sort(KList)).

% 判断指定的元素是否在排序列表中
member_order(Elem, [H | T]) when Elem > H ->
	member_order(Elem, T);
member_order(Elem, [H | _T]) ->
	Elem =:= H;
member_order(_, _) ->
	false.

% 获得单查询的键
match_0key([{Key, Table} | T], KList) ->
	case member_order(Key, KList) of
		true ->
			{Table, 0, Key};
		false ->
			match_0key(T, KList)
	end;
match_0key(_, _KList) ->
	none.

% 根据查询索引，处理查询语句
handle_query_index({Table, 2, Keys}, KVQuery, KQuery, OtherQuery) ->
	handle_query_index2(Table, Keys, KVQuery, [], lists:reverse(KQuery, OtherQuery));
handle_query_index({Table, 1, Key}, KVQuery, KQuery, OtherQuery) ->
	handle_query_index1(Table, Key, KVQuery, lists:reverse(KQuery, OtherQuery));
handle_query_index({Table, 0, Key}, KVQuery, KQuery, OtherQuery) ->
	handle_query_index0(Table, Key, KQuery, lists:reverse(KVQuery, OtherQuery));
handle_query_index(_, KVQuery, KQuery, OtherQuery) ->
	lists:reverse(KVQuery, lists:reverse(KQuery, OtherQuery)).

% 根据查询索引，获得查询语句
handle_query_index2(Table, [Key | Keys], QL, VL, Query) ->
	handle_query_index2(Table, Keys, Key, QL, VL, Query, []);
handle_query_index2(Table, _, QL, VL, Query) ->
	{Table, VL, lists:reverse(QL, Query)}.

% 根据查询索引，获得查询语句
handle_query_index2(Table, Keys, Key, [{K, _} = H | T], VL, Query, L) when Key > K ->
	handle_query_index2(Table, Keys, Key, T, VL, Query, [H | L]);
handle_query_index2(Table, Keys, Key, [{Key, V} | T], VL, Query, L) ->
	handle_query_index2(Table, Keys, lists:reverse(L, T), [V | VL], Query).

% 根据查询索引，获得查询语句
handle_query_index1(Table, Key, [{Key, V} | T], Query) ->
	{Table, V, lists:reverse(T, Query)};
handle_query_index1(Table, Key, [H | T], Query) ->
	handle_query_index1(Table, Key, T, [H | Query]).

% 根据查询索引，获得查询语句
handle_query_index0(Table, Key, [Key | T], Query) ->
	{Table, lists:reverse(T, Query)};
handle_query_index0(Table, Key, [H | T], Query) ->
	handle_query_index0(Table, Key, T, [H | Query]).


% 匹配最佳的联合查询索引
match_query_index({UnionIndex, Index}, OrderKey, KeyList, KList) ->
	match_nkey(UnionIndex, UnionIndex, Index, OrderKey, KeyList, KList).

% 匹配联合查询的键
match_nkey(UnionIndex, [{[OrderKey | Keys], Table, _SortKeys, SortKeys1} | T], Index, OrderKey, KeyList, KList) ->
	case match_keys(SortKeys1, KeyList) of
		true ->
			{Table, 'order', Keys};
		false ->
			match_nkey(UnionIndex, T, Index, OrderKey, KeyList, KList)
	end;
match_nkey(UnionIndex, [_ | T], Index, OrderKey, KeyList, KList) ->
	match_nkey(UnionIndex, T, Index, OrderKey, KeyList, KList);
match_nkey(UnionIndex, _, Index, OrderKey, KeyList, KList) ->
	match_1key(UnionIndex, Index, Index, OrderKey, KeyList, KList).

% 获得单查询的键
match_1key(_UnionIndex, _Index, [{OrderKey, Table} | _], OrderKey, _KeyList, _KList) ->
	{Table, 'order'};
match_1key(UnionIndex, Index, [_ | T], OrderKey, KeyList, KList) ->
	match_1key(UnionIndex, Index, T, OrderKey, KeyList, KList);
match_1key(UnionIndex, Index, _, _OrderKey, KeyList, KList) ->
	match_nkey(UnionIndex, Index, KeyList, KList).

% 根据查询索引，获得查询语句
handle_query_order({Table, 'order', Keys}, _OrderKey, Action, V1, V2, Limit, KVQuery, KQuery, OtherQuery) ->
	handle_query_order(Table, Action, Limit, Keys, KVQuery, [V1], [V2], lists:reverse(KQuery, OtherQuery));
handle_query_order({Table, 'order'}, _OrderKey, Action, V1, V2, Limit, KVQuery, KQuery, OtherQuery) ->
	{Table, Action, Limit, V1, V2, lists:reverse(KVQuery, lists:reverse(KQuery, OtherQuery))};
handle_query_order(QueryIndex, OrderKey, prev, V1, V2, Limit, KVQuery, KQuery, OtherQuery) ->
	{OrderKey, prev, Limit, handle_query_index(QueryIndex, KVQuery, KQuery,
		[{OrderKey, '-', V2, V1} | OtherQuery])};
handle_query_order(QueryIndex, OrderKey, Action, V1, V2, Limit, KVQuery, KQuery, OtherQuery) ->
	{OrderKey, Action, Limit, handle_query_index(QueryIndex, KVQuery, KQuery,
		[{OrderKey, '-', V1, V2} | OtherQuery])}.

% 根据查询索引，获得查询语句
handle_query_order(Table, Action, Limit, [Key | Keys], QL, L1, L2, Query) ->
	handle_query_order(Table, Action, Limit, Keys, Key, QL, L1, L2, Query, []);
handle_query_order(Table, Action, Limit, _, QL, L1, L2, Query) ->
	{Table, Action, Limit, L1, L2, lists:reverse(QL, Query)}.

% 根据查询索引，获得查询语句
handle_query_order(Table, Action, Limit, Keys, Key, [{K, _} = H | T], L1, L2, Query, L) when Key > K ->
	handle_query_order(Table, Action, Limit, Keys, T, L1, L2, Query, [H | L]);
handle_query_order(Table, Action, Limit, Keys, Key, [{Key, V} | T], L1, L2, Query, L) ->
	handle_query_order(Table, Action, Limit, Keys, lists:reverse(L, T), [V | L1], [V | L2], Query).


% 从表中选出符合查询条件的记录
select_table(IDEts, AttrEts, Query, false) ->
	L = ets:select(IDEts, [{{'$1','_'},[],['$1']}]),
	if
		is_list(Query) ->
			[ID || ID <- L, match_query_list(AttrEts, ID, Query)];
		true ->
			[ID || ID <- L, match_query(AttrEts, ID, Query)]
	end;
select_table(_IDEts, _AttrEts, Query, _Strict) ->
	erlang:error({stict_module, invalid_query, Query}).

% 从索引中选出记录
select_index(Table, Index, Key, L) ->
	case ets:prev(Table, Key) of
		{Index, ID} = Prev ->
			select_index(Table, Index, Prev, [ID | L]);
		_ ->
			L
	end.

% 从索引中选出符合查询条件的记录
select_index_query(Ets, Table, Index, Key, Query, L) ->
	case ets:prev(Table, Key) of
		{Index, ID} = Prev ->
			case match_query_list(Ets, ID, Query) of
				true ->
					select_index_query(Ets, Table, Index, Prev, Query, [ID | L]);
				_ ->
					select_index_query(Ets, Table, Index, Prev, Query, L)
			end;
		_ ->
			L
	end.

%匹配查询条件
match_query(Ets, ID, {K, A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V =:= A
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {'$key', 'has', K}) ->
	ets:member(Ets, {ID, K});
match_query(Ets, ID, {'$key', 'not', K}) ->
	not ets:member(Ets, {ID, K});
match_query(_Ets, ID, {'$id', 'in', A}) ->
	lists:member(ID, A);
match_query(_Ets, ID, {'$id', 'out', A}) ->
	lists:member(ID, A) =/= true;
match_query(Ets, ID, {'$id', F, A}) ->
	F(A, Ets, ID) =:= true;
match_query(Ets, ID, {K, 'in', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		lists:member(V, A)
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {K, 'out', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		lists:member(V, A) =/= true
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {K, '>', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V > A
	catch
		_:_ ->
			false
	end;
match_query(Ets, ID, {K, '>=', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V >= A
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {K, '<', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V < A
	catch
		_:_ ->
			false
	end;
match_query(Ets, ID, {K, '=<', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V =< A
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {K, '!', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V =/= A
	catch
		_:_ ->
			false
	end;
match_query(Ets, ID, {K, '-', A1, A2}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V >= A1 andalso V =< A2
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {K, '!', A1, A2}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		V < A1 orelse V > A2
	catch
		_:_ ->
			false
	end;
match_query(Ets, ID, {K, 'member', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		lists:member(A, V)
	catch
		_:_ -> false
	end;
match_query(Ets, ID, {K, 'notmemeber', A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		lists:member(A, V) =/= true
	catch
		_:_ ->
			false
	end;
match_query(Ets, ID, {K, F, A}) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		F(A, V) =:= true
	catch
		_:_ -> false
	end.

%匹配查询条件列表
match_query_list(Ets, ID, [H | T]) ->
	case match_query(Ets, ID, H) of
		true ->
			match_query_list(Ets, ID, T);
		_ ->
			false
	end;
match_query_list(_Ets, _ID, []) ->
	true.

% 从索引中选出从大到小的记录
select_order_prev(Table, Key1, Key2, Limit) when Limit > 0 ->
	case ets:prev(Table, Key1) of
		{_, ID} = K when K > Key2 ->
			[ID | select_order_prev(Table, K, Key2, Limit - 1)];
		_ ->
			[]
	end;
select_order_prev(_Table, _Key1, _Key2, _Limit) ->
	[].

% 从索引中选出从小到大的记录
select_order_next(Table, Key1, Key2, Limit) when Limit > 0 ->
	case ets:next(Table, Key1) of
		{_, ID} = K when K < Key2 ->
			[ID | select_order_next(Table, K, Key2, Limit - 1)];
		_ ->
			[]
	end;
select_order_next(_Table, _Key1, _Key2, _Limit) ->
	[].

% 从索引中选出符合查询条件的从大到小的记录
select_order_prev_query(Ets, Table, Key1, Key2, Limit, Query) when Limit > 0 ->
	case ets:prev(Table, Key1) of
		{_, ID} = K when K > Key2 ->
			case match_query_list(Ets, ID, Query) of
				true ->
					[ID | select_order_prev_query(Ets, Table, K, Key2, Limit - 1, Query)];
				_ ->
					select_order_prev_query(Ets, Table, K, Key2, Limit, Query)
			end;
		_ ->
			[]
	end;
select_order_prev_query(_Ets, _Table, _Key1, _Key2, _Limit, _Query) ->
	[].

% 从索引中选出符合查询条件的从小到大的记录
select_order_next_query(Ets, Table, Key1, Key2, Limit, Query) when Limit > 0 ->
	case ets:next(Table, Key1) of
		{_, ID} = K when K < Key2 ->
			case match_query_list(Ets, ID, Query) of
				true ->
					[ID | select_order_next_query(Ets, Table, K, Key2, Limit - 1, Query)];
				_ ->
					select_order_next_query(Ets, Table, K, Key2, Limit, Query)
			end;
		_ ->
			[]
	end;
select_order_next_query(_Ets, _Table, _Key1, _Key2, _Limit, _Query) ->
	[].

% 根据指定键的值进行排序，取前n名
order_limit(Ets, Key, Action, Limit, IDList) ->
	[ID || {_, ID} <- lists:sublist(order_sort(Ets, Key, Action, IDList, []), Limit)].

% 根据指定键的值进行排序
order_sort(Ets, Key, Action, [ID | T], L) ->
	try ets:lookup_element(Ets, {ID, Key}, 2) of
		V -> order_sort(Ets, Key, Action, T, [{V, ID} | L])
	catch
		_:_ -> order_sort(Ets, Key, Action, T, L)
	end;
order_sort(_Ets, _, prev, _, L) ->
	lists:reverse(lists:sort(L));
order_sort(_Ets, _, _, _, L) ->
	lists:sort(L).


% 插入数据，使用索引缓存快速获得设置索引
insert_(IDEts, AttrEts, ID, Index, Cache, KVList) ->
	insert_index(AttrEts, ID, get_index_key(Index, Cache, insert_(IDEts, AttrEts, ID, KVList, []))).

% 插入数据，返回键列表
insert_(IDEts, AttrEts, ID, [{K, V} | T], L) ->
	ets:insert(AttrEts, {{ID, K}, V}),
	insert_(IDEts, AttrEts, ID, T, [K | L]);
insert_(IDEts, _AttrEts, ID, _, L) ->
	ets:insert(IDEts, {ID, L}),
	L.

insert_index(Ets, ID, {Key, Keys}) ->
	insert_key_index(Ets, ID, Key),
	insert_union_index(Ets, ID, Keys).

insert_key_index(Ets, ID, [{Table, Key} | T]) ->
	try ets:lookup_element(Ets, {ID, Key}, 2) of
		V -> ets:insert(Table, {{V, ID}})
	catch
		_:_ -> false
	end,
	insert_key_index(Ets, ID, T);
insert_key_index(_Ets, ID, []) ->
	ID.

% 插入联合索引
insert_union_index(Ets, ID, [{Table, Keys} | T]) ->
	insert_union_index(Ets, ID, Table, Keys, []),
	insert_union_index(Ets, ID, T);
insert_union_index(_Ets, ID, _) ->
	ID.

% 插入联合索引
insert_union_index(Ets, ID, Table, [Key | T], L) ->
	try ets:lookup_element(Ets, {ID, Key}, 2) of
		V -> insert_union_index(Ets, ID, Table, T, [V | L])
	catch
		_:_ -> false
	end;
insert_union_index(_Ets, ID, Table, _, L) ->
	ets:insert(Table, {{L, ID}}).

% 修改记录
modify(Ets, ID, [{K, V} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		V -> modify(Ets, ID, T, L);
		R -> modify(Ets, ID, T, [{modify, K, {R, V}} | L])
	catch
		_:_ -> modify(Ets, ID, T, [{insert, K, V} | L])
	end;
modify(Ets, ID, [{'$key', 'del', K} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		R -> modify(Ets, ID, T, [{delete, K, R} | L])
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(Ets, ID, [{K, '+', A} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		V -> modify(Ets, ID, T, [{modify, K, {V, V + A}} | L])
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(Ets, ID, [{K, '-', A} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		V -> modify(Ets, ID, T, [{modify, K, {V, V - A}} | L])
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(Ets, ID, [{K, '*', A} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		V -> modify(Ets, ID, T, [{modify, K, {V, V * A}} | L])
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(Ets, ID, [{K, '/', A} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		V -> modify(Ets, ID, T, [{modify, K, {V, V / A}} | L])
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(Ets, ID, [{K, 'div', A} | T], L) ->
	try ets:lookup_element(Ets, {ID, K}, 2) of
		V -> modify(Ets, ID, T, [{modify, K, {V, V div A}} | L])
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(Ets, ID, [{K, F, A} | T], L) ->
	try
		V = ets:lookup_element(Ets, {ID, K}, 2),
		case F(A, V) of
			V -> modify(Ets, ID, T, L);
			R -> modify(Ets, ID, T, [{modify, K, {V, R}} | L])
		end
	catch
		_:_ -> modify(Ets, ID, T, L)
	end;
modify(_Ets, _ID, [], L) ->
	L.

% 更新替换记录
update_(IDEts, AttrEts, ID, [_ | _] = Diff, KeyIndex) ->
	delete_index(AttrEts, ID, KeyIndex),
	replace_diff(IDEts, AttrEts, ID, Diff, [], []),
	insert_index(AttrEts, ID, KeyIndex),
	Diff;
update_(_IDEts, _AttrEts, _ID, Diff, _KeyIndex) ->
	Diff.

% 更新替换记录
update_(IDEts, AttrEts, Index, Cache, Modify, ID) ->
	Modify1 = if
		is_list(Modify) -> Modify;
		true -> [Modify]
	end,
	case modify(AttrEts, ID, Modify1, []) of
		[_ | _] = Diff ->
			update_(IDEts, AttrEts, ID, Diff,
					get_index_key(Index, Cache, [K || {_, K, _} <- Diff]));
		_ ->
			[]
	end.

% 更新ID列表
update_list(IDEts, AttrEts, Index, Cache, Modify, [ID]) ->
	update_(IDEts, AttrEts, Index, Cache, Modify, ID);
update_list(IDEts, AttrEts, Index, Cache, Modify, [_ | _] = IDList) ->
	Modify1 = if
		is_list(Modify) -> Modify;
		true -> [Modify]
	end,
	KeyIndex = get_index_key(Index, Cache, [case I of
			{K1, _} -> K1;
			K2 when is_atom(K2) -> K2;
			{K3, _, _} -> K3
		end || I <- Modify1]),
	[{ID, update_(IDEts, AttrEts, ID, modify(AttrEts, ID, Modify1, []), KeyIndex)} ||
		ID <- IDList];
update_list(_IDEts, _AttrEts, _Index, _Cache, _Modify, _) ->
	[].

% 获得改变的键对应的索引，结果保留在Cache中
get_index_key(Index, Cache, KeyList) ->
	case ets:lookup(Cache, KeyList) of
		[{_, KeyIndex}] ->
			KeyIndex;
		_ ->
			KeyIndex = get_index_key(Index, KeyList, [], []),
			ets:insert(Cache, {KeyList, KeyIndex}),
			KeyIndex
	end.

% 将改变的键展开，剔除重复的键，返回所有受影响的单键索引和联合索引
get_index_key(Index, [K | T], L1, L2) ->
	case sb_trees:get(K, Index, none) of
		{Table, Union} when Table =/= 0 ->
			get_index_key(Index, T, [{Table, K} | L1], lists:reverse(Union, L2));
		{_, Union} ->
			get_index_key(Index, T, L1, lists:reverse(Union, L2));
		_ ->
			get_index_key(Index, T, L1, L2)
	end;
get_index_key(_Index, [], L1, L2) ->
	{L1, lists:usort(L2)}.

% 替换新旧键值
replace_diff(IDEts, AttrEts, ID, [{modify, K, {_, V}} | T], AddKL, DelKL) when is_atom(K) ->
	ets:insert(AttrEts, {{ID, K}, V}),
	replace_diff(IDEts, AttrEts, ID, T, AddKL, DelKL);
replace_diff(IDEts, AttrEts, ID, [{insert, K, V} | T], AddKL, DelKL) ->
	ets:insert(AttrEts, {{ID, K}, V}),
	replace_diff(IDEts, AttrEts, ID, T, [K | AddKL], DelKL);
replace_diff(IDEts, AttrEts, ID, [{delete, K, _} | T], AddKL, DelKL) ->
	ets:delete(AttrEts, {ID, K}),
	replace_diff(IDEts, AttrEts, ID, T, AddKL, [K | DelKL]);
replace_diff(_IDEts, _AttrEts, _ID, _, [], []) ->
	ok;
replace_diff(IDEts, _AttrEts, ID, _, AddKL, DelKL) ->
	Keys = ets:lookup_element(IDEts, ID, 2),
	ets:insert(IDEts, {ID, AddKL ++ list_delete(DelKL, Keys)}).

% 在指定列表中删除另一个列表的全部元素
list_delete([H | T], L) ->
	list_delete(T, lists:delete(H, L));
list_delete(_, L) ->
	L.

% 删除指定的ID
delete_(IDEts, AttrEts, Index, Cache, ID) ->
	try ets:lookup_element(IDEts, ID, 2) of
		Keys ->
			ets:delete(IDEts, ID),
			delete_index(AttrEts, ID, get_index_key(Index, Cache, Keys)),
			[KV || K <- Keys, (KV = del_attr_(AttrEts, ID, K)) =/= none]
	catch
		_:_ -> []
	end.

% 删除索引
delete_index(Ets, ID, {Key, Keys}) ->
	delete_key_index(Ets, ID, Key),
	delete_union_index(Ets, ID, Keys).

% 删除单键索引
delete_key_index(Ets, ID, [{Table, Key} | T]) ->
	try ets:lookup_element(Ets, {ID, Key}, 2) of
		V -> ets:delete(Table, {V, ID})
	catch
		_:_ -> false
	end,
	delete_key_index(Ets, ID, T);
delete_key_index(_Ets, _ID, _) ->
	ok.

% 删除联合索引
delete_union_index(Ets, ID, [{Table, Keys} | T]) ->
	delete_union_index(Ets, ID, Table, Keys, []),
	delete_union_index(Ets, ID, T);
delete_union_index(_Ets, _ID, _) ->
	ok.

% 删除联合索引
delete_union_index(Ets, ID, Table, [Key | T], L) ->
	try ets:lookup_element(Ets, {ID, Key}, 2) of
		V -> delete_union_index(Ets, ID, Table, T, [V | L])
	catch
		_:_ -> false
	end;
delete_union_index(_Ets, ID, Table, [], L) ->
	ets:delete(Table, {L, ID}).


% 删除指定ID的键值
del_attr_(Ets, ID, K) ->
	Key = {ID, K},
	try
		V = ets:lookup_element(Ets, Key, 2),
		ets:delete(Ets, Key),
		{K, V}
	catch
		_:_ -> none
	end.


%对IDList匹配查询列表
matchs_list(Ets, IDList, [{_, 'order', _, _, _} = H | T], _Order) ->
	matchs_list(Ets, IDList, T, H);
matchs_list(Ets, IDList, [Q | T], Order) ->
	case [ID || ID <- IDList, match_query(Ets, ID, Q)] of
		[] ->
			[];
		L ->
			matchs_list(Ets, L, T, Order)
	end;
matchs_list(_Ets, IDList, _, none) ->
	IDList;
matchs_list(Ets, IDList, _, {K, _, V1, V2, Limit}) when V1 > V2 ->
	lists:sublist([ID || {V, ID} <- order_sort(Ets, K, prev, IDList, []), V =< V1, V >= V2], Limit);
matchs_list(Ets, IDList, _, {K, _, V1, V2, Limit}) ->
	lists:sublist([ID || {V, ID} <- order_sort(Ets, K, prev, IDList, []), V >= V1, V =< V2], Limit).

%对IDList匹配查询
match_list(Ets, IDList, {K, 'order', V1, V2, Limit}) when V1 > V2 ->
	lists:sublist([ID || {V, ID} <- order_sort(Ets, K, prev, IDList, []), V =< V1, V >= V2], Limit);
match_list(Ets, IDList, {K, 'order', V1, V2, Limit}) ->
	lists:sublist([ID || {V, ID} <- order_sort(Ets, K, next, IDList, []), V >= V1, V =< V2], Limit);
match_list(Ets, IDList, Query) ->
	[ID || ID <- IDList, match_query(Ets, ID, Query)].
