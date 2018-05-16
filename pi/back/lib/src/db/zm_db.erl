%%@doc 数据库模块
%%```
%%% 表的配置：{Table, Duplication, Options}
%%'''
%%@end


-module(zm_db).

-description("db module").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([list/0, get/1, set/3, unset/3, delete/1, nodes/0, active_nodes/2]).
-export([dht_node/2, dht_node/4, active_dht_node/2, active_dht_node/3]).
-export([first_active_dht_node/4, check_complete/1,check_cluster_complete/1, check_table_state/1]).
-export([relation_node/2, active_relation_node/2]).
-export([default/1]).

%%%=======================DEFINE=======================
-define(DB, db).

-define(RANGE, 16#ffff).

-define(NIL, '$nil').

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  列出当前所有的表配置
%% @spec  list() -> return()
%% where
%%  return() =  [{Table, Duplication, NodeList}] | none
%%@end
%% -----------------------------------------------------------------
list() ->
	zm_config:get(?MODULE).

%% -----------------------------------------------------------------
%%@doc  获得指定的库配置
%% @spec  get(Table::atom()) -> return()
%% where
%%  return() =  none | {Table, Duplication, Options}
%%@end
%% -----------------------------------------------------------------
get(Table) ->
	zm_config:get(?MODULE, Table).

%% -----------------------------------------------------------------
%%@doc  设置指定的库配置
%% @spec  set(Table::atom(), Duplication::integer(), Options::list()) -> return()
%% where
%%  return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Table, Duplication, Options)
	when is_atom(Table), is_integer(Duplication), is_list(Options) ->
	zm_config:set(?MODULE, {Table, Duplication, zm_db_util:table_opts(Options)}).

%% -----------------------------------------------------------------
%%@doc  取消设置指定的库配置
%% @spec  unset(Table::atom(), Duplication::integer(), Options::list()) -> return()
%% where
%%  return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Table, Duplication, Options) when is_atom(Table) ->
	zm_config:unset(?MODULE, {Table, Duplication, Options}).

%% -----------------------------------------------------------------
%%@doc  delete Node
%% @spec  delete(Table::atom()) -> return()
%% where
%%  return() =  none | ok
%%@end
%% -----------------------------------------------------------------
delete(Table) when is_atom(Table) ->
	zm_config:delete(?MODULE, Table).

%% -----------------------------------------------------------------
%%@doc  列出当前存储层节点
%% @spec  nodes() -> return()
%% where
%%  return() =  {Table, list(), tuple(), list(), tuple()}
%%@end
%% -----------------------------------------------------------------
nodes() ->
	zm_node:layer(?DB).

%% -----------------------------------------------------------------
%%@doc  获得活动的节点列表
%% @spec  active_nodes(NL, NAL) -> return()
%% where
%%  return() =  NodeList
%%@end
%% -----------------------------------------------------------------
active_nodes(NL, NAL) ->
	active_nodes(NL, NAL, []).

active_nodes([N1 | _] = NL, [{N2, _} | NAL], L) when N1 > N2 ->
	active_nodes(NL, NAL, L);
active_nodes([N | NL], [{N, _} | NAL], L) ->
	active_nodes(NL, NAL, [N | L]);
active_nodes([_ | NL], [_ | _] = NAL, L) ->
	active_nodes(NL, NAL, L);
active_nodes(_, _, L) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc  DHT node
%% @spec  dht_node(Table::atom(), Key) -> return()
%% where
%%  return() =  NodeList
%%@end
%% -----------------------------------------------------------------
dht_node(Table, Key) ->
	case zm_config:get(?MODULE, Table) of
		{_, Duplication, _} ->
			case zm_node:layer(?DB) of
				{_, _, NT, _, _} ->
					dht_node(Key, ?RANGE, Duplication, NT);
				none ->
					[]
			end;
		_ ->
			[]
	end.

%% -----------------------------------------------------------------
%%@doc  DHT node
%% @spec  dht_node(Key, Range, Duplication, NT) -> return()
%% where
%%  return() =  NodeList
%%@end
%% -----------------------------------------------------------------
dht_node(Key, Range, Duplication, NT) ->
	Size = tuple_size(NT),
	D = if
		Duplication >= Size -> Size - 1;
		true -> Duplication
	end,
	Step = Range / Size,
	Hash = erlang:phash2(Key, Range),
	I = trunc(Hash / Step) + 1,
	if
		Hash + Step / 2 < I * Step ->
			dht_node(NT, Size, I, 1, D, [element(I, NT)]);
		I < Size ->
			dht_node(NT, Size, I + 1, -1, D, [element(I + 1, NT)]);
		true ->
			dht_node(NT, Size, 1, -1, D, [element(1, NT)])
	end.

dht_node(NT, Size, I, R, D, L) when D > 0 ->
	N = if
		I + R < 1 -> I + R + Size;
		I + R > Size -> I + R - Size;
		true -> I + R
	end,
	if
		R > 0 ->
			dht_node(NT, Size, N, -R - 1, D - 1, [element(N, NT) | L]);
		true ->
			dht_node(NT, Size, N, -R + 1, D - 1, [element(N, NT) | L])
	end;
dht_node(_NT, _Size, _I, _R, _D, L) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc  active DHT node
%% @spec  active_dht_node(Table, Key) -> return()
%% where
%%  return() =  NodeList
%%@end
%% -----------------------------------------------------------------
active_dht_node(Table, Key) ->
	case dht_node(Table, Key) of
		[] ->
			[];
		NL ->
			NAL = zm_service:get(Table),
			[N || N <- NL, zm_service:node_member(N, NAL)]
	end.

%% -----------------------------------------------------------------
%%@doc  active DHT node
%% @spec  active_dht_node(Table, Key, Skip) -> return()
%% where
%%  return() =  NodeList
%%@end
%% -----------------------------------------------------------------
active_dht_node(Table, Key, Skip) ->
	case dht_node(Table, Key) of
		[] ->
			[];
		NL ->
			NAL = zm_service:get(Table),
			[N || N <- NL, N =/= Skip, zm_service:node_member(N, NAL)]
	end.

%% -----------------------------------------------------------------
%%@doc  获得指定键对应的第一个活动的DHT节点
%% @spec  first_active_dht_node(Key, Duplication, NT, NL) -> return()
%% where
%%  return() =  none | Node
%%@end
%% -----------------------------------------------------------------
first_active_dht_node(Key, Duplication, NT, NL) ->
	first_active_dht_node(dht_node(Key, ?RANGE, Duplication, NT), NL).

first_active_dht_node([H | T], NL) ->
	case node_member(H, NL) of
		true ->
			H;
		false ->
			first_active_dht_node(T, NL)
	end;
first_active_dht_node([], _NL) ->
	none.

node_member(N, [H | T]) when N > H ->
	node_member(N, T);
node_member(N, [N | _]) ->
	true;
node_member(_N, _) ->
	false.

%% -----------------------------------------------------------------
%%@doc  判断表的完整性
%% @spec  check_complete(Table::atom()) -> return()
%% where
%%  return() =  {ok, NL, true} | {ok, NL, false} | none_table
%%@end
%% -----------------------------------------------------------------
check_complete(Table) ->
	case zm_config:get(?MODULE, Table) of
		{_, Duplication, _} ->
			case zm_node:layer(?DB) of
				{_, NL, _, _, _} ->
					check_complete(Duplication, NL, zm_service:get(Table), Duplication, []);
				none ->
					none_table
			end;
		_ ->
			none_table
	end.

check_complete(_Duplication, NL, NAL, I, L) when I < 0 ->
	{ok, active_nodes(NL, NAL, L), false};
check_complete(Duplication, [N1 | _] = NL, [{N2, _} | NAL], I, L) when N1 > N2 ->
	check_complete(Duplication, NL, NAL, I, L);
check_complete(Duplication, [N | NL], [{N, _} | NAL], _I, L) ->
	check_complete(Duplication, NL, NAL, Duplication, [N | L]);
check_complete(Duplication, [_ | NL], [_ | _] = NAL, I, L) ->
	check_complete(Duplication, NL, NAL, I - 1, L);
check_complete(_Duplication, _, _, _I, L) ->
	{ok, lists:reverse(L), true}.

%% -----------------------------------------------------------------
%%@doc  判断集群表的完整、一致、可用性
%% @spec  check_cluster_complete(Table::atom()) -> return()
%% where
%%  return() =  {ok, NL, true} | {ok, NL, false} | none
%%@end
%% -----------------------------------------------------------------
check_cluster_complete(Table) ->
	case zm_db:nodes() of
		{_, AllDbNode, _ , ActiveNode, _} ->
			if 
				length(AllDbNode) =:= length(ActiveNode) ->
					 {RunL, OtherL} = lists:partition(
					       			 fun (Node) ->
											  States = case zm_service:get(Table,Node) of
																  {_,{_,State}} ->
																	  State;
																  _ ->
																	  none
															  end,
					            			States =:= running
					        		end, ActiveNode),
					 if
						 length(RunL) =:= length(ActiveNode) ->
							 {ok, RunL ,true};
						 true ->
							 {ok, OtherL, false}
					 end;
				true ->
					none
			end;
		_ ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc  判断表状态,传入[]表示判断所有表是否都是running
%% @spec  check_table_state(Tables::is_list()) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
check_table_state([]) ->
	TableInfoList = zm_config:get(zm_db_server),
	lists:all(fun (TableInfo) -> 
					   case TableInfo of
						   {_, _, _, running, _} ->
							   true;
						   _ ->
							   false
					   end end, TableInfoList);
check_table_state(Tables) when is_list(Tables) ->
	TableInfoList = [zm_service:get(Table) || Table <- Tables],
	lists:all(fun (TableInfo) -> 
					   case TableInfo of
						   [{_,{_,running}}] ->
							   true;
						   _ ->
							   false
					   end end, TableInfoList).

%% -----------------------------------------------------------------
%%@doc  获得指定节点的关联节点
%% @spec  relation_node(Table, Node) -> return()
%% where
%%  return() =  this | none_table | {LeftNodeList, RightNodeList} | NodeList
%%@end
%% -----------------------------------------------------------------
relation_node(Table, Node) ->
	case zm_config:get(?MODULE, Table) of
		{_, Duplication, _} when Duplication > 0 ->
			case zm_node:layer(?DB) of
				{_, NL, _, _, _} ->
					relation_node((Duplication + 1) div 2, Node, NL, []);
				none ->
					none
			end;
		{_, _, _} ->
			this;
		_ ->
			none_table
	end.

relation_node(Duplication, Node, [Node | T], L) ->
	Left = add_list(L, T, [], Duplication),
	Right = add_list(T, L, [], Duplication),
	case [N || N <- Right, not lists:member(N, Left)] of
		[] ->
			Left;
		R ->
			{Left, R}
	end;
relation_node(Duplication, Node, [H | T], L) ->
	relation_node(Duplication, Node, T, [H | L]);
relation_node(_Duplication, _Node, [], _L) ->
	none.

add_list(_, _, L, 0) ->
	L;
add_list([H | T], L2, L, N) ->
	add_list(T, L2, [H | L], N - 1);
add_list([], L2, L, N) ->
	add_list(lists:reverse(L2), L, N).

add_list(_, L, 0) ->
	L;
add_list([H | T], L, N) ->
	add_list(T, [H | L], N - 1);
add_list([], L, _N) ->
	L.

%% -----------------------------------------------------------------
%%@doc  获得指定节点的活动的关联节点
%% @spec  active_relation_node(Table, Node) -> return()
%% where
%%  return() =  this | none | {LeftNodeList, RightNodeList} | NodeList
%%@end
%% -----------------------------------------------------------------
active_relation_node(Table, Node) ->
	case relation_node(Table, Node) of
		{Left, Right} ->
			NAL = zm_service:get(Table),
			{[N || N <- Left, zm_service:node_member(N, NAL)],
				[N || N <- Right, zm_service:node_member(N, NAL)]};
		none ->
			none;
		this ->
			this;
		[] ->
			this;
		NL ->
			NAL = zm_service:get(Table),
			[N || N <- NL, zm_service:node_member(N, NAL)]
	end.

%% -----------------------------------------------------------------
%%@doc  获得指定表的默认值
%% @spec  default(Table) -> return()
%% where
%%  return() =  term()
%%@end
%% -----------------------------------------------------------------
default(Table) ->
	case zm_config:get(?MODULE, Table) of
		{_, _, Opts} ->
			case z_lib:get_value(Opts, 'format', []) of
				{_, {_, _, _, DefVal}} ->
					{ok, DefVal};
				{_, _} ->
					none_default;
				[] ->
					invalid_default
			end;
		none ->
			invalid_default
	end.
