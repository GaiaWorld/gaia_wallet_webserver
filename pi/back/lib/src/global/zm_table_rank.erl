%%@doc进程的分布式管理
%%```
%%%进程在节点上被zm_pid创建并维护。
%%%用指定的进程表记录了进程Pid。
%%%进程表的数据被zm_cacher缓冲。需要在zm_event上设置zm_db_cache。
%%%需要在zm_cacher上设置加载MFA，
%%%	{[calc], zm_cacher, [Type, Sizeout, Timeout, ReadTimeUpdate, {zm_pid_dist, create, {Layer, Divisor, RandNodeCount, MFA}}]}.
%%%	{[calc], zm_cacher, [game_scene, 99999999, 4294967295, false, {zm_pid_dist, create, {[calc, gateway], 100, 2, {zm_scene, start_link, []}}}]}.
%%%定时器定时到所有的计算节点上通过遍历进程，累加获取节点负荷，并以""为键存储在进程表上。
%%%进程必须实现handle_call(burden, _, _)的协议，以返回负荷。
%%%需要在zm_dtimer上设置负荷整理MFA，
%%%	{[calc], zm_dtimer, [{Src, _Type}, {M, F, A}, {time, Cycle, Timeout}]}.
%%%	{[calc], zm_dtimer, [{Src, scene_dist}, {zm_pid_dist, collate, [calc, Table]}, {time, 60000, 5000}]}.
%%%create创建进程前，根据负荷和误差，计算出到那个节点上创建进程。
%%%进程退出时，应由zm_pid发出的pid_exit的事件来清理进程表上的记录。
%%%需要在zm_event上设置监听MFA，
%%%	{[calc], zm_event, [{zm_pid, pid_exit}, {zm_pid_dist, pid_exit, Table}, 3000]}.
%%%节点退出时，应由zm_node发出的pid_exit的事件来清理进程表上的记录。
%%%需要在zm_event上设置监听MFA，
%%%	{[calc], zm_event, [{zm_node, nodedown}, {zm_pid_dist, node_down, {Layer, Table}}, 3000]}.
%%'''
%%@end


-module(zm_table_rank).

-description("pid distributed manager").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([read/2, load/2, create/3, collate/2, collate_burden/1, pid_exit/4, node_down/4]).

%%%=======================DEFINE=======================
-define(LOCK_TIME, 2000).
-define(READ_TIMEOUT, 1000).
-define(NIL, '$nil').

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  read pid
%% @spec  read(Table, Name) -> return()
%% where
%%  return() =  {ok, {Pid, Vsn, Time}} | {none, MFA} | none | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read(Table, Name) ->
	zm_cacher:read({?MODULE, Table}, Name).

%% -----------------------------------------------------------------
%%@doc  load pid
%% @spec  load(Table, Name) -> return()
%% where
%%  return() =  {ok, {Pid, Vsn, Time}} | none | {error, Reason}
%%@end
%% -----------------------------------------------------------------
load(Table, Name) ->
	zm_cacher:load({?MODULE, Table}, Name).

%% -----------------------------------------------------------------
%%@doc 创建
%%```
%%	Divisor为将负荷除数，将负荷缩小到一定的区间内，
%%	RandNodeCount为随机节点的数量，表示每次创建时，至少用该数量的节点进行随机。
%%'''
%% @spec create({Layer, Divisor, RandNodeCount, MFA}, Type, Name) -> return()
%% where
%%  return() = {ok, Pid} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
create({Layer, Divisor, RandNodeCount, MFA}, {_, Table} = Type, Name) ->
	NL = zm_node:active_nodes(Layer),
	case zm_cacher:load(Type, "") of
		{ok, NodeBurdenTree} ->
			create_pid(select_node(NL, NodeBurdenTree,
				Divisor, RandNodeCount, []), MFA, Table, Name);
		{none, _MFA} ->
			create_pid(erlang:hd(z_lib:shuffle(NL)), MFA, Table, Name);
		none ->
			{error, cacher_not_config};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc  dtimer collate, compute burden
%% @spec  collate({Layer, Table}, TimeDate) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
collate({Layer, Table}, _TimeDate) ->
	{ResL, _BadNodes} = rpc:multicall(zm_node:active_nodes(Layer),
		?MODULE, collate_burden, [Table]),
	zm_db_client:write(Table, "", sb_trees:from_orddict(lists:sort(ResL)), 0, ?MODULE, 0).

%% -----------------------------------------------------------------
%%@doc  dtimer collate, compute burden
%% @spec  collate_burden(Table) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
collate_burden(Table) ->
	MT = {?MODULE, Table},
	{node(), lists:sum([get_burden(Pid) || {Name, Pid} <- zm_pid:list(), Name =:= MT])}.

%% -----------------------------------------------------------------
%%@doc  pid exit event listen function
%% @spec  pid_exit(Table, any(), any(), {{Name, Pid}, Reason}) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
pid_exit(Table, _, _, {{Name, _Pid}, _Reason}) ->
	case zm_db_client:lock(Table, Name, ?MODULE, ?LOCK_TIME, ?READ_TIMEOUT) of
		ok ->
			zm_db_client:delete(Table, Name, 0, ?MODULE);
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc  pid exit event listen function
%% @spec  node_down({Layer, Table}, any(), any(), Node) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
node_down({Layer, Table}, _, _, Node) ->
	{_, NodeList, _, ActiveNodeList, _} = zm_node:layer(Layer),
	case order_member(Node, NodeList) of
		true ->
			ActiveNode = erlang:hd(ActiveNodeList),
			if
				%最小活动节点负责删除进程
				ActiveNode =:= node() ->
					select_delete(Table, ActiveNodeList, ascending);
				true ->
					false
			end;
		false ->
			false
	end.

%%%===================LOCAL FUNCTIONS==================
% 选择节点
select_node([H | T], Tree, Divisor, Count, L) ->
	Burden = sb_trees:get(Tree, H, 0),
	select_node(T, Tree, Divisor, Count, [{Burden div Divisor, H} | L]);
select_node([], _, _, Count, L) ->
	select_node(lists:sort(L), Count, 0, []).

% 选择节点
select_node([{Burden, N} | T], Count, Limit, L) when Count > 0, Burden =< Limit ->
	select_node(T, Count - 1, Limit, [N | L]);
select_node([{Burden, N} | T], Count, _Limit, L) when Count > 0 ->
	select_node(T, Count - 1, Burden, [N | L]);
select_node([{Burden, N} | T], _Count, Limit, L) when Burden =< Limit ->
	select_node(T, 0, Limit, [N | L]);
select_node(_, _, _, L) ->
	erlang:hd(z_lib:shuffle(L)).

% 创建进程
create_pid(Node, {M, F, A}, Table, Name) ->
	Fun = fun() ->
		case zm_db_client:read(Table, Name, ?MODULE, ?LOCK_TIME, ?READ_TIMEOUT) of
			{ok, ?NIL, _Vsn, _Time} ->
				% 创建进程
				try M:F(A, Name) of
					{ok, Pid} = R when is_pid(Pid) ->
						case zm_db_client:write(Table, Name, Pid, 1, ?MODULE, 0) of
							ok ->
								R;
							E ->
								exit(Pid, kill),
								E
						end;
					{ok, V} ->
						zm_db_client:lock(Table, Name, ?MODULE, 0, 0),
						{error, {invalid_pid, V}};
					E ->
						zm_db_client:lock(Table, Name, ?MODULE, 0, 0),
						E
				catch
					_:Reason ->
						zm_db_client:lock(Table, Name, ?MODULE, 0, 0),
						{error, Reason, erlang:get_stacktrace()}
				end;
			{ok, Pid, Vsn, Time} ->
				zm_db_client:lock(Table, Name, ?MODULE, 0, 0),
				{error, {already, {Pid, Vsn, Time}}};
			E ->
				E
		end
	end,
	case zm_pid:create(Node, Name, Fun) of
		{ok, Pid} ->
			{ok, {Pid, 2, zm_time:now_millisecond()}};
		{error, {already, Pid}} ->
			{ok, Pid};
		R ->
			R
	end.

% 获取进程的负荷
get_burden(Pid) ->
	try
		gen_server:call(Pid, burden)
	catch
		_:_ ->
		0
	end.

% 判断指定的元素是否在有序列表中
order_member(E, [H | T]) when E > H ->
	order_member(E, T);
order_member(E, [H | _]) ->
	E =:= H;
order_member(_, _) ->
	false.

% 删除所有不在活动节点上的进程
select_delete(Table, NodeList, KeyRange) ->
	F = fun
		({_Key, Value}) when is_pid(Value) ->
			order_member(node(Value), NodeList);
		(_) ->
			false
	end,
	case zm_db_client:select(Table, KeyRange, F, value, key, true) of
		{ok, R} ->
			% 没有先锁住，所以不保证一定删除
			zm_db_client:deletes([{Table, Key, 0} || Key <- R], ?MODULE);
		{limit, R} ->
			zm_db_client:deletes([{Table, Key, 0} || Key <- R], ?MODULE),
			select_delete(Table, NodeList, {ascending, open, lists:max(R)});
		E ->
			E
	end.
