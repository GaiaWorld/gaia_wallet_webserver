%%@doc 数据库内部操作模块。负责修复、重组数据。
%%@end


-module(zm_db_handler).

-description("data base handler").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).

%%
%% %%%=======================EXPORT=======================
%% -export([auto_repair/3, auto_repair_table/3, repair/3, repair_table/3]).
%%
%% %%%=======================DEFINE=======================
%% -define(GET_TIMEOUT, 3000).
%% -define(REPAIR_TABLE_TIMEOUT, 30000).
%%
%% -define(MODIFY_TIME_ERROR, 10000).
%% -define(LIMIT_DATA, 1024*1024).
%%
%% %%%=================EXPORTED FUNCTIONS=================
%% %% -----------------------------------------------------------------
%% %%@doc  auto_repair
%% %% @spec  auto_repair(Server, Table, Pid) -> return()
%% %% where
%% %%  return() =  Pid
%% %%@end
%% %% -----------------------------------------------------------------
%% auto_repair(Server, Table, Pid) ->
%% 	spawn(?MODULE, auto_repair_table, [Server, Table, Pid]).
%%
%% %% -----------------------------------------------------------------
%% %%@doc  repair
%% %% @spec  repair(Server, Table, Pid) -> return()
%% %% where
%% %%  return() =  Pid
%% %%@end
%% %% -----------------------------------------------------------------
%% repair(Server, Table, Pid) ->
%% 	spawn(?MODULE, repair_table, [Server, Table, Pid]).
%%
%% %% -----------------------------------------------------------------
%% %%@doc  auto_repair_table
%% %% @spec  auto_repair_table(Server, Table, Pid) -> return()
%% %% where
%% %%  return() =  ok
%% %%@end
%% %% -----------------------------------------------------------------
%% auto_repair_table(Server, Table, Pid) ->
%% 	try
%% 		%根据依赖节点是否可用来决定是否自行恢复
%% 		case get_relation_node(Table) of
%% 			[] ->
%% 				%无需修复
%% 				gen_server:cast(Server, {repair_ok, Table, Pid, self()});
%% 			NL when is_list(NL) ->
%% 				repair_table1(Table, Pid, NL),
%% 				gen_server:cast(Server, {repair_ok, Table, Pid, self()});
%% 			E ->
%% 				gen_server:cast(Server, {repair_error, Table, E, Pid, self()})
%% 		end
%% 	catch
%% 		Error:Reason ->
%% 			gen_server:cast(Server, {repair_error, Table,
%% 				{Error, Reason, erlang:get_stacktrace()}, Pid, self()})
%% 	end.
%%
%% %% -----------------------------------------------------------------
%% %%@doc  repair_table
%% %% @spec  repair_table(Server, Table, Pid) -> return()
%% %% where
%% %%  return() =  ok
%% %%@end
%% %% -----------------------------------------------------------------
%% repair_table(Server, Table, Pid) ->
%% 	try
%% 		%从每个节点上获得当前可用的子表
%% 		case get_active_node(Table) of
%% 			[] ->
%% 				%无需修复
%% 				gen_server:cast(Server, {repair_ok, Table, Pid, self()});
%% 			NL when is_list(NL) ->
%% 				repair_table1(Table, Pid, NL),
%% 				gen_server:cast(Server, {repair_ok, Table, Pid, self()});
%% 			E ->
%% 				gen_server:cast(Server, {repair_error, Table, E, Pid, self()})
%% 		end
%% 	catch
%% 		Error:Reason ->
%% 			gen_server:cast(Server, {repair_error, Table,
%% 				{Error, Reason, erlang:get_stacktrace()}, Pid, self()})
%% 	end.
%%
%% %%%===================LOCAL FUNCTIONS==================
%% % 获得该表所对应的依赖节点
%% get_relation_node(Table) ->
%% 	case zm_db_util:active_relation_node(Table, node()) of
%% 		this ->
%% 			[];
%% 		none ->
%% 			{error, "table repair, relation node config error"};
%% 		[] ->
%% 			{error, "table repair, relation node none"};
%% 		{[], _Right} ->
%% 			{error, "table repair, left relation node none"};
%% 		{_Left, []} ->
%% 			{error, "table repair, right relation node none"};
%% 		{Left, Right} ->
%% 			Left ++ Right;
%% 		NL ->
%% 			NL
%% 	end.
%%
%% % TODO 获得该表所对应的可用节点
%% get_active_node(Table) ->
%% 	case zm_db_util:active_relation_node(Table, node()) of
%% 		this ->
%% 			[];
%% 		none ->
%% 			{error, "table repair, relation node config error"};
%% 		[] ->
%% 			{error, "table repair, relation node none"};
%% 		{[], _Right} ->
%% 			{error, "table repair, left relation node none"};
%% 		{_Left, []} ->
%% 			{error, "table repair, right relation node none"};
%% 		{Left, Right} ->
%% 			Left ++ Right;
%% 		NL ->
%% 			NL
%% 	end.
%%
%% %获取上次修改时间
%% get_modify_time(_Table, Pid) ->
%% 	case gen_server:call(Pid, modify_time, ?GET_TIMEOUT) of
%% 		Time when Time > ?MODIFY_TIME_ERROR ->
%% 			Time - ?MODIFY_TIME_ERROR;
%% 		Time ->
%% 			Time
%% 	end.
%%
%% %获取指定节点上表的数据平均大小
%% get_avg_size(Node, Table) ->
%% 	gen_server:call({Table, Node}, get_avg_size, ?GET_TIMEOUT).
%%
%% %修复表，先同步数据，然后锁定全表，再进行同步一次，最后打开全表
%% repair_table1(Table, Pid, NL) ->
%% 	CTime = zm_time:now_millisecond(),
%% 	%获取上次修改时间，同步所有上次修改时间以后的数据
%% 	MTime = get_modify_time(Table, Pid),
%% 	sync_table(Table, Pid, NL, MTime),
%% 	%TODO 锁定全表
%% 	sync_table(Table, Pid, NL, CTime),
%% 	%TODO 打开全表
%% 	ok.
%%
%% %同步数据
%% sync_table(Table, Pid, NL, Time) ->
%% 	MapFun = fun(_Parent, Args) -> sync_node_table(Args, Table, Pid, Time) end,
%% 	%TODO
%% 	ReduceFun = fun
%% 		(L, map_reduce, _R) -> {ok, L};
%% 		(L, map_reduce_over, _R) -> {ok, L};
%% 		(_L, map_reduce_error, R) -> throw(R)
%% 	end,
%% 	{ok, []} = z_lib:map_reduce(MapFun, ReduceFun, [], NL).
%%
%% %同步指定节点上的表数据
%% sync_node_table(Node, Table, Pid, Time) ->
%% 	Filter = {zm_db_util, filter_time, {'>=', Time}},
%% 	Limit = (?LIMIT_DATA div get_avg_size(Node, Table)) + 1,
%% 	sync_node_table(Node, Table, Pid, first, Filter, Limit).
%%
%% %同步指定节点上的表数据
%% sync_node_table(Node, Table, Pid, Key, Filter, Limit) ->
%% 	case gen_server:call({Table, Node}, {values, Key, Filter, Limit}, ?REPAIR_TABLE_TIMEOUT) of
%% 		{ok, Node, Table, _L} ->
%% 			case whereis(Table) of
%% 				Pid -> ok;
%% 				_ -> erlang:error("sync error, table reset")
%% 			end,
%% 			%TODO sync
%% 			sync_node_table(Node, Table, Pid, {next, Key}, Filter, Limit);
%% 		{over, Node, Table, _L} ->
%% 			case whereis(Table) of
%% 				Pid -> ok;
%% 				_ -> erlang:error("sync error, table reset")
%% 			end,
%% 			%TODO sync
%% 			ok
%% 	end.
