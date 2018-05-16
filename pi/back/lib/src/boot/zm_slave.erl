%%@doc 从节点服务器
%%@end


-module(zm_slave).

-description("slave server").
-vsn(1).

%%%=======================EXPORT=======================
-export([start_link/5]).
-export([reset/3, reset/4, path_filter/3, sync_run/3, asyn_run/3, stop/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(MASTER, zm_master).
-define(CODE, code).

%%%=======================RECORD=======================
-record(state, {mapping, cfgs, node}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/4
%% Description: Starts a slave.
%% -----------------------------------------------------------------
-spec start_link(list(), integer(), list(), list(), atom()) ->
	{'ok', pid()} | {'error', term()}.
%% -----------------------------------------------------------------
start_link(Mapping, Ets, Inits, ProjectPathL, Node) ->
	case gen_server:start_link({local, ?MODULE},
		?MODULE, {Mapping, Ets, Inits, ProjectPathL, Node}, []) of
		{ok, _Pid} = R ->
			cast_master(Node, {zm_slave_start, node()}),
			R;
		{error, Reason} = E ->
			cast_master(Node, {zm_slave_error, node(), Reason}),
			E
	end.

%% -----------------------------------------------------------------
%% Function: reset
%% Description: 重置指定的FL_MA_List
%% Returns: {[Replies],[BadNodes]}
%% -----------------------------------------------------------------
reset(Nodes, {Mapping, FL_MA_List, ProjectPath}, Timeout)
	when is_list(Nodes), is_tuple(Mapping), is_list(FL_MA_List), is_list(ProjectPath) ->
	gen_server:multi_call(Nodes, ?MODULE,
		{reset, Mapping, FL_MA_List, ProjectPath}, Timeout).

%% -----------------------------------------------------------------
%% Function: reset
%% Description: 重置指定的FL_MA_List，undo and do
%% Returns: ok | {error, Reason} | {exit, Reason}
%% -----------------------------------------------------------------
reset(Mapping, FL_MA_List, OldMapping, Old_FL_MA_List) ->
	Old = lists:sort([{MA, FL} || {FL, MA} <- Old_FL_MA_List]),
	New = lists:sort([{MA, FL} || {FL, MA} <- FL_MA_List]),
	{_Same, Left, Right} = z_lib:compare_order(Old, New, fun compare/2),
	case zm_plugin:execute_undo(
		OldMapping, lists:sort([{FL, MA} || {MA, FL} <- Left])) of
		ok ->
			case zm_plugin:execute_do(
				Mapping, lists:sort([{FL, MA} || {MA, FL} <- Right]), true) of
				[] ->
					%执行初始化配置
					case execute_init() of
						[] ->
							ok;
						{error, Reason} ->
							{exit, Reason};
						LL ->
							{exit, {reset_init_error, LL}}
					end;
				{error, Reason} ->
					{exit, Reason};
				L ->
					{exit, {reset_error, L}}
			end;
		E ->
			E
	end.


%% -----------------------------------------------------------------
%% Function: path_filter
%% Description: 根据配置的路径来过滤
%% Returns: {[Replies],[BadNodes]}
%% -----------------------------------------------------------------
path_filter([{{File, _CodeFile}, _} = H | T], Path, L) ->
	case lists:prefix(Path, File) of
		true ->
			path_filter(T, Path, [H | L]);
		_ ->
			path_filter(T, Path, L)
	end;
path_filter([{File, _} = H | T], Path, L) ->
	case lists:prefix(Path, File) of
		true ->
			path_filter(T, Path, [H | L]);
		_ ->
			path_filter(T, Path, L)
	end;
path_filter([], _Path, L) ->
	L.

%% -----------------------------------------------------------------
%% Function: run
%% Description: 在zm_slave进程上运行其它进程
%% Returns: ok | {error, Reason} | {exit, Reason}
%% -----------------------------------------------------------------
sync_run(M, F, A) ->
	gen_server:call(?MODULE, {run, M, F, A}, infinity).

%% -----------------------------------------------------------------
%% Function: run
%% Description: 在zm_slave进程上运行其它进程
%% Returns: ok | {error, Reason} | {exit, Reason}
%% -----------------------------------------------------------------
asyn_run(M, F, A) ->
	gen_server:cast(?MODULE, {run, M, F, A}, infinity).

%% -----------------------------------------------------------------
%% Function: stop
%% Description: 关闭slave
%% Returns: ok | {error, Reason} | {exit, Reason}
%% -----------------------------------------------------------------
stop(Reason) ->
	case stop_db(Reason) of
		ok ->
			stop_indexer(Reason);
		E ->
			E
	end.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Mapping, FL_MA_List, Inits, ProjectPathL, Node}) ->
	case zm_plugin:execute_do(Mapping, FL_MA_List, false) of
		[] ->
			%执行初始化配置
			case execute_init(Inits) of
				[] ->
					set_project_path(ProjectPathL), 
					{ok, #state{mapping = Mapping, cfgs = FL_MA_List, node = Node}};
				{error, Reason} ->
					{stop, Reason};
				LL ->
					{stop, {boot_init_error, LL}}
			end;
		{error, Reason} ->
			{stop, Reason};
		L ->
			{stop, {boot_error, L}}
	end.

%% -----------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State} |
%%		{reply, Reply, State, Timeout} |
%%		{noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, Reply, State} | (terminate/2 is called)
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_call({reset, Mapping, FL_MA_List, ProjectPath}, _From,
	#state{mapping = OldMapping, cfgs = Cfgs} = State) ->
	% 根据项目路径来进行过滤
	List = path_filter(Cfgs, ProjectPath, []),
	case reset(Mapping, FL_MA_List, OldMapping, List) of
		ok ->
			NewCfgs=reset_insert(FL_MA_List, reset_delete(List, Cfgs)),
			zm_config:set(zm_plugin, {ProjectPath, []}),
			{reply, ok, State#state{mapping = Mapping, cfgs = NewCfgs}};
		{error, _} = E ->
			{reply, E, State};
		{exit, Reason} = E ->
			{stop, E, {error, Reason}, State}
	end;

handle_call({run, M, F, A}, _From, State) ->
	try
		{reply, apply(M, F, A), State}
	catch
		Error:Why ->
			{reply, {Error, {Why, {M, F, A}, erlang:get_stacktrace()}}, State}
	end;

handle_call(_Request, _From, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_cast({run, M, F, A}, State) ->
	try
		apply(M, F, A)
	catch
		_:_ ->
			ignore
	end,
	{noreply, State};

handle_cast(_Msg, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_info(_Info, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(Reason, #state{node = Node}) ->
	cast_master(Node, {zm_slave_terminate, node(), Reason}),
	ignored.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
%比较2个配置是否相同，返回大于 1、小于 -1、等于 0
compare({FA1, _}, {FA2, _}) when FA1 > FA2 ->
	1;
compare({FA1, _}, {FA2, _}) when FA1 < FA2 ->
	-1;
compare({_, _}, {_, _}) ->
	0.

% 执行全局初始化配置
execute_init() ->
	%执行数据库服务器加载，如果节点上无数据库服务器则忽略
	case zm_db_server:load() of
		Result when is_atom(Result) ->
			[];
		E ->
			E
	end.

% 执行全局初始化配置
execute_init(List) ->
	case zm_db_server:load() of
		Result when is_atom(Result) ->
			[R || {FL, MA} <- List, (R = execute_init(FL, MA)) =/= ok];
		E ->
			E
	end.

% 执行全局初始化函数
execute_init(Info, {_, _, {M, F, A}} = MA) ->
	try
		apply(M, F, A),
		ok
	catch
		Error:Why ->
			{Error, {Why, {Info, MA}, erlang:get_stacktrace()}}
	end;
execute_init(Info, MA) ->
	{error, {invalid_init, {Info, MA}}}.

% 通知管理节点
cast_master(Node, Msg) ->
	gen_server:cast({?MASTER, Node}, Msg).

%记录项目路径
set_project_path([ProjectPath|T]) ->
	zm_config:set(zm_plugin, {ProjectPath, []}),
	set_project_path(T);
set_project_path([]) ->
	ok.

%配置删除函数
reset_delete([{FL, _}|T], Cfgs) ->
	reset_delete(T, lists:keydelete(FL, 1, Cfgs));
reset_delete([], Cfgs) ->
	Cfgs.

%配置插入函数
reset_insert([{NewFL, _} = KV|T], Cfgs) ->
	reset_insert(T, lists:keystore(NewFL, 1, Cfgs, KV));
reset_insert([], Cfgs) ->
	Cfgs.

%关闭数据库相关服务
stop_db(Reason) ->
	case stop_db1([{Server, try 
								Server:stop(Reason) 
							catch 
								_:{noproc, {gen_server, call, _}} = Reason1 ->
									Reason1; 
								_:Reason1 -> {error, Reason1} 
							end} || Server <- [gossip_server, compare_initiator, compare_accepter, scale_server]]) of
		ok ->
			try zm_db_server:stop(Reason) of
				ok ->
					ok;
				{error, stopping} ->
					ok;
				{error, _} = E ->
					E
			catch
				_:{noproc, {gen_server, call, _}} ->
					ok;
				_:Reason1 ->
					{error, Reason1}
			end;
		E ->
			E
	end.

stop_db1([{_, ok}|T]) ->
	stop_db1(T);
stop_db1([{_, {noproc, {gen_server, call, _}}}|T]) ->
	stop_db1(T);
stop_db1([{Server, {error, Reason}}|_]) ->
	{error, {Server, Reason}};
stop_db1([{Server, Reason}|_]) ->
	{error, {Server, Reason}};
stop_db1([]) ->
	ok.

stop_indexer(Reason) ->
	try three_gram_indexer:stop(Reason) of
		ok ->
			stop_indexer1(Reason);
		E ->
			E
	catch
		_:{noproc, {gen_server, call, _}} ->
			stop_indexer1(Reason);
		_:Reason ->
			{error, {Reason, erlang:get_stacktrace()}}
	end.

stop_indexer1(Reason) ->
	try indexer_storage:stop(Reason) of
		ok ->
			init:stop();
		E ->
			E
	catch
		_:{noproc, {gen_server, call, _}} ->
			init:stop();
		_:ErrReason ->
			{error, {ErrReason, erlang:get_stacktrace()}}
	end.
