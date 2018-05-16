%%@doc 中央节点服务器
%%```
%%% 中央节点管理物理机器和子集群的主节点
%%% 1、记录每个物理机器的IP和SSH端口，账号和密码。
%%% 2、记录物理机器的负荷，包括CPU，LA，内存，网络IO，磁盘IO和容量。
%%% 3、负责创建、启动和管理每个子集群的主节点，创建时会指定提供哪些物理机器。
%%% 4、记录主节点上集群的负荷，包括erlang节点内的负荷，及请求的吞吐量和数据库的吞吐量。
%%% 5、备份主节点上DB。
%%'''
%%@end


-module(zm_center).

-description("center server").
-vsn(1).

%%%=======================EXPORT=======================
-export([start_link/1, get_master/0, file_monitor/2, stop/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(NODE_AUTH, node_auth).
-define(DEFAULT_CALL_TIMEOUT, 3000).

-define(DIR_CFG, "cfg/").
-define(DIR_CODE, "ebin/").
-define(DIR_RES, "res/").

-define(CFG, "cfg").
-define(CODE, "beam").
-define(CODE_DIR, ".ebin").
-define(RES_DIR, ".res").

%%%=======================RECORD=======================
-record(state, {center, master}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/2
%% Description: Starts a master.
%% -----------------------------------------------------------------
-spec start_link(list()) ->
	{'ok', pid()} | {'error', term()}.
%% -----------------------------------------------------------------
start_link(PluginPath) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, PluginPath, []).

%% -----------------------------------------------------------------
%% Function: get_master
%% Description: 获得master参数
%% Returns: tuple()
%% -----------------------------------------------------------------
get_master() ->
	gen_server:call(?MODULE, get_master).


remove_monitor_path(File, Path) ->	
	 Index = string:str(File, Path),	 
	 if 
		Index < 1 ->
		 	File;
    	true ->
			string:strip(string:substr(File, Index + string:len(Path ++ "/")))
	 end.

%% -----------------------------------------------------------------
%% Function: file_monitor/2
%% Description: monitor file.
%% -----------------------------------------------------------------
file_monitor({Path, Mapping}, {monitors, Adds, Removes, Modifys,_Adds2,_Remove2,_Modifys}) ->
	file_add(Path, Mapping, [remove_monitor_path(Add_Item, Path) || Add_Item <- Adds]),
	file_remove(Path, Mapping, [remove_monitor_path(Remove_Item, Path) || Remove_Item <- Removes]),
	file_modify(Path, Mapping,[remove_monitor_path(Modify_Item, Path) || Modify_Item <- Modifys]);
file_monitor(_Server, _) ->
	ok.

%% -----------------------------------------------------------------
%% Function: stop
%% Description: 关闭slave
%% Returns: ok | {error, Reason} | {exit, Reason}
%% -----------------------------------------------------------------
stop(Reason) ->
	case stop_db(Reason) of
		ok ->
			%关闭数据库相关服务成功
			init:stop(); %关闭节点
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
init(Path) ->
	ok=net_kernel:monitor_nodes(true, [{node_type, all}]),
	N = node(),
	ProjectL = zm_plugin:parse(Path),
	{Mapping, RunL, InitL, _VersionL} = zm_plugin:config(ProjectL),
	CodeResL = zm_plugin:config_extend(N, Path, ProjectL),
	NodeL = [{{"~node", N}, {[], zm_node, 
								 [N, [self, center, db, log, gateway, calc]]}}],
	case zm_plugin:execute_do(Mapping, RunL ++ NodeL ++ CodeResL, false) of
		[] ->
			%执行全局初始化配置
			case execute_init(InitL) of
				[] ->					
					{ok, _} = zm_file_monitor:start_link(),
                    zm_file_monitor:set(Path,
						{?MODULE, file_monitor, {Path, Mapping}}),
					zm_event:notify(?MODULE, init, {Path, Mapping, RunL, InitL}),
					Center={node(), self()},
					{ok, #state{center = Center, master = []}};
				{error, Reason} ->
					{stop, Reason};
				LL ->
					{stop, {boot_init_error, LL}}
			end;
		{error, Reason} ->
			{stop, Reason};
		LL ->
			{stop, {boot_error, LL}}
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
handle_call(get_master, _From, #state{master = Master} = State) ->
	{reply, {ok, Master}, State};
handle_call({set_master, Node, NodeAuthInfo}, _From, #state{master = Master} = State) ->
	try gen_server:call({zm_master, Node}, {?NODE_AUTH, NodeAuthInfo}, ?DEFAULT_CALL_TIMEOUT) of
		ok ->
			{reply, ok, State#state{master = [Node|Master]}};
		E ->
			{reply, E, State#state{master = [Node|Master]}}
	catch
		Type:Why ->
			{reply, {Type, Why}, State}
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
handle_cast(_Msg, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_info({nodeup, Node, InfoList}, #state{master = Master} = State) ->
	zm_event:notify(?MODULE, nodeup, {Node, InfoList}),
	{noreply, State#state{master = [Node|Master]}};
handle_info({{Node, _} = From, Event, Args}, State) when is_atom(Node) ->
	zm_event:notify(?MODULE, {From, Event}, Args),
	{noreply, State};
handle_info({nodedown, Node, InfoList}, #state{master = Master} = State) ->
	case lists:member(Node, Master) of
		true ->
			zm_event:notify(?MODULE, nodedown, {Node, InfoList}),
			{noreply, State#state{master = lists:delete(Node, Master)}};
		false ->
			{noreply, State}
	end;
	
handle_info(_Info, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(_Reason, _State) ->
	ignored.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
% 执行全局初始化配置
execute_init(List) ->
	%执行数据库服务器加载，如果节点上无数据库服务器则忽略
	case zm_db_server:load() of
		Result when is_atom(Result) ->
			%执行全局初始化
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

% 文件添加的函数
file_add(Path, Mapping, [H | T]) ->
	case zm_plugin:get_file_type(H) of
		cfg ->
			parse_cfg(Path, Mapping, H);
		{code, Path, _FileName} ->
			code:add_pathz(Path ++ [$/ | Path]);
		{res, {P, MP}, _FileName} ->
			zm_resource:set(P, node(), Path ++ [$/ | MP]);
		R ->
			R
	end,
	file_add(Path, Mapping, T);
file_add(_Path, _Mapping, []) ->
	ok.

% 文件移除的函数
file_remove(Path, Mapping, [H | T]) ->
	case get_file_type(H) of
		{code, _Path, Mod} ->			
			code:purge(Mod),
			code:delete(Mod);
		{res, {P, _MP}, FileName} ->			
			zm_res_loader:clear(P ++ [$/ | FileName]);
		cfg ->			
			%TODO cfg undo
			cfg;
		R ->			
			R
	end,
	file_remove(Path, Mapping, T);
file_remove(_Path, _Mapping, []) ->	
	ok.

% 文件修改的函数
file_modify(Path, Mapping, [H | T]) ->
	case get_file_type(H) of
		cfg ->
			%TODO cfg undo
			parse_cfg(Path, Mapping, H);
		{code, _Path, Mod} ->
			code:purge(Mod),
			code:delete(Mod);
		{res, {P, _MP}, FileName} ->
			zm_res_loader:purge(P ++ [$/ | FileName]);
		R ->
			R
	end,
	file_modify(Path, Mapping, T);
file_modify(_Path, _Mapping, []) ->
	ok.

% 分析指定文件，返回元组列表
parse_path_file(Path, File) ->
	case string:chr(File, $/) of
		I when I > 0 ->
			Project = lists:sublist(File, I - 1),
			P = list_to_atom(Project),
			CfgDir = filename:dirname(File),
			VarMap = gb_trees:insert('CurCfgPath', filename:dirname(CfgDir),
				gb_trees:insert('ProjectPath', Project,
				gb_trees:insert('Project', P,
				gb_trees:empty()))),
			zm_plugin:parse_file(Path ++ [$/ | File], VarMap);
		_ ->
			[]
	end.

%判断指定文件是否是插件目录下的配置、代码或资源
get_file_type(File) ->
	case z_lib:split_first(File, "/.") of
		{_, ?DIR_CFG ++ Name} ->
			case z_lib:split_first(Name, $.) of
				{_, ?CFG} ->
					cfg;
				_ ->
					other
			end;
		{Path, ?DIR_CODE ++ Name} ->
			case z_lib:split_first(Name, $.) of
				{Mod, ?CODE} ->
					{code, Path ++ [$/ | ?CODE_DIR], list_to_atom(Mod)};
				_ ->
					other
			end;
		{Path, ?DIR_RES ++ Name} ->
			{res, {Path, Path ++ [$/ | ?RES_DIR]}, Name};
		_ ->
			other
	end.

% 分析配置文件
parse_cfg(Path, Mapping, File) ->
	TL = parse_path_file(Path, File),
	{CfgL, InitL, _VersionL} = zm_plugin:project_config(node(), Path, {"", [], [], lists:reverse(TL)}, false),
	case zm_plugin:execute_do(Mapping, CfgL, true) of
		[] ->
			%执行全局初始化配置
			case execute_init(InitL) of
				[] ->
					ok;
				{error, Reason} = E ->
					error_logger:warning_report({"parse_cfg, excute_init, ", Path, File, Reason}),
					E;
				LL ->
					error_logger:warning_report({"parse_cfg, file_monitor_init_error, ", Path, File, LL}),
					{error, {file_monitor_init_error, LL}}
			end;
		{error, Reason} = E ->
			error_logger:warning_report({"parse_cfg, excute, ", Path, File, Reason}),
			E;
		LL ->
			error_logger:warning_report({"parse_cfg, file_monitor_error, ", Path, File, LL}),
			{stop, {file_monitor_error, LL}}
	end.

%记录项目路径
set_project_path([ProjectPath|T]) ->
	zm_config:set(zm_plugin, {ProjectPath, []}),
	set_project_path(T);
set_project_path([]) ->
	ok.

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
			%数据库相关支持服务启动成功
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
