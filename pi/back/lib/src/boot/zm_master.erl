%%@doc 主节点服务器
%%@end


-module(zm_master).

-description("master server").
-vsn(1).

%%%=======================EXPORT=======================
-export([start_link/2, stop/1]).
-export([get_state/0, reset/1, reset_plugin_path/1, debug_read/0]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(MAPPING, mod_fun_mapping).
-define(INIT, init).
-define(START, start).
-define(ENV_DB_PATH, "ERL_DB_PATH").

-define(DB_SERVER_MODULE, zm_db_server).
-define(DB_SERVER_FUN, start_link).
-define(DEFAULT_FLAG, {"default", 1}).

%%%=======================RECORD=======================
-record(state, {master_path, plugin_path, mapping, master_files, master, plugin, inits}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/2
%% Description: Starts a master.
%% -----------------------------------------------------------------
-spec start_link(list(), atom()) ->
	{'ok', pid()} | {'error', term()}.
%% -----------------------------------------------------------------
start_link(PluginPath, Parent) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {PluginPath, Parent}, []).

%% -----------------------------------------------------------------
%% Function: get_state
%% Description: 获得状态参数
%% Returns: tuple()
%% -----------------------------------------------------------------
get_state() ->
	gen_server:call(?MODULE, get_state).

%% -----------------------------------------------------------------
%% Function: reset
%% Description: 重设指定项目
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
reset(Path) ->
	gen_server:call(?MODULE, {reset, Path}).

%% -----------------------------------------------------------------
%% Function: reset_plugin_path
%% Description: 重设plugin的路径
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
reset_plugin_path(Path) ->
	gen_server:call(?MODULE, {reset_plugin_path, Path}, 30000).

%% -----------------------------------------------------------------
%% Function: debug_read
%% Description: 用于debug的数据读取
%% Returns: {ok, DebugInfo} | {error, Reason}
%% -----------------------------------------------------------------
debug_read() ->
	gen_server:call(?MODULE, debug_read).

%% -----------------------------------------------------------------
%% Function: stop
%% Description: 关闭slave
%% Returns: ok | {error, Reason} | {exit, Reason}
%% -----------------------------------------------------------------
stop(Reason) ->
	case stop_db(Reason) of
		ok ->
			init:stop(); 
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
init({{MasterPath, PluginPath}, Parent}) ->
	ok=net_kernel:monitor_nodes(true, [{node_type, all}]),
	%解析master配置
	MasterL = zm_plugin:parse(MasterPath),
	N = node(),
	DBPath=os:getenv(?ENV_DB_PATH),
	{Mapping, RL, InitL, _VersionL} = zm_plugin:config(MasterL),
	RunL=zm_plugin:reset_db_server_path(RL, DBPath), 
	NodeL = [{{"~node", Parent}, {[], zm_node, [Parent, [parent, center]]}}, 
			 {{"~node", N}, {[], zm_node, [N, [self, master, db, log, gateway, calc, res]]}}],
	CodeResL = zm_plugin:config_extend(N, MasterPath, MasterL),
	MasterCfgL=RunL ++ NodeL ++ CodeResL,
	MasterPathL = [Path || {Path, _} <- MasterCfgL],  
	%解析plugin配置
	PluginL = zm_plugin:parse(PluginPath),
	case zm_plugin:execute_do(Mapping, MasterCfgL, false) of
		[] ->
			case execute_init(InitL) of
				[] ->
					{ok, _} = erl_boot_server:start([]),
					set_project_path(MasterPathL),
					zm_event:notify(?MODULE, init, {MasterPath, Mapping, MasterCfgL, InitL}),
					{ok, #state{master_path = MasterPath, plugin_path = PluginPath,
						mapping = Mapping, master_files = MasterPathL, master = MasterCfgL, plugin = PluginL, inits = InitL}};
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
handle_call(get_state, _From, #state{master_path = MasterPath, plugin_path = PluginPath, 
	mapping = Mapping, master_files = MasterPathL, plugin = PluginCfgL, inits = Inits} = State) ->
	{reply, {MasterPath, Mapping, MasterPathL, Inits, PluginPath, PluginCfgL}, State};

handle_call({reset, Project}, _From, #state{master_path = MasterPath, plugin_path = PluginPath,
						mapping = Mapping, master_files = MasterPathL, plugin = PluginL, inits = InitL} = State) ->
	case zm_plugin:parse(MasterPath, Project) of
		{{_, _, _, [{_, {_, ?MAPPING, _}} | _], _}, _} = MasterL ->
			RLL=tl(MasterPathL),
			InitLL=tl(InitL),
			{NewMapping, RL, NewInitL, _VersionL} = zm_plugin:config(MasterL),
			NewRL = lists:flatten([RL | RLL]),
			NewInitL = lists:flatten([InitL | InitLL]),
			case zm_slave:reset(NewMapping, NewRL, Mapping, MasterPathL) of
				ok ->
					{reply, ok, State#state{mapping = NewMapping, master_files = NewRL, inits = NewInitL}};
				{error, _} = E ->
					{reply, E, State};
				{exit, Reason} = E ->
					{stop, E, {error, Reason}, State}
			end;
		P ->
%% 			CfgLL = zm_slave:path_filter(PluginL, PluginPath, []),
%% 			{{FLL, _} = CfgL, {FILL, _} = InitL, _VersionL} = zm_plugin:project_config(node(), Path, P, false),
%% 			NewCfgs=lists:sort(lists:flatten([FL_MA||{FL, _} = FL_MA <- CfgLL, FL =/= FLL], CfgL)),
%% 			NewInits=lists:sort(lists:flatten([FIL_MA||{FIL, _} = FIL_MA <- Inits, FIL =/= FILL], InitL)),
%% 			case zm_slave:reset(Mapping, NewCfgs, Mapping, Cfgs) of
%% 				ok ->
%% 					{reply, ok, State#state{master_cfgs = NewCfgs, inits = NewInits}};
%% 				{error, _} = E ->
%% 					{reply, E, State};
%% 				{exit, Reason} = E ->
%% 					{stop, E, {error, Reason}, State}
%% 			end
			{reply, ok, State}
	end;

handle_call({reset_plugin_path, PluginPath}, _From, State) ->
	PluginL = zm_plugin:parse(PluginPath),
	{reply, ok, State#state{plugin_path = PluginPath, plugin = PluginL}};

handle_call(debug_read, _From, #state{inits = Inits, master = MasterCfgs} = State) ->
	{reply, {ok, {Inits, MasterCfgs}}, State};

handle_call(_Request, _From, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State} (terminate/2 is called)
%% -----------------------------------------------------------------
handle_cast({zm_slave_start, Node}, State) ->
	zm_event:notify(?MODULE, zm_slave_start, Node),
	{noreply, State};
handle_cast({zm_slave_error, Node, Reason}, State) ->
	zm_event:notify(?MODULE, zm_slave_error, {Node, Reason}),
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
handle_info({slaveinfo, Node, TimeDate, Msg}, State) ->
	zm_event:notify(?MODULE, slaveinfo, {Node, TimeDate, Msg}),
	{noreply, State};
handle_info({nodeup, Node, InfoList}, State) ->
	zm_event:notify(?MODULE, nodeup, {Node, InfoList}),
	{noreply, State};
handle_info({nodedown, Node, InfoList}, State) ->
	zm_event:notify(?MODULE, nodedown, {Node, InfoList}),
	{noreply, State};

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