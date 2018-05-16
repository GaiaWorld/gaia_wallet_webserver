%% 
%% @doc 项目管理器,用于解析,检索项目开发资源
%%


-module(debug_project_manager).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-record(state, {
				master_path,
				plugin_path,
				reader,
				init,
				project,
				start,
				socket,
				tcp_port,
				http_port,
				log_server,
				log,
				db,
				event,
				timer,
				resource,
				process,
				sample,
				unique,
				communicator,
				user_config,
				code,
				res,
				master,
				plugin,
				master_global_var_tab,
				master_local_var_tab,
				master_var_ref_tab,
				master_var_by_ref_tab,
				master_table_by_ref_tab,
				plugin_global_var_tab,
				plugin_local_var_tab,
				plugin_var_ref_tab,
				plugin_var_by_ref_tab,
				plugin_table_by_ref_tab
}).

-define(LOCAL_VAR_TABLE(Path), list_to_atom(lists:concat([Path, "_local_var"]))).
-define(GLOBAL_VAR_TABLE(Path), list_to_atom(lists:concat([Path, "_global_var"]))).
-define(VAR_REF_TABLE(Path), list_to_atom(lists:concat([Path, "_var_ref"]))).
-define(VAR_BY_REF_TABLE(Path), list_to_atom(lists:concat([Path, "_var_by_ref"]))).
-define(TABLE_BY_REF_TABLE(Path), list_to_atom(lists:concat([Path, "_table_by_ref"]))).

-define(INIT_CFG, init).
-define(START_CFG, start).
-define(SOCKET_CFG, zm_net_server).
-define(TCP_PORT_CFG, zm_session).
-define(HTTP_PORT_CFG, zm_http).
-define(LOG_SERVER_CFG, zm_logger).
-define(LOG_CFG, zm_log).
-define(TABLE_CFG, zm_db).
-define(EVENT_CFG, zm_event).
-define(TIMER_CFG, zm_dtimer).
-define(RES_CFG, zm_resource).
-define(PROCESS_CFG, zm_pid_dist).
-define(SAMPLE_CFG, zm_sample).
-define(UNIQUE_CFG, zm_unique_int).
-define(COMMUNICATOR_CFG, zm_communicator).
-define(USER_CFG, zm_config).

-define(CODE, zm_code).
-define(RES, zm_res_loader).

-define(PROJECT_ITEMS, [".cfg", ".src", "include", ".ebin", ".res"]).
-define(PARSE_DEV_RES_TYPE(Path), case Path of
							  '.cfg' ->
								  cfg;
							  '.src' ->
								  src;
							  '.include' ->
								  include;
							  '.ebin' ->
								  ebin;
							  '.res' ->
								  res
						end).

-define(CFG_TABLE(Type), list_to_atom(lists:concat([?MODULE, "_", Type]))).

-define(PROJECT_MANAGER_HIBERNATE_TIMEOUT, 5000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/2, desc_project/1, project_config/3, config/3, vars/2, project_vars/3, var_def/3, var_by_ref/3, desc_var_def/2, desc_var_by_ref/2, 
		 tables/2, project_tables/3, desc_table_by_ref/2]).

%%
%%启动项目管理器
%%
start_link(Args, Reader) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {Args, Reader}, []).

%%
%%获取项目结构描述
%%
desc_project(Timeout) ->
	gen_server:call(?MODULE, desc_project, Timeout).

%%
%%获取指定项目的指定配置信息
%%
project_config(Project, CfgType, Timeout) ->
	gen_server:call(?MODULE, {project_config, Project, CfgType}, Timeout).

%%
%%获取指定项目的指定条目的配置信息
%%
config(CfgType, Keys, Timeout) ->
	gen_server:call(?MODULE, {config, CfgType, Keys}, Timeout).

%%
%%获取所有变量列表
%%
vars(PluginName, Timeout) ->
	gen_server:call(?MODULE, {vars, PluginName}, Timeout).

%%
%%获取指定项目的所有变量列表
%%
project_vars(PluginName, Project, Timeout) ->
	gen_server:call(?MODULE, {project_vars, PluginName, Project}, Timeout).

%%
%%获取指定变量声明描述
%%
var_def(PluginName, Key, Timeout) ->
	gen_server:call(?MODULE, {var_def, PluginName, Key}, Timeout).

%%
%%获取指定变量被引用描述
%%
var_by_ref(PluginName, Key, Timeout) ->
	gen_server:call(?MODULE, {var_by_ref, PluginName, Key}, Timeout).

%%
%%获取变量声明结构
%%
desc_var_def(PluginName, Timeout) ->
	gen_server:call(?MODULE, {desc_var_def, PluginName}, Timeout).

%%
%%获取变量被引用结构
%%
desc_var_by_ref(PluginName, Timeout) ->
	gen_server:call(?MODULE, {desc_var_by_ref, PluginName}, Timeout).

%%
%%获取指定项目的表列表
%%
tables(PluginName, Timeout) ->
	gen_server:call(?MODULE, {tables, PluginName}, Timeout).

%%
%%获取指定项目的表列表
%%
project_tables(PluginName, Project, Timeout) ->
	gen_server:call(?MODULE, {project_table, PluginName, Project}, Timeout).

%%
%%获取表被引用结构
%%
desc_table_by_ref(PluginName, Timeout) ->
	gen_server:call(?MODULE, {desc_table_by_ref, PluginName}, Timeout).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init({{MasterPath, SlavePath} = Paths, Reader}) ->
	case pprofiler_server:start_link([]) of
		{ok, _} ->
			case nmon_server:start_link([]) of
				{ok, _} ->
					State=new_cfg_res_table(#state{master_path = MasterPath, plugin_path = SlavePath, 
																			project = ets:new(?CFG_TABLE(project), [named_table, protected, ordered_set])}),
				    {ok, init_var_ref_table(Paths, init_var_table(Paths, parse_project(Reader, State)))};
				Reason ->
					{stop, Reason}
			end;
		Reason ->
			{stop, Reason}
	end;
init({PluginPath, Reader}) ->
	case pprofiler_server:start_link([]) of
		{ok, _} ->
			case nmon_server:start_link([]) of
				{ok, _} ->
					State=new_cfg_res_table(#state{plugin_path = PluginPath, project = ets:new(?CFG_TABLE(project), [named_table, protected, ordered_set])}),
					{ok, init_var_ref_table(PluginPath, init_var_table(PluginPath, parse_project(Reader, State)))};
				Reason ->
					{stop, Reason}
			end;
		Reason ->
			{stop, Reason}
	end.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================
handle_call(desc_project, _, #state{project = ProjectTable} = State) ->
	{reply, desc_project1(ProjectTable), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({project_config, Project, CfgType}, _, State) ->
	{reply, get_configs(Project, CfgType, State), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({config, CfgType, Keys}, _, State) ->
	{reply, get_config(Keys, CfgType, State, []), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({vars, master}, _, #state{master_global_var_tab = GlobalVarTab, master_local_var_tab = LocalVarTab} = State) ->
	{reply, [{global, get_global_var(GlobalVarTab)}, {local, get_local_var(LocalVarTab)}], State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({vars, _}, _, #state{plugin_global_var_tab = GlobalVarTab, plugin_local_var_tab = LocalVarTab} = State) ->
	{reply, [{global, get_global_var(GlobalVarTab)}, {local, get_local_var(LocalVarTab)}], State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({project_vars, master, Project}, _, #state{master_path = RootPath, master_global_var_tab = GlobalVarTab, master_local_var_tab = LocalVarTab} = State) ->
	{reply, [{global, get_global_var(GlobalVarTab)}, {local, get_project_local_var(LocalVarTab, RootPath, Project)}], State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({project_vars, _, Project}, _, #state{plugin_path = RootPath, plugin_global_var_tab = GlobalVarTab, plugin_local_var_tab = LocalVarTab} = State) ->
	{reply, [{global, get_global_var(GlobalVarTab)}, {local, get_project_local_var(LocalVarTab, RootPath, Project)}], State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({desc_var_def, master}, _, #state{master_var_ref_tab = VarRefTab} = State) ->
	{reply, get_var_def_desc(VarRefTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({desc_var_def, _}, _, #state{plugin_var_ref_tab = VarRefTab} = State) ->
	{reply, get_var_def_desc(VarRefTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({desc_var_by_ref, master}, _, #state{master_var_by_ref_tab = VarByRefTab} = State) ->
	{reply, get_var_by_ref_desc(VarByRefTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({desc_var_by_ref, _}, _, #state{plugin_var_by_ref_tab = VarByRefTab} = State) ->
	{reply, get_var_by_ref_desc(VarByRefTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({var_def, master, Key}, _, #state{master_global_var_tab = GlobalVarTab, master_local_var_tab = LocalVarTab, master_var_ref_tab = VarRefTab} = State) ->
	{reply, get_var_def(GlobalVarTab, LocalVarTab, VarRefTab, Key), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({var_def, _, Key}, _, #state{plugin_global_var_tab = GlobalVarTab, plugin_local_var_tab = LocalVarTab, plugin_var_ref_tab = VarRefTab} = State) ->
	{reply, get_var_def(GlobalVarTab, LocalVarTab, VarRefTab, Key), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({var_by_ref, master, Key}, _, #state{master_global_var_tab = GlobalVarTab, master_local_var_tab = LocalVarTab, master_var_by_ref_tab = VarByRefTab} = State) ->
	{reply, get_by_ref(GlobalVarTab, LocalVarTab, VarByRefTab, Key), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({var_by_ref, _, Key}, _, #state{plugin_global_var_tab = GlobalVarTab, plugin_local_var_tab = LocalVarTab, plugin_var_by_ref_tab = VarByRefTab} = State) ->
	{reply, get_by_ref(GlobalVarTab, LocalVarTab, VarByRefTab, Key), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({tables, master}, _, #state{master_global_var_tab = GlobalVarTab, master_local_var_tab = LocalVarTab, master_table_by_ref_tab = TableTab} = State) ->
	{reply, get_tables(TableTab, GlobalVarTab, LocalVarTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({tables, _}, _, #state{plugin_global_var_tab = GlobalVarTab, plugin_local_var_tab = LocalVarTab, plugin_table_by_ref_tab = TableTab} = State) ->
	{reply, get_tables(TableTab, GlobalVarTab, LocalVarTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({project_table, master, Project}, _, 
			#state{master_path = RootPath, master_global_var_tab = GlobalVarTab, master_local_var_tab = LocalVarTab, master_table_by_ref_tab = TableTab} = State) ->
	{reply, get_project_tables(TableTab, GlobalVarTab, LocalVarTab, RootPath, Project), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({project_table, _, Project}, _, 
			#state{plugin_path = RootPath, plugin_global_var_tab = GlobalVarTab, plugin_local_var_tab = LocalVarTab, plugin_table_by_ref_tab = TableTab} = State) ->
	{reply, get_project_tables(TableTab, GlobalVarTab, LocalVarTab, RootPath, Project), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({desc_table_by_ref, master}, _, #state{master_var_by_ref_tab = VarByRefTab, master_table_by_ref_tab = TableByRefTab} = State) ->
	{reply, get_table_by_ref_desc(VarByRefTab, TableByRefTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call({desc_table_by_ref, _}, _, #state{plugin_var_by_ref_tab = VarByRefTab, plugin_table_by_ref_tab = TableByRefTab} = State) ->
	{reply, get_table_by_ref_desc(VarByRefTab, TableByRefTab), State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {noreply, State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(Msg, State) ->
    {noreply, State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(Info, State) ->
    {noreply, State, ?PROJECT_MANAGER_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

init_var_table({MasterPath, SlavePath}, State) ->
	State#state{master_global_var_tab = ?GLOBAL_VAR_TABLE(MasterPath), master_local_var_tab = ?LOCAL_VAR_TABLE(MasterPath),
				plugin_global_var_tab = ?GLOBAL_VAR_TABLE(SlavePath), plugin_local_var_tab = ?LOCAL_VAR_TABLE(SlavePath)};
init_var_table(PluginPath, State) ->
	State#state{plugin_global_var_tab = ?GLOBAL_VAR_TABLE(PluginPath), plugin_local_var_tab = ?LOCAL_VAR_TABLE(PluginPath)}.

init_var_ref_table({MasterPath, SlavePath}, State) ->
	State#state{master_var_ref_tab = ?VAR_REF_TABLE(MasterPath), master_var_by_ref_tab = ?VAR_BY_REF_TABLE(MasterPath), master_table_by_ref_tab = ?TABLE_BY_REF_TABLE(MasterPath),
				plugin_var_ref_tab = ?VAR_REF_TABLE(SlavePath), plugin_var_by_ref_tab = ?VAR_BY_REF_TABLE(SlavePath), plugin_table_by_ref_tab = ?TABLE_BY_REF_TABLE(SlavePath)};
init_var_ref_table(PluginPath, State) ->
	State#state{plugin_var_ref_tab = ?VAR_REF_TABLE(PluginPath), plugin_var_by_ref_tab = ?VAR_BY_REF_TABLE(PluginPath), plugin_table_by_ref_tab = ?TABLE_BY_REF_TABLE(PluginPath)}.

new_cfg_res_table(State) ->
	State#state{
				init = ets:new(?CFG_TABLE(init), [protected, ordered_set]),
				start = ets:new(?CFG_TABLE(start), [protected, ordered_set]), 
				socket = ets:new(?CFG_TABLE(socket), [protected, ordered_set]),
				tcp_port = ets:new(?CFG_TABLE(tcp_port), [protected, ordered_set]),
				http_port = ets:new(?CFG_TABLE(http_port), [protected, ordered_set]),
				log_server = ets:new(?CFG_TABLE(log_server), [protected, ordered_set]),
				log = ets:new(?CFG_TABLE(log), [protected, ordered_set]),
				db = ets:new(?CFG_TABLE(db), [protected, ordered_set]),
				event = ets:new(?CFG_TABLE(event), [protected, ordered_set]),
				timer = ets:new(?CFG_TABLE(timer), [protected, ordered_set]),
				resource = ets:new(?CFG_TABLE(resource), [protected, ordered_set]),
				process = ets:new(?CFG_TABLE(process), [protected, ordered_set]),
				sample = ets:new(?CFG_TABLE(sample), [protected, ordered_set]),
				unique = ets:new(?CFG_TABLE(unique), [protected, ordered_set]),
				communicator = ets:new(?CFG_TABLE(communicator), [protected, ordered_set]),
				user_config = ets:new(?CFG_TABLE(user_config), [protected, ordered_set]),
				code = ets:new(?CFG_TABLE(code), [protected, ordered_set]),
				res = ets:new(?CFG_TABLE(res), [protected, ordered_set])
			}.

parse_project({master, {M, A}}, #state{master_path = RootPath} = State) ->
	case apply(M, debug_read, A) of
		{ok, {Inits, MasterCfgs}} ->
			parse_init(Inits, RootPath, State),
			{CodeResL, NewState}=parse_cfg(MasterCfgs, RootPath, State, []),
			parse_code_res(CodeResL, NewState);
		{error, Reason} ->
			erlang:error(Reason)
	end;
parse_project({_, {M, A}}, #state{plugin_path = RootPath} = State) ->
	case apply(M, debug_read, A) of
		{ok, {Inits, MasterCfgs}} ->
			parse_init(Inits, RootPath, State),
			{CodeResL, NewState}=parse_cfg(MasterCfgs, RootPath, State, []),
			parse_code_res(CodeResL, NewState);
		{error, Reason} ->
			erlang:error(Reason)
	end.

parse_path({RootPath, plugin}, RootPath) ->
	projects;
parse_path({Path, _}, RootPath) ->
	[H|T]=lists:reverse([list_to_atom(Dir) || Dir <- filename:split(string:substr(filename:dirname(Path), length(RootPath) + 2))]),
	{?PARSE_DEV_RES_TYPE(H), lists:reverse(T)}.

parse_init([{FileInfo, {Layer, CfgType, InitCfg} = InitInfo}|T], RootPath, #state{init = InitTab} = State) ->
	{DevResType, ProjectInfo}=parse_path(FileInfo, RootPath),
	case ets:insert_new(InitTab, {FileInfo, DevResType, CfgType, Layer, ProjectInfo, InitCfg}) of
		true ->
			parse_init(T, RootPath, State);
		false ->
			erlang:error({parse_init_failed, {add_init_error, FileInfo, InitInfo}})
	end;
parse_init([], _, _) ->
	ok.

filter_project([Item|T], Items, SubProjects, Others) ->
	case lists:member(Item, ?PROJECT_ITEMS) of
		true ->
			filter_project(T, [Item|Items], SubProjects, Others);
		false when hd(Item) =/= $. ->
			filter_project(T, Items, [Item|SubProjects], Others);
		_ ->
			filter_project(T, Items, SubProjects, [Item|Others])
	end;
filter_project([], Items, SubProjects, Others) ->
	{lists:reverse(Items), SubProjects, lists:reverse(Others)}.

parse_project_desc(RootPath, Project, ProjectPath) ->
	Path=filename:join(RootPath, ProjectPath),
	{Items, SubProjects, Others}=filter_project(filelib:wildcard("*", Path), [], [], []),
	{Project, Path, Items, SubProjects, Others}.

write_cfg_table(Table, FileInfo, RootPath, Type, Layer, Cfg) ->
	case parse_path(FileInfo, RootPath) of
		{DevResType, ProjectInfo} ->
			case ets:insert(Table, {FileInfo, DevResType, Type, Layer, ProjectInfo, Cfg}) of
				true ->
					ok;
				false ->
					erlang:error({parse_cfg_failed, {Type, FileInfo}})
			end;
		projects ->
			[_, Projects, _]=Cfg,
			case ets:insert(?CFG_TABLE(project), [parse_project_desc(RootPath, Project, ProjectPath) || {Project, ProjectPath} <- Projects]) of
				true ->
					ok;
				false ->
					erlang:error({parse_project_failed, {projects, Projects}})
			end
	end.

parse_cfg([{FileInfo, {Layer, ?START_CFG, Cfg}}|T], RootPath, #state{start = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?START_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?SOCKET_CFG, Cfg}}|T], RootPath, #state{socket = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?SOCKET_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?TCP_PORT_CFG, Cfg}}|T], RootPath, #state{tcp_port = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?TCP_PORT_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?HTTP_PORT_CFG, Cfg}}|T], RootPath, #state{http_port = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?HTTP_PORT_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?LOG_SERVER_CFG, Cfg}}|T], RootPath, #state{log_server = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?LOG_SERVER_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?LOG_CFG, Cfg}}|T], RootPath, #state{log = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?LOG_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?TABLE_CFG, Cfg}}|T], RootPath, #state{db = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?TABLE_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?EVENT_CFG, Cfg}}|T], RootPath, #state{event = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?EVENT_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?TIMER_CFG, Cfg}}|T], RootPath, #state{timer = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?TIMER_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?PROCESS_CFG, Cfg}}|T], RootPath, #state{process = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?PROCESS_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?SAMPLE_CFG, Cfg}}|T], RootPath, #state{sample = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?SAMPLE_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?UNIQUE_CFG, Cfg}}|T], RootPath, #state{unique = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?SAMPLE_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?COMMUNICATOR_CFG, Cfg}}|T], RootPath, #state{communicator = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?COMMUNICATOR_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([{FileInfo, {Layer, ?USER_CFG, Cfg}}|T], RootPath, #state{user_config = Table} = State, L) ->
	write_cfg_table(Table, FileInfo, RootPath, ?USER_CFG, Layer, Cfg),
	parse_cfg(T, RootPath, State, L);
parse_cfg([H|T], RootPath, State, L) ->
	parse_cfg(T, RootPath, State, [H|L]);
parse_cfg([], _, State, L) ->
	{lists:reverse(L), State}.

parse_code_file(File) ->
	{filename:split(filename:dirname(filename:dirname(File))), filename:basename(File, ".beam")}.

parse_res_file(File) ->
	{filename:split(string:substr(File, 1, string:str(File, ".res") - 2)), filename:basename(File)}.

write_code_res_table(Table, FileInfo, ProjectInfo, FileName, {{Size, Time}, CheckSum}) ->
	case ets:insert(Table, {FileInfo, ProjectInfo, FileName, Size, Time, CheckSum}) of
		true ->
			ok;
		false ->
			erlang:error({parse_code_res_failed, FileInfo})
	end.
	
parse_code_res([{{FileInfo, CodeInfo}, {_, ?CODE, _}}|T], #state{code = Table} = State) ->
	{ProjectInfo, CodeName}=parse_code_file(FileInfo),
	write_code_res_table(Table, FileInfo, ProjectInfo, CodeName, CodeInfo),
	parse_code_res(T, State);
parse_code_res([{{FileInfo, ResInfo}, {_, ?RES, _}}|T], #state{res = Table} = State) ->
	{ProjectInfo, ResName}=parse_res_file(FileInfo),
	write_code_res_table(Table, FileInfo, ProjectInfo, ResName, ResInfo),
	parse_code_res(T, State);
parse_code_res([_|T], State) ->
	parse_code_res(T, State);
parse_code_res([], State) ->
	State.

desc_project1(ProjectTable) ->
	desc_project(ProjectTable, ets:first(ProjectTable), [], []).

desc_project(_, '$end_of_table', _, L) ->
	lists:reverse(L);
desc_project(ProjectTable, Project, Filters, L) ->
	case lists:member(Project, Filters) of
		true ->
			desc_project(ProjectTable, ets:next(ProjectTable, Project), Filters, L);
		false ->
			case desc_project(ProjectTable, Project, Filters) of
				{ProjectInfo, NewFilters} ->
					desc_project(ProjectTable, ets:next(ProjectTable, Project), NewFilters, [ProjectInfo|L]);
				false ->
					desc_project(ProjectTable, ets:next(ProjectTable, Project), Filters, L)
			end
	end.

desc_project(ProjectTable, Project, Filters) ->
	case lists:member(Project, Filters) of
		true ->
			false;
		false ->
			case ets:lookup(ProjectTable, Project) of
				[{Project, ProjectPath, Items, SubProjects, Others}] ->
					{NewSubProjects, NewFilters}=desc_sub_project(SubProjects, ProjectTable, Project, Filters, []),
					{{Project, [ProjectPath, Items, NewSubProjects, Others]}, NewFilters};
				_ ->
					false
			end
	end.

desc_sub_project([H|T], ProjectTable, Project, Filters, L) ->
	SubProject=list_to_atom(lists:concat([Project, "/", H])),
	case desc_project(ProjectTable, SubProject, Filters) of
		false ->
			desc_sub_project(T, ProjectTable, Project, [SubProject|Filters], L);
		{ProjectInfo, NewFilters} ->
			desc_sub_project(T, ProjectTable, Project, [SubProject|NewFilters], [ProjectInfo|L])
	end;
desc_sub_project([], _, _, Filters, L) ->
	{L, Filters}.

get_table(?INIT_CFG, #state{init = Table}) ->
	Table;
get_table(?START_CFG, #state{start = Table}) ->
	Table;
get_table(?SOCKET_CFG, #state{socket = Table}) ->
	Table;
get_table(?TCP_PORT_CFG, #state{tcp_port = Table}) ->
	Table;
get_table(?HTTP_PORT_CFG, #state{http_port = Table}) ->
	Table;
get_table(?LOG_SERVER_CFG, #state{log_server = Table}) ->
	Table;
get_table(?LOG_CFG, #state{log = Table}) ->
	Table;
get_table(?TABLE_CFG, #state{db = Table}) ->
	Table;
get_table(?EVENT_CFG, #state{event = Table}) ->
	Table;
get_table(?TIMER_CFG, #state{timer = Table}) ->
	Table;
get_table(?RES_CFG, #state{resource = Table}) ->
	Table;
get_table(?PROCESS_CFG, #state{process = Table}) ->
	Table;
get_table(?SAMPLE_CFG, #state{sample = Table}) ->
	Table;
get_table(?UNIQUE_CFG, #state{unique = Table}) ->
	Table;
get_table(?COMMUNICATOR_CFG, #state{communicator = Table}) ->
	Table;
get_table(?USER_CFG, #state{user_config = Table}) ->
	Table;
get_table(?CODE, #state{code = Table}) ->
	Table;
get_table(?RES, #state{res = Table}) ->
	Table.

get_cfg_intro(?SOCKET_CFG, CfgInfo) ->
	project_lib:parse_socket_cfg(CfgInfo);
get_cfg_intro(?TCP_PORT_CFG, CfgInfo) ->
	project_lib:parse_tcp_cfg(CfgInfo);
get_cfg_intro(?HTTP_PORT_CFG, CfgInfo) ->
	project_lib:parse_http_cfg(CfgInfo);
get_cfg_intro(?LOG_SERVER_CFG, CfgInfo) ->
	project_lib:parse_log_server_cfg(CfgInfo);
get_cfg_intro(?LOG_CFG, CfgInfo) ->
	project_lib:parse_log_cfg(CfgInfo);
get_cfg_intro(?TABLE_CFG, CfgInfo) ->
	project_lib:parse_table_cfg(CfgInfo);
get_cfg_intro(?EVENT_CFG, CfgInfo) ->
	project_lib:parse_event_cfg(CfgInfo);
get_cfg_intro(?TIMER_CFG, CfgInfo) ->
	project_lib:parse_timer_cfg(CfgInfo);
get_cfg_intro(_, _) ->
	[].

get_configs(Project, CfgType, State) ->
	case ets:select(get_table(CfgType, State), [{{'$1', '_', '$3', '$4', '$5', '$6'}, [{'=:=', '$5', Project}, {'=:=', '$3', CfgType}], [{{'$1', '$4', '$6'}}]}]) of
		[] ->
			false;
		L ->
			{Project, CfgType, project_lib:get_config_item(CfgType), [{Path, Layer, get_cfg_intro(CfgType, CfgInfo), lists:flatten(io_lib:format("~p", [CfgInfo]))} || {Path, Layer, CfgInfo} <- L]}
	end.

get_config([Key|T], CfgType, State, L) ->
	case ets:lookup(get_table(CfgType, State), Key) of
		[{Path, _, _, Layer, Project, CfgInfo}] ->
			get_config(T, CfgType, State, 
					   [{Project, CfgType, project_lib:get_config_item(CfgType), Path, Layer, get_cfg_intro(CfgType, CfgInfo), lists:flatten(io_lib:format("~p", [CfgInfo]))}|L]);
		[] ->
			[]
	end;
get_config([], _, _, L) ->
	lists:reverse(L).

get_global_var(GlobalVarTab) ->
	get_vars(GlobalVarTab, ets:first(GlobalVarTab), []).
	
get_local_var(LocalVarTab) ->
	get_vars(LocalVarTab, ets:first(LocalVarTab), []).

get_vars(_, '$end_of_table', L) ->
	lists:reverse(L);
get_vars(VarTab, {_, Var} = Key, L) when Var =:= 'Project'; Var =:= 'ProjectPath'; Var =:= 'CurProject'; Var =:= 'CurProjectPath' ->
	get_vars(VarTab, ets:next(VarTab, Key), L);
get_vars(VarTab, Key, L) ->
	case ets:lookup(VarTab, Key) of
		[{_, {ok, Value}}] ->
			case Key of
				{FileInfo, Var} ->
					get_vars(VarTab, ets:next(VarTab, Key), [[Var, Value, FileInfo]|L]);
				Var ->
					get_vars(VarTab, ets:next(VarTab, Key), [[Var, Value, ""]|L])
			end;
		[{_, {Value}}] ->
			case Key of
				{FileInfo, Var} ->
					get_vars(VarTab, ets:next(VarTab, Key), [[Var, Value, FileInfo]|L]);
				Var ->
					get_vars(VarTab, ets:next(VarTab, Key), [[Var, Value, ""]|L])
			end;
		[{_, {{VarName}, File}}] ->
			Value={VarName, File},
			case Key of
				{FileInfo, Var} ->
					get_vars(VarTab, ets:next(VarTab, Key), [[Var, Value, FileInfo]|L]);
				Var ->
					get_vars(VarTab, ets:next(VarTab, Key), [[Var, Value, ""]|L])
			end;
		_ ->
			get_vars(VarTab, ets:next(VarTab, Key), L)
	end.

get_project_local_var(GlobalVarTab, RootPath, Project) ->
	Path=filename:join(RootPath, filename:join(filename:join(Project), ".cfg")),
	get_project_vars(GlobalVarTab, Path, ets:first(GlobalVarTab), []).

get_project_vars(_, _, '$end_of_table', L) ->
	lists:reverse(L);
get_project_vars(VarTab, Path, {_, Var} = Key, L) when Var =:= 'Project'; Var =:= 'ProjectPath'; Var =:= 'CurProject'; Var =:= 'CurProjectPath' ->
	get_project_vars(VarTab, Path, ets:next(VarTab, Key), L);
get_project_vars(VarTab, Path, {FileInfo, Var} = Key, L) ->
	case string:str(FileInfo, Path) of
		0 ->
			get_project_vars(VarTab, Path, ets:next(VarTab, Key), L);
		_ ->
			case ets:lookup(VarTab, Key) of
				[{_, {ok, Value}}] ->
					get_project_vars(VarTab, Path, ets:next(VarTab, Key), [[Var, Value, FileInfo]|L]);
				[{_, {Value}}] ->
					get_project_vars(VarTab, Path, ets:next(VarTab, Key), [[Var, Value, FileInfo]|L]);
				[{_, {{VarName}, File}}] ->
					Value={VarName, File},
					get_project_vars(VarTab, Path, ets:next(VarTab, Key), [[Var, Value, FileInfo]|L]);
				_ ->
					get_project_vars(VarTab, Path, ets:next(VarTab, Key), L)
			end
	end.

get_var_def_desc(VarRefTab) ->
	get_var_def_desc(VarRefTab, ets:first(VarRefTab), []).

get_var_def_desc(_, '$end_of_table', L) ->
	lists:reverse(L);
get_var_def_desc(VarRefTab, Key, L) ->
	get_var_def_info(VarRefTab, Key, L).
	
get_var_def_info(VarRefTab, {FileInfo, Var} = Key, L) ->
	case ets:lookup(VarRefTab, Key) of
		[{_, Defs}] ->
			get_var_def_desc(VarRefTab, ets:next(VarRefTab, Key), [[Var, FileInfo, [local_to_list(Def) || Def <- Defs]]|L]);
		[] ->
			get_var_def_desc(VarRefTab, ets:next(VarRefTab, Key), [[Var, FileInfo, []]|L])
	end;
get_var_def_info(VarRefTab, Var, L) ->
	case ets:lookup(VarRefTab, Var) of
		[{_, Defs}] ->
			get_var_def_desc(VarRefTab, ets:next(VarRefTab, Var), [[Var, "", [local_to_list(Def) || Def <- Defs]]|L]);
		[] ->
			get_var_def_desc(VarRefTab, ets:next(VarRefTab, Var), [[Var, "", []]|L])
	end.

get_var_by_ref_desc(VarByRefTab) ->
	get_var_by_ref_desc(VarByRefTab, ets:first(VarByRefTab), []).

get_var_by_ref_desc(_, '$end_of_table', L) ->
	lists:reverse(L);
get_var_by_ref_desc(VarByRefTab, Key, L) ->
	get_var_by_ref_info(VarByRefTab, Key, L).

get_var_by_ref_info(VarByRefTab, {FileInfo, Var} = Key, L) ->
	case ets:lookup(VarByRefTab, Key) of
		[{_, ByRefs}] ->
			get_var_by_ref_desc(VarByRefTab, ets:next(VarByRefTab, Key), [[Var, FileInfo, [local_to_list(ByRef) || ByRef <- ByRefs]]|L]);
		[] ->
			get_var_by_ref_desc(VarByRefTab, ets:next(VarByRefTab, Key), [[Var, FileInfo, []]|L])
	end;
get_var_by_ref_info(VarByRefTab, Var, L) ->
	case ets:lookup(VarByRefTab, Var) of
		[{_, ByRefs}] ->
			get_var_def_desc(VarByRefTab, ets:next(VarByRefTab, Var), [[Var, "", [local_to_list(ByRef) || ByRef <- ByRefs]]|L]);
		[] ->
			get_var_def_desc(VarByRefTab, ets:next(VarByRefTab, Var), [[Var, "", []]|L])
	end.

get_var_def(GlobalVarTab, LocalVarTab, VarRefTab, Key) ->
	case ets:lookup(GlobalVarTab, Key) of
		[] ->
			case ets:lookup(LocalVarTab, Key) of
				[{_, _}] ->
					Defs=case ets:lookup(VarRefTab, Key) of
						[{_, R}] ->
							R;
						[] ->
							[]
					end,
					{FileInfo, Var}=Key,
					[Var, FileInfo, [local_to_list(Def) || Def <- Defs]];
				[] ->
					[]
			end;
		[{_, _}] ->
			Defs=case ets:lookup(VarRefTab, Key) of
				[{_, R}] ->
					R;
				[] ->
					[]
			end,
			[Key, "", [local_to_list(Def) || Def <- Defs]]
	end.

get_by_ref(GlobalVarTab, LocalVarTab, VarByRefTab, Key) ->
	case ets:lookup(GlobalVarTab, Key) of
		[] ->
			case ets:lookup(LocalVarTab, Key) of
				[{_, _}] ->
					ByRefs=case ets:lookup(VarByRefTab, Key) of
						[{_, R}] ->
							R;
						[] ->
							[]
					end,
					{FileInfo, Var}=Key,
					[Var, FileInfo, [local_to_list(ByRef) || ByRef <- ByRefs]];
				[] ->
					[]
			end;
		[{_, _}] ->
			ByRefs=case ets:lookup(VarByRefTab, Key) of
				[{_, R}] ->
					R;
				[] ->
					[]
			end,
			[Key, "", [local_to_list(ByRef) || ByRef <- ByRefs]]
	end.

get_tables(TableTab, GlobalVarTab, LocalVarTab) ->
	get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:first(TableTab), [], []).

get_tables(_, _, _, '$end_of_table', GlobalTables, LocalTables) ->
	[{global, lists:reverse(GlobalTables)}, {local, lists:reverse(LocalTables)}];
get_tables(TableTab, GlobalVarTab, LocalVarTab, {FileInfo, Var} = Key, GlobalTables, LocalTables) ->
	case ets:lookup(LocalVarTab, Key) of
		[{_, {ok, Value}}] ->
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Key), GlobalTables, [[Var, Value, FileInfo]|LocalTables]);
		[{_, {Value}}] ->
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Key), GlobalTables, [[Var, Value, FileInfo]|LocalTables]);
		[{_, {{VarName}, File}}] ->
			Value={VarName, File},
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Key), GlobalTables, [[Var, Value, FileInfo]|LocalTables]);
		_ ->
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Key), GlobalTables, LocalTables)
	end;
get_tables(TableTab, GlobalVarTab, LocalVarTab, Var, GlobalTables, LocalTables) ->
	case ets:lookup(GlobalVarTab, Var) of
		[{_, {ok, Value}}] ->
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Var), [[Var, Value, ""]|GlobalTables], LocalTables);
		[{_, {Value}}] ->
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Var), [[Var, Value, ""]|GlobalTables], LocalTables);
		[{_, {{VarName}, File}}] ->
			Value={VarName, File},
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Var), [[Var, Value, ""]|GlobalTables], LocalTables);
		_ ->
			get_tables(TableTab, GlobalVarTab, LocalVarTab, ets:next(TableTab, Var), GlobalTables, LocalTables)
	end.

get_project_tables(TableTab, GlobalVarTab, LocalVarTab, RootPath, Project) ->
	Path=filename:join(RootPath, filename:join(filename:join(Project), ".cfg")),
	get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:first(TableTab), [], []).

get_project_tables(_, _, _, _, '$end_of_table', GlobalTables, LocalTables) ->
	[{global, lists:reverse(GlobalTables)}, {local, lists:reverse(LocalTables)}];
get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, {FileInfo, Var} = Key, GlobalTables, LocalTables) ->
	case string:str(FileInfo, Path) of
		0 ->
			get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Key), GlobalTables, LocalTables);
		_ ->
			case ets:lookup(LocalVarTab, Key) of
				[{_, {ok, Value}}] ->
					get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Key), GlobalTables, [[Var, Value, FileInfo]|LocalTables]);
				[{_, {Value}}] ->
					get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Key), GlobalTables, [[Var, Value, FileInfo]|LocalTables]);
				[{_, {{VarName}, File}}] ->
					Value={VarName, File},
					get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Key), GlobalTables, [[Var, Value, FileInfo]|LocalTables]);
				_ ->
					get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Key), GlobalTables, LocalTables)
			end
	end;
get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, Var, GlobalTables, LocalTables) ->
	case ets:lookup(GlobalVarTab, Var) of
		[{_, {ok, Value}}] ->
			get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Var), [[Var, Value, ""]|GlobalTables], LocalTables);
		[{_, {Value}}] ->
			get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Var), [[Var, Value, ""]|GlobalTables], LocalTables);
		[{_, {{VarName}, File}}] ->
			Value={VarName, File},
			get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Var), [[Var, Value, ""]|GlobalTables], LocalTables);
		_ ->
			get_project_tables(TableTab, GlobalVarTab, LocalVarTab, Path, ets:next(TableTab, Var), GlobalTables, LocalTables)
	end.

get_table_by_ref_desc(VarByRefTab, TableByRefTab) ->
	get_table_by_ref_desc(VarByRefTab, TableByRefTab, ets:first(TableByRefTab), []).

get_table_by_ref_desc(_, _, '$end_of_table', L) ->
	lists:reverse(L);
get_table_by_ref_desc(VarByRefTab, TableByRefTab, Key, L) ->
	get_table_by_ref_info(VarByRefTab, TableByRefTab, Key, L).

get_table_by_ref_info(VarByRefTab, TableByRefTab, {FileInfo, Var} = Key, L) ->
	case ets:lookup(VarByRefTab, Key) of
		[{_, ByRefs}] ->
			get_table_by_ref_desc(VarByRefTab, TableByRefTab, ets:next(TableByRefTab, Key), [[Var, FileInfo, [local_to_list(ByRef) || ByRef <- ByRefs]]|L]);
		[] ->
			get_table_by_ref_desc(VarByRefTab, TableByRefTab, ets:next(TableByRefTab, Key), [[Var, FileInfo, []]|L])
	end;
get_table_by_ref_info(VarByRefTab, TableByRefTab, Var, L) ->
	case ets:lookup(VarByRefTab, Var) of
		[{_, ByRefs}] ->
			get_table_by_ref_desc(VarByRefTab, TableByRefTab, ets:next(TableByRefTab, Var), [[Var, "", [local_to_list(ByRef) || ByRef <- ByRefs]]|L]);
		[] ->
			get_table_by_ref_desc(VarByRefTab, TableByRefTab, ets:next(TableByRefTab, Var), [[Var, "", []]|L])
	end.

local_to_list({var_def, {FileInfo, Var}}) ->
	[var, Var, FileInfo];
local_to_list({var_def, Var}) ->
	[var, Var];
local_to_list({Type, {File, Line}}) ->
	[Type, File, Line];
local_to_list({FileInfo, Var}) ->
	[Var, FileInfo];
local_to_list(Var) ->
	Var.


	