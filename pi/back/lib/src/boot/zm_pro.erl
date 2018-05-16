%%@doc 生产环境服务器
%%@end


-module(zm_pro).

-description("production environment server").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1, debug_read/0]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================RECORD=======================
-define(DEFAULT_MAX_HEAP_SIZE, 256 * 1024 * 1024).
-record(state, {path, cfgs}).


%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%% Function: start/1
%% Description: Starts a production environment.
%% -----------------------------------------------------------------
-spec start_link(list()) ->
	{'ok', pid()} | {'error', term()}.
%% -----------------------------------------------------------------
start_link(PluginPath) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, PluginPath, []).

%% -----------------------------------------------------------------
%% Function: debug_read
%% Description: 用于debug的数据读取
%% Returns: {ok, DebugInfo} | {error, Reason}
%% -----------------------------------------------------------------
debug_read() ->
	gen_server:call(?MODULE, debug_read).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Path) ->
	%z_process:local_process(?MODULE, ?DEFAULT_MAX_HEAP_SIZE, false, true),
	N = node(),
	ProjectL = zm_plugin:parse(Path),
	{Mapping, RunL, InitL, _VersionL} = CfgL = zm_plugin:config(ProjectL),
	CodeResL = zm_plugin:config_extend(N, Path, ProjectL),
	case zm_plugin:execute_do(Mapping, handle_node_config(RunL, [{{"~node", N}, {[], zm_node, [N, [pro, db, log, gateway, calc]]}}]) ++ CodeResL, false) of
		[] ->
			%执行全局初始化配置
			case execute_init(InitL) of
				[] ->
					zm_event:notify(node, boot, []),
					{ok, #state{path = Path, cfgs = {CfgL, CodeResL}}};
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
%%		{stop, Reason, Reply, State} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_call(debug_read, _From, #state{cfgs = {{_, RunL, InitL, _}, CodeResL}} = State) ->
	%读取debug数据
	{reply, {ok, {InitL, RunL ++ CodeResL}}, State};
handle_call(_Request, _From, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
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

handle_node_config(RunL, NodeL) ->
	lists:foldr(fun({_, {_, start, {zm_service, _, _}}} = H, T) -> 
						[H|NodeL ++ T];
				   (H, T) ->
						[H|T]
				end, [], RunL).