%%@doc 开发服务器
%%@end


-module(zm_dev).

-description("develop server").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1, file_monitor/4, debug_read/0, start_monitor/4]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================RECORD=======================
-define(IP_KEY, ip).
-define(IP, {127, 0, 0, 1}).
-define(SRC, htdocs).
-define(RES, ".res").
-define(RES_CHANGED, "res_changed").
-define(SEQ_KEY, "").
-define(ADD_KEY, "add").
-define(MODIFY_KEY, "modify").
-define(REMOVE_KEY, "remove").
-define(ADD_DIR_KEY, "add_dir").
-define(REMOVE_DIR_KEY, "remove_dir").
-define(INIT_SEQ, 0).
-record(state, {path, cfgs, ws_con, res_args, seq}).


%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%% Function: start/1
%% Description: Starts a develop.
%% -----------------------------------------------------------------
-spec start_link(list()) ->
	{'ok', pid()} | {'error', term()}.
%% -----------------------------------------------------------------
start_link(PluginPath) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, PluginPath, []).

%% -----------------------------------------------------------------
%%@doc monitor file
%% @spec file_monitor(Args, Src, Type, Info) -> any()
%%@end
%% -----------------------------------------------------------------
file_monitor(_, _, create, File) ->
	gen_server:call(?MODULE, {monitors, [z_lib:char_replace(File, $\\, $/)], [], []}, 60000);
file_monitor(_, _, modify, File) ->
	gen_server:call(?MODULE, {monitors, [], [z_lib:char_replace(File, $\\, $/)], []}, 60000);
file_monitor(_, _, remove, File) ->
	gen_server:call(?MODULE, {monitors, [], [], [z_lib:char_replace(File, $\\, $/)]}, 60000);
file_monitor(_, _, Type, File) ->
	ignore.

%% -----------------------------------------------------------------
%% Function: debug_read
%% Description: 用于debug的数据读取
%% Returns: {ok, DebugInfo} | {error, Reason}
%% -----------------------------------------------------------------
debug_read() ->
	gen_server:call(?MODULE, debug_read).

%% -----------------------------------------------------------------
%% Function: start_monitor
%% Description: 资源监控连接启动事件
%% Returns: any
%% -----------------------------------------------------------------
start_monitor(Args, ?SRC, {ws_con, init}, {Session, Addr}) ->
	case lists:keyfind(?IP_KEY, 1, Addr) of
		{?IP_KEY, ?IP} ->
			gen_server:call(?MODULE, {start_monitor, element(2, Session), Args});
		_ ->
			ignore
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
	N = node(),
	ProjectL = zm_plugin:parse(Path),
	{Mapping, RunL, InitL, _VersionL} = CfgL = zm_plugin:config(ProjectL),
	CodeResL = zm_plugin:config_extend(N, Path, ProjectL),
	case zm_plugin:execute_do(Mapping, handle_node_config(RunL, [{{"~node", N}, {[], zm_node, [N, [dev, db, log, gateway, calc]]}}]) ++ CodeResL, false) of
		[] ->
			%执行全局初始化配置
			case execute_init(InitL) of
				[] ->
					{ok, _} = z_file_monitor:start_link([]),
					z_file_monitor:monitor(Path, {?MODULE, file_monitor, []}, 60000),
					zm_event:notify(node, boot, []),
					{ok, #state{path = Path, cfgs = {CfgL, CodeResL}, seq = ?INIT_SEQ}};
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
handle_call({monitors, Adds2, Modifys2, Removes2}, _From, #state{path = Path,																 
	cfgs = {{OldMapping, OldRunL, _, _} = OldCfgL, CodeResL}, ws_con = Con, res_args = ResArgs, seq = Seq} = State) ->
	Adds = [parse_file_path(Add_Item, Path) || Add_Item <- Adds2],
	Removes = [parse_file_path(Remove_Item, Path) || Remove_Item <- Removes2],
	Modifys =  [parse_file_path(Modify_Item, Path) || Modify_Item <- Modifys2],
	NewSeq=notify_changed(ResArgs, Con, Seq, Adds, Modifys, Removes, [], [], []),
	try
		{CodeResL1, IsCodeRes1, IsCfg1} = file_monitor(
			Path, Adds, add, CodeResL, false, false),
		{CodeResL2, IsCodeRes2, IsCfg2} = file_monitor(
			Path, Removes, remove, CodeResL1, IsCodeRes1, IsCfg1),
		case file_monitor(Path, Modifys, modify, CodeResL2, IsCodeRes2, IsCfg2) of
			{_, false, false} ->
				{reply, other, State#state{seq = NewSeq}};
			{CodeResL3, true, false} ->
				ok = execute_(OldMapping, CodeResL, OldMapping, CodeResL3),
        		notify(reload_code,CodeResL,CodeResL3),
				{reply, code_res, State#state{cfgs = {OldCfgL, CodeResL3}, seq = NewSeq}};
			{_, false, true} ->
				ProjectL = zm_plugin:parse(Path),
				{Mapping, RunL, _, _} = CfgL = zm_plugin:config(ProjectL),
				ok = execute_(OldMapping, OldRunL, Mapping, RunL),
        		notify(reload_config,OldRunL,RunL),
				{reply, code_res, State#state{cfgs = {CfgL, CodeResL}, seq = NewSeq}};
			{CodeResL3, true, true} ->
				ProjectL = zm_plugin:parse(Path),
				{Mapping, RunL, _, _} = CfgL = zm_plugin:config(ProjectL),
				ok = execute_(OldMapping, OldRunL ++ CodeResL,
					Mapping, RunL ++ CodeResL3),
		        notify(reload_config,OldRunL,RunL),
		        notify(reload_code,CodeResL,CodeResL3),
				{reply, code_res, State#state{cfgs = {CfgL, CodeResL3}, seq = NewSeq}}
		end
	catch
		Error:Why ->
			error_logger:warning_report(
				{Error, Why, erlang:get_stacktrace()}),
			{reply, error, State#state{seq = NewSeq}}
	end;
handle_call(debug_read, _From, #state{cfgs = {{_, RunL, InitL, _}, CodeResL}} = State) ->
	%读取debug数据
	{reply, {ok, {InitL, RunL ++ CodeResL}}, State};
handle_call({start_monitor, Con, Args}, _From, State) ->
	%开始资源文件监控
	{reply, ok, State#state{ws_con = Con, res_args = Args, seq = ?INIT_SEQ}};
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
% 监控文件变动
file_monitor(Path, [H | T], Type, CodeResL, IsCodeRes, IsCfg) ->
	case zm_plugin:get_file_type_config(Path, H, Type, CodeResL) of
		CodeResL1 when is_list(CodeResL1) ->
			file_monitor(Path, T, Type, CodeResL1, true, IsCfg);
		cfg ->
			file_monitor(Path, T, Type, CodeResL, IsCodeRes, true);
		_ ->
			file_monitor(Path, T, Type, CodeResL, IsCodeRes, IsCfg)
	end;
file_monitor(_Path, [], _Type, CodeResL, IsCodeRes, IsCfg) ->
	{CodeResL, IsCodeRes, IsCfg}.

% 分析变动的文件路径
parse_file_path(File, Path) ->	
	 Index = string:str(File, Path),	 
	 if 
		Index < 1 ->
		 	File;
    	true ->
			string:strip(string:substr(File, Index + string:len(Path ++ "/")))
	 end.

% 通知前端资源文件改变
notify_changed(Args, {ws_con, _} = Con, Seq, Add, Modify, Remove, AddsPath, RemovesPath, _ModifyPath) ->
	notify_changed1(Args, Con, Seq, res_filter(Add, Args, []), res_filter(Modify, Args, []), res_filter(Remove, Args, []), AddsPath, RemovesPath),
	Seq + 1;
notify_changed(_, _, Seq, _, _, _,_,_,_) ->
	Seq.
	
notify_changed1(_Args, _Con, _Seq, [], [], [], _, _) ->
	ignore;
notify_changed1(_Args, Con, Seq, Files, [], [], _, _) ->
	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?ADD_KEY, lists:flatten(zm_json:encode(Files))}]});
notify_changed1(_Args, Con, Seq, [], Files, [], _, _) ->
	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?MODIFY_KEY, lists:flatten(zm_json:encode(Files))}]});
notify_changed1(_Args, Con, Seq, [], [], Files, _, _) ->
	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?REMOVE_KEY, lists:flatten(zm_json:encode(Files))}]}).
%% notify_changed1(_Args, Con, Seq, [], [], [], Path, []) ->
%% 	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?ADD_DIR_KEY, Path}]});
%% notify_changed1(_Args, Con, Seq, [], [], [], [], Path) ->
%% 	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?REMOVE_DIR_KEY, Path}]}).


%过滤资源文件
res_filter([File|T], Args, L) ->
	case res_filter1(Args, filename:extension(File), false) of
		true ->
			res_filter(T, Args, [File|L]);
		false ->
			[]
	end;
res_filter([], _, L) ->
	lists:reverse(L).

%判断是否是需要过滤的扩展名
res_filter1([[$-|Ext]|_], Ext, _) ->
	false;
res_filter1(["-*"|_], _, _) ->
	false;
res_filter1([[$-|_]|T], Ext, _) ->
	res_filter1(T, Ext, true);
res_filter1([Ext|_], Ext, _) ->
	true;
res_filter1(["*"|_], _, _) ->
	true;
res_filter1([_|T], Ext, Bool) ->
	res_filter1(T, Ext, Bool);
res_filter1([], _, Bool) ->
	Bool.

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


% 执行全局初始化配置
execute_init() ->
	%执行数据库服务器加载，如果节点上无数据库服务器则忽略
	case zm_db_server:load() of
		Result when is_atom(Result) ->
			[];
		E ->
			E
	end.

% 执行全局DO配置和UNDO配置
execute_(OldMapping, OldCfgs, Mapping, Cfgs) ->
	New = lists:sort([{MA, FL} || {FL, MA} <- Cfgs]),
	Old = lists:sort([{MA, FL} || {FL, MA} <- OldCfgs]),
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
%%@doc%%通知配置或代码被热更新
%% @spec notify(ResetType::type(),OldRunL,NewRunL) -> return()
%% where
%% type() = reload_config | reload_code
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
notify(ReloadType,OldRunL, NewRunL)->
  New = lists:sort([{MA, FL} || {FL, MA} <- NewRunL]),
  Old = lists:sort([{MA, FL} || {FL, MA} <- OldRunL]),
  {_Same, Left, Right} = z_lib:compare_order(Old, New, fun compare/2),
  %%广播消息，代码或配置被热更新。Left=就配置，Right=新配置
  zm_event:notify(node,ReloadType,{Left,Right}).

%比较2个配置是否相同，返回大于 1、小于 -1、等于 0
compare({FA1, _}, {FA2, _}) when FA1 > FA2 ->
	1;
compare({FA1, _}, {FA2, _}) when FA1 < FA2 ->
	-1;
compare({_, _}, {_, _}) ->
	0.

handle_node_config(RunL, NodeL) ->
	lists:foldr(fun({_, {_, start, {zm_service, _, _}}} = H, T) -> 
						[H|NodeL ++ T];
				   (H, T) ->
						[H|T]
				end, [], RunL).