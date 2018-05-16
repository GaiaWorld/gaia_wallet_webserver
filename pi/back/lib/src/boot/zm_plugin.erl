%%@doc插件模块，
%%```
%%%插件目录的结构：第一级目录为项目，
%%%任何目录名以;或~开头表示跳过，.cfg, .ebin, .res, .vsn 为系统路径，配置文件必须以.cfg结尾
%%%
%%%可以被读取的配置有：
%%%	{local | global, 变量, any()}.
%%%	{local | global, 变量, func, "匿名函数定义"}. 例如: "fun(L) -> sb_trees:from_orddict(lists:sort([{atom_to_list(X), X} || X <- L])) end."
%%%	{local | global, 变量, func, 变量, list()}.
%%%	{local | global, 变量, atom(), atom(), list()}.
%%% {template, 应用模板, [] |{}}.（详见zm_plugin:parse_template/2）
%%%	{层列表, 模块, 参数}.
%%%	插件配置有默认变量：
%%%		项目(atom): Project, 项目路径(string): ProjectPath,
%%%		当前项目(atom): CurProject, 当前项目路径(string): CurProjectPath
%%%	返回{{File, Line}, 元组}列表
%%%	注意：不能在一行中放2个配置描述
%%'''
%%@end


-module(zm_plugin).

-description("plugin").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([parse/1, config/1, config_extend/3, reset_db_server_path/2]).
-export([execute_do/3, execute_undo/2, get_file_type_config/4, main_project/2]).

%%%=======================DEFINE=======================
-define(SKIP1, $;).
-define(SKIP2, $~).
-define(SYS, $.).
-define(SYS_PROJECT, "#").

-define(CFG_DIR, ".cfg").
-define(CODE_DIR, ".ebin").
-define(RES_DIR, ".res").

-define(DIR_CFG, "cfg/").
-define(DIR_CODE, "ebin/").
-define(DIR_RES, "res/").

-define(CFG_FILE, ".cfg").
-define(CODE_FILE, ".beam").

-define(RES_DIR_SKIP, [".svn", ".hg"]).

-define(MAPPING, mod_fun_mapping).
-define(START, start).
-define(INIT, init).
-define(VERSION, version).

-define(DB_SERVER, zm_db_server).
-define(DB, zm_db).

-define(PLUGIN_START_MOD_ENV, "ERL_PLUGIN_START_MOD").
-define(RELEASE_START_MOD, "release").
-define(DEBUG_START_MOD, "debug").

-define(LOCAL_VAR_TABLE(Path), list_to_atom(lists:concat([Path, "_local_var"]))).
-define(GLOBAL_VAR_TABLE(Path), list_to_atom(lists:concat([Path, "_global_var"]))).
-define(LOCAL_SCOPE, local).
-define(GLOBAL_SCOPE, global).

-define(VAR_REF_TABLE(Path), list_to_atom(lists:concat([Path, "_var_ref"]))).
-define(VAR_BY_REF_TABLE(Path), list_to_atom(lists:concat([Path, "_var_by_ref"]))).
-define(TABLE_BY_REF_TABLE(Path), list_to_atom(lists:concat([Path, "_table_by_ref"]))).

-define(FUN_TYPE, func).
-define(TEMPLATE, template).

-define(DIR_DEPTH, 256).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc	分析指定目录，返回{项目及子项目列表, 代码路径列表, 资源路径列表, 配置元组列表}列表
%% @spec	parse(Path) -> return()
%% where
%%	return() =	[{list(), list(), list(), [tuple()]}]
%%@end
%% -----------------------------------------------------------------
parse(Path) when is_list(Path) ->
	{ok, L} = z_lib:list_file(Path, 1),
	Mod=os:getenv(?PLUGIN_START_MOD_ENV),
	{EtsOptions, VarRefTable}=case Mod of
		?DEBUG_START_MOD ->
			put(?PLUGIN_START_MOD_ENV, list_to_atom(?DEBUG_START_MOD)),
			Options=[named_table, protected],
			{Options, {ets:new(?VAR_REF_TABLE(Path), Options), ets:new(?VAR_BY_REF_TABLE(Path), Options), ets:new(?TABLE_BY_REF_TABLE(Path), Options)}};
		Mod ->
			put(?PLUGIN_START_MOD_ENV, Mod),
			{[], []}
	end,	
	VarTable = {ets:new(?GLOBAL_VAR_TABLE(Path), EtsOptions), ets:new(?LOCAL_VAR_TABLE(Path), EtsOptions)},
	L1 = [R || {F, _, directory, _, _} <- lists:reverse(L),
	(R = pretreatment(Path, F, VarTable, VarRefTable, Mod)) =/= none],
	L2 = [{Projects, CodeDirs, ResDirs,
	parse_cfg(VarTable, Project, TokensL)} ||
	{[Project | _] = Projects, CodeDirs, ResDirs, TokensL} <- L1],
	[ets:delete(Tab) || Tab <- tuple_to_list(VarTable), EtsOptions =:= []],
	%防止配置冲突
	Ets = ets:new(?MODULE, [private, {keypos, 2}]),
	ConflictL = [R || {_, _, _, CfgL} <- L2, E <- CfgL, (R = conflict(Ets, E)) =/= true],
	ets:delete(Ets),
	[erlang:error({cfg_conflict, ConflictL}) || ConflictL =/= []],
	L2.

%% -----------------------------------------------------------------
%%@doc 将配置展开
%% @spec	config(ConfigList) -> return()
%% where
%%	return() =	{Mapping, RunL, InitL, VersionL}
%%@end
%% -----------------------------------------------------------------
config([{[?SYS_PROJECT | _],
	_CodeL, _ResL, [{_, {_, ?MAPPING, L}} | SysCfgL]} | T]) ->
	Mapping = sb_trees:from_orddict(lists:ukeysort(1, L)),
	Cfgs = lists:flatten([SysCfgL | [CfgL || {_, _, _, CfgL} <- T]]),
	{OtherL, InitL} = lists:partition(
	fun({_, {_, Mod, _}}) -> Mod =/= ?INIT end, Cfgs),
	{RunL, VersionL} = lists:partition(
	fun({_, {_, Mod, _}}) -> Mod =/= ?VERSION end, OtherL),
	{Mapping, RunL, InitL, VersionL};
config([{[?SYS_PROJECT | _], _CodeL, _ResL, _CfgL} | _]) ->
	erlang:error("need mod-fun mapping");
config([{ProjectL, _CodeL, _ResL, _CfgL} | _]) ->
	erlang:error({"need '#' system project", ProjectL}).

%% -----------------------------------------------------------------
%%@doc 将项目及子项目，代码及代码路径，资源及资源路径，变成配置
%% @spec	config_extend(Node, Path, ProjectL) -> return()
%% where
%%	return() =	CodeResL
%%@end
%% -----------------------------------------------------------------
config_extend(Node, Path, ProjectL) ->
	Codes = [CodeCfg || {_, CodeDirs, _, _} <- ProjectL, Dir <- CodeDirs,
	CodeCfg <- code_path(Path, Dir ++ [$/ | ?CODE_DIR])],
	%防止代码冲突
	Ets = ets:new(?MODULE, [private]),
	ConflictL = [R || {Info, {_, _, [Module, _Info, _Crc]}} <- Codes,
	(R = conflict(Ets, {Module, Info})) =/= true],
	ets:delete(Ets),
	[erlang:error({code_conflict, ConflictL}) || ConflictL =/= []],
	CodePaths = [{{Dir1, code_path}, {[], code, [Path ++ [$/ | Dir1]]}} ||
	{_, CodeDirs, _, _} <- ProjectL, Dir <- CodeDirs,
	(Dir1 = Dir ++ [$/ | ?CODE_DIR]) =/= []],
	Res = [ResCfg || {_, _, ResDirs, _} <- ProjectL, Dir <- ResDirs,
	ResCfg <- res_path(Path, Dir, Dir ++ [$/ | ?RES_DIR])],
	ResPaths = [{{Dir, res_path}, {[], zm_resource,
	[Dir, Node, Path ++ [$/ | Dir] ++ [$/ | ?RES_DIR]]}} ||
	{_, _, ResDirs, _} <- ProjectL, Dir <- ResDirs],
	Plugin = {{Path, plugin}, {[], zm_config, [?MODULE,
	[{list_to_atom(P), P} || {Projects, _, _, _} <- ProjectL,
		P <- Projects], ordered_set]}},
	Codes ++ CodePaths ++ [Plugin | Res] ++ ResPaths.

%% -----------------------------------------------------------------
%%@doc 重置配置中的db_server的路径
%% @spec	reset_db_server_path(CfgL, Path) -> return()
%% where
%%	return() =	CfgL
%%@end
%% -----------------------------------------------------------------
reset_db_server_path(CfgL, Path) ->
	[reset_db_server_path1(E, Path) || E <- CfgL].

%% -----------------------------------------------------------------
%%@doc	执行do配置FL_MA_List
%% @spec	execute_do(Mapping, List, SkipStart) -> return()
%% where
%%	return() =	[] | [{error, Resaon, StackTrace}] | {error, Reason}
%%@end
%% -----------------------------------------------------------------
execute_do(Mapping, List, SkipStart) ->
	try
	[R || {FL, MA} <- List, (R = execute_do(Mapping, FL, MA, SkipStart)) =/= ok]
	catch
	_Error:Why ->
		{error, {Why, erlang:get_stacktrace()}}
	end.

%% -----------------------------------------------------------------
%%@doc	执行undo配置FL_MA_List
%% @spec	execute_undo(Mapping, List) -> return()
%% where
%%	return() =	[] | [{error, Resaon, StackTrace}] | {error, Reason}
%%@end
%% -----------------------------------------------------------------
execute_undo(Mapping, List) ->
	case [R || {FL, MA} <- lists:reverse(List),
	(R = execute_undo(Mapping, FL, MA)) =/= ok] of
	[] ->
		ok;
	L ->
		case [R || {FL, MA} <- lists:reverse(L),
		(R = execute_do(Mapping, FL, MA, true)) =/= ok] of
		[] ->
			{error, {undo_error, L}};
		LL ->
			{exit, {undo_redo_error, LL}}
		end
	end.

%% -----------------------------------------------------------------
%%@doc 判断指定文件和改变类型下的配置及变动
%% @spec	get_file_type_config(Path, File, Type, CodeResL) -> return()
%% where
%%	return() =	ignore | cfg | CodeResL1 | other
%%@end
%% -----------------------------------------------------------------
get_file_type_config(Path, File, Type, CodeResL) ->
	case file_ignore(File) of
	true ->
		ignore;
	false ->
		case z_lib:split_first(File, "/.") of
		{_, ?DIR_CFG ++ Name} ->
			case filename:extension(Name) of
			?CFG_FILE ->
				cfg;
			_ ->
				other
			end;
		{_, ?DIR_CODE ++ Name} ->
			case filename:extension(Name) of
			?CODE_FILE when Type =:= add ->
				[code_cfg(Path, File) | CodeResL];
			?CODE_FILE when Type =:= modify ->
				L = [E || {{F, _}, _} = E <- CodeResL, F =/= File],
				[code_cfg(Path, File) | L];
			?CODE_FILE when Type =:= remove ->
				[E || {{F, _}, _} = E <- CodeResL, F =/= File];
			_ ->
				other
			end;
		{Path1, ?DIR_RES ++ Name} ->
			case Type of
			add ->
				R = res_cfg(Path, Path1 ++ [$/ | Name], File),
				[R || R =/= false] ++ CodeResL;
			modify ->
				L = [E || {{F, _}, _} = E <- CodeResL, F =/= File],
				R = res_cfg(Path, Path1 ++ [$/ | Name], File),
				[R || R =/= false] ++ L;
			remove ->
				[E || {{F, _}, _} = E <- CodeResL, F =/= File];
			_ ->
				other
			end;
		_ ->
			other
		end
	end.

%% -----------------------------------------------------------------
%%@doc	获取对应根项目路径，将"xxx_yy/zzz"中的_yy去掉，xxx可以包含_
%% @spec	main_project(Path, Type:: str | atom) -> return()
%% where
%%	return() =	ProjectName | none
%%@end
%% -----------------------------------------------------------------
main_project([_ | _] = Path, str) ->
	{H, T} = first_char_split(Path, $/, []),
	{_H1, T1} = first_char_split(H, $_, []),
	lists:reverse(T1, [$/ | T]);
main_project(Path, atom) ->
	list_to_atom(main_project(Path, str));
main_project(_, _) ->
	none.

%%%===================LOCAL FUNCTIONS==================
% 预处理，主要对变量声明进行处理
pretreatment(Path, [?SKIP1 | _], _, _, _) when is_list(Path) ->
	none;
pretreatment(Path, [?SKIP2 | _], _, _, _) when is_list(Path) ->
	none;
pretreatment(Path, [?SYS | _], _, _, _) when is_list(Path) ->
	none;
pretreatment(Path, Project, VarTable, VarRefTable, Mod) when is_list(Path), is_list(Project) ->
	ProjectPath = Path ++ [$/ | Project],
	{ok, L} = z_lib:list_file(ProjectPath, 1),
	case parse_project(Path, Project, L, [], [], [], []) of
	{[], [], [], []} ->
		none;
	{Projects, CodeDirs, ResDirs, CfgDirs} ->
		{Projects, CodeDirs, ResDirs, parse_project_var(
		VarTable, VarRefTable, Project, Path, CfgDirs, Mod, [])}
	end.

% 分析项目目录，返回项目子项目列表, 代码路径列表, 资源路径列表, 配置路径列表
parse_project(Path, Dir, [{[?SKIP1 | _], _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, CodeL, ResL, CfgL);
parse_project(Path, Dir, [{[?SKIP2 | _], _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, CodeL, ResL, CfgL);
parse_project(Path, Dir, [{?CFG_DIR, _, directory, _, _} | T], [Dir | _] = DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, CodeL, ResL, [Dir | CfgL]);
parse_project(Path, Dir, [{?CFG_DIR, _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, [Dir | DirL], CodeL, ResL, [Dir | CfgL]);
parse_project(Path, Dir, [{?CODE_DIR, _, directory, _, _} | T], [Dir | _] = DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, [Dir | CodeL], ResL, CfgL);
parse_project(Path, Dir, [{?CODE_DIR, _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, [Dir | DirL], [Dir | CodeL], ResL, CfgL);
parse_project(Path, Dir, [{?RES_DIR, _, directory, _, _} | T], [Dir | _] = DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, CodeL, [Dir | ResL], CfgL);
parse_project(Path, Dir, [{?RES_DIR, _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, [Dir | DirL], CodeL, [Dir | ResL], CfgL);
parse_project(Path, Dir, [{[?SYS | _], _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, CodeL, ResL, CfgL);
parse_project(Path, Dir, [{H, _, directory, _, _} | T], DirL, CodeL, ResL, CfgL) ->
	Dir1 = Dir ++ [$/ | H],
	{ok, L} = z_lib:list_file(Path ++ [$/ | Dir1], 1),
	{DirL1, CodeL1, ResL1, CfgL1} = parse_project(Path, Dir1, L, DirL, CodeL, ResL, CfgL),
	parse_project(Path, Dir, T, DirL1, CodeL1, ResL1, CfgL1);
parse_project(Path, Dir, [_ | T], DirL, CodeL, ResL, CfgL) ->
	parse_project(Path, Dir, T, DirL, CodeL, ResL, CfgL);
parse_project(_Path, _Dir, [], DirL, CodeL, ResL, CfgL) ->
	{DirL, CodeL, ResL, CfgL}.

% 分析所有项目的变量声明，返回项目的词条列表，如果变量声明冲突或值格式错误则抛出异常
parse_project_var(VarTable, VarRefTable, Project, Path, [H | T], Mod, L) ->
	ProjectPath = Path ++ [$/ | H],
	CfgDir = ProjectPath ++ [$/ | ?CFG_DIR],
	{ok, FL} = z_lib:list_file(CfgDir, 1),
	parse_project_var(VarTable, VarRefTable, Project, Path, T, Mod,
	parse_cfg_var(VarTable, VarRefTable, Project, H, CfgDir, lists:reverse(FL), Mod, L));
parse_project_var(_VarTable, _VarRefTable, _Project, _Path, [], _Mod, L) ->
	lists:reverse(L).

% 分析所有配置文件中的变量
parse_cfg_var({_GlobalVarTab, LocalVarTab} = VarTable, VarRefTable,
	Project, SubProject, CfgPath, [{FileName, _Size, regular, _, _} | T], Mod, L) ->
	case filename:extension(FileName) of
	?CFG_FILE ->
		CfgFile = CfgPath ++ [$/ | FileName],
		%定义系统本地变量
		def_sys_var(LocalVarTab, CfgFile, Project, SubProject),
		parse_cfg_var(VarTable, VarRefTable, Project, SubProject, CfgPath, T, Mod,
		[{SubProject, CfgFile, parse_file_var(VarTable, VarRefTable, Mod, CfgFile)} | L]);
	_ ->
		parse_cfg_var(VarTable, VarRefTable, Project, SubProject, CfgPath, T, Mod, L)
	end;
parse_cfg_var(VarTable, VarRefTable, Project, SubProject, CfgPath, [{_, _, _, _, _} | T], Mod, L) ->
	parse_cfg_var(VarTable, VarRefTable, Project, SubProject, CfgPath, T, Mod, L);
parse_cfg_var(_VarTable, _VarRefTable, _Project, _SubProject, _CfgPath, [], _Mod, L) ->
	L.

% 分析文件中的变量
parse_file_var(VarTable, VarRefTable, Mod, File) ->
	{ok, Bin} = file:read_file(File),
	case unicode:characters_to_list(Bin, utf8) of
	Text when is_list(Text) ->
		parse_text_var(VarTable, VarRefTable, Mod, File, Text);
	{error, _, _} ->
		erlang:error({utf8_error, File});
	{incomplete, _, _} ->
		erlang:error({utf8_incomplete, File})
	end.

% 分析文本中的变量, 返回词条列表
parse_text_var(VarTable, VarRefTable, Mod, File, Text) ->
	case erl_scan:string(Text) of
	{ok, Tokens, _EndLocation} ->
		parse_token_var(VarTable, VarRefTable, Mod, File, Tokens, -1),
		Tokens;
	{error, ErrorInfo, EndLocation} ->
		erlang:error({scan_error, File, ErrorInfo, EndLocation})
	end.

% 分析词条中的变量
parse_token_var(_VarTable, _VarRefTable, _Mod, File, [{_, Line} = H | _], Line) ->
	erlang:error({token_need_newline, File, H});
parse_token_var(VarTable, VarRefTable, Mod, File,
	[{'{', Line} = H, {atom, _, Scope}, {',', _}, {var, _, Var}, {',', _} | T], _Line)
	when Scope =:= ?LOCAL_SCOPE; Scope =:= ?GLOBAL_SCOPE ->
	%有变量声明的元组
	Rest = register_var(VarTable, VarRefTable, Mod, Scope, File, Var, T, H),
	parse_token_var(VarTable, VarRefTable, Mod, File, ignore_dot(Rest), Line);
parse_token_var(VarTable, VarRefTable, Mod, File, [{'{', Line} = H | T], _Line) ->
	%普通元组
	{Tuple, Rest, Vs} = parse_tuple_var(File, T, 0, [], [H]),
	parse_var_by_ref(Mod, VarTable, VarRefTable, Tuple, Vs, File, Line),
	parse_token_var(VarTable, VarRefTable, Mod, File, ignore_dot(Rest), Line);
parse_token_var(_VarTable, _VarRefTable, _Mod, File, [H | _], _Line) ->
	erlang:error({invalid_token, File, H});
parse_token_var(_VarTable, _VarRefTable, _Mod, _File, [], _Line) ->
	ok.

% 注册变量
register_var({GlobalVarTab, LocalVarTab} = VarTable, VarRefTable, Mod, 
	?LOCAL_SCOPE, File, Var, Tokens, LineInfo) ->
	Key = {File, Var},
	case ets:member(LocalVarTab, Key) of
	false ->
		case parse_tuple_var(File, Tokens, 0, [], [LineInfo]) of
		{{V}, R, Vs} ->
			ets:insert(LocalVarTab, {Key, {V}}),
			parse_var_by_ref(Mod, VarTable, VarRefTable, Key, Vs, File, 0),
			parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Key, File),
			R;
		{{?FUN_TYPE, V}, R, Vs} ->
			try
				{value, Fun, _} = z_lib:scan_parse_eval(V, []),
				ets:insert(LocalVarTab, {Key, {Fun}}),
				parse_var_by_ref(Mod, VarTable, VarRefTable, Key, Vs, File, 0),
				parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Key, File)
			catch
			_:Reason ->
				erlang:error({invalid_fun_var, File, LineInfo, Var, Reason})
			end,
			R;
		{{?FUN_TYPE, F, A}, R, Vs} ->
			ets:insert(LocalVarTab, {Key, {F, A}}),
			parse_var_by_ref(Mod, VarTable, VarRefTable, Key, Vs, File, 0),
			parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Key, File),
			R;
		{{_, _, _} = MFA, R, Vs} ->
			ets:insert(LocalVarTab, {Key, MFA}),
			parse_var_by_ref(Mod, VarTable, VarRefTable, Key, Vs, File, 0),
			parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Key, File),
			R;
		_ ->
			erlang:error({invalid_var, File, LineInfo, Var})
		end;
	true ->
		erlang:error({var_conflict, File, LineInfo, Var})
	end;
register_var({GlobalVarTab, _LocalVarTab} = VarTable, VarRefTable, Mod, 
	?GLOBAL_SCOPE, File, Var, Tokens, LineInfo) ->
	case ets:member(GlobalVarTab, Var) of
	false ->
		case parse_tuple_var(File, Tokens, 0, [], [LineInfo]) of
		{{V}, R, Vs} ->
			ets:insert(GlobalVarTab, {Var, {{V}, File}}),
			parse_var_by_ref(Mod, VarTable, VarRefTable, Var, Vs, File, 0),
			parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Var, File),
			R;
		{{?FUN_TYPE, V}, R, Vs} ->
			try
				{value, Fun, _} = z_lib:scan_parse_eval(V, []),
				ets:insert(GlobalVarTab, {Var, {{Fun}, File}}),
				parse_var_by_ref(Mod, VarTable, VarRefTable, Var, Vs, File, 0),
				parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Var, File)
			catch
			_:Reason ->
				erlang:error({invalid_fun_var, File, LineInfo, Var, Reason})
			end,
			R;
		{{?FUN_TYPE, F, A}, R, Vs} ->
			ets:insert(GlobalVarTab, {Var, {{F, A}, File}}),
			parse_var_by_ref(Mod, VarTable, VarRefTable, Var, Vs, File, 0),
			parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Var, File),
			R;
		{{_, _, _} = MFA, R, Vs} ->
			ets:insert(GlobalVarTab, {Var, {MFA, File}}),
			parse_var_by_ref(Mod, VarTable, VarRefTable, Var, Vs, File, 0),
			parse_var_ref(Vs, Mod, GlobalVarTab, VarRefTable, Var, File),
			R;
		_ ->
			erlang:error({invalid_var, File, LineInfo, Var})
		end;
	true ->
		erlang:error({var_conflict, File, LineInfo, Var})
	end.

% 分析元组中的变量
parse_tuple_var(File, [{'}', Line} = H | T], 0, Vars, L) ->
	case erl_parse:parse_term(lists:reverse([{dot, Line}, H | L])) of
	{ok, Tuple} ->
		{Tuple, T, Vars};
	{error, Reason} ->
		erlang:error({Reason, File, H})
	end;
parse_tuple_var(File, [{'}', _} = H | T], N, Vars, L) ->
	parse_tuple_var(File, T, N - 1, Vars, [H | L]);
parse_tuple_var(File, [{'{', _} = H | T], N, Vars, L) ->
	parse_tuple_var(File, T, N + 1, Vars, [H | L]);
parse_tuple_var(File, [{var, Line, Var} | T], N, Vars, L) ->
	parse_tuple_var(File, T, N, [Var|Vars], [{atom, Line, Var} | L]);
parse_tuple_var(File, [H | T], N, Vars, L) ->
	parse_tuple_var(File, T, N, Vars, [H | L]);
parse_tuple_var(File, [], _N, _Vars, [H | _]) ->
	erlang:error({tuple_incomplete, File, H}).

% 分析变量
parse_var_by_ref(?DEBUG_START_MOD, {GlobalVarTable, _}, {_, VarByRefTable, TableByRefTable}, {Layer, CfgType, CfgInfo}, Vars, File, Line) when is_list(Layer), is_atom(CfgType) ->																															 
	OtherVars=registe_table_var(GlobalVarTable, TableByRefTable, CfgType, CfgInfo, Vars, File, Line),
	[register_var_by_ref(GlobalVarTable, VarByRefTable, CfgType, Var, File, Line) || Var <- OtherVars];
parse_var_by_ref(?DEBUG_START_MOD, {GlobalVarTable, _}, {_, VarByRefTable, _}, Ref, Vars, File, _) ->
	register_var_def_by_ref(Vars, GlobalVarTable, VarByRefTable, Ref, File);
parse_var_by_ref(_, _, _, _, _, _, _) ->
	ok.

% 注册表名变量
registe_table_var(GlobalVarTable, TableByRefTable, CfgType, CfgInfo, Vars, File, Line) ->
	case parse_table_var(CfgType, GlobalVarTable, CfgInfo, Vars, File) of
		{TableVar, Vars_} ->
			true=ets:insert_new(TableByRefTable, {TableVar, [{File, Line}]}),
			Vars_;
		false ->
			Vars
	end.

% 分析表配置项中的表名变量
parse_table_var(?DB, GlobalVarTable, [TableName, _, _], Vars, File) ->
	case lists:member(TableName, Vars) of
		true ->
			case ets:lookup(GlobalVarTable, TableName) of
				[] ->
					{{File, TableName}, lists:delete(TableName, Vars)};
				_ ->
					{TableName, lists:delete(TableName, Vars)}
			end;
		false ->
			false
	end;
parse_table_var(_, _, _, _, _) ->
	false.

% 注册剩余变量
register_var_by_ref(_, _, _, 'Project', _, _) ->
	ignore;
register_var_by_ref(_, _, _, 'ProjectPath', _, _) ->
	ignore;
register_var_by_ref(_, _, _, 'CurProject', _, _) ->
	ignore;
register_var_by_ref(_, _, _, 'CurProjectPath', _, _) ->
	ignore;
register_var_by_ref(GlobalVarTable, VarByRefTable, CfgType, Var, File, Line) ->
	case ets:lookup(GlobalVarTable, Var) of
		[] ->
			parse_var_by_ref(VarByRefTable, {File, Var}, CfgType, File, Line);
		_ ->
			parse_var_by_ref(VarByRefTable, Var, CfgType, File, Line)
	end.

parse_var_by_ref(VarByRefTable, Key, CfgType, File, Line) ->
	case ets:lookup(VarByRefTable, Key) of
		[] ->
			true=ets:insert_new(VarByRefTable, {Key, [{CfgType, {File, Line}}]});
		[{_, ByRefL}] ->
			true=ets:insert(VarByRefTable, {Key, [{CfgType, {File, Line}}|ByRefL]})
	end.

register_var_def_by_ref(['Project'|T], GlobalVarTable, VarByRefTable, Ref, File) ->
	register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Ref, File);
register_var_def_by_ref(['ProjectPath'|T], GlobalVarTable, VarByRefTable, Ref, File) ->
	register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Ref, File);
register_var_def_by_ref(['CurProject'|T], GlobalVarTable, VarByRefTable, Ref, File) ->
	register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Ref, File);
register_var_def_by_ref(['CurProjectPath'|T], GlobalVarTable, VarByRefTable, Ref, File) ->
	register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Ref, File);
register_var_def_by_ref([Ref|T], GlobalVarTable, VarByRefTable, Ref, File) ->
	register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Ref, File);
register_var_def_by_ref([Var|T], GlobalVarTable, VarByRefTable, Ref, File) ->
	case ets:lookup(GlobalVarTable, Var) of
		[] ->
			parse_var_def_by_ref(VarByRefTable, {File, Var}, Ref),
			register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Var, File);
		_ ->
			parse_var_def_by_ref(VarByRefTable, Var, Ref),
			register_var_def_by_ref(T, GlobalVarTable, VarByRefTable, Var, File)
	end;
register_var_def_by_ref([], _, _, _, _) ->
	ok.

parse_var_def_by_ref(VarByRefTable, Key, Ref) ->
	case ets:lookup(VarByRefTable, Key) of
		[] ->
			true=ets:insert_new(VarByRefTable, {Key, [{var_def, Ref}]});
		[{_, ByRefL}] ->
			true=ets:insert(VarByRefTable, {Key, [{var_def, Ref}|ByRefL]})
	end.

parse_var_ref([Var|T], ?DEBUG_START_MOD, GlobalVarTable, VarRefTable, Var, File) ->
	parse_var_ref(T, ?DEBUG_START_MOD, GlobalVarTable, VarRefTable, Var, File);
parse_var_ref([Var|T], ?DEBUG_START_MOD, GlobalVarTable, VarRefTable, {_, Var} = Key, File) ->
	parse_var_ref(T, ?DEBUG_START_MOD, GlobalVarTable, VarRefTable, Key, File);
parse_var_ref([Var|T], ?DEBUG_START_MOD, GlobalVarTable, {VarDefTable, _, _} = VarRefTable, Key, File) ->
	RealVar=case ets:lookup(GlobalVarTable, Var) of
		[] ->
			{File, Var};
		_ ->
			Var
	end,
	case ets:lookup(VarDefTable, Key) of
		[] ->
			true=ets:insert_new(VarDefTable, {Key, [RealVar]}),
			parse_var_ref(T, ?DEBUG_START_MOD, GlobalVarTable, VarRefTable, Key, File);
		[{_, Vars}] ->
			true=ets:insert(VarDefTable, {Key, [RealVar|Vars]}),
			parse_var_ref(T, ?DEBUG_START_MOD, GlobalVarTable, VarRefTable, Key, File)
	end;
parse_var_ref([], _, _, _, _, _) ->
	ok;
parse_var_ref(_, _, _, _, _, _) ->
	ok.

% 忽略句点
ignore_dot([{dot, _} | T]) ->
	ignore_dot(T);
ignore_dot([{'..', _} | T]) ->
	ignore_dot(T);
ignore_dot([{'...', _} | T]) ->
	ignore_dot(T);
ignore_dot(L) ->
	L.

% 分析配置路径列表，实际开始求值和模板解析
parse_cfg(VarTable, Project, TokensL) ->
	CfgL = parse_cfg_dirs(VarTable, Project, TokensL, []),
	% 解析模板
	NewCfgL = lists:foldl(fun(Cfg, L) ->
	case Cfg of
		{FileInfo, {?TEMPLATE, TemplateL, DataL}} when is_list(TemplateL), is_list(DataL) ->
		[{FileInfo, parse_template(Template, Data)} || Data <- DataL, Template<-TemplateL] ++ L;
		{FileInfo, {?TEMPLATE, TemplateL, Data}} when is_list(TemplateL), is_tuple(Data) ->
			[{FileInfo, parse_template(Template, Data)} || Template<-TemplateL] ++ L;
		{FileInfo, {?TEMPLATE, Template, DataL}} when is_tuple(Template), is_list(DataL) ->
			[{FileInfo, parse_template(Template, Data)} || Data <- DataL] ++ L;
		{FileInfo, {?TEMPLATE, Template, Data}} when is_tuple(Template), is_tuple(Data) ->
		[{FileInfo, parse_template(Template, Data)} | L];
		_ ->
		[Cfg | L]
	end
	end, [], CfgL),
	lists:reverse(NewCfgL).

%% -----------------------------------------------------------------
%%@doc 模板解析
%%```
%% 模板使用格式：
%% A={global, Template, [{[getway], zm_db，["$1",{ {format, {"$2","$3"}} }]}] |{...}   }
%% B={template, Template, [{'db_name',{Key, integer, 0},{Value, integer, 0}}] | {...} }
%% B中引用了A的模板Template，用'db_name'替换A中的"$1", {Key, integer, 0}替换"$2", {Value, integer, 0}替换"$3"
%% A的模板和B的数据都支持列表形式，最终配置是两个列表的数据依次混合
%%'''
%% @spec parse_template(Template, Data) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
parse_template(Template, Data) when is_tuple(Template) ->
	list_to_tuple(parse_template(tuple_to_list(Template), Data));
parse_template(Template, Data) when is_list(Template) ->
	[begin
		case Item of
			[$$ | T] ->
				element(list_to_integer(T), Data);
			_ ->
				parse_template(Item, Data)
		end
	end
	|| Item <- Template];
parse_template(Template, _Data) ->
	Template.


% 分析配置路径列表，实际开始求值
parse_cfg_dirs(VarTable, Project, [{_SubProject, CfgFile, Tokens} | T], L) ->
	parse_cfg_dirs(VarTable, Project, T,
	parse_tokens(CfgFile, VarTable, Tokens, -1, L));
parse_cfg_dirs(_VarTable, _Project, [], L) ->
	lists:reverse(L).

% 分析词法分析树，返回元组列表
parse_tokens(File, _VarTable, [{_, Line} = H | _], Line, _L) ->
	erlang:error({token_need_newline, File, H});
parse_tokens(File, VarTable,
	[{'{', Line} = H, {atom, _, Scope}, {',', _}, {var, _, _Var}, {',', _} | T], _Line, L)
	when Scope =:= ?LOCAL_SCOPE; Scope =:= ?GLOBAL_SCOPE ->
	%有变量定义的元组，
	{_, Rest} = parse_tuple(File, VarTable, T, 0, [H]),
	parse_tokens(File, VarTable, ignore_dot(Rest), Line, L);
parse_tokens(File, VarTable, [{'{', Line} = H | T], _Line, L) ->
	%普通元组，
	{Tuple, Rest} = parse_tuple(File, VarTable, T, 0, [H]),
	parse_tokens(File, VarTable, ignore_dot(Rest), Line, [{{File, Line}, Tuple} | L]);
parse_tokens(File, _VarTable, [H | _], _Line, _L) ->
	erlang:error({invalid_token, File, H});
parse_tokens(_File, _VarTable, [], _Line, L) ->
	L.

% 分析元组，元组中有变量则替换
parse_tuple(File, _VarTable, [{'}', Line} = H | T], 0, L) ->
	case erl_parse:parse_term(lists:reverse([{dot, Line}, H | L])) of
	{ok, Tuple} ->
		{Tuple, T};
	{error, Reason} ->
		erlang:error({Reason, File, H})
	end;
parse_tuple(File, VarTable, [{'}', _} = H | T], N, L) ->
	parse_tuple(File, VarTable, T, N - 1, [H | L]);
parse_tuple(File, VarTable, [{'{', _} = H | T], N, L) ->
	parse_tuple(File, VarTable, T, N + 1, [H | L]);
parse_tuple(File, VarTable, [{var, Line, Var} = H | T], N, L) ->
	parse_tuple(File, VarTable, T, N,
	[{atom, Line, var_eval(VarTable, File, H, Var, [])} | L]);
parse_tuple(File, VarTable, [H | T], N, L) ->
	parse_tuple(File, VarTable, T, N, [H | L]);
parse_tuple(File, _VarTable, [], _N, [H | _]) ->
	erlang:error({tuple_incomplete, File, H}).

% 变量求值, 变量未声明或变量求值出现回路时抛出异常
var_eval({GlobalVarTab, LocalVarTab} = VarTable, File, H, Var, L) ->
	%本地变量求值，因为先对本地变量求值，
	%所以如果与全局变量同名，则会直接返回本地变量值。
	Key = {File, Var},
	case ets:lookup(LocalVarTab, Key) of
		[{Key, {V}}] ->
			Val = v_var_eval(V, VarTable, File, H, L),
			ets:insert(LocalVarTab, {Key, {ok, Val}}),
			Val;
		[{Key, {ok, V}}] ->
			V;
		[{Key, {F, A} = FA}] ->
			Fun = v_var_eval(F, VarTable, File, H, L),
			A1 = list_var_eval(A, VarTable, File, H, [], L),
			V = try
				apply(Fun, A1)
			catch
				_:Reason ->
					erlang:error({apply_error, Reason, FA, File, H})
			end,
			ets:insert(LocalVarTab, {Key, {ok, V}}),
			V;
		[{Key, {M, F, A} = MFA}] ->
			A1 = list_var_eval(A, VarTable, File, H, [], L),
			V = try
				apply(M, F, A1)
			catch
				_:Reason ->
					erlang:error({apply_error, Reason, MFA, File, H})
			end,
			ets:insert(LocalVarTab, {Key, {ok, V}}),
			V;
		_ ->
			%全局变量求值
			case ets:lookup(GlobalVarTab, Var) of
				[{Var, {{V}, VarFile}}] ->
					Val = v_var_eval(V, VarTable, VarFile, H, L),
					ets:insert(GlobalVarTab, {Var, {ok, Val}}),
					Val;
				[{Var, {ok, V}}] ->
					V;
				[{Var, {{F, A} = FA, VarFile}}] ->
					Fun = v_var_eval(F, VarTable, File, H, L),
					A1 = list_var_eval(A, VarTable, VarFile, H, [], L),
					V = try
						apply(Fun, A1)
					catch
						_:Reason ->
							erlang:error({apply_error, Reason, FA, File, H})
					end,
					ets:insert(GlobalVarTab, {Var, {ok, V}}),
					V;
				[{Var, {{M, F, A} = MFA, VarFile}}] ->
					A1 = list_var_eval(A, VarTable, VarFile, H, [], L),
					V = try
						apply(M, F, A1)
					catch
						_:Reason ->
							erlang:error({apply_error, Reason, MFA, File, H})
					end,
					ets:insert(GlobalVarTab, {Var, {ok, V}}),
					V;
				_ ->
					%变量未声明
					erlang:error({var_not_found, File, H})
			end
	end.

%列表变量求值
list_var_eval([Arg | T], VarTable, File, LineInfo, Args, L) when is_atom(Arg) ->
	[H | _] = atom_to_list(Arg),
	case z_lib:check_list_range([H], $A, $Z) of
		false ->
			list_var_eval(T, VarTable, File, LineInfo, [Arg | Args], L);
		_ ->
			%参数是变量
			case lists:member(Arg, L) of
			false ->
				V = var_eval(VarTable, File, LineInfo, Arg, [Arg | L]),
				list_var_eval(T, VarTable, File, LineInfo, [V | Args], L);
			true ->
				%变量引用回路
				erlang:error({var_back_loop, File, LineInfo, Arg})
			end
	end;
list_var_eval([Arg | T], VarTable, File, LineInfo, Args, L) when is_list(Arg) ->
	V = list_var_eval(Arg, VarTable, File, LineInfo, [], L),
	list_var_eval(T, VarTable, File, LineInfo, [V | Args], L);
list_var_eval([Arg | T], VarTable, File, LineInfo, Args, L) when is_tuple(Arg) ->
	V = tuple_var_eval(Arg, VarTable, File, LineInfo, [], L),
	list_var_eval(T, VarTable, File, LineInfo, [V | Args], L);
list_var_eval([Arg | T], VarTable, File, LineInfo, Args, L) ->
	list_var_eval(T, VarTable, File, LineInfo, [Arg | Args], L);
list_var_eval([], _VarTable, _File, _LineInfo, [], _L) ->
	[];
list_var_eval([], _VarTable, _File, _LineInfo, Args, _L) ->
	lists:reverse(Args).

%元组变量求值
tuple_var_eval(Arg, VarTable, File, LineInfo, Args, L) ->
	list_to_tuple(list_var_eval(tuple_to_list(Arg), VarTable, File, LineInfo, Args, L)).

%值变量求值
v_var_eval(Arg, VarTable, File, LineInfo, L) when is_atom(Arg) ->
	[H | _] = atom_to_list(Arg),
	case z_lib:check_list_range([H], $A, $Z) of
		false ->
			Arg;
		_ ->
			%值是变量
			case lists:member(Arg, L) of
				false ->
					var_eval(VarTable, File, LineInfo, Arg, [Arg | L]);
				true ->
					%变量引用回路
					erlang:error({var_back_loop, File, LineInfo, Arg})
			end
	end;
v_var_eval(Arg, VarTable, File, LineInfo, L) when is_list(Arg) ->
	list_var_eval(Arg, VarTable, File, LineInfo, [], L);
v_var_eval(Arg, VarTable, File, LineInfo, L) when is_tuple(Arg) ->
	tuple_var_eval(Arg, VarTable, File, LineInfo, [], L);
v_var_eval(Arg, _VarTable, _File, _LineInfo, _L) ->
	Arg.

% 检查冲突
conflict(Ets, {_Info, Cfg} = E) ->
	case ets:lookup(Ets, Cfg) of
	[Old] ->
		{Old, E};
	[] ->
		ets:insert(Ets, E)
	end.

% 配置代码文件
code_path(Path, Dir) ->
	case z_lib:list_file(Path ++ [$/ | Dir], 1) of
	{ok, Files} ->
		[R || {File, _, regular, _, _} <- Files,
		(R = code_cfg(Path, Dir ++ [$/ | File])) =/= false];
	_ ->
		[]
	end.

code_cfg(Path, File) ->
	case filename:extension(File) of
	?CODE_FILE ->
		Name = Path ++ [$/ | File],
		{_, Size, _, MTime, _} = Info = z_lib:get_file_info(Name),
		{ok, Bin} = file:read_file(Name),
		Crc = erlang:crc32(Bin),
		Module = list_to_atom(filename:basename(File, ?CODE_FILE)),
		{{File, {{Size, MTime}, Crc}}, {[], zm_code, [Module, Info, Crc]}};
	_ ->
		false
	end.

% 配置资源文件
res_path(Path, Dir, MapDir) ->
	Filter = fun(_, FileName, _) ->
	lists:member(FileName, ?RES_DIR_SKIP) =/= true
	end,
	case z_lib:list_file(Path ++ [$/ | MapDir], ?DIR_DEPTH, Filter) of
	{ok, Files} ->
		[R || {File, _, regular, _, _} <- Files, (R = res_cfg
		(Path, Dir ++ [$/ | File], MapDir ++ [$/ | File])) =/= false];
	_ ->
		[]
	end.

res_cfg(Path, File, MapFile) ->
	Name = Path ++ [$/ | MapFile],
	case z_lib:get_file_info(Name) of
	{_, Size, _, MTime, _} = Info ->
		case file:read_file(Name) of
		{ok, Bin} ->
			Crc = erlang:crc32(Bin),
			{{MapFile, {{Size, MTime}, Crc}}, {[], zm_res_loader, [File, Info, Crc]}};
		_ ->
			false
		end;
	_ ->
		false
	end.


% 重置配置中的db_server的路径
reset_db_server_path1({Info,
	{Layer, ?START, {?DB_SERVER, Method, _}}}, Path) ->
	{Info, {Layer, ?START, {?DB_SERVER, Method, [Path]}}};
reset_db_server_path1(E, _Path) ->
	E.

% 执行do配置MA
execute_do(_Mapping, Info, {_, ?START, {M, F, A}} = MA, false) ->
	try apply(M, F, A) of
	{ok, Pid} ->
		link(Pid),
		ok;
	{error, {already_started, Pid}} ->
		link(Pid),
		ok;
	{error, R} ->
		{error, {R, {Info, MA}}};
	E ->
		{error, {E, {Info, MA}}}
	catch
	_Error:Why ->
		{error, {Why, {Info, MA}, erlang:get_stacktrace()}}
	end;
execute_do(_Mapping, Info, {_, ?START, _A} = MA, false) ->
	{error, {invalid_start, {Info, MA}}};
execute_do(_Mapping, _Info, {_, ?START, _A}, _SkipStart) ->
	ok;
execute_do(Mapping, Info, {_, M, A} = MA, _SkipStart) ->
	case sb_trees:get(M, Mapping, none) of
		{F, _} ->
			try
				apply(M, F, A),
				ok
			catch
			Error:Why ->
				{Error, {Why, {Info, MA}, erlang:get_stacktrace()}}
			end;
		none ->
			{error, {undef_module, {Info, MA}}}
	end.

% 执行undo配置MA
execute_undo(_Mapping, _Info, {_, ?START, _A}) ->
	ok;
execute_undo(Mapping, Info, {_, M, A} = MA) ->
	case sb_trees:get(M, Mapping, none) of
		{_, F} ->
			try
				apply(M, F, A),
				ok
			catch
			Error:Why ->
				{Error, {Why, {Info, MA}, erlang:get_stacktrace()}}
			end;
		none ->
			{error, {undef_module, {Info, MA}}}
	end.

% 按第一个出现的指定字符劈分字符串
first_char_split([Char | T], Char, L) ->
	{L, T};
first_char_split([H | T], Char, L) ->
	first_char_split(T, Char, [H | L]);
first_char_split([], _Char, L) ->
	{L, []}.

% 检查文件是否可以忽略
file_ignore(File) ->
	case lists:member($~, File) of
	true ->
		true;
	_ ->
		L = [E || E <- ?RES_DIR_SKIP, string:str(File, E) > 0],
		L =/= []
	end.

% 定义系统本地变量
def_sys_var(LocalVarTab, CfgFile, Project, SubProject) ->
	ets:insert(LocalVarTab, {{CfgFile, 'Project'}, {ok, list_to_atom(Project)}}),
	ets:insert(LocalVarTab, {{CfgFile, 'ProjectPath'}, {ok, Project}}),
	ets:insert(LocalVarTab, {{CfgFile, 'CurProject'}, {ok, list_to_atom(SubProject)}}),
	ets:insert(LocalVarTab, {{CfgFile, 'CurProjectPath'}, {ok, SubProject}}).
