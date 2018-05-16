%%%@doc main 主启动文件
%%@end
-module(main).

%%%=======================DEFINE=======================
-define(USER_CONFIG, "user.config").

-define(ENV_PARENT, "ERL_PARENT").
-define(PLUGIN_START_MOD_ENV, "ERL_PLUGIN_START_MOD").
-define(RELEASE_START_MOD, "release").
-define(DEBUG_START_MOD, "debug").

%%%=======================EXPORT=======================
-export([startup/4, startup/3, startup/2, startup/1]).


%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 启动函数
%%```
%% P 父
%% Args 项目路径
%% Start_Type 启动类型
%% User_Script 用户脚本
%%'''
%% @spec startup(P::list(), Args::list(), Start_Type::atom(), User_Script::list()) -> return()
%% where
%% return() =  any()
%%@end
%% -----------------------------------------------------------------
startup(P, Args, Start_Type, User_Script) when is_atom(Start_Type), is_list(User_Script) ->
	show_start_info(),
	%%load main application
	Reader=case Start_Type of
		center ->
			case zm_center_boot:boot(Args) of
				ok ->
					Main=whereis(zm_center),
					show_tail_info(),
					zm_log:info('$main', main_boot, ?MODULE, "startup/2, load main application ok", [{main, Main}, {apps, application:loaded_applications()}]);
				Error ->
					throw(Error)
			end;
		master ->
			Parent=if
				is_list(P) ->
					list_to_atom(P);
				is_atom(P) ->
					P;
				true ->
					throw({error, invalid_parent})
			end,
			case zm_master_boot:boot(Args, Parent) of
				ok ->
					Main=whereis(zm_master),
					show_tail_info(),
					zm_log:info('$main', main_boot, ?MODULE, "startup/2, load main application ok", [{main, Main}, {apps, application:loaded_applications()}]),
					{zm_master, []};
				Error ->
					throw(Error)
			end;
		simple_cluster ->
			case zm_boot:boot(zm_sc, Args) of
				ok ->
					Main=whereis(zm_sc),
					show_tail_info(),
					zm_log:info('$main', main_boot, ?MODULE, "startup/2, load main application ok", [{main, Main}, {apps, application:loaded_applications()}]);
				Error ->
					throw(Error)
			end;
		standalone ->
			case zm_boot:boot(zm_dev, Args) of
				ok ->
					Main=whereis(zm_dev),
					show_tail_info(),
					zm_log:info('$main', main_boot, ?MODULE, "startup/2, load main application ok", [{main, Main}, {apps, application:loaded_applications()}]),
					{zm_dev, []};
				Error ->
					throw(Error)
			end;
		production ->
			case zm_boot:boot(zm_pro, Args) of
				ok ->
					Main=whereis(zm_pro),
					show_tail_info(),
					zm_log:info('$main', main_boot, ?MODULE, "startup/2, load main application ok", [{main, Main}, {apps, application:loaded_applications()}]),
					{zm_pro, []};
				Error ->
					throw(Error)
			end
	end,
	%%start project manager
	case os:getenv(?PLUGIN_START_MOD_ENV) of
		?DEBUG_START_MOD ->
			case debug_project_manager:start_link(Args, {Start_Type, Reader}) of
				{ok, Pid} ->
					io:format("=== Debugging ===~n"),
					zm_log:info('$main', main_boot, ?MODULE, "startup/2, start project manager ok", [{pid, Pid}]);
				ignore ->
					zm_log:warn('$main', main_boot, ?MODULE, "startup/2, start project manager failed", [{reason, ignore}]);
				{error, Reason} ->
					zm_log:warn('$main', main_boot, ?MODULE, "startup/2, start project manager failed", [{reason, Reason}])
			end;
		_ ->
			ignore
	end,
	%%eval config after load
	case file:eval(User_Script) of
		{error,enoent} ->
			conntinue;
		{error, Reason1} ->
			throw({error, Reason1, erlang:get_stacktrace()});
		_ ->
			zm_log:info('$main', main_boot, ?MODULE, "startup/2, user script eval ok", [{file, User_Script}])
	end.

%% -----------------------------------------------------------------
%%@doc 启动函数
%%```
%% P 父
%% Args 项目路径
%% Start_Type 启动类型
%%'''
%% @spec startup(P::list(), Args::list(), Start_Type::atom()) -> return()
%% where
%% return() =  any()
%%@end
%% -----------------------------------------------------------------
startup(Parent, Args, Start_Type) ->
	startup(Parent, Args, Start_Type, ?USER_CONFIG).

%% -----------------------------------------------------------------
%%@doc 启动函数
%%```
%% Args 项目路径
%% Start_Type 启动类型
%%'''
%% @spec startup(Args::list(), Start_Type::atom()) -> return()
%% where
%% return() =  any()
%%@end
%% -----------------------------------------------------------------
startup(Args, Start_Type) ->
	case os:getenv(?ENV_PARENT) of
		false ->
			throw({error, invalid_parent});
		Parent ->
			startup(Parent, Args, Start_Type, ?USER_CONFIG)
	end.

%% -----------------------------------------------------------------
%%@doc 启动函数
%%```
%% Args 项目路径
%%'''
%% @spec startup(Args::list()) -> return()
%% where
%% return() =  any()
%%@end
%% -----------------------------------------------------------------
startup(Args) ->
	startup(Args, standalone).

%%%=================LOCAL FUNCTIONS=================
	
%%print startup info
show_start_info() ->
	io:format("=== System Info ===~n"),
	OS_Type=os:type(),
	io:format("> OS: ~p~n", [OS_Type]),
	io:format("> Version: ~p~n", [os:version()]),
	case OS_Type of
		{unix, _} ->
			case public_lib:exec_local_cmd("./shell/get_sys_info.sh") of
				{error, _} ->
					conntinue;
				Reply when is_list(Reply) ->
					io:format("> ~p~n", [z_lib:char_filter(Reply, "\n")]);
				_ ->
					conntinue
			end;
		{win32, _} ->
			case public_lib:exec_local_cmd("./shell/get_sys_info.bat") of
				{error, _} ->
					conntinue;
				Reply when is_list(Reply) ->
					io:format("> ~p~n", [z_lib:char_filter(Reply, "\r\n")]);
				_ ->
					conntinue
			end;
		_ ->
			io:format("> Unknow OS")
	end,
	{ok, NetInfo}=inet:getifaddrs(),
	io:format("> Net: ~p~n", [NetInfo]),
	io:format("> Host: ~p~n~n", [net_adm:localhost()]),
	io:format("=== Erts Info ===~n"),
	io:format("> Erl Home: ~p~n", [code:root_dir()]),
	{ok, PWD}=file:get_cwd(),
	io:format("> PWD: ~p~n", [PWD]),
	io:format("> Lib Path: ~p~n~n", [os:getenv("ERL_LIBS")]).

show_tail_info() ->
	io:format("=== Running ===~n"),
	io:format("> Already Load App: ~p~n~n", [application:loaded_applications()]).
	