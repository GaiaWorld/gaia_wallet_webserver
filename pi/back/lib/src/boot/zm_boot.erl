%%@doc 启动监控应用
%%@end


-module(zm_boot).

-description("boot supervisor application").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([boot/2, boot/3]).

%% -----------------------------------------------------------------
%% application callbacks
%% -----------------------------------------------------------------
-behaviour(application).
-export([start/2, start_phase/3, prep_stop/1, stop/1, config_change/3]).

%% -----------------------------------------------------------------
%% supervisor callbacks
%% -----------------------------------------------------------------
-behaviour(supervisor).
-export([init/1]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc  启动指定模块和参数的启动应用，默认监控重启频率为10秒3次
%% @spec  boot(Mod::atom(), PluginPath::list()) -> return()
%% where
%%  return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
boot(Mod, PluginPath) when is_atom(Mod), is_list(PluginPath) ->
	boot(Mod, PluginPath, {3, 10}).

%% -----------------------------------------------------------------
%%@doc  启动指定模块和参数及监控重启频率的启动应用
%% @spec  boot(Mod::atom(), PluginPath::list(), Frequency::{Count::integer(), Time::integer()}) -> return()
%% where
%%  return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
boot(Mod, PluginPath, {Count, Time} = Frequency)
	when is_atom(Mod), is_list(PluginPath),
	is_integer(Count), Count > 1, is_integer(Time), Time > 1 ->
	ok = application:load(
		{application,
			?MODULE,
			[
				{description, atom_to_list(Mod)},
				{vsn, "1.00"},
				{modules, [?MODULE, Mod]},
				{registered, [Mod]},
				{applications, [kernel, stdlib]},
				{mod, {?MODULE, {Mod, PluginPath, Frequency}}}
			]
		}
	),
	% 以持久应用的方式启动，如果终止了，则运行时系统会被终止
	application:start(?MODULE, permanent).

%% -----------------------------------------------------------------
%% Starts a one_for_all supervisor application
%%   start(Type, Args) -> return()
%% where
%%  return() =  {ok, Pid, State} | Error
%% -----------------------------------------------------------------
start(_Type, Args) ->
	supervisor:start_link(?MODULE, Args).

%% -----------------------------------------------------------------
%% start_phase
%%   start_phase(Phase, StartType, PhaseArgs) -> return()
%% where
%%  return() =  ok | {error, Reason}
%% -----------------------------------------------------------------
start_phase(_Phase, _StartType, _PhaseArgs) ->
	ok.

%% -----------------------------------------------------------------
%% prep_stop
%%   prep_stop() -> return()
%% where
%%  return() =  NewState
%% -----------------------------------------------------------------
prep_stop(State) ->
	State.

%% -----------------------------------------------------------------
%% stop
%%   stop() -> return()
%% where
%%  return() =  ok
%% -----------------------------------------------------------------
stop(_Args) ->
	ok.

%% -----------------------------------------------------------------
%% config_change
%%   config_change() -> return()
%% where
%%  return() =  ok
%% -----------------------------------------------------------------
config_change(_Changed, _New, _Removed) ->
	ok.

%% -----------------------------------------------------------------
%% Func: init/1
%% Description: init
%% Returns: {ok,
%%		{SupFlags, [ChildSpec]}} |
%%		ignore |
%%		{error, Reason}
%% -----------------------------------------------------------------
init({Mod, PluginPath, {Count, Time}}) ->
	{ok, {
		{one_for_one, Count, Time}, %重启策略和最大重启频率。
		[{
			Mod, % Id 是督程内部用于标识子进程规范的名称。
			{Mod, start_link, [PluginPath]}, % StartFunc 定义用于启动子进程的模块调用。
			permanent, % Restart 定义一个被终止的子进程要在何时被重启。
			brutal_kill, % Shutdown 定义一个子进程应如何被终止。
			worker, % Type 指定子进程是督程还是佣程。
			[Mod] % Modules 应该为只有一个元素的列表 [Module]，其中 Module 是回调模块的名称。
		}]
	}}.
