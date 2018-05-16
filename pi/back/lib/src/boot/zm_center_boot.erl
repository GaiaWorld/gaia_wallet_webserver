%% Author: Administrator
%% Created: 2012-4-12
%% Description: TODO: Add description to zm_center_boot
-module(zm_center_boot).
-behaviour(application).
-behaviour(supervisor).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([boot/1]).
-export([start/2, stop/1]).
-export([init/1]).

%%
%% API Functions
%%

%% -----------------------------------------------------------------
%% Func: boot/3
%% Description: 启动指定FL_MA列表的启动应用
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
boot(PluginPath) when is_list(PluginPath) ->
	ok = application:load(
		{application,
			?MODULE,
			[
				{description, atom_to_list(?MODULE)},
				{vsn, ""},
				{modules, [?MODULE, zm_center]},
				{registered, [zm_center]},
				{applications, [kernel, stdlib]},
				{mod, {?MODULE, PluginPath}}
			]
		}
	),
	% 以持久应用的方式启动，如果终止了，则运行时系统会被终止
	application:start(?MODULE, permanent).

%% -----------------------------------------------------------------
%% Function: start/2
%% Description: Starts a one_for_all supervisor application.
%% Returns: {ok, Pid, State} | Error
%% -----------------------------------------------------------------
start(_Type, Args) ->
	supervisor:start_link(?MODULE, Args).

%% -----------------------------------------------------------------
%% Function: stop/1
%% Description: stop
%% Returns: ok
%% -----------------------------------------------------------------
stop(_Args) ->
	ok.

%% -----------------------------------------------------------------
%% Func: init/1
%% Description: init
%% Returns: {ok,
%%		{SupFlags, [ChildSpec]}} |
%%		ignore |
%%		{error, Reason}
%% -----------------------------------------------------------------
init(PluginPath) ->
	{ok, {
		{one_for_one, 3, 10}, %重启策略和最大重启频率，10秒3次。
		[{
			zm_center, % Id 是督程内部用于标识子进程规范的名称。
			{zm_center, start_link, [PluginPath]}, % StartFunc 定义用于启动子进程的模块调用。
			permanent, % Restart 定义一个被终止的子进程要在何时被重启。
			brutal_kill, % Shutdown 定义一个子进程应如何被终止。
			worker, % Type 指定子进程是督程还是佣程。
			[zm_center] % Modules 应该为只有一个元素的列表 [Module]，其中 Module 是回调模块的名称。
		}]
	}}.

%%
%% Local Functions
%%

