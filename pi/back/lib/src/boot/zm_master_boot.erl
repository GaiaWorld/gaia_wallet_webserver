%%% 主节点启动监控应用


-module(zm_master_boot).

-description("master boot supervisor application").
-vsn(1).

%%%=======================EXPORT=======================
-export([boot/2]).

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
%% Func: boot/2
%% Description: 启动指定FL_MA列表的启动应用
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
boot(PluginPath, Parent) when is_atom(Parent) ->
	ok = application:load(
		{application,
			?MODULE,
			[
				{description, atom_to_list(?MODULE)},
				{vsn, ""},
				{modules, [?MODULE, zm_master]},
				{registered, [zm_master]},
				{applications, [kernel, stdlib]},
				{mod, {?MODULE, {PluginPath, Parent}}}
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
%% Function: start_phase/1
%% Description: start_phase
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
start_phase(_Phase, _StartType, _PhaseArgs) ->
	ok.

%% -----------------------------------------------------------------
%% Function: prep_stop/1
%% Description: prep_stop
%% Returns: NewState
%% -----------------------------------------------------------------
prep_stop(State) ->
	State.

%% -----------------------------------------------------------------
%% Function: stop/1
%% Description: stop
%% Returns: ok
%% -----------------------------------------------------------------
stop(_Args) ->
	ok.

%% -----------------------------------------------------------------
%% Function: config_change/3
%% Description: config_change
%% Returns: ok
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
init({PluginPath, Parent}) ->
	{ok, {
		{one_for_one, 3, 10}, %重启策略和最大重启频率，10秒3次。
		[{
			zm_master, % Id 是督程内部用于标识子进程规范的名称。
			{zm_master, start_link, [PluginPath, Parent]}, % StartFunc 定义用于启动子进程的模块调用。
			permanent, % Restart 定义一个被终止的子进程要在何时被重启。
			brutal_kill, % Shutdown 定义一个子进程应如何被终止。
			worker, % Type 指定子进程是督程还是佣程。
			[zm_master] % Modules 应该为只有一个元素的列表 [Module]，其中 Module 是回调模块的名称。
		}]
	}}.
