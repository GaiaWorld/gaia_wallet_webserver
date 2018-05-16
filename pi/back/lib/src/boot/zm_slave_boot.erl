%%% 从节点启动监控应用


-module(zm_slave_boot).

-description("slave boot supervisor application").
-vsn(1).

%%%=======================EXPORT=======================
-export([boot/4, boot/5]).

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
%% Func: boot/3
%% Description: 启动指定Mapping和指定FL_MA列表的启动应用，默认监控重启频率为10秒3次
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
boot(Mapping, FL_MA_List, Inits, ProjectPathL) when is_tuple(Mapping), is_list(FL_MA_List), is_list(Inits), is_list(ProjectPathL) ->
	boot(Mapping, FL_MA_List, Inits, ProjectPathL, {3, 10}).

%% -----------------------------------------------------------------
%% Func: boot/3
%% Description: 启动指定Mod和指定FL_MA列表的启动应用及监控重启频率的启动应用
%% Returns: ok | {error, Reason}
%% -----------------------------------------------------------------
boot(Mapping, FL_MA_List, Inits, ProjectPathL, {Count, Time} = Frequency)
	when is_tuple(Mapping), is_list(FL_MA_List), is_list(Inits),
	is_integer(Count), Count > 1, is_integer(Time), Time > 1 ->
	Mod = zm_slave,
	case application:load(
		{application,
			?MODULE,
			[
				{description, atom_to_list(Mod)},
				{vsn, "1.00"},
				{modules, [?MODULE, Mod]},
				{registered, [Mod]},
				{applications, [kernel, stdlib]},
				{mod, {?MODULE, {Mapping, FL_MA_List, Inits, ProjectPathL,
					Frequency, node(group_leader())}}}
			]
		}
	) of
		ok ->
			% 以持久应用的方式启动，如果终止了，则运行时系统会被终止
			application:start(?MODULE, permanent);
		{error, _} = E ->
			E
	end.

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
init({Mapping, FL_MA_List, Inits, ProjectPathL, {Count, Time}, Node}) ->
	Mod = zm_slave,
	{ok, {
		{one_for_one, Count, Time}, %重启策略和最大重启频率。
		[{
			Mod, % Id 是督程内部用于标识子进程规范的名称。
			{Mod, start_link, [Mapping, FL_MA_List, Inits, ProjectPathL, Node]}, % StartFunc 定义用于启动子进程的模块调用。
			permanent, % Restart 定义一个被终止的子进程要在何时被重启。
			brutal_kill, % Shutdown 定义一个子进程应如何被终止。
			worker, % Type 指定子进程是督程还是佣程。
			[Mod] % Modules 应该为只有一个元素的列表 [Module]，其中 Module 是回调模块的名称。
		}]
	}}.
