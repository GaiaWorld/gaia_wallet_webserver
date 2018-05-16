%% @author lenovo
%% @doc js服务器


-module(z_js_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
%%根据操作系统获取文件监控模块
-define(GET_V8C_NAME(), case os:type() of
							{win32, _} ->
								"v8c";
							_ ->
								"libv8c"
						end).
-define(V8C_NAME, ?GET_V8C_NAME()).
-define(V8C_MOD, list_to_atom(?V8C_NAME)).

%%JS虚拟机表
-define(JS_VM_TABLE, js_vm_tab).
%%JS环境表
-define(JS_CONTEXT_TABLE, js_context_tab).

%%进程休眠超时时长
-define(HIBERNATE_TIMEOUT, 30000).


%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, info/0, spawn_link/0, batch_spawn_link/0]).

%%
%%启动js服务器
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%获取js服务器信息
%%
info() ->
	gen_server:call(?MODULE, get_info).

%%
%%创建一个js工作进程
%%
spawn_link() ->
	{_, enabled, Schedules}=bindings(),
	Scheduler=lists:nth(z_lib:random(1, length(Schedules)), Schedules), %随机选择一个调度器
	new_worker(Scheduler).

%%
%%创建每调度器一个工作进程
%%
batch_spawn_link() ->
	{_, enabled, Schedules}=bindings(),
	[begin
		 {ok, W}=new_worker(Scheduler),
		 W
	 end || Scheduler <- lists:seq(2, length(Schedules) + 1)].

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {platform, vm_tab, context_tab, workers, counter}).

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
init(ScheduleTimeout) ->
	erlang:process_flag(trap_exit, true), %注册为系统进程
    init(ScheduleTimeout, #state{}).


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
handle_call(get_info, _, State) ->
	{reply, get_info(State), State, ?HIBERNATE_TIMEOUT};
handle_call({spawn_link, Scheduler, VM, Root}, _, State) ->
	case z_js_worker:spawn_link(Scheduler, VM, Root) of
		{ok, Worker} = OK ->
			{reply, OK, set_info(Scheduler, VM, Worker, State), ?HIBERNATE_TIMEOUT};
		ignore ->
			{reply, {error, ignore}, State, ?HIBERNATE_TIMEOUT};
		E ->
			{reply, E, State, ?HIBERNATE_TIMEOUT}
	end;
handle_call(_, _, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


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
handle_cast(_, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


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
handle_info({monitor, PidOrPort, long_schedule, Info}, #state{workers = Workers} = State) ->
	%长调度监控信息
	handle_scheduler_monitor(PidOrPort, Info, Workers),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({'EXIT', From, Reason}, State) ->
	%成功关闭长调度进程
	{noreply, handle_exit(From, Reason, State), ?HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, State) ->
    error_logger:error_msg("======>~p stop~n Pid:~p~n Reason:~p~n State:~p~n", [?MODULE, self(), Reason, State]).


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

%%获取当前调度器的绑定类型和状态
bindings() ->
	{erlang:system_info(scheduler_bind_type), erlang:system_info(multi_scheduling), 
	 lists:seq(2, length(tuple_to_list(erlang:system_info(scheduler_bindings))))}.

%%获取当前进程所在调度器
which() ->
	erlang:system_info(scheduler_id).

%%初始化js服务器
init(ScheduleTimeout, State) ->
	%将js服务器进程绑定到1号调度器
	case which() of
		1 ->
			continue;
		_ ->
			erlang:process_flag(scheduler, 1)
	end,
	case
		case lib_npc:start_link([]) of
			{ok, _} ->
				ok;
			{error,{already_started, _}} ->
				ok;
			E ->
				E
		end
	of
		ok ->
			lib_npc:register(?V8C_NAME, "int64", "v8c_init", "char#", sync),
			lib_npc:register(?V8C_NAME, "int64", "v8c_new_vm", "", sync),
			lib_npc:register(?V8C_NAME, "int64", "v8c_new_context", "int64, int64", sync),
			lib_npc:register(?V8C_NAME, "int64", "v8c_new_context_from_file", "int64, char#", sync),
			lib_npc:register(?V8C_NAME, "char#;65535", "v8c_get", "int64, int64, char#, char#", sync),
			lib_npc:register(?V8C_NAME, "char#;65535", "v8c_chain_get", "int64, int64, char#", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_eval", "int64, int64, int64, char#", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_call", "int64, int64, int64, char#, char#, char#", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_chain_call", "int64, int64, int64, char#, char#", sync),
			lib_npc:register(?V8C_NAME, "char#;65535", "v8c_hygienic_eval", "int64, int64, char#", sync),
			lib_npc:register(?V8C_NAME, "char#;65535", "v8c_hygienic_call", "int64, int64, char#, char#, char#", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_destroy_context", "int64", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_destroy_vm", "int64", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_destroy", "int64", sync),
			lib_npc:register(?V8C_NAME, "int64", "v8c_new_handle", "int64, int64", sync),
			lib_npc:register(?V8C_NAME, "", "v8c_bind_process", "int64", asyn),
			lib_npc:register(?V8C_NAME, "", "v8c_startup_inspector", "int64, int64", sync),
			case (?V8C_MOD):v8c_init(?V8C_NAME) of
				{ok, V8} ->
					{_, enabled, Schedules}=bindings(),
					VMTab=ets:new(?JS_VM_TABLE, [protected, named_table]),
					VML=init_vm(Schedules, VMTab, []),
					ContextTab=ets:new(?JS_CONTEXT_TABLE, [ordered_set, protected, named_table]),
					init_root_context(VML, ContextTab),
					erlang:system_monitor(self(), [{long_schedule, ScheduleTimeout}]), %设置调度超时时长
		    		{ok, State#state{platform = V8, vm_tab = VMTab, context_tab = ContextTab, workers = maps:new(), counter = maps:new()}};
				Reason1 ->
					{stop, Reason1}
			end;
		Reason ->
			{stop, Reason}
	end.

%%初始化js虚拟机
init_vm([Scheduler|T], Tab, VML) ->
	{ok, VM}=(?V8C_MOD):v8c_new_vm(), 
	ets:insert_new(Tab, {Scheduler, VM}),
	init_vm(T, Tab, [{Scheduler, VM}|VML]);
init_vm([], _, VML) ->
	VML.

%%初始化根环境
init_root_context([{_, VM}|T], Tab) ->
	{ok, Root}=(?V8C_MOD):v8c_new_context(VM, 0),
	ets:insert_new(Tab, {{VM, 0}, Root}),
	init_root_context(T, Tab);
init_root_context([], _) ->
	ok.

%%创建绑定指定调度器的工作进程
new_worker(Scheduler) ->
	case ets:lookup(?JS_VM_TABLE, Scheduler) of
		[{_, VM}] ->
			%获取指定调度器的js虚拟机成功
			case ets:lookup(?JS_CONTEXT_TABLE, {VM, 0}) of
				[{_, Root}] ->
					%获取指定js虚拟机对应的根环境成功
					gen_server:call(?MODULE, {spawn_link, Scheduler, VM, Root});
				_ ->
					{error, invalid_context}
			end;
		_ ->
			{error, invalid_vm}
	end.

%%获取统计信息
get_info(#state{workers = Workers, counter = Counter}) ->
	{_, _, Schedules}=bindings(),
	[{schedules, Schedules}, {size, maps:size(Workers)}] ++ maps:to_list(Counter).

%%设置统计信息
set_info(Scheduler, VM, Worker, #state{workers = Workers, counter = Counter} = State) ->
	case maps:get(Scheduler, Counter, 0) of
		0 ->
			State#state{workers = maps:put(Worker, {Scheduler, VM}, Workers), counter = maps:put(Scheduler, 1, Counter)};
		Count ->
			State#state{workers = maps:put(Worker, {Scheduler, VM}, Workers), counter = maps:update(Scheduler, Count + 1, Counter)}
	end.

%%处理调度器监控信息
handle_scheduler_monitor(Pid, Info, Workers) when is_pid(Pid) ->
	case maps:get(Pid, Workers, nil) of
		nil ->
			%不是js进程
			ignore;
		_ ->
			Module=?V8C_MOD,
			case z_lib:get_values(Info, [{timeout, 0}, {in, nil}, {out, nil}]) of
				[Timeout, {Module, _, _}, {Module, _, _}] ->
					handle_scheduler_monitor1(Pid, Timeout);
				[Timeout, {Module, _, _}, _] ->
					handle_scheduler_monitor1(Pid, Timeout);
				_ ->
					ignore
			end
	end;
handle_scheduler_monitor(Port, _, _) when is_port(Port) ->
	ignore.

handle_scheduler_monitor1(Pid, Timeout) ->
	Status=element(2, erlang:process_info(Pid, status)),
	QueueLen=element(2, erlang:process_info(Pid, message_queue_len)),
	WordSize=erlang:system_info(wordsize),
	MemInfo=[
		{stack_size, element(2, erlang:process_info(Pid, stack_size)) * WordSize},
		{heap_size, element(2, erlang:process_info(Pid, heap_size)) * WordSize},
		{max_heap_size, maps:get(size, element(2, lists:keyfind(max_heap_size, 1, element(2, erlang:process_info(self(), garbage_collection))))) * WordSize},
		{total_size, element(2, erlang:process_info(Pid, memory))}
	],
	Stacktrace=[erlang:process_info(Pid, initial_call), 
				{stacktrace, element(2, erlang:process_info(Pid, current_stacktrace))}],
	erlang:exit(Pid, kill), %强制关闭此进程
	error_logger:error_msg("======>js worker run too long~n Pid:~p~n Timeout:~p~n Status:~p~n Queue:~p~n Mem:~p~n Stacktrace:~p~n", 
						   [Pid, Timeout, Status, QueueLen, MemInfo, Stacktrace]).

%%处理js进程退出消息
handle_exit(Pid, Reason, #state{workers = Workers, counter = Counter} = State) ->
	case maps:get(Pid, Workers, nil) of
		nil ->
			State;
		{Scheduler, VM} ->
			error_logger:error_msg("======>js worker exit~n Pid:~p~n Scheduler:~p~n VM:~p~n Reason:~p~n", [Pid, Scheduler, VM, Reason]),
			case maps:get(Scheduler, Counter, 0) of
				0 ->
					State#state{workers = maps:remove(Pid, Workers)};
				Count ->
					State#state{workers = maps:remove(Pid, Workers), counter = maps:update(Scheduler, Count - 1, Counter)}
			end
	end.
	