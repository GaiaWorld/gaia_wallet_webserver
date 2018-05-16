%% 
%% @doc 节点监控服务
%%


-module(nmon_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-record(state, {sub}).
-define(NMON_HIBERNATE_TIMEOUT, 3000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, sub_list/0, sub/10, count/1, pub/3, architecture_info/0, cpu_utilization/0, 
		 memory/1, memory_usage/1, memory_info/1, alloters_info/0, alloter_info/1, gcs/1, minor_gcs/2, bin_leak_info/2,
		 app_info/1, apps_info/0, apps/0, process_info/3, process_info_str/3, processes/1, port_info/2, port_info_str/2, ports/1, ports/0, 
		 sockets/3, socket_ports/2, sockets_size/3, sockets_size/1, sockets_info/4, sockets_info/2, sockets_io_info/2, 
		 ets/2, source_info/1, 
		 gc/0, send/2, exit_process/2]).

%%
%%启动节点监控服务
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%订阅清单
%%
sub_list() ->
	element(1, lists:unzip(element(2, lists:keyfind(exports, 1, nmon_event:module_info())))) -- [module_info, module_info].

%%
%%订阅
%%
sub(Args, Type, Attr, Node, Count, MF, Recount, Interval, Time, Timeout) ->
	gen_server:call(?MODULE, {sub, Args, Type, Attr, Node, Count, MF, Recount, Interval, Time, Timeout}).

%%
%%订阅计数
%%
count({_, _, _} = Key) ->
	gen_server:cast(?MODULE, {count, Key}).

%%
%%退订
%%
pub(Src, Node, Type) ->
	gen_server:call(?MODULE, {pub, Src, Node, Type}).

%%
%%获取系统体系结构数据
%%
architecture_info() ->
	get_rt_info(get_sys_info([])).

%%
%%获取节点cpu占用
%%
cpu_utilization() ->
	cpu_utilization(erlang:statistics(scheduler_wall_time)).

%%
%%内存使用信息
%%
memory(Key) ->
	[{Type, recon_alloc:memory(Type, Key)} || Type <- [allocated, used, unused]].

%%
%%内存使用率
%%
memory_usage(Key) ->
	[{mem, recon_alloc:memory(usage, Key)}, {alloc, recon_alloc:memory(allocated_instances, Key)}].

%%
%%整合的内存信息
%%
memory_info(Key) ->
	[
	 {instances, begin
					 case erlang:system_info(multi_scheduling) of
						 enabled ->
							 erlang:system_info(schedulers_online) + 1;
						 _ ->
							 2
					 end
				 end},
	 {memory, memory(Key)},
	 {alloter, alloters_info()}
	].

%%
%%获取内存分配器简单统计信息
%%
alloters_info() ->
    try 
		alloc_info(erlang:system_info({allocator_sizes, erlang:system_info(alloc_util_allocators)}), [], 0, 0, true)
    catch 
		_:_ -> 
			[]
    end.

%%
%%获取指定分配器的详细信息
%%
alloter_info(Allocators) when is_list(Allocators) ->
	[{{A, N}, lists:sort(proplists:delete(version, proplists:delete(versions, Props)))} ||
        A <- Allocators,
        Allocs <- [erlang:system_info({allocator, A})],
        Allocs =/= false,
        {_, N, Props} <- Allocs];
alloter_info(Allocator) ->
	alloter_info([Allocator]).	

%%
%%获取指定时间内完全gc的次数和gc后回收的内存大小
%%
gcs(Interval) when is_integer(Interval), Interval >= 0 ->
	{OldGCS, OldWords, _}=erlang:statistics(garbage_collection),
	timer:sleep(Interval),
	{GCS, Words, _}=erlang:statistics(garbage_collection),
	[{gc, GCS - OldGCS}, {free, (Words - OldWords) * erlang:system_info(wordsize)}].

%%
%%获取指定指间内进程小gc的次数
%%
minor_gcs(Pid, Interval) when is_pid(Pid), is_integer(Interval), Interval >= 0 ->
	OldGCS=z_lib:get_value(element(2, hd(process_info(Pid, garbage_collection, 200))), minor_gcs, 0),
	timer:sleep(Interval),
	GCS=z_lib:get_value(element(2, hd(process_info(Pid, garbage_collection, 200))), minor_gcs, 0),
	GCS - OldGCS.

%%
%%获取当前二进制泄漏信息
%%
bin_leak_info(0, true) ->
	bin_leak_info(recon:bin_leak(length(erlang:processes())));
bin_leak_info(Count, true) ->
	bin_leak_info(recon:bin_leak(Count));
bin_leak_info(0, false) ->
	bin_leak_info(lists:reverse(recon:bin_leak(length(erlang:processes()))));
bin_leak_info(Count, false) ->
	bin_leak_info(lists:reverse(recon:bin_leak(Count))).
%%
%%获取指定应用的详细数据
%%
app_info(App) ->
	Master = application_controller:get_master(App),
	Leader = element(2, erlang:process_info(Master, group_leader)),
	if
		is_pid(Master) andalso is_pid(Leader) ->
			{App, process_tree(Master, Leader, [Master, Leader])};
		true ->
			[]
	end.

%%
%%获取所有的详细数据
%%
apps_info() ->
	[{App, process_tree(Master, Leader, [Master, Leader])} || {App, _, _} <- application_controller:which_applications(), 
													is_pid(Master = application_controller:get_master(App)), is_pid(Leader = element(2, erlang:process_info(Master, group_leader)))].

%%获取所有的应用
apps() ->
	[{App, Vsn, application_controller:get_master(App)} || {App, _, Vsn} <- application:which_applications()].

%%
%%获取进程详细数据
%%
process_info(Pid, Key, Timeout) when is_list(Pid) ->
	case Pid of
		[$<|_] ->
			process_info(erlang:list_to_pid(Pid), Key, Timeout);
		_ ->
			process_info(whereis(list_to_atom(Pid)), Key, Timeout)
	end;
process_info(Pid, all, Timeout) ->
	State=try  
		recon:get_state(Pid, Timeout)
	catch
		_:_ ->
			[]	
	end,
	[{pid, Pid}, {context, State} | recon:info(Pid)];
process_info(Pid, state, Timeout) ->
	recon:get_state(Pid, Timeout);
process_info(Pid, Key, _) ->
	[recon:info(Pid, Key)].

%%
%%获取进程详细数据的字符串表达形式
%%
process_info_str(Pid, Key, Timeout) ->
	filter_info([[pid], 
						 [context], 
						 [meta, group_leader], 
						 [meta, {kv, dictionary}], 
						 [signals, links], 
						 [signals, monitors], 
						 [signals, monitored_by], 
						 [location, {mfa, initial_call}], 
						 [location, current_stacktrace]], process_info(Pid, Key, Timeout)). 

%%
%%获取所有进程
%%
processes(all) ->
	[process_simple(Pid) || Pid <- erlang:processes()];
processes(Len) when Len >= 0 ->
	[process_simple(Pid) || Pid <- lists:sublist(erlang:processes(), Len)].

%%
%%获取指定port的详细数据
%%
port_info(Port, all) ->
	recon:port_info(Port);
port_info(Port, Keys) when is_list(Keys) ->
	[begin
		 try 
		 	recon:port_info(Port, Key)
		 catch
			 _:_ ->
				 {Key, []}
		 end
	 end || Key <- Keys];
port_info(Port, Key) ->
	[recon:port_info(Port, Key)].

%%
%%获取port详细数据的字符串表达形式
%%
port_info_str(Port, Key) ->
	filter_info([[signals, connected], 
				 [signals, links],
				 [signals, {kv, monitors}]], port_info(Port, Key)).

%%
%%获取指定类型的所有port
%%
ports(file) ->
	recon:files();
ports(tcp) ->
	recon:tcp();
ports(udp) ->
	recon:udp();
ports(sctp) ->
	recon:sctp().

%%
%%获取所有类型的port
%%
ports() ->
	[{file, ports(file)}, {tcp, ports(tcp)}, {udp, ports(udp)}, {sctp, ports(sctp)}].

%%
%%获取指定Protocol, 端口和状态的socket列表
%%
sockets(Protocol, any, State) ->
	lists:foldl(fun({LocalPort, Port}, L) -> 
						case lists:keyfind(LocalPort, 1, L) of
							{_, Ports} ->
								lists:keyreplace(LocalPort, 1, L, {LocalPort, [Port|Ports]});
							false ->
								lists:keystore(LocalPort, 1, L, {LocalPort, [Port]})
						end
				end, [], 
				[{LocalPort, P} || P <- ports(Protocol), 
								   (LocalPort = element(2, element(2, prim_inet:sockname(P)))) >= 0, 
								   lists:member(State, element(2, prim_inet:getstatus(P)))]);
sockets(Protocol, LocalPort, State) ->
	lists:foldl(fun({LocalPort, Port}, L) -> 
						case lists:keyfind(LocalPort, 1, L) of
							{_, Ports} ->
								lists:keyreplace(LocalPort, 1, L, {LocalPort, [Port|Ports]});
							false ->
								lists:keystore(LocalPort, 1, L, {LocalPort, [Port]})
						end
				end, [], 
				[{LocalPort, P} || P <- ports(Protocol), 
							 LocalPort =:= element(2, element(2, prim_inet:sockname(P))), 
							 lists:member(State, element(2, prim_inet:getstatus(P)))]).

%%
%%获取指定Protocol的端口列表
%%
socket_ports(any, State) ->
	socket_ports(tcp, State) ++ socket_ports(udp, State) ++socket_ports(sctp, State);
socket_ports(Protocol, State) ->
	[{Protocol, [{LocalPort, [term_to_string(Port) || Port <- Ports]} || {LocalPort, Ports} <- sockets(Protocol, any, State)]}].

%%
%%获取socket的指定实例数量
%%
sockets_size(Protocol, LocalPort, State) ->
	length(lists:flatten(element(2, lists:unzip(sockets(Protocol, LocalPort, State))))).

%%
%%获取socket的实例数量
%%
sockets_size(State) ->
	sockets_size(tcp, 0, State) + sockets_size(udp, 0, State) + sockets_size(sctp, 0, State).

%%
%%获取socket的详细数据
%%
sockets_info(Protocol, Keys, 1, 16#ffffffff) ->
	sockets_info(ports(Protocol), Keys, Protocol);
sockets_info(Protocol, Keys, Start, Len) ->
	sockets_info(lists:sublist(ports(Protocol), Start, Len), Keys, Protocol).

%%
%%获取socket的所有详细数据
%%
sockets_info(Start, Len) ->
	sockets_info(lists:sublist(ports(tcp), Start, Len), all, tcp) ++ 
		sockets_info(lists:sublist(ports(udp), Start, Len), all, udp) ++ 
		sockets_info(lists:sublist(ports(sctp), Start, Len), all, sctp).

%%
%%获取指定端口的socket的连接数和流量
%%
sockets_io_info(Protocol, LocalPort) ->
	sockets_io_info(sockets(Protocol, LocalPort, connected), Protocol, []).

sockets_io_info([{LocalPort, Ports}|T], Protocol, L) ->
	sockets_io_info(T, Protocol, 
					[
					 {LocalPort, [
								 {connected_size, length(Ports)},
								 {in, lists:sum([socket_info(recv, P, Protocol) || P <- Ports])},
								 {out, lists:sum([socket_info(sent, P, Protocol) || P <- Ports])}
								]}
					|L]);
sockets_io_info([], _, L) ->
	L.

%%
%%获取ets
%%
ets(Opts, Len) when Len >= 0 ->
	HideUnread = proplists:get_value(unread_hidden, Opts, true),
    HideSys = proplists:get_value(sys_hidden, Opts, true),
    Fun = fun(_, {0, Acc}) ->
				  {0, Acc};
			 (Id, {Count, Acc}) ->
				  try
					  TabId = case ets:info(Id, named_table) of
								  true -> ignore;
								  false -> Id
							  end,
					  Name = ets:info(Id, name),
					  Protection = ets:info(Id, protection),
				      ignore(HideUnread andalso Protection == private, unreadable),
					  Pid=ets:info(Id, owner),
					  Owner=case Pid of
							  undefined ->
								  "";
							  Pid ->
								  erlang:pid_to_list(Pid)
					  end,
				      RegName = case catch process_info(Pid, registered_name) of
						    [] -> ignore;
						    {registered_name, ProcName} -> ProcName
					  end,
				      ignore(HideSys andalso ordsets:is_element(RegName, sys_ets()), system_tab),
				      ignore(HideSys andalso ordsets:is_element(Name, sys_ets()), system_tab),
				      ignore((RegName == mnesia_monitor)
					     andalso Name /= schema
					     andalso is_atom((catch mnesia:table_info(Name, where_to_read))), mnesia_tab),
				      Memory = ets:info(Id, memory) * erlang:system_info(wordsize),
				      Tab = [{name,Name},
					     {id,TabId},
					     {protection,Protection},
					     {owner,Owner},
					     {size,ets:info(Id, size)},
					     {reg_name,RegName},
					     {type,ets:info(Id, type)},
					     {keypos,ets:info(Id, keypos)},
					     {heir,ets:info(Id, heir)},
					     {memory,Memory},
					     {compressed,ets:info(Id, compressed)},
					     {fixed,ets:info(Id, fixed)}
					    ],
				      {Count - 1, [Tab|Acc]}
				  catch
					  _:_ ->
						{Count, Acc}
				  end
	end,
    {_, L}=lists:foldl(Fun, {Len, []}, ets:all()),
	L.

%%
%%查询指定模块的源码
%%
source_info(Mod) ->
	recon:source(Mod).

%%
%%主动垃圾回收
%%
gc() ->
    [erlang:garbage_collect(P) || P <- erlang:processes(),
                           {status, waiting} == erlang:process_info(P, status)],
    erlang:garbage_collect(),
    ok.

%%
%%向指定进程发送消息
%%
send(Pid, Msg) when is_list(Pid) ->
	erlang:list_to_pid(Pid) ! string_to_term(Msg),
	ok.

%%
%%中止指定进程
%%
exit_process(Pid, Reason) when is_list(Pid) ->
	exit(erlang:list_to_pid(Pid), string_to_term(Reason));
exit_process(Pid, Reason) when Pid =/= self() ->
	exit(Pid, Reason).

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
init(_) ->
	erlang:system_flag(scheduler_wall_time, true),
	case application:start(recon) of
		ok ->
    		{ok, #state{sub = ets:new(?MODULE, [ordered_set])}};
		{error, {already_started,recon}} ->
			{ok, #state{sub = ets:new(?MODULE, [ordered_set])}};
		{error, Reason} ->
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
handle_call({sub, Args, Type, Attr, Node, Count, MF, Recount, Interval, Time, Timeout}, _, #state{sub = Sub} = State) ->
	Src=z_lib:get_value(Args, user_name, ""),
	case sub(Sub, Count, {Args, Src, Type, Attr, Node, MF, Recount, Interval}) of
		true ->
			{reply, zm_dtimer:set({?MODULE, {node_monitor, Src, Node, Type}}, 
								  {nmon_event, Type, [Args, Src, Type, Attr, Node, MF, Recount, Interval]}, {time, Time, Timeout}), State, ?NMON_HIBERNATE_TIMEOUT};
		E ->
			{reply, E, State, ?NMON_HIBERNATE_TIMEOUT}
	end;
handle_call({pub, Src, Node, Type}, _, #state{sub = Sub} = State) ->
	ets:delete(Sub, {Src, Node, Type}),
	{reply, pub1(Src, Node, Type), State, ?NMON_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {noreply, State, ?NMON_HIBERNATE_TIMEOUT}.


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
handle_cast({count, Key}, #state{sub = Sub} = State) ->
	count(Sub, Key),
	{noreply, State, ?NMON_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?NMON_HIBERNATE_TIMEOUT}.


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
handle_info(_, State) ->
    {noreply, State, ?NMON_HIBERNATE_TIMEOUT}.


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

sub(Table, Count, {_, Src, Type, _, Node, _, _, _} = Value) ->
	Key={Src, Node, Type},
	case ets:lookup(Table, Key) of
		[] ->
			ets:insert_new(Table, {Key, {Count, Value}});
		_ ->
			false
	end.

pub1(Src, Node, Type) ->
	ok =:= zm_dtimer:delete(?MODULE, {node_monitor, Src, Node, Type}).
		
count(Table, {Src, Node, Type} = Key) ->
	case ets:lookup_element(Table, Key, 2) of
		{1, _} ->
			ets:delete(Table, Key),
			pub1(Src, Node, Type);
		{0, _} ->
			true;
		{Count, Args} ->
			ets:update_element(Table, Key, {2, {erlang:abs(Count) - 1, Args}});
		_ ->
			pub1(Src, Node, Type)
	end.

get_sys_info(L) ->
	{A, B, C}=os:version(),
	[
	 {net, [
			{address, element(2, inet:getifaddrs())},
			{socket, [
					   {tcp, [Local || [{local, Local}, {state, State}] <- sockets_info(ports(tcp), [local, state], tcp), State =:= accepting]},
					   {udp, [Local || [{local, Local}, {state, State}] <- sockets_info(ports(udp), [local, state], udp), State =:= accepting]},
					   {sctp, [Local || [{local, Local}, {state, State}] <- sockets_info(ports(sctp), [local, state], sctp), State =:= accepting]}
					  ]}
	 ]},
	 {host, element(2, inet:gethostname())},
	 {kernel_version, lists:concat([A, ".", B, ".", C])},
	 {os, erlang:system_info(system_architecture)},
	 {cpu, [
			{smp_support, erlang:system_info(smp_support)},
			{count, erlang:system_info(logical_processors)},
			{available_count, erlang:system_info(logical_processors_available)},
			{online_count, erlang:system_info(logical_processors_online)}
	 ]}
	 |L].

get_rt_info(L) ->
	IO=erlang:system_info(check_io),
	Type=erlang:system_info(multi_scheduling),
	SchedulersOnline=erlang:system_info(schedulers_online),
	SchedulersAvailable = case Type of
							  enabled -> 
								  SchedulersOnline;
							  _ -> 
								  1
	end,
	Parent=case zm_node:layer(center) of
			   none ->
				   case zm_node:layer(master) of
					   none ->
						   none;
					   {_, _, {Master}, _, _} ->
						   Master
				   end;
			   {_, _, {Center}, _, _} ->
				   Center
	end,
	[
	 {time, [
			 {uptime, element(1, erlang:statistics(wall_clock))},
			 {sys, calendar:now_to_local_time(os:timestamp())},
			 {erl, calendar:now_to_local_time(now())}
	 ]},
	 {path, [ 
			 {erl_home, code:root_dir()},
			 {erl_pwd, element(2, file:get_cwd())},
			 {erl_lib, lists:concat([code:lib_dir(), ";", os:getenv("ERL_LIBS")])},
			 {db, os:getenv("ERL_DB_PATH")}
	 ]},
	 {node, [
			 {parent, Parent},
			 {self, node()},
			 {buddy, nodes() -- [Parent]} 
	 ]},
	 {limit, [
			  {process, erlang:system_info(process_limit)},
			  {port, erlang:system_info(port_limit)},
			  {ets, erlang:system_info(ets_limit)}	  
	 ]},
	 {io, [
		   lists:keyfind(kernel_poll, 1, IO),
		   lists:keyfind(max_fds, 1, IO)
	 ]},
	 {scheduler, [
		 {type, Type},
		 {bind_type, erlang:system_info(scheduler_bind_type)},
		 {count, erlang:system_info(schedulers)},
		 {available_count, SchedulersAvailable},
		 {online_count, SchedulersOnline}
	 ]},
	 {erts, [
			 {thread_pool_size, erlang:system_info(thread_pool_size)},
			 {thread_support, erlang:system_info(threads)},
			 {debug, erlang:system_info(debug_compiled)},
			 {process_wordsize, erlang:system_info(wordsize)},
			 {emulator_wordsize, erlang:system_info({wordsize, external})},
			 {dirver_version, erlang:system_info(driver_version)},
			 {erts_version, erlang:system_info(version)},
			 {system_version, erlang:system_info(otp_release)},
			 {build, erlang:system_info(build_type)}
	 ]}
	 |L].

cpu_utilization(undefined) ->
    0;
cpu_utilization(RTInfo) ->
    Sum = lists:foldl(fun({_, A, T},{AAcc, TAcc}) -> 
							  {A + AAcc, T + TAcc} 
					  end, {0, 0}, RTInfo),
    case Sum of
		{0, 0} ->
	    	0;
		{Active,Total} ->
	    	round(100 * Active / Total)
    end.

alloc_info([{Type, Instances}|Allocators], TypeAcc, TotalBS, TotalCS, IncludeTotal) ->
    {BS, CS, NewTotalBS, NewTotalCS, NewIncludeTotal}=sum_alloc_instances(Instances, 0, 0, TotalBS, TotalCS),
    alloc_info(Allocators, [{Type, BS, CS}|TypeAcc], NewTotalBS, NewTotalCS,
	       IncludeTotal andalso NewIncludeTotal);
alloc_info([], TypeAcc, TotalBS, TotalCS, IncludeTotal) ->
    Types = [X || X = {_, BS, CS} <- TypeAcc, (BS > 0 orelse CS > 0)],
    case IncludeTotal of
		true ->
		    [{total, TotalBS, TotalCS}|lists:reverse(Types)];
		false ->
		    lists:reverse(Types)
    end.

sum_alloc_instances(false, BS, CS, TotalBS, TotalCS) ->
    {BS, CS, TotalBS, TotalCS, false};
sum_alloc_instances([{_, _, Data}|Instances], BS, CS, TotalBS, TotalCS) ->
    {NewBS, NewCS, NewTotalBS, NewTotalCS} = sum_alloc_one_instance(Data, BS, CS, TotalBS, TotalCS),
    sum_alloc_instances(Instances, NewBS, NewCS, NewTotalBS, NewTotalCS);
sum_alloc_instances([], BS, CS, TotalBS, TotalCS) ->
    {BS, CS, TotalBS, TotalCS, true}.

sum_alloc_one_instance([{sbmbcs, [{blocks_size, BS, _ , _}, {carriers_size, CS, _, _}]}|Rest], OldBS, OldCS, TotalBS, TotalCS) ->
    sum_alloc_one_instance(Rest, OldBS + BS,OldCS + CS, TotalBS, TotalCS);
sum_alloc_one_instance([{_, [{blocks_size, BS, _, _}, {carriers_size, CS, _, _}]}|Rest], OldBS, OldCS, TotalBS, TotalCS) ->
    sum_alloc_one_instance(Rest, OldBS + BS, OldCS + CS, TotalBS + BS, TotalCS + CS);
sum_alloc_one_instance([{_, [{blocks_size, BS}, {carriers_size, CS}]}|Rest], OldBS, OldCS, TotalBS, TotalCS) ->
    sum_alloc_one_instance(Rest, OldBS + BS, OldCS + CS, TotalBS + BS, TotalCS + CS);
sum_alloc_one_instance([_|Rest], BS, CS, TotalBS, TotalCS) ->
    sum_alloc_one_instance(Rest, BS, CS, TotalBS, TotalCS);
sum_alloc_one_instance([], BS, CS, TotalBS, TotalCS) ->
    {BS, CS, TotalBS, TotalCS}.

bin_leak_info(Info) ->
	Fun=fun({Key, {M, F, A}}) ->
				{Key, lists:concat([M, ":", F, "/", A])};
		   (Name) ->
				{name, Name}
		end,
	[{term_to_string(Pid), [{leak, Leak} | [Fun(X) || X <- L]]} || {Pid, Leak, L} <- Info].

string_to_term(Str) ->
	{ok, Tokens, _EndLine} = erl_scan:string(lists:reverse([$.|lists:reverse(Str)])),
	{ok, Term} = erl_parse:parse_term(Tokens),
	Term.

term_to_string([]) ->
	[];
term_to_string(Pid) when is_pid(Pid) ->
	erlang:pid_to_list(Pid);
term_to_string(Port) when is_port(Port) ->
	erlang:port_to_list(Port);
term_to_string(Term) ->
	lists:flatten(io_lib:format("~p", [Term])).

filter_info([Key|T], Info) ->
	filter_info(T, filter_info(Info, Key, []));
filter_info([], Info) ->
	Info.

filter_info([{_, undefined} = KV|T], Keys, L) ->
	filter_info(T, Keys, [KV|L]);
filter_info([{Key, Value}|T], [{kv, Key}], L) ->
	if
		is_list(Value) ->
			filter_info(T, [], [{Key, [{term_to_string(K), term_to_string(V)} || {K, V} <- Value]}|L]);
		true ->
			filter_info(T, [], [{Key, term_to_string(Value)}|L])
	end;
filter_info([{Key, Value}|T], [{mfa, Key}], L) ->
	case Value of
		{M, F, A} ->
			filter_info(T, [], [{Key, lists:concat([M, ":", F, "/", A])}|L]);
		_ ->
			filter_info(T, [], [{Key, term_to_string(Value)}|L])
	end;
filter_info([{Key, Value}|T], [Key], L) ->
	if
		is_list(Value) ->
			filter_info(T, [], [{Key, [term_to_string(X) || X <- Value]}|L]);
		true ->
			filter_info(T, [], [{Key, term_to_string(Value)}|L])
	end;
filter_info([{Key, Value}|T], [Key|Next], L) ->
	filter_info(T, [], [{Key, filter_info(Value, Next, [])}|L]);
filter_info([KV|T], Keys, L) ->
	filter_info(T, Keys, [KV|L]);
filter_info([], _, L) ->
	lists:reverse(L).

process_tree(Pid, Leader, Filters) ->
	{_, Pids}=erlang:process_info(Pid, links),
	case erlang:process_info(Pid, registered_name) of
		{_, Name} ->
			{Name, process_childs(Pids -- Filters, Leader, Pid, Filters, [])};
		_ ->
			{erlang:pid_to_list(Pid), process_childs(Pids -- Filters, Leader, Pid, Filters, [])}
	end.
	
process_childs([Pid|T], Leader, Parent, Filters, Childs) when is_pid(Pid) ->
	case erlang:process_info(Pid, group_leader) of
		{_, Leader} ->
			process_childs(T, Leader, Parent, Filters, [process_tree(Pid, Leader, [Pid|Filters])|Childs]);
		_ ->
			process_childs(T, Leader, Parent, Filters, Childs)
	end;
process_childs([Port|T], Leader, Parent, Filters, Childs) when is_port(Port) ->
	process_childs(T, Leader, Parent, Filters, [{port, erlang:port_to_list(Port)}|Childs]);
process_childs([_|T], Leader, Parent, Filters, Childs) ->
	process_childs(T, Leader, Parent, Filters, Childs);
process_childs([], _, _, _, Childs) ->
	lists:reverse(Childs).

process_name(Pid) ->
	case erlang:process_info(Pid, registered_name) of
		{_, Name} ->
			Name;
		_ ->
			case erlang:process_info(Pid, initial_call) of
				{_, {M, F, A}} ->
					lists:concat([M, ":", F, "/", A]);
				_ ->
					undefined
			end
	 end.

process_simple(Pid) ->
	{
	 erlang:pid_to_list(Pid),
	 process_name(Pid),
	 case erlang:process_info(Pid, reductions) of
		 {_, Reds} ->
			 Reds;
		 _ ->
			 0
	 end,
	 case erlang:process_info(Pid, memory) of
		 {_, Memory} ->
			 Memory;
		 _ ->
			 0
	 end,
	 case erlang:process_info(Pid, message_queue_len) of
		 {_, MsgQ} ->
			 MsgQ;
		 _ ->
			 0
	 end,
	 case erlang:process_info(Pid, current_function) of
		 {_, {M, F, A}} ->
			 lists:concat([M, ":", F, "/", A]);
		 _ ->
			 undefined
	 end
	}.
	
ignore(true, Reason) -> 
	throw(Reason);
ignore(_,_ ) -> 
	ok.

sys_ets() ->
    [auth, code_server, global_name_server, inet_db,
     mnesia_recover, net_kernel, timer_server, wxe_master].
	
fmt_addr({error, enotconn}, _) -> 
	"*:*";
fmt_addr({error, _}, _) -> 
	" ";
fmt_addr({ok, Addr}, Protocol) ->
    case Addr of
		{{0, 0, 0, 0}, Port} -> 
			"*:" ++ fmt_port(Port, Protocol);
		{{0, 0, 0, 0, 0, 0, 0, 0}, Port} -> 
			"*:" ++ fmt_port(Port, Protocol);
		{{127, 0, 0, 1}, Port} -> 
			"localhost:" ++ fmt_port(Port, Protocol);
		{{0, 0, 0, 0, 0, 0, 0, 1}, Port} -> 
			"localhost:" ++ fmt_port(Port, Protocol);
		{IP, Port} -> 
			inet_parse:ntoa(IP) ++ ":" ++ fmt_port(Port, Protocol)
    end.

fmt_port(N, Protocol) ->
    case inet:getservbyport(N, Protocol) of
		{ok, Name} -> 
			Name;
		_ -> 
			integer_to_list(N)
    end.

fmt_status(Status) ->
    case lists:sort(Status) of
		[accepting | _] -> 
			accepting;
		[bound,busy,connected|_] -> 
			connected;
		[bound,connected|_] -> 
			connected;
		[bound,listen,listening | _] -> 
			listening;
		[bound,listen | _] -> 
			listen;
		[bound,connecting | _] -> 
			connecting;
		[bound,open] -> 
			bound;
		[open] -> 
			idle;
		[] -> 
			closed;
		_  ->
			nil
    end.

sockets_info(Ports, all, Protocol) -> 
	sockets_info(Ports, [id, port, mod, recv, sent, owner, local, foreign, state, type], Protocol);
sockets_info(Ports, Keys, Protocol) -> 
	[[{Key, socket_info(Key, P, Protocol)} || {P, Key} <- L] || L <- [[{P, Key} || Key <- Keys] || P <- Ports]].
	
socket_info(id, P, _) -> 
	case port_info(P, id) of
		[{id, Id}]  -> 
			Id;
		undefined -> 
			nil
    end;
socket_info(port, P, _) ->
	P;
socket_info(mod, P, _) ->
	case inet_db:lookup_socket(P) of
		{ok, Mod} -> 
			Mod;
		_ -> 
			"prim_inet"
	end;
socket_info(recv, P, _) ->
	case  prim_inet:getstat(P, [recv_oct]) of
		{ok,[{recv_oct, N}]} -> 
			N;
		_ -> 
			0
    end;
socket_info(sent, P, _) ->
	case prim_inet:getstat(P, [send_oct]) of
		{ok,[{send_oct, N}]} -> 
			N;
		_ ->
			0
	end;
socket_info(owner, P, _) ->
	case erlang:port_info(P, connected) of
		{connected, Owner} -> 
			pid_to_list(Owner);
		_ ->
			nil
    end;
socket_info(local, P, Protocol) ->
	fmt_addr(prim_inet:sockname(P), Protocol);
socket_info(foreign, P, Protocol) ->
	fmt_addr(prim_inet:peername(P), Protocol);
socket_info(state, P, _) ->
	case prim_inet:getstatus(P) of
		{ok,Status} -> 
			fmt_status(Status);
		_ -> 
			nil
	end;
socket_info(type, P, _) ->
	case prim_inet:gettype(P) of
		{ok,{_, Type}}  -> 
			Type;
		_ -> 
			nil
    end.
