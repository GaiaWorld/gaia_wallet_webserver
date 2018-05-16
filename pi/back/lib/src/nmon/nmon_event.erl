%% 
%% @doc 节点监控订阅事件处理器
%%


-module(nmon_event).

%% ====================================================================
%% API functions
%% ====================================================================
-export([node/2, process/2, port/2, memory/2, alloc/2, bcs/2, tcp/2, udp/2, sctp/2]).

%%
%%获取节点状态数据
%%
node([Args, Src, node, Attr, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, node}),
	{{input,In},{output,Out}} = erlang:statistics(io),
	[{Start, Tail}]=recon:node_stats_list(1, 0),
	FreeBytes=z_lib:get_value(Tail, gc_words_reclaimed, 0) * erlang:system_info(wordsize),
	NewTail=lists:keyreplace(gc_words_reclaimed, 1, lists:keyreplace(bytes_out, 1, lists:keyreplace(bytes_in, 1, Tail, {bytes_in, In}), {bytes_out, Out}), {gc_words_reclaimed, FreeBytes}),
	push(Node, Args, node(), node, TimeDate, filter_node_state(Attr, Start ++ NewTail, []), MF, Recount, Interval).

%%
%%获取进程实时数据
%%
process([Args, Src, process, Attr, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, process}),
	WordSize=erlang:system_info(wordsize),
	Pid=erlang:list_to_pid(Attr),
	Info=[
	 {mql, z_lib:get_value(nmon_server:process_info(Pid, message_queue_len, 200), message_queue_len, 0)},
	 {mem, z_lib:get_value(nmon_server:process_info(Pid, memory, 200), memory, 0)},
	 {hs, z_lib:get_value(nmon_server:process_info(Pid, heap_size, 200), heap_size, 0) * WordSize},
	 {ths, z_lib:get_value(nmon_server:process_info(Pid, total_heap_size, 200), total_heap_size, 0) * WordSize},
	 {gc, nmon_server:minor_gcs(Pid, 200)}
	],
	push(Node, Args, node(), process, TimeDate, Info, MF, Recount, Interval).

%%
%%获取port实时数据
%%
port([Args, Src, port, Port, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, port}),
	push(Node, Args, node(), port, TimeDate, nmon_server:port_info(Port, [io, memory, queue_size]), MF, Recount, Interval).

%%
%%获取内存状态数据
%%
memory([Args, Src, memory, _, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, memory}),
	push(Node, Args, node(), memory, TimeDate, nmon_server:memory_usage(current) ++ nmon_server:gcs(100), MF, Recount, Interval).

%%
%%获取分配器状态数据
%%
alloc([Args, Src, alloc, {mseg_alloc, Instance}, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, alloc}),
	push(Node, Args, node(), alloc, TimeDate, [{A, proplists:get_value(calls, proplists:get_value(memkind, List, []), [])} || {{A, N}, List} <- nmon_server:alloter_info(mseg_alloc), N =:= Instance], MF, Recount, Interval);
alloc([Args, Src, alloc, {Alloc, Instance}, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, alloc}),
	push(Node, Args, node(), alloc, TimeDate, [{A, proplists:get_value(calls, List, [])} || {{A, N}, List} <- nmon_server:alloter_info(Alloc), N =:= Instance], MF, Recount, Interval).

%%
%%获取分配器块载体状态数据
%%
bcs([Args, Src, bcs, {Alloc, Instance}, Node, MF, Recount, Interval], TimeDate) ->
	nmon_server:count({Src, Node, bcs}),
	Info=[
		  {fragmentation, [{A, List} || {{A, N}, List} <- recon_alloc:fragmentation(current), A =:= Alloc, N =:= Instance]},
		  {cache_hit_rates, [{A, List} || {{A, N}, List} <- recon_alloc:cache_hit_rates(), N =:= Instance]},
		  {average_block_sizes, [{A, List} || {A, List} <- recon_alloc:average_block_sizes(current), A =:= Alloc]},
		  {sbcs_to_mbcs, [{A, Value} || {{A, N}, Value} <- recon_alloc:sbcs_to_mbcs(current), A =:= Alloc, N =:= Instance]}
		  ],
	push(Node, Args, node(), bcs, TimeDate, Info, MF, Recount, Interval).

%%
%%获取tcp状态数据
%%
tcp([Args, Src, tcp, LocalPort, Node, MF, Recount, Interval], TimeDate) ->
	socket(Args, Src, tcp, LocalPort, Node, MF, Recount, Interval, TimeDate).

%%
%%获取udp状态数据
%%
udp([Args, Src, udp, LocalPort, Node, MF, Recount, Interval], TimeDate) ->
	socket(Args, Src, udp, LocalPort, Node, MF, Recount, Interval, TimeDate).

%%
%%获取tcp状态数据
%%
sctp([Args, Src, sctp, LocalPort, Node, MF, Recount, Interval], TimeDate) ->
	socket(Args, Src, sctp, LocalPort, Node, MF, Recount, Interval, TimeDate).

%% ====================================================================
%% Internal functions
%% ====================================================================

filter_node_state([Key|T], States, L) ->
	case lists:keyfind(Key, 1, States) of
		false ->
			filter_node_state(T, States, L);
		KV ->
			filter_node_state(T, States, [KV|L])
	end;
filter_node_state(all, States, _) ->
	States;
filter_node_state([], _, L) ->
	L.

push(_, _, _, _, _, _, _, 0, _) ->
	ok;
push(Node, Args, Slave, Type, TimeDate, Data, {M, F} = MF, Recount, Interval) ->
	case rpc:call(Node, M, F, [Args, Slave, Type, TimeDate, Data]) of
		ok ->
			ok;
		{error, _} ->
			timer:sleep(Interval),
			push(Node, Args, Slave, Type, TimeDate, Data, MF, Recount - 1, Interval);
		{badrpc, _} ->
			timer:sleep(Interval),
			push(Node, Args, Slave, Type, TimeDate, Data, MF, Recount - 1, Interval)
	end.

socket(Args, Src, Protocol, LocalPort, Node, MF, Recount, Interval, TimeDate) ->
	nmon_server:count({Src, Node, Protocol}),
	push(Node, Args, node(), Protocol, TimeDate, nmon_server:sockets_io_info(Protocol, LocalPort), MF, Recount, Interval).
