%% @author lenovo
%% @doc 本地进程


-module(z_process).

%% ====================================================================
%% Include files
%% ====================================================================
%标记关键字
-define(FLAG_KEY(Pid), z_lib:to_atom(["$", ?MODULE, "_", pid_to_list(Pid)])).
%上级进程标记
-define(PARENT_FLAG, parent).
%进程类型标记
-define(TYPE_FLAG, type).
%默认进程类型
-define(DEFAULT_TYPE, z_lib:to_atom(['$', ?MODULE])).
%创建子进程数量标记
-define(SPAWN_COUNT_FLAG, spawn_count).
%进程创建时间标记
-define(CREATE_TIME_FLAG, create_time).
%默认最大堆配置
-define(DEFAULT_MAX_HEAP_CONFIG, {0, true, true}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([spawn/1, spawn/2, spawn/3, spawn_link/1, spawn_link/2, spawn_link/3, 
		 exist/1, is_process/1, is_system_process/1, is_link_parent/1, 
		 name/1, 
		 parent/1, parent_stack/1, 
		 type/1, type_stack/1, 
		 status/1, msg_size/1, stack/1,
		 memory/1, set_max_heap/3, 
		 local_process/4, 
		 system_process/1, 
		 links/1, 
		 spawn_count/1,
		 create_time/1, running_time/1, 
		 gc/0, gc/1,
		 yield/0, 
		 exit/1, exit/2
		 ]).

%%创建一个本地进程
spawn(F) ->
	?MODULE:spawn(?DEFAULT_TYPE, ?DEFAULT_MAX_HEAP_CONFIG, F).

%%创建一个指定类型的本地进程
spawn(Type, F) ->
	?MODULE:spawn(Type, ?DEFAULT_MAX_HEAP_CONFIG, F).

%%创建一个指定类型和最大堆信息的本地进程
spawn(Type, {Size, IsKill, IsEvent}, {M, F, A}) 
  when is_atom(Type), is_integer(Size), is_boolean(IsKill), is_boolean(IsEvent), is_atom(M), is_atom(F), is_list(A) ->
	RealSize=if
		Size =< 0 ->
			0;
		true ->
			Size div erlang:system_info(wordsize)
	end,
	erlang:spawn(init(Type, {RealSize, IsKill, IsEvent}, {M, F, A}));
spawn(Type, {Size, IsKill, IsEvent}, Fun) 
  when is_atom(Type), is_integer(Size), is_boolean(IsKill), is_boolean(IsEvent), is_function(Fun, 0) ->
	RealSize=if
		Size =< 0 ->
			0;
		true ->
			Size div erlang:system_info(wordsize)
	end,
	erlang:spawn(init(Type, {RealSize, IsKill, IsEvent}, Fun)).

%%创建并连接一个本地进程
spawn_link(F) ->
	?MODULE:spawn_link(?DEFAULT_TYPE, ?DEFAULT_MAX_HEAP_CONFIG, F).

%%创建并连接一个指定类型的本地进程
spawn_link(Type, F) ->
	?MODULE:spawn_link(Type, ?DEFAULT_MAX_HEAP_CONFIG, F).

%%创建并连接一个指定类型和最大堆信息的本地进程
spawn_link(Type, {Size, IsKill, IsEvent}, {M, F, A}) 
  when is_atom(Type), is_integer(Size), is_boolean(IsKill), is_boolean(IsEvent), is_atom(M), is_atom(F), is_list(A) ->
	RealSize=if
		Size =< 0 ->
			0;
		true ->
			Size div erlang:system_info(wordsize)
	end,
	erlang:spawn_link(init(Type, {RealSize, IsKill, IsEvent}, {M, F, A}));
spawn_link(Type, {Size, IsKill, IsEvent}, Fun) 
  when is_atom(Type), is_integer(Size), is_boolean(IsKill), is_boolean(IsEvent), is_function(Fun, 0) ->
	RealSize=if
		Size =< 0 ->
			0;
		true ->
			Size div erlang:system_info(wordsize)
	end,
	erlang:spawn_link(init(Type, {RealSize, IsKill, IsEvent}, Fun)).

%%判断进程是否存在
exist(Pid) when is_pid(Pid) ->
	case erlang:process_info(Pid, memory) of
		undefined ->
			false;
		_ ->
			true
	end;
exist(_) ->
	false.
	
%%判断是否是本地进程
is_process(Pid) ->
	case get_flag_info(Pid) of
		false ->
			false;
		_ ->
			is_process_alive(Pid)
	end.

%%判断本地进程是否是系统进程
is_system_process(Pid) ->
	element(2, erlang:process_info(Pid, trap_exit)).

%%判断本地进程与上级进程是否有link
is_link_parent(Pid) ->
	lists:member(?MODULE:parent(Pid), element(2, erlang:process_info(Pid, links))).

%%获取进程的名称
name(Pid) ->
	case erlang:process_info(Pid, registered_name) of
		[] ->
			Pid;
		{_, Name} ->
			Name
	end.

%%获取上级进程
parent(Pid) ->
	case get_flag_info(Pid) of
		false ->
			false;
		{_, Tree} ->
			sb_trees:get(?PARENT_FLAG, Tree)
	end.

%%获取上级进程栈
parent_stack(Pid) ->
	parent_stack(Pid, []).

%%获取进程类型
type(Pid) ->
	case get_flag_info(Pid) of
		false ->
			false;
		{_, Tree} ->
			sb_trees:get(?TYPE_FLAG, Tree)
	end.

%%获取进程类型栈
type_stack(Pid) ->
	case ?MODULE:type(Pid) of
		false ->
			[];
		Type ->
			type_stack(Pid, [Type])
	end.

%%获取进程状态信息
status(Pid) ->
	element(2, erlang:process_info(Pid, status)).

%%获取进程消息数量
msg_size(Pid) ->
	element(2, erlang:process_info(Pid, message_queue_len)).

%%获取进程运行栈
stack(Pid) ->
	[{main, erlang:process_info(Pid, initial_call)},
	 {stacktrace, element(2, erlang:process_info(Pid, current_stacktrace))}].

%%获取进程内存信息
memory(Pid) ->
	WordSize=erlang:system_info(wordsize),
	[
		{stack_size, element(2, erlang:process_info(Pid, stack_size)) * WordSize},
		{heap_size, element(2, erlang:process_info(Pid, heap_size)) * WordSize},
		{max_heap_size, maps:get(size, element(2, lists:keyfind(max_heap_size, 1, element(2, erlang:process_info(self(), garbage_collection))))) * WordSize},
		{total_size, element(2, erlang:process_info(Pid, memory))}
	].

%%设置当前进程最大堆
set_max_heap(Size, IsKill, IsEvent) ->
	erlang:process_flag(max_heap_size, #{size => Size, kill => IsKill, error_logger => IsEvent}).

%%设置当前进程为本地进程
local_process(Type, Size, IsKill, IsLog) ->
	Self=self(),
	case is_process_alive(Self) and not ?MODULE:is_process(Self) of
		true ->
			?MODULE:set_max_heap(Size, IsKill, IsLog),
			Parent=case lists:keyfind('$ancestors', 1, element(2, erlang:process_info(Self, dictionary))) of
				{_, [H|_]} when is_pid(H) ->
					H;
				{_, [H|_]} when is_atom(H) ->
					whereis(H);
				{_, []} ->
					element(2, erlang:process_info(Self, group_leader))
			end,
			put(?FLAG_KEY(self()), set_flag(sb_trees:empty(), Parent, Type)),
			ok;
		false ->
			{error, invalid_process}
	end.
			
%%设置当前进程为系统进程
system_process(Bool) when is_boolean(Bool) ->
	erlang:process_flag(trap_exit, Bool).

%%获取创建子进程的数量
spawn_count(Pid) ->
	case get_flag_info(Pid) of
		false ->
			false;
		{_, Tree} ->
			sb_trees:get(?SPAWN_COUNT_FLAG, Tree)
	end.

%%获取连接的进程
links(Pid) ->
	element(2, erlang:process_info(Pid, links)).

%%获取本地进程创建时间
create_time(Pid) ->
	case get_flag_info(Pid) of
		false ->
			false;
		{_, Tree} ->
			sb_trees:get(?CREATE_TIME_FLAG, Tree)
	end.

%%获取本地进程运行时长
running_time(Pid) ->
	case get_flag_info(Pid) of
		false ->
			false;
		{_, Tree} ->
			z_lib:now_millisecond() - sb_trees:get(?CREATE_TIME_FLAG, Tree)
	end.

%%对当前进程进行gc
gc() ->
	erlang:garbage_collect(self()).

%%对指定进程进行gc
gc(Pid) ->
	Ref=erlang:make_ref(),
	erlang:garbage_collect(Pid, [{async, Ref}]),
	Ref.

%%让出当前进程的执行时间
yield() ->
	erlang:yield().

%%退出当前进程
exit(Reason) ->
	erlang:exit(Reason).

%%退出指定进程
exit(Pid, Reason) ->
	erlang:exit(Pid, Reason).

%% ====================================================================
%% Internal functions
%% ====================================================================

%%初始化本地进程
init(Type, {Size, IsKill, IsLog}, {M, F, A} = MFA) ->
	Parent=self(),
	add_child_count(Parent),
	fun() ->
		Key=?FLAG_KEY(self()),
		case get(Key) of
			undefined ->
				?MODULE:set_max_heap(Size, IsKill, IsLog),
				put(Key, set_flag(sb_trees:empty(), Parent, Type)),
				apply(M, F, A);
			_ ->
				erlang:error({init_z_process_failed, MFA})
		end
	end;
init(Type, {Size, IsKill, IsLog}, Fun) ->
	Parent=self(),
	add_child_count(Parent),
	fun() ->
		Key=?FLAG_KEY(self()),
		case get(Key) of
			undefined ->
				?MODULE:set_max_heap(Size, IsKill, IsLog),
				put(Key, set_flag(sb_trees:empty(), Parent, Type)),
				apply(Fun, []);
			_ ->
				erlang:error({init_z_process_failed, Fun})
		end
	end.

%%增加本地进程创建子进程的数量
add_child_count(Self) ->
	case ?MODULE:is_process(Self) of
		true ->
			Key=?FLAG_KEY(Self),
			case get(Key) of
				undefined ->
					erlang:error({add_child_count_failed, invalid_flag});
				Tree ->
					case sb_trees:lookup(?SPAWN_COUNT_FLAG, Tree) of
						{_, Count} ->
							NewCount=Count + 1,
							put(Key, sb_trees:update(?SPAWN_COUNT_FLAG, NewCount, Tree)),
							NewCount;
						R ->
							erlang:error({add_child_count_failed, R})
					end
			end;
		false ->
			-1
	end.

%%设置本地进程标记
set_flag(Tree, Parent, Type) ->
	sb_trees:insert(?PARENT_FLAG, Parent, 
					sb_trees:insert(?TYPE_FLAG, Type, 
									sb_trees:insert(?SPAWN_COUNT_FLAG, 0, 
													sb_trees:insert(?CREATE_TIME_FLAG, z_lib:now_millisecond(), Tree)))).

%%获取指定本地进程的标记信息
get_flag_info(Pid) ->
	lists:keyfind(?FLAG_KEY(Pid), 1, element(2, erlang:process_info(Pid, dictionary))).

%%获取上级进程栈
parent_stack(Pid, L) ->
	case ?MODULE:parent(Pid) of
		false ->
			L;
		Parent ->
			parent_stack(Parent, [Parent|L])
	end.

%%获取进程类型栈
type_stack(Pid, L) ->
	case ?MODULE:parent(Pid) of
		false ->
			L;
		Parent ->
			case ?MODULE:is_process(Parent) of
				true ->
					type_stack(Parent, [?MODULE:type(Parent)|L]);
				false ->
					L
			end
	end.