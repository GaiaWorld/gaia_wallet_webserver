%%%@doc 每节点的数据库服务器模块。
%%```
%%% 负责加载每个表进程，根据文件锁来确定是否为正常启动，管理表的服务状态。
%%'''
%%@end


-module(zm_db_server).

-description("data base server").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1]).
-export([load/0, load/1, join_load/0, auto_repair/1, handle_repair_event/4, get_table_opts/1, table_status/0, exist_table/2, table_size/1, table_memory/1, lock_write/1, unlock_write/1, tables/0, stop/2, stop/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(OPTS, ".opts").

-define(HIBERNATE_TIMEOUT, 3000).

-define(DB_LAYER, db).
-define(RUNNING, running).
-define(ONLY_READ, only_read).
-define(SYNC, sync).
-define(STOPPING, stopping).
-define(WAIT_REPAIR, wait_repair).
-define(REPAIRING, repairing).
-define(CONTINUE_REPAIR, continue_repair).
-define(REPAIR_ERROR, repair_error).

-define(NORMAL_REASON, normal).
-define(EXIT_REASON, shutdown).

%%%=======================RECORD=======================
-record(state, {from, ets, path, status, queue}).
-record(statistics, {start_time, read, read_lock, write, delete,
	dyn_read, dyn_read_lock, dyn_write, select, iterate, iterate_next,
	lock, lock_time, lock_error, lock_error_info, unlock, unlock_info,
	lock0, lock0_info, lock_timeout, lock_timeout_info}).


%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/3
%% Description: Starts a table.
%% Returns: {ok, Pid} | {error, Reason}
%% -----------------------------------------------------------------
start_link(Path) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Path, []).

%% -----------------------------------------------------------------
%%@doc  load
%% @spec  load() -> return()
%% where
%%  return() =  undefined | ok | already_ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
load() ->
	case whereis(?MODULE) of
		Pid when is_pid(Pid) ->
			gen_server:call(Pid, load, infinity);
		undefined ->
			undefined
	end.

%%
%%加载指定的表
%%
load(Table) ->
	case whereis(?MODULE) of
		Pid when is_pid(Pid) ->
			gen_server:call(Pid, {load, Table}, infinity);
		undefined ->
			undefined
	end.

%% -----------------------------------------------------------------
%%@doc  join_load
%% @spec  join_load() -> return()
%% where
%%  return() =  undefined | ok | already_ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
join_load() ->
	case whereis(?MODULE) of
		Pid when is_pid(Pid) ->
			gen_server:call(Pid, join_load, infinity);
		undefined ->
			undefined
	end.

%% -----------------------------------------------------------------
%%@doc  auto_repair
%% @spec  auto_repair(Table) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
auto_repair(Table) ->
	Local=node(),
	case zm_node:layer(?DB_LAYER) of
		{?DB_LAYER, _, _, [Local], {Local}} ->
			ignore;
		_ ->
			%开始在本地对指定表进行对比同步
			case compare_initiator:start(Table) of
				{error, {timeout, _}} ->
					%同步超时则需要重新同步
					continue;
				R ->
					R
			end
	end.

%%
%%处理修复事件
%%
handle_repair_event(_, _, finish1, {Table, Local, Remote}) ->
	error_logger:error_msg("!!!!!!repair i section ok, Type:~p, Table:~p, Local:~p, Remote:~p~n", [finish1, Table, Local, Remote]);
handle_repair_event(_, _, finish2, {Table, Local, Remote, Next}) ->
	error_logger:error_msg("!!!!!!repair ii section ok, next node, Type:~p, Table:~p, Local:~p, Remote:~p, Next:~p~n", [finish2, Table, Local, Remote, Next]);
handle_repair_event(_, _, finish2, {Table, Local, Remote}) ->
	error_logger:error_msg("!!!!!!repair ok, Type:~p, Table:~p, Local:~p, Remote:~p~n", [finish2, Table, Local, Remote]),
	gen_server:cast(?MODULE, {repair_ok, Table, whereis(compare_initiator)}).

%% -----------------------------------------------------------------
%%@doc  get_table_opts
%% @spec  get_table_opts(Table) -> return()
%% where
%%  return() =  none | Opts
%%@end
%% -----------------------------------------------------------------
get_table_opts(Table) ->
	gen_server:call(?MODULE, {get_table_opts, Table}).

%% -----------------------------------------------------------------
%%@doc  table_status
%% @spec  table_status() -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
table_status() ->
	gen_server:call(?MODULE, table_status).

%% -----------------------------------------------------------------
%%@doc  exist table
%% @spec  exist_table(Table, Status) -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
exist_table(Table, Status) ->
	gen_server:call(?MODULE, {exist_table, Table, Status}).

%% -----------------------------------------------------------------
%%@doc  get size of table
%% @spec  table_size(Table) -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
table_size(Table) ->
	gen_server:call(?MODULE, {table_size, Table}).

%% -----------------------------------------------------------------
%%@doc  get memory of table
%% @spec  table_memory(Table) -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
table_memory(Table) ->
	gen_server:call(?MODULE, {table_memory, Table}).
	
%% -----------------------------------------------------------------
%%@doc  lock table only read
%% @spec  lock_write(Table) -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
lock_write(Table) ->
	gen_server:call(?MODULE, {lock_write, Table}).

%% -----------------------------------------------------------------
%%@doc  unlock table only read
%% @spec  unlock_write(Table) -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
unlock_write(Table) ->
	gen_server:call(?MODULE, {unlock_write, Table}).

%% -----------------------------------------------------------------
%%@doc  get all table of local
%% @spec  tables() -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
tables() ->
	gen_server:call(?MODULE, tables).

%%
%%关闭指定的表
%%
stop(Table, Reason) ->
	gen_server:call(?MODULE, {stop, Table, Reason}, 60000).

%%
%%关闭本地数据库，需要正常关闭需要传参数normal, 强制关闭需要传参数shutdown
%%
stop(Reason) ->
	gen_server:call(?MODULE, {stop, Reason}, 60000).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Path) ->
	case z_lib:make_dir(Path) of
		ok ->
			process_flag(trap_exit, true),
			{ok, #state{ets = ets:new(?MODULE, [named_table]),
				path = Path, status = initing}, ?HIBERNATE_TIMEOUT};
		E ->
			{stop, E}
	end.

%% -----------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State} |
%%		{reply, Reply, State, Timeout} |
%%		{noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, Reply, State} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_call(load, _From, #state{ets = Ets, path = Path, status = initing} = State) ->
	Queue=init_table(Ets, Path, ?RUNNING, zm_db:list(), queue:new()),
	zm_service:set(?MODULE, ?RUNNING),
	{reply, ok, State#state{status = ?RUNNING, queue = Queue}, ?HIBERNATE_TIMEOUT};
		
handle_call(load, _From, #state{ets = Ets, path = Path, status = ?RUNNING, queue = Queue} = State) ->
	L = ets:tab2list(Ets),
	NewQueue=init_table(Ets, Path, ?RUNNING, zm_db:list(), Queue),
	close_table(Ets, L),
	{reply, already_ok, State#state{queue = NewQueue}, ?HIBERNATE_TIMEOUT};

handle_call({load, Table}, _From, #state{ets = Ets, path = Path, status = ?RUNNING, queue = Queue} = State) ->
	case queue:len(Queue) of
		0 ->
			NewQueue=init_table(Ets, Path, ?RUNNING, [zm_db:get(Table)], Queue),
			zm_service:set(?MODULE, ?RUNNING),
			{reply, ok, State#state{status = ?RUNNING, queue = NewQueue}, ?HIBERNATE_TIMEOUT};
		_ ->
			{reply, {error, busy}, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call(join_load, _From, #state{ets = Ets, path = Path, status = initing} = State) ->
	Queue=init_table(Ets, Path, joining, zm_db:list(), queue:new()),
	zm_service:set(?MODULE, joining),
	{reply, ok, State#state{status = ?RUNNING, queue = Queue}, ?HIBERNATE_TIMEOUT};
handle_call(join_load, _From, #state{ets = Ets, path = Path, status = ?RUNNING, queue = Queue} = State) ->
	%重置加载
	L = ets:tab2list(Ets),
	NewQueue=init_table(Ets, Path, joining, zm_db:list(), Queue),
	close_table(Ets, L),
	{reply, already_ok, State#state{queue = NewQueue}, ?HIBERNATE_TIMEOUT};

handle_call({get_table_opts, Table}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{_Table, Opts, _Dir, _S, _Pid}] ->
			{reply, Opts, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, none, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call(table_status, _From, #state{ets = Ets} = State) ->
	{reply, [table_status(Table, S, Pid) || {Table, _, _, S, Pid} <- ets:tab2list(Ets)],
		State, ?HIBERNATE_TIMEOUT};

handle_call({exist_table, Table, Status}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{_, _, _, Status, _}] ->
			{reply, true, State, ?HIBERNATE_TIMEOUT};
		_ ->
			{reply, false, State, ?HIBERNATE_TIMEOUT}
	end;
handle_call({table_size, Table}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{_, _, _, Status, Pid}] when Status =:= ?RUNNING; Status =:= ?ONLY_READ; Status =:= ?SYNC ->
			{reply, zm_db_table:size(Pid), State, ?HIBERNATE_TIMEOUT};
		[{_, _, _, {Status, _}, Pid}] when Status =:= ?REPAIRING; Status =:= ?WAIT_REPAIR ->
			{reply, zm_db_table:size(Pid), State, ?HIBERNATE_TIMEOUT};
		_ ->
			{reply, {error, invalid_table}, State, ?HIBERNATE_TIMEOUT}
	end;
handle_call({table_memory, Table}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{_, _, _, Status, Pid}] when Status =:= ?RUNNING; Status =:= ?ONLY_READ; Status =:= ?SYNC ->
			{reply, zm_db_table:memory(Pid), State, ?HIBERNATE_TIMEOUT};
		[{_, _, _, {Status, _}, Pid}] when Status =:= ?REPAIRING; Status =:= ?WAIT_REPAIR ->
			{reply, zm_db_table:memory(Pid), State, ?HIBERNATE_TIMEOUT};
		_ ->
			{reply, {error, invalid_table}, State, ?HIBERNATE_TIMEOUT}
	end;
handle_call({lock_write, Table}, _From, #state{ets = Ets} = State) ->
	Reply=case ets:lookup(Ets, Table) of
		[{_, Opts, Path, ?RUNNING, Pid}] ->
			true=ets:insert(Ets, {Table, Opts, Path, ?ONLY_READ, Pid}),
			ok;
		[{_, _, _, ?ONLY_READ, _}] ->
			ok;
		[{_, _, _, Status, _}] ->
			{error, {invalid_table_status, Status}};
		[] ->
			{error, {invalid_table, Table}}
	end,
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};
handle_call({unlock_write, Table}, _From, #state{ets = Ets} = State) ->
	Reply=case ets:lookup(Ets, Table) of
		[{_, Opts, Path, ?ONLY_READ, Pid}] ->
			true=ets:insert(Ets, {Table, Opts, Path, ?RUNNING, Pid}),
			ok;
		[{_, _, _, ?RUNNING, _}] ->
			ok;
		[{_, _, _, Status, _}] ->
			{error, {invalid_table_status, Status}};
		[] ->
			{error, {invalid_table, Table}}
	end,
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};
handle_call(tables, _From, #state{ets = Ets} = State) ->
	{reply, tables(Ets, ets:first(Ets), []), State, ?HIBERNATE_TIMEOUT};
handle_call({get_table_pid, Table}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{_, _, _, Status, Pid}] when Status =:= ?RUNNING; Status =:= ?ONLY_READ; Status =:= ?SYNC ->
			{reply, {ok, Pid}, State, ?HIBERNATE_TIMEOUT};
		[{_, _, _, {Status, _}, Pid}] when Status =:= ?REPAIRING; Status =:= ?WAIT_REPAIR ->
			{reply, {ok, Pid}, State, ?HIBERNATE_TIMEOUT};
		_ ->
			{reply, {error, invalid_table}, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({stop, Table, Reason}, From, #state{from = undefined, ets = Ets} = State) ->
	try stop_table(Table, Ets, Reason) of
        ok ->
            {noreply, State#state{from = {From, ok}}, ?HIBERNATE_TIMEOUT};
        E ->
            {reply, E, State, ?HIBERNATE_TIMEOUT}
    catch
        _:Reason ->
            {reply, {error, Reason}, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call({stop, Reason}, From, #state{from = undefined, ets = Ets} = State) ->
	try stop_db(Ets, Reason) of
        skip_stop ->
            {reply, ok, State, ?HIBERNATE_TIMEOUT};
        R ->
            {noreply, State#state{from = {From, R}}, ?HIBERNATE_TIMEOUT}
    catch
        _:Reason ->
            {reply, {error, Reason}, State, ?HIBERNATE_TIMEOUT}
    end;

handle_call({stop, _}, _, #state{from = _} = State) ->
	{reply, {error, ?STOPPING}, State, ?HIBERNATE_TIMEOUT};

handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({repair_ok, Table, ActionPid}, #state{ets = Ets, status = ?RUNNING, queue = Queue} = State) ->
	error_logger:error_msg("!!!!!!~p, repair ok~n", [Table]),
	case queue:out(Queue) of
		{{value, {Table, _, _, _}}, TmpQueue} ->
			NewQueue=case ets:lookup(Ets, Table) of
				[{_Table, Opts, Dir, {?REPAIRING, ActionPid}, Pid}] ->
					ets:insert(Ets, {Table, Opts, Dir, ?RUNNING, Pid}),
					zm_service:set(Table, {Opts, ?RUNNING}),
					auto_repair_next(TmpQueue, Ets);
				_ ->
					queue:new()
			end,
			{noreply, State#state{queue = NewQueue}, ?HIBERNATE_TIMEOUT};
		{empty, _} ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_cast({need_repair, Table, ActionPid}, #state{ets = Ets, status = running} = State) ->
	error_logger:error_msg("??????~p~n", [Table]),
	case ets:lookup(Ets, Table) of
		[{_Table, Opts, Dir, {repairing, ActionPid}, Pid}] ->
			ets:insert(Ets, {Table, Opts, Dir, need_repair, Pid}),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_cast({?REPAIR_ERROR, Table, E, ActionPid}, #state{ets = Ets, status = running, queue = Queue} = State) ->
	error_logger:error_msg("!!!!!!~p repair failed, E:~p~n", [Table, E]),
	case queue:out(Queue) of
		{{value, {Table, _, _, _}}, TmpQueue} ->
			NewQueue=case ets:lookup(Ets, Table) of
				[{_Table, Opts, Dir, {?REPAIRING, ActionPid}, Pid}] ->
					ets:insert(Ets, {Table, Opts, Dir, {?REPAIR_ERROR, E}, Pid}),
					auto_repair_next(TmpQueue, Ets);
				E ->
					queue:new()
			end,
			{noreply, State#state{queue = NewQueue}, ?HIBERNATE_TIMEOUT};
		{empty, _} ->
			error_logger:error_msg("!!!!!!!!!!!!!!!!!!!!!repair execption, queue null~n"),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({'EXIT', Pid, Reason}, #state{ets = Ets, from = {From, Reply}} = State) ->
	case table_exit(Ets, Pid, From, Reason, Reply) of
		all_ok ->
			{noreply, State#state{from = undefined}, ?HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info(timeout, State) ->
	{noreply, State, hibernate};

handle_info(_Info, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% -----------------------------------------------------------------
terminate(_Reason, State) ->
	State.

%% -----------------------------------------------------------------
%% Function: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% -----------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================LOCAL FUNCTIONS==================
% 初始化所有的表
init_table(Ets, Path, InitStatus, [{Table, _Duplication, Opts} | T], Queue) when is_atom(Table) ->
	NewQueue=init_table(Ets, Table, Opts, filename:join(Path, Table), InitStatus, Queue),
	init_table(Ets, Path, InitStatus, T, NewQueue);
init_table(_Ets, _Path, _InitStatus, [H | _T], Queue) ->
	erlang:error({init_table_error, H, Queue});
init_table(_Ets, _Path, _InitStatus, [], Queue) ->
	Queue;
init_table(_Ets, _Path, _InitStatus, none, Queue) ->
	Queue.

% 初始化表
init_table(Ets, Table, Opts, Dir, InitStatus, Queue) ->
	case ets:lookup(Ets, Table) of
		[{_Table, Opts, _Dir, _S, Pid}] when is_pid(Pid) ->
			Queue;
		[{_Table, _, _Dir, S, Pid}] when is_pid(Pid) ->
			zm_db_table:set_opts(Pid, Opts),
			ets:insert(Ets, {Table, Opts, Dir, S, Pid}),
			Queue;
		[] ->
			case zm_db_table:status(Dir, Opts) of
				{ok, Status} ->
					start_table(Ets, Table, Opts, Dir, Status, InitStatus, Queue);
				E ->
					ets:insert(Ets, {Table, Opts, Dir, {status_error, E}, none}),
					Queue
			end
	end.

% 启动表
start_table(Ets, Table, Opts, Dir, Status, InitStatus, Queue) ->
	try zm_db_table:start_link(Table, Opts, Dir) of
		{ok, Pid} ->
			load_table(Ets, Table, Opts, Dir, Status, InitStatus, Pid, Queue);
		E ->
			ets:insert(Ets, {Table, Opts, Dir, {start_error, E}, none, none}),
			Queue
	catch
		Error:Reason ->
			ets:insert(Ets, {Table, Opts, Dir, {start_error,
				{Error, Reason, erlang:get_stacktrace()}}, none, none}),
			Queue
	end.

% 加载表
load_table(Ets, Table, Opts, Dir, Status, InitStatus, Pid, Queue) ->
	try zm_db_table:load(Pid, Status, Opts, true) of
		load ->
			ets:insert(Ets, {Table, Opts, Dir, InitStatus, Pid}),
			zm_service:set(Table, {Opts, InitStatus}),
			Queue;
		load_repair ->
			case queue:len(Queue) of
				0 ->
					ets:insert(Ets, {Table, Opts, Dir, ?WAIT_REPAIR, Pid}),
					zm_service:set(Table, {Opts, ?WAIT_REPAIR}),
					case ?MODULE:auto_repair(Table) of
						ok ->
							error_logger:error_msg("!!!!!!~p start repair~n", [Table]),
							ets:insert(Ets, {Table, Opts, Dir, {?REPAIRING, whereis(compare_initiator)}, Pid}),
							zm_service:set(Table, {Opts, ?REPAIRING}),
							queue:in({Table, Opts, Dir, Pid}, Queue);
						continue ->
							error_logger:error_msg("!!!!!!~p continue repair~n", [Table]),
							ets:insert(Ets, {Table, Opts, Dir, ?CONTINUE_REPAIR, Pid}),
							zm_service:set(Table, {Opts, ?CONTINUE_REPAIR}),
							queue:in({Table, Opts, Dir, Pid}, Queue);
						ignore ->
							error_logger:error_msg("!!!!!!~p ignore repair~n", [Table]),
							ets:insert(Ets, {Table, Opts, Dir, InitStatus, Pid}),
							zm_service:set(Table, {Opts, InitStatus}),
							Queue;
						E ->
							ets:insert(Ets, {Table, Opts, Dir, {?REPAIR_ERROR, E}, Pid}),
							zm_service:set(Table, {Opts, ?REPAIR_ERROR}),
							Queue
					end;
				_ ->
					queue:in({Table, Opts, Dir, Pid}, Queue)
			end;
		E ->
			ets:insert(Ets, {Table, Opts, Dir, {load_error, E}, Pid}),
			zm_service:set(Table, {Opts, load_error}),
			Queue
	catch
		Error:Reason ->
			ets:insert(Ets, {Table, Opts, Dir, {load_error,
				{Error, Reason, erlang:get_stacktrace()}}, Pid}),
			zm_service:set(Table, {Opts, load_error}),
			Queue
	end.

auto_repair_next(Queue, Ets) ->
	case queue:peek(Queue) of
		{value, {Table, Opts, Dir, Pid}} ->
			ets:insert(Ets, {Table, Opts, Dir, ?WAIT_REPAIR, Pid}),
			zm_service:set(Table, {Opts, ?WAIT_REPAIR}),
			case ?MODULE:auto_repair(Table) of
				ok ->
					ets:insert(Ets, {Table, Opts, Dir, {?REPAIRING, whereis(compare_initiator)}, Pid}),
					zm_service:set(Table, {Opts, ?REPAIRING}),
					Queue;
				continue ->
					ets:insert(Ets, {Table, Opts, Dir, ?CONTINUE_REPAIR, Pid}),
					zm_service:set(Table, {Opts, ?CONTINUE_REPAIR}),
					Queue;
				E ->
					ets:insert(Ets, {Table, Opts, Dir, {?REPAIR_ERROR, E}, Pid}),
					queue:drop(Queue)
			end;
		empty ->
			Queue
	end.

% 关闭不存在的表
close_table(Ets, [{Table, _Opts, _Dir, _S, Pid} | T]) ->
	case zm_db:get(Table) of
		none ->
			zm_service:delete(Table),
			[zm_db_table:close(Pid, normal) || is_pid(Pid)],
			ets:delete(Ets, Table),
			close_table(Ets, T);
		_ ->
			close_table(Ets, T)
	end;
close_table(_Ets, []) ->
	ok.

%表状态
table_status(Table, ?RUNNING, Pid) ->
	{_, #statistics{start_time = Time,
		read = Read, read_lock = ReadLock, write = Write, delete = Delete,
		dyn_read = DynRead, dyn_read_lock = DynReadLock, dyn_write = DynWrite,
		select = Select, iterate = Iterate, iterate_next = IterateNext,
		lock = Lock, lock_time = LockTime,
		lock_error = LockError, lock_error_info = LockErrorInfo,
		unlock = Unlock, unlock_info = UnlockInfo,
		lock0 = Lock0, lock0_info = Lock0Info,
		lock_timeout = LockTimeout, lock_timeout_info = LockTimeoutInfo}
	} = z_lib:pid_dictionary(Pid, statistics),
	[{table, Table}, {status, ?RUNNING},
		{start_time, Time}, {read, Read}, {read_lock, ReadLock},
		{write, Write}, {delete, Delete}, {dyn_read, DynRead},
		{dyn_read_lock, DynReadLock}, {dyn_write, DynWrite},
		{select, Select}, {iterate, Iterate}, {iterate_next, IterateNext},
		{lock, Lock}, {lock_time, LockTime}, {lock_error, LockError},
		{lock_error_info, LockErrorInfo}, {unlock, Unlock},
		{unlock_info, UnlockInfo}, {lock0, Lock0},
		{lock0_info, Lock0Info}, {lock_timeout, LockTimeout},
		{lock_timeout_info, LockTimeoutInfo}, {pid, Pid}];
table_status(Table, Status, Pid) ->
	[{table, Table}, {status, Status}, {pid, Pid}].

%表退出
table_exit(Ets, Pid, From, Reason, Reply) ->
	case ets:select(Ets, [{{'_', '_', '_', '_', '$1'}, [{'=:=', '$1', Pid}], ['$_']}]) of
		[{Table, Opts, Dir, Status, _Pid}] when Status =:= ?RUNNING; Status =:= ?ONLY_READ ->
			ets:insert(Ets, {Table, Opts, Dir, {run_error, Reason}, none}),
			zm_service:delete(Table),
			z_lib:reply(From, {error, {run_error, Reason}});
        [{Table, _, _, ?STOPPING, _}] ->
            zm_service:delete(Table),
            ets:delete(Ets, Table),
            case ets:info(zm_db_server, size) of
                0 ->
                    z_lib:reply(From, Reply),
					all_ok;
                _ ->
                    ok
            end;
		[{Table, Opts, Dir, {S, _E}, _Pid}] ->
			ets:insert(Ets, {Table, Opts, Dir, {S, Reason}, none});
		[{Table, Opts, Dir, S, _Pid}] ->
			ets:insert(Ets, {Table, Opts, Dir, {S, Reason}, none});
		[] ->
			[]
	end.

tables(_, '$end_of_table', L) ->
	lists:reverse(L);
tables(Ets, Table, L) ->
	tables(Ets, ets:next(Ets, Table), [Table|L]).

select_stop_table('$end_of_table', _, _, []) ->
    skip_stop;
select_stop_table('$end_of_table', _, _, L) ->
    L;
select_stop_table(Table, Ets, Reason, L) ->
    case ets:lookup(Ets, Table) of
        [{_, Opts, Dir, Status, Pid}] when is_pid(Pid) ->
            case {Reason, Status} of
                {?NORMAL_REASON, Status} when Status =:= ?RUNNING; Status =:= ?ONLY_READ ->
                    ets:insert(Ets, {Table, Opts, Dir, ?STOPPING, Pid}),
                    zm_service:set(Table, {Opts, stopping}),
                    select_stop_table(ets:next(Ets, Table), Ets, Reason, [Pid|L]);
                {?NORMAL_REASON, _} ->
                    ets:insert(Ets, {Table, Opts, Dir, ?STOPPING, Pid}),
                    zm_service:set(Table, {Opts, stopping}),
                    select_stop_table(ets:next(Ets, Table), Ets, Reason, L); 
                {?EXIT_REASON, _} ->
                    ets:insert(Ets, {Table, Opts, Dir, ?STOPPING, Pid}),
                    zm_service:set(Table, {Opts, stopping}),
                    select_stop_table(ets:next(Ets, Table), Ets, Reason, [Pid|L]);
                 {_, _} ->
                     erlang:error({stop_db_failed, Table, Pid, Reason, Status, invalid_stop_reason})
            end;
        _ ->
            erlang:error({stop_db_failed, Table, invalid_table})
    end.

stop_tables([Pid|T], Reason) ->
    zm_db_table:close(Pid, Reason),
    stop_tables(T, Reason);
stop_tables([], _) ->
    ok;
stop_tables(skip_stop, _) ->
    skip_stop.

stop_db(Ets, Reason) ->
    stop_tables(select_stop_table(ets:first(Ets), Ets, Reason, []), Reason).

stop_table(Table, Ets, Reason) ->
	case ets:lookup(Ets, Table) of
        [{_, Opts, Dir, Status, Pid}] when is_pid(Pid) ->
            case {Reason, Status} of
                {?NORMAL_REASON, Status} when Status =:= ?RUNNING; Status =:= ?ONLY_READ ->
                    ets:insert(Ets, {Table, Opts, Dir, ?STOPPING, Pid}),
                    zm_service:set(Table, {Opts, stopping}),
					zm_db_table:close(Pid, Reason);
                {?NORMAL_REASON, _} ->
                    ets:insert(Ets, {Table, Opts, Dir, ?STOPPING, Pid}),
                    zm_service:set(Table, {Opts, stopping}),
					ok;
                {?EXIT_REASON, _} ->
                    ets:insert(Ets, {Table, Opts, Dir, ?STOPPING, Pid}),
                    zm_service:set(Table, {Opts, stopping}),
					zm_db_table:close(Pid, Reason);
                 {_, _} ->
                     erlang:error({stop_table_failed, Table, Pid, Reason, Status, invalid_stop_reason})
            end;
        _ ->
            erlang:error({stop_table_failed, Table, invalid_table})
    end.


