%%@doc 数据库的表模块，
%%```
%%% 启动进程，处理具体的操作命令，提供数据锁，调用实际的存储模块。
%%% 统计信息包括：表的无锁读、锁读、写、删除、动态读、动态锁读、动态写、选择和遍历及遍历后续的次数。锁的次数，累计时长，冲突的次数和最近一次冲突的键及信息，unlock的次数和最近一次unlock的键及信息，lock0的次数和最近一次lock0的键及信息，锁超时的次数及最近一次超时的键及信息。
%%'''
%%@end


-module(zm_db_table).

-description("data base table").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/3, status/2, load/4, set_opts/2, size/1, memory/1, leadin/2, read/3, iterate/2, iterate_next/1, close/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(MOD, "zm_storage_").
-define(STORAGE, "storage:").
-define(TYPE, memory).

-define(OPTS, ".opts").
-define(OPTS_RUN, ".opts.run").

-define(NIL, '$nil').

-define(TIMEOUT, 5000).
-define(LOAD_TIMEOUT, 30*60*1000).
-define(HIBERNATE_TIMEOUT, 3000).

-define(COLLATE_TIMEOUT, 10*60*1000).

-define(LOCK_INFINITY_TIME, 30000).
-define(LOCK_SLEEP_TIME, 30).
-define(ITERATE_KEY_COUNT, 50).
-define(ITERATE_TIMEOUT, 5000).

%%%=======================RECORD=======================
-record(state, {table, path, storage, collate}).
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
start_link(Table, Opts, Path) ->
	gen_server:start_link({local, Table}, ?MODULE, {Table, Opts, Path}, []).

%% -----------------------------------------------------------------
%%@doc  获得表的状态，并重写配置
%% @spec  status(Path, Opts) -> return()
%% where
%%  return() =  {ok, Status} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
status(Path, Opts) ->
	case z_lib:make_dir(Path) of
		ok ->
			case write_opts(filename:join(Path, ?OPTS_RUN), Opts, false) of
				ok ->
					{ok, load_repair};
				false ->
					case write_opts(filename:join(Path, ?OPTS), Opts, true) of
						ok ->
							{ok, load};
						E ->
							E
					end;
				E ->
					E
			end;
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc  load table
%% @spec  load(Pid, Status, Opts, IgnoreError) -> return()
%% where
%%  return() =  load | need_repair | {error, Reason}
%%@end
%% -----------------------------------------------------------------
load(Pid, Status, Opts, IgnoreError) ->
	gen_server:call(Pid, {load, Status, Opts, IgnoreError}, ?LOAD_TIMEOUT).

%% -----------------------------------------------------------------
%%@doc  set_opts
%% @spec  set_opts(Pid, Opts) -> return()
%% where
%%  return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
set_opts(Pid, Opts) ->
	gen_server:call(Pid, {set_opts, Opts}, ?TIMEOUT).

%% -----------------------------------------------------------------
%%@doc  get size of table
%% @spec  size(Pid) -> return()
%% where
%%  return() =  {ok, size} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
size(Pid) ->
	gen_server:call(Pid, size, ?TIMEOUT).

%% -----------------------------------------------------------------
%%@doc  get memory of table
%% @spec  memory(Pid) -> return()
%% where
%%  return() =  {ok, memory} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
memory(Pid) ->
	gen_server:call(Pid, memory, ?TIMEOUT).

%% -----------------------------------------------------------------
%%@doc  batch lead in table
%% @spec  leadin(Pid, [{Key, Value, Vsn, Time}|T]) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
leadin(Pid, L) ->
	{M, A}=gen_server:call(Pid, get_storage, ?TIMEOUT),
	batch_write(L, M, A).

%% -----------------------------------------------------------------
%%@doc  read data
%% @spec  read(Pid, Key, Timeout) -> return()
%% where
%%  return() =  {ok, Value, Vsn, Time} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read(Table, Key, Timeout) ->
	try
		read_(Table, Key, none, -1, Timeout, zm_time:now_millisecond())
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc  get iterator
%% @spec  iterate(Pid, KeyRange) -> return()
%% where
%%  return() =  {ok, Iterator} | {error, Reason} | {error, Reason, StackTrace}
%%@end
%% -----------------------------------------------------------------
iterate(Pid, KeyRange) ->
	Node=node(),
	try
		case iterate_list(
				sort_type(KeyRange), [{Node, gen_server:call(Pid, {iterate, KeyRange, none, attr, attr, ?ITERATE_KEY_COUNT}, ?ITERATE_TIMEOUT)}], sb_trees:empty(), []) of
			{error, _} = E ->
				E;
			{Nodes, Keys} ->
				{ok, {Pid, KeyRange, Nodes, Keys}}
		end
	catch
		_:Reason ->
			{error, Reason, erlang:get_stacktrace()}
	end.

%% -----------------------------------------------------------------
%%@doc  iterator next
%% @spec  iterate_next(Iterator) -> return()
%% where
%%  return() =  {ok, {Key, Vsn, Time}, Iterator} | over | {error, Reason} | {error, Reason, StackTrace}
%%@end
%% -----------------------------------------------------------------
iterate_next({Pid, Range, Nodes, [H | T]}) ->
	try
		iterate_next_(Pid, Range, Nodes, H, T)
	catch
		_:Reason ->
			{error, Reason, erlang:get_stacktrace()}
	end;
iterate_next({_, _, _, []}) ->
	over.

%% -----------------------------------------------------------------
%%@doc  close table
%% @spec  close(Pid, Reason) -> return()
%% where
%%  return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
close(Pid, Reason) ->
	gen_server:cast(Pid, {close, Reason}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Table, Opts, Path}) ->
	Time = zm_time:now_millisecond(),
	put(statistics, #statistics{start_time = Time,
		read = 0, read_lock = 0, write = 0, delete = 0,
		dyn_read = 0, dyn_read_lock = 0, dyn_write = 0,
		select = 0, iterate = 0, iterate_next = 0, lock = 0, lock_time = 0,
		lock_error = 0, unlock = 0, lock0 = 0, lock_timeout = 0}),
	{ok, #state{table = Table, path = Path,
		storage = z_lib:to_atom(?MOD, z_lib:get_value(Opts, type, ?TYPE)),
		collate = Time + ?COLLATE_TIMEOUT}, ?HIBERNATE_TIMEOUT}.

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
handle_call({load, Status, Opts, IgnoreError}, _From, #state{
	table = Table, path = Path, storage = Mod} = State) when is_atom(Mod) ->
	case start_storage(Mod, Table, Opts, Path, IgnoreError) of
		{ok, Args} ->
			{reply, Status, State#state{
				storage = {Mod, Args}}, ?HIBERNATE_TIMEOUT};
		E ->
			{reply, E, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({set_opts, Opts}, _From, #state{storage = {M, A}} = State) ->
	M:set_opts(A, Opts),
	{reply, ok, State, ?HIBERNATE_TIMEOUT};
handle_call({set_opts, _Opts}, _From, State) ->
	{reply, ok, State, ?HIBERNATE_TIMEOUT};
handle_call(size, _From, #state{storage = {M, A}} = State) ->
	{reply, M:size(A), State, ?HIBERNATE_TIMEOUT};
handle_call(memory, _From, #state{storage = {M, A}} = State) ->
	{reply, M:memory(A), State, ?HIBERNATE_TIMEOUT};

handle_call(get_storage, _From, #state{storage = Storage} = State) ->
	{reply, Storage, State, ?HIBERNATE_TIMEOUT};

handle_call(get_modify_time, _From, #state{storage = {M, A}} = State) ->
	{reply, M:get_modify_time(A), State, ?HIBERNATE_TIMEOUT};

handle_call(count, _From, #state{storage = {M, A}} = State) ->
	{reply, M:count(A), State, ?HIBERNATE_TIMEOUT};

handle_call(clear, _From, #state{storage = {M, A}} = State) ->
	{reply, M:clear(A), State, ?HIBERNATE_TIMEOUT};

handle_call({lock, Key, Lock, LockTime, LockMulti},
	From, #state{storage = {M, A}} = State) ->
	Now = zm_time:now_millisecond(),
	handle(M, A, lock, Key, Key, Lock, LockTime, LockMulti, From, Now),
	{noreply, collate(State, Now), ?HIBERNATE_TIMEOUT};

handle_call({read, Key, _Lock, LockTime, _LockMulti}, From,
	#state{storage = {M, A}} = State) when LockTime < 0 ->
	#statistics{read = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{read = C + 1}),
	M:read(A, Key, Key, zm_time:now_millisecond(), From),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_call({read, Key, Lock, LockTime, LockMulti},
	From, #state{storage = {M, A}} = State) ->
	Now = zm_time:now_millisecond(),
	handle(M, A, read, Key, Key, Lock, LockTime, LockMulti, From, Now),
	{noreply, collate(State, Now), ?HIBERNATE_TIMEOUT};

handle_call({write, Key, Value, Vsn, Lock, LockTime, LockMulti}, From,
	#state{storage = {M, A}} = State) ->
	Now = zm_time:now_millisecond(),
	% TODO notify write
	handle(M, A, write, Key, {Value, Vsn},
				Lock, LockTime, LockMulti, From, Now),
	{noreply, collate(State, Now), ?HIBERNATE_TIMEOUT};

handle_call({delete, Key, Vsn, Lock, LockTime, LockMulti}, From,
	#state{storage = {M, A}} = State) ->
	Now = zm_time:now_millisecond(),
	handle(M, A, delete, Key, Vsn, Lock, LockTime, LockMulti, From, Now),
	% TODO notify delete
	{noreply, collate(State, Now), ?HIBERNATE_TIMEOUT};

handle_call({dyn_read, Key, Fun, _Lock, LockTime, LockMulti}, From,
	#state{storage = {M, A}} = State) when LockTime < 0 ->
	#statistics{dyn_read = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{dyn_read = C + 1}),
	M:dyn_read(A, Key, Fun, zm_time:now_millisecond(), LockMulti, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_call({dyn_read, Key, Fun, Lock, LockTime, LockMulti}, From,
	#state{storage = {M, A}} = State) ->
	Now = zm_time:now_millisecond(),
	handle(M, A, dyn_read, Key, Fun,
		Lock, LockTime, LockMulti, From, Now),
	{noreply, collate(State, Now), ?HIBERNATE_TIMEOUT};

handle_call({dyn_write, Key, Fun, Vsn, Lock, LockTime, LockMulti}, From,
	#state{storage = {M, A}} = State) ->
	Now = zm_time:now_millisecond(),
	handle(M, A, dyn_write, Key, {Fun, Vsn},
		Lock, LockTime, LockMulti, From, Now),
	{noreply, collate(State, Now), ?HIBERNATE_TIMEOUT};

handle_call({select, KeyRange, Filter, FilterType, ResultType, NL, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{read = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{read = C + 1}),
	M:select(A, KeyRange, Filter, FilterType, ResultType, NL, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({select, KeyRange, Filter, FilterType, ResultType, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{read = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{read = C + 1}),	
	M:select(A, KeyRange, Filter, FilterType, ResultType, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({iterate, KeyRange, Filter, FilterType, ResultType, NL, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{iterate = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{iterate = C + 1}),
	M:select(A, KeyRange, Filter, FilterType, ResultType, NL, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({iterate, KeyRange, Filter, FilterType, ResultType, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{iterate = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{iterate = C + 1}),
	M:select(A, KeyRange, Filter, FilterType, ResultType, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({iterate_next, KeyRange, Filter, FilterType, ResultType, NL, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{iterate_next = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{iterate_next = C + 1}),
	M:select(A, KeyRange, Filter, FilterType, ResultType, NL, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({iterate_next, KeyRange, Filter, FilterType, ResultType, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{iterate_next = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{iterate_next = C + 1}),
	M:select(A, KeyRange, Filter, FilterType, ResultType, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({select_index, ValueKeyRange, Filter, FilterType, ResultType, NL, Limit}, From,
	#state{storage = {M, A}} = State) ->
	#statistics{read = C} = Statistics = get(statistics),
	put(statistics, Statistics#statistics{read = C + 1}),
	M:select_index(A, ValueKeyRange, Filter, FilterType, ResultType, NL, Limit, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call({restore, Key, Value, Vsn, Time}, From,
	#state{storage = {M, A}} = State) ->
	% TODO notify restore
	M:restore(A, Key, {Value, Vsn}, Time, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({unlock, Key, Lock}, State) ->
	unlock(Key, Lock),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({lock0, Key, Lock}, State) ->
	lock0(Key, Lock),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({close, Reason}, #state{path = Path, storage = {M, A}} = State) ->
	Args = M:close(A, Reason),
	file:rename(filename:join(Path, ?OPTS_RUN), filename:join(Path, ?OPTS)),
	{stop, Reason, State#state{storage = {M, Args}}};

handle_cast({close, Reason}, #state{path = Path} = State) ->
	file:rename(filename:join(Path, ?OPTS_RUN), filename:join(Path, ?OPTS)),
	{stop, Reason, State};

handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info(timeout, State) ->
	collate(zm_time:now_millisecond()),
	{noreply, State#state{collate = 0}, hibernate};

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
% 重写配置
write_opts(File, Opts, Write) ->
	case z_lib:get_file_info(File) of
		{_File, _Size, regular, _MTime, _CTime} ->
			file:write_file(File, [z_lib:string(Opts), $.]);
		{_File, _Size, _Type, _MTime, _CTime} ->
			{error, {table_run_file_invalid, File}};
		{error, enoent} when Write =:= true ->
			file:write_file(File, [z_lib:string(Opts), $.]);
		{error, enoent} ->
			false;
		{error, Reason} ->
			{error, {table_run_file_error, file:format_error(Reason)}}
	end.

% 启动储存模块
start_storage(Mod, Table, Opts, Path, IgnoreError) ->
	try Mod:start_link(z_lib:to_atom(?STORAGE, Table),
		Table, Opts, Path, IgnoreError) of
		{ok, Pid} ->
			file:rename(filename:join(Path, ?OPTS),
				filename:join(Path, ?OPTS_RUN)),
			{ok, Mod:get_argument(Pid)};
		E ->
			E
	catch
		Error:Reason ->
			{Error, Reason, erlang:get_stacktrace()}
	end.

% unlock解锁
unlock(Key, Lock) ->
	#statistics{unlock = C} = S = get(statistics),
	LK = {lock, Key},
	case get(LK) of
		{Lock, _, _Expire, _LockMulti, _Pid} = E ->
			put(statistics, S#statistics{unlock = C + 1, unlock_info ={Key, E}}),
			erase(LK);
		E ->
			put(statistics, S#statistics{
				unlock = C + 1, unlock_info = {lock_error, {Key, E}}})
	end.

% lock0解锁
lock0(Key, Lock) ->
	#statistics{lock0 = C} = S = get(statistics),
	LK = {lock, Key},
	case get(LK) of
		{Lock, _, _Expire, _LockMulti, _Pid} = E ->
			put(statistics, S#statistics{lock0 = C + 1, lock0_info = {Key, E}}),
			erase(LK);
		E ->
			put(statistics, S#statistics{
				lock0 = C + 1, lock0_info = {lock_error, {Key, E}}})
	end.

% 执行动作
handle(M, A, Action, Key, V, Lock, LockTime, LockMulti, From, Now) ->
	{FromPid, _} = From,
	LK = {lock, Key},
	case get(LK) of
		undefined when LockTime > 0 ->
			statistics_lock(Action, 1, 0),
			put(LK, {Lock, Now, Now + LockTime, LockMulti, FromPid}),
			M:Action(A, Key, V, Now, From);
		undefined ->
			statistics_lock(Action, 0, 0),
			M:Action(A, Key, V, Now, From);
		{_Locked, _Old, Expire, _LockMulti, _Pid} = Info when Now >= Expire ->
			if
				LockTime > 0 ->
					statistics_lock_timeout(Action, {Key, Info}),
					put(LK, {Lock, Now, Now + LockTime, LockMulti, FromPid});
				true ->
					erase(LK)
			end,
			M:Action(A, Key, V, Now, From);
		{Lock, Old, _Expire, _LockMulti, _Pid} ->
			if
				LockTime > 0 ->
					statistics_lock(Action, 1, Now - Old),
					put(LK, {Lock, Now, Now + LockTime, LockMulti, FromPid});
				true ->
					statistics_lock(Action, 0, Now - Old),
					erase(LK)
			end,
			M:Action(A, Key, V, Now, From);
		{Locked, _Old, Expire, Multi, Pid} = Info ->
			#statistics{lock_error = C} = S = get(statistics),
			put(statistics, S#statistics{
				lock_error = C + 1, lock_error_info = {Key, Info}}),
			z_lib:reply(From, {lock_error, Locked, Expire - Now, Multi, Pid})
	end.

% 超时锁统计
statistics_lock_timeout(read, Info) ->
	#statistics{read_lock = C1, lock = C2,
		lock_timeout = C3} = S = get(statistics),
	put(statistics, S#statistics{read_lock = C1 + 1, lock = C2 + 1,
		lock_timeout = C3 + 1, lock_timeout_info = Info});
statistics_lock_timeout(write, Info) ->
	#statistics{write = C1, lock = C2,
		lock_timeout = C3} = S = get(statistics),
	put(statistics, S#statistics{write = C1 + 1, lock = C2 + 1,
		lock_timeout = C3 + 1, lock_timeout_info = Info});
statistics_lock_timeout(lock, Info) ->
	#statistics{lock = C2,
		lock_timeout = C3} = S = get(statistics),
	put(statistics, S#statistics{lock = C2 + 1,
		lock_timeout = C3 + 1, lock_timeout_info = Info});
statistics_lock_timeout(delete, Info) ->
	#statistics{delete = C1, lock = C2,
		lock_timeout = C3} = S = get(statistics),
	put(statistics, S#statistics{delete = C1 + 1, lock = C2 + 1,
		lock_timeout = C3 + 1, lock_timeout_info = Info});
statistics_lock_timeout(dyn_read, Info) ->
	#statistics{dyn_read = C1, lock = C2,
		lock_timeout = C3} = S = get(statistics),
	put(statistics, S#statistics{dyn_read = C1 + 1, lock = C2 + 1,
		lock_timeout = C3 + 1, lock_timeout_info = Info});
statistics_lock_timeout(dyn_write, Info) ->
	#statistics{dyn_write = C1, lock = C2,
		lock_timeout = C3} = S = get(statistics),
	put(statistics, S#statistics{dyn_write = C1 + 1, lock = C2 + 1,
		lock_timeout = C3 + 1, lock_timeout_info = Info}).

% 锁统计
statistics_lock(lock, LockCount, LockTime) ->
	#statistics{lock = C1, lock_time = C2} = S = get(statistics),
	put(statistics, S#statistics{lock = C1 + LockCount, lock_time = C2 + LockTime});
statistics_lock(read, LockCount, LockTime) ->
	#statistics{read_lock = C1, lock = C2, lock_time = C3} = S = get(statistics),
	put(statistics, S#statistics{read_lock = C1 + 1,
		lock = C2 + LockCount, lock_time = C3 + LockTime});
statistics_lock(write, LockCount, LockTime) ->
	#statistics{write = C1, lock = C2, lock_time = C3} = S = get(statistics),
	put(statistics, S#statistics{write = C1 + 1,
		lock = C2 + LockCount, lock_time = C3 + LockTime});
statistics_lock(delete, LockCount, LockTime) ->
	#statistics{delete = C1, lock = C2, lock_time = C3} = S = get(statistics),
	put(statistics, S#statistics{delete = C1 + 1,
		lock = C2 + LockCount, lock_time = C3 + LockTime});
statistics_lock(dyn_read, LockCount, LockTime) ->
	#statistics{dyn_read = C1, lock = C2, lock_time = C3} = S = get(statistics),
	put(statistics, S#statistics{dyn_read = C1 + 1,
		lock = C2 + LockCount, lock_time = C3 + LockTime});
statistics_lock(dyn_write, LockCount, LockTime) ->
	#statistics{dyn_write = C1, lock = C2, lock_time = C3} = S = get(statistics),
	put(statistics, S#statistics{dyn_write = C1 + 1,
		lock = C2 + LockCount, lock_time = C3 + LockTime}).

%整理超时锁
collate(Now) ->
	case [{Key, Info} || {{lock, Key} = LK,
		{_, _, Expire, _, _} = Info} <- get(), Now > Expire, erase(LK) =/= ok] of
		[H | T] ->
			#statistics{lock_timeout = C} = S = get(statistics),
			put(statistics, S#statistics{
				lock_timeout = C + length(T) + 1, lock_timeout_info = H});
		[] ->
			none
	end.

%整理超时锁
collate(#state{collate = C} = State, Now) when C < Now ->
	State;
collate(State, Now) ->
	collate(Now),
	State#state{collate = Now + ?COLLATE_TIMEOUT}.

batch_write([{_, _, _, _} = KVVT|T], M, A) ->
	M:backup(A, KVVT),
	batch_write(T, M, A);
batch_write([], _, _) ->
	ok.

read_(Pid, Key, Lock, LockTime, Timeout, Now) when Timeout > 1 ->
	Node=node(),
	Ref = make_ref(),
	call_send(Pid, {self(), Ref}, {read, Key, Lock, LockTime, 0}),
	receive
		{Ref, {ok, _Value, _Vsn, _Time} = R} ->
			R;
		{Ref, {lock_error, Locked, Expire, Multi, Pid}} ->
			if
				Expire >= ?LOCK_INFINITY_TIME ->
					erlang:error({lock_error, Locked, Expire, Multi, Pid, Node});
				Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
					timer:sleep(?LOCK_SLEEP_TIME),
					Now1 = zm_time:now_millisecond(),
					read_(Pid, Key, Lock, LockTime,
						Timeout + Now - Now1, Now1);
				true ->
					erlang:error({lock_error, Locked, Expire, Multi, Pid, Node})
			end;
		{Ref, {error, R}} ->
			erlang:error({R, Node})
		after Timeout ->
			erlang:error({timeout, Node})
	end;
read_(_Table, _Key, _Lock, _LockTime, _Timeout, _Now) ->
	erlang:error({timeout, none}).

call_send(Dest, From, Msg) ->
	z_lib:send(Dest, {'$gen_call', From, Msg}).

% 迭代结果列表
iterate_list(SortType, [{Node, {ok, R}} | T], Nodes, L) ->
	iterate_list(SortType, T, sb_trees:insert(Node, {ok, length(R)}, Nodes),
		z_lib:SortType([{K, Node} || K <- R], lists:reverse(L)));
iterate_list(SortType, [{Node, {limit, R}} | T], Nodes, L) ->
	iterate_list(SortType, T, sb_trees:insert(Node, {limit, length(R)}, Nodes),
		z_lib:SortType([{K, Node} || K <- R], lists:reverse(L)));
iterate_list(_SortType, [{error, Reason} | _T], _Nodes, _L) ->
	erlang:error(Reason);
iterate_list(_SortType, [], Nodes, L) ->
	{Nodes, L}.

sort_type(ascending) ->
	merge_order_desc;
sort_type(descending) ->
	merge_order;
sort_type({ascending, _, _}) ->
	merge_order_desc;
sort_type({descending, _, _}) ->
	merge_order;
sort_type({_, Start, _, End}) when Start < End ->
	merge_order_desc;
sort_type({_, _Start, _, _End}) ->
	merge_order.

iterate_next_(Pid, KeyRange, Nodes, {{Key, _Vsn, _Time} = K, Node}, T) ->
	case sb_trees:get(Node, Nodes, none) of
		{limit, C} when C > 1 ->
			{ok, K, {Pid, KeyRange,
				sb_trees:enter(Node, {limit, C - 1}, Nodes), T}};
		{limit, _} ->
			iterate_node(K, Node, Pid, iterate_range_modify(KeyRange, Key), Nodes, T);
		{ok, C} ->
			{ok, K, {Pid, KeyRange,
				sb_trees:enter(Node, {ok, C - 1}, Nodes), T}}
	end.

iterate_range_modify(ascending, Key) ->
	{ascending, open, Key};
iterate_range_modify(descending, Key) ->
	{descending, open, Key};
iterate_range_modify({ascending, _, _}, Key) ->
	{ascending, open, Key};
iterate_range_modify({descending, _, _}, Key) ->
	{descending, open, Key};
iterate_range_modify({_, _Start, Open, End}, Key) ->
	{open, Key, Open, End}.

iterate_node(Key, Node, Pid, KeyRange, Nodes, L) ->
	case gen_server:call(Pid, {iterate_next, KeyRange, none, attr, attr, ?ITERATE_KEY_COUNT}, ?ITERATE_TIMEOUT) of
		{ok, R} ->
			SortType = sort_type(KeyRange),
			{ok, Key, {Pid, KeyRange,
				sb_trees:enter(Node, {ok, length(R)}, Nodes),
				z_lib:SortType([{K, Node} || K <- R], lists:reverse(L))}};
		{limit, R} ->
			SortType = sort_type(KeyRange),
			{ok, Key, {Pid, KeyRange,
				sb_trees:enter(Node, {limit, length(R)}, Nodes),
				z_lib:SortType([{K, Node} || K <- R], lists:reverse(L))}};
		E ->
			E
	end.

