%%@doc 基于文件的存储模块
%%```
%%% 数据表的格式为：{Key, {File, Loc or {BlockSize, Loc}, KSize, VSize}, Vsn, Time}
%%% 负责定期整理缓存
%%% 负责根据时间切换新文件
%%% 负责根据时间做快照
%%'''
%%@end


-module(zm_storage_file).

-description("data base storage by file").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/5]).
-export([set_opts/2, get_argument/1, get_lasttime/1, count/1, clear/1, size/1, memory/1, close/2]).
-export([lock/5, read/5, write/5, delete/5, dyn_read/5, dyn_write/5, backup/2]).
-export([select/8, select/7, select_index/8]).
-export([restore/5]).
-export([snapshot/8, snapshot/9]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(NIL, '$nil').

-define(ZEROS_COUNT, 5).

-define(TIMER_TIME, 60*60*1000).
-define(SNAPSHOT_RANDOM_TIME, 30*60*1000).

-define(CACHE_COLLATE_TIMEOUT, 20*1000).
-define(HIBERNATE_TIMEOUT, 3000).

-define(TEMP_FILE, ".snapshot").

%%%=======================RECORD=======================
-record(state, {pid, name, ets, index, table, opts, path, mod, args, cache,
	interval, snapshot, dir, reads, write, wsize, lasttime, snapshot_pid}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/5
%% Description: Starts a file storage.
%% Returns: {ok, Pid} | {error, Reason}
%% -----------------------------------------------------------------
start_link(Name, Table, Opts, Path, IgnoreError) ->
	gen_server:start_link({local, Name}, ?MODULE, {Name, Table, Opts, Path, IgnoreError}, []).

%% -----------------------------------------------------------------
%%@doc  set_opts
%% @spec  set_opts(State::state, Opts) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
set_opts(#state{pid = Pid}, Opts) ->
	gen_server:call(Pid, {set_opts, Opts}).

%% -----------------------------------------------------------------
%%@doc  get argument
%% @spec  get_argument(Pid) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
get_argument(Pid) ->
	gen_server:call(Pid, get_argument).

%% -----------------------------------------------------------------
%%@doc  get_lasttime
%% @spec  get_lasttime(State::state) -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
get_lasttime(#state{lasttime = Time}) ->
	Time.

%% -----------------------------------------------------------------
%%@doc  count
%% @spec  count(State::state) -> return()
%% where
%%  return() =  undefined | integer()
%%@end
%% -----------------------------------------------------------------
count(#state{ets = Ets}) ->
	ets:info(Ets, size).

%% -----------------------------------------------------------------
%%@doc  clear
%% @spec  clear(State::state) -> return()
%% where
%%  return() =  undefined | integer()
%%@end
%% -----------------------------------------------------------------
clear(#state{pid = Pid}) ->
	gen_server:call(Pid, clear).

%% -----------------------------------------------------------------
%%@doc  size
%% @spec  size(State::state) -> return()
%% where
%%  return() =  undefined | integer()
%%@end
%% -----------------------------------------------------------------
size(#state{ets = Ets}) ->
	ets:info(Ets, size).

%% -----------------------------------------------------------------
%%@doc  memory
%% @spec  memory(State::state) -> return()
%% where
%%  return() =  undefined | integer()
%%@end
%% -----------------------------------------------------------------
memory(#state{ets = Ets}) ->
	case ets:info(Ets, memory) of
		undefined ->
			undefined;
		Word ->
			Word * erlang:system_info(wordsize)
	end.

%% -----------------------------------------------------------------
%%@doc  close
%% @spec  close(Pid, Reason) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
close(#state{pid = Pid}, Reason) ->
	gen_server:call(Pid, {close, Reason}).

%% -----------------------------------------------------------------
%%@doc  lock
%% @spec  lock(any(), Key, Key, Now, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
lock(_, _Key, _Key, _Now, From) ->
	z_lib:reply(From, ok).

%% -----------------------------------------------------------------
%%@doc  read
%% @spec  read(State::state, Key, Any,Now, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
read(#state{pid = Pid}, Key, _, Now, From) ->
	Pid ! {read, Key, Now, From}.

%% -----------------------------------------------------------------
%%@doc  write
%% @spec  write(State::state, Key, ValueVsn, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
write(#state{pid = Pid}, Key, ValueVsn, Now, From) ->
	Pid ! {write, Key, ValueVsn, Now, From}.

%% -----------------------------------------------------------------
%%@doc  delete
%% @spec  delete(State::state, Key, Vsn, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
delete(#state{pid = Pid}, Key, Vsn, Now, From) ->
	Pid ! {delete, Key, Vsn, Now, From}.

%% -----------------------------------------------------------------
%%@doc  dyn_read
%% @spec  dyn_read(State::state, Key, Fun, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
dyn_read(#state{pid = Pid}, Key, Fun, Now, From) ->
	Pid ! {dyn_read, Key, Fun, Now, From}.

%% -----------------------------------------------------------------
%%@doc  dyn_write
%% @spec  dyn_write(State::state, Key, FunVsn, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
dyn_write(#state{pid = Pid}, Key, FunVsn, Now, From) ->
	Pid ! {dyn_write, Key, FunVsn, Now, From}.

%% -----------------------------------------------------------------
%%@doc  backup
%% @spec  backup(State::state, KVVT) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
backup(#state{pid = Pid}, KVVT) ->
	gen_server:cast(Pid, {backup, KVVT}).

%% -----------------------------------------------------------------
%%@doc  select
%% @spec  select(State::state, KeyRange, Filter, FilterType, ResultType, NL, Limit, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
select(#state{pid = Pid, ets = Ets, table = Table,
	mod = Mod, args = Args, cache = Cache}, KeyRange, Filter, FilterType, ResultType, NL, Limit, From) ->
	spawn(fun() ->
		try zm_storage_util:select(Ets, Table, KeyRange,
				Filter, FilterType, ResultType, NL, Limit, node(),
				fun(Key, Type) -> storage_get(
					Pid, Ets, Mod, Args, Cache, Key, Type) end) of
				R -> z_lib:reply(From, R)
		catch
			Error:Reason ->
				z_lib:reply(From, {error,
					{Error, Reason, erlang:get_stacktrace()}})
		end
	end).

%% -----------------------------------------------------------------
%%@doc  select local
%% @spec  select(State::state, KeyRange, Filter, FilterType, ResultType, Limit, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
select(#state{pid = Pid, ets = Ets, table = Table,
	mod = Mod, args = Args, cache = Cache}, KeyRange, Filter, FilterType, ResultType, Limit, From) ->
	spawn(fun() ->
		try zm_storage_util:select(Ets, Table, KeyRange,
				Filter, FilterType, ResultType, Limit,
				fun(Key, Type) -> storage_get(
					Pid, Ets, Mod, Args, Cache, Key, Type) end) of
				R -> z_lib:reply(From, R)
		catch
			Error:Reason ->
				z_lib:reply(From, {error,
					{Error, Reason, erlang:get_stacktrace()}})
		end
	end).

%% -----------------------------------------------------------------
%%@doc  select_index
%% @spec  select_index(State::state(), ValueKeyRange, Filter, FilterType, ResultType, NL, Limit, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
select_index(#state{pid = Pid}, ValueKeyRange, Filter, FilterType, ResultType, NL, Limit, From) ->
	Pid ! {select_index, ValueKeyRange, Filter, FilterType, ResultType, NL, Limit, From}.

%% -----------------------------------------------------------------
%%@doc  restore
%% @spec  restore(State::state, Key, ValueVsn, Time, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
restore(#state{pid = Pid}, Key, ValueVsn, Time, From) ->
	Pid ! {restore, Key, ValueVsn, Time, From}.

%% -----------------------------------------------------------------
%%@doc  给整个表做快照，先休眠0-30分钟的随机时间
%% @spec  snapshot(Ets, Path, Mod, Args, Cache, Reads, Parent, Dir) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
snapshot(Ets, Path, Mod, Args, Cache, Reads, Parent, Dir) ->
	timer:sleep(z_lib:random(0, ?SNAPSHOT_RANDOM_TIME)),
	S = pid_to_list(self()),
	Temp = filename:join(Path,
		z_lib:char_replace(S, [$<, $[, $>, $]]) ++ ?TEMP_FILE),
	case zm_file:start_link(Temp, rw) of
		{ok, Pid} ->
			%因为键位置表的数据量巨大，并要发给存储进程，所以使用ets来传递
			TempTable = snapshot_next(Ets, ets:first(Ets),
				Mod, Args, Cache, Dir, Reads, Pid, 0, ets:new(snapshot, [])),
			ok = zm_file:flush(Pid),
			ets:give_away(TempTable, Parent, {snapshot_ok, Dir, Pid});
			% 正常结束，无需unlink
		Error ->
			error_logger:error_report({?MODULE, snapshot, {Path, Dir}, Error})
	end.

%% -----------------------------------------------------------------
%%@doc  指定休眠时间，给整个表做快照
%% @spec  snapshot(Ets, Path, Mod, Args, Cache, Reads, Parent, Dir, SleepTime) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
snapshot(Ets, Path, Mod, Args, Cache, Reads, Parent, Dir, SleepTime) ->
	timer:sleep(SleepTime),
	S = pid_to_list(self()),
	Temp = filename:join(Path,
		z_lib:char_replace(S, [$<, $[, $>, $]]) ++ ?TEMP_FILE),
	case zm_file:start_link(Temp, rw) of
		{ok, Pid} ->
			%因为键位置表的数据量巨大，并要发给存储进程，所以使用ets来传递
			TempTable = snapshot_next(Ets, ets:first(Ets),
				Mod, Args, Cache, Dir, Reads, Pid, 0, ets:new(snapshot, [])),
			ok = zm_file:flush(Pid),
			ets:give_away(TempTable, Parent, {snapshot_ok, Dir, Pid});
			% 正常结束，无需unlink
		Error ->
			error_logger:error_report({?MODULE, snapshot, {Path, Dir}, Error})
	end.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Name, Table, Opts, Path, IgnoreError}) ->
	[Interval, Snapshot, Length] = z_lib:get_values(Opts, [{interval, 0}, {snapshot, 0}, {length, 0}]),
	{ok, FL} = file:list_dir(Path),
	{Dir, RFiles, WFile} = get_number_file(Path, FL, []),
	[IndexEts, Sizeout, Timeout] = case z_lib:get_value(Opts, index, false) of
		true ->
			[ets:new(z_lib:to_atom(Name, "$index"), [ordered_set, protected]), infinity, infinity];
		_ ->
			[0 | z_lib:get_values(Opts, [{cache_size, 0}, {cache_time, 0}])]
	end,
	Ets = ets:new(Name, [ordered_set, protected]),
	Mod = if
		Length > 0 ->
			z_lib:to_atom(?MODULE, "_fixed_length");
		true ->
			z_lib:to_atom(?MODULE, "_variable_length")
	end,
	Args = Mod:init(Ets, Length),
	put(lasttime, 0),

	Cache = zm_cache:new(Sizeout, Timeout),
	{RL, {WPid, Size}} = case IndexEts of
		0 ->
			{[{R, load_file(Path, Mod, Args, {false, none}, Dir, R, IgnoreError)} || R <- RFiles],
				load_file(Path, Mod, Args, {true, Cache}, Dir, WFile, IgnoreError)};
		_ ->
			{CacheEts, _, _, _} = Cache,
			RR = {[{R, load_file(Path, Mod, Args, {false, Cache}, Dir, R, IgnoreError)} || R <- RFiles],
				load_file(Path, Mod, Args, {true, Cache}, Dir, WFile, IgnoreError)},
			% 遍历cache表，将数据放入到索引表中
			F = fun
				(A, K, V) ->
					ets:insert(IndexEts, {{V, K}}),
					A
			end,
			z_lib:ets_value_select(CacheEts, 2, F, none),
			RR
	end,
	Reads = sb_trees:from_orddict(lists:sort([{R, P} || {R, {P, _S}} <- RL])),
	[erlang:start_timer(?CACHE_COLLATE_TIMEOUT, self(), cache_collate) ||
		zm_cache:memory_size(Cache) > 0],
	add_timer(Interval, Snapshot),
	{ok, #state{pid = self(), name = Name, ets = Ets, index = IndexEts,
		table = Table, path = Path, mod = Mod, args = Args, cache = Cache,
		interval = Interval, snapshot = Snapshot, dir = Dir,
		reads = sb_trees:insert(WFile, WPid, Reads), write = {WFile, WPid},
		wsize = Size, lasttime = get(lasttime), snapshot_pid = none}, hibernate}.

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
handle_call({set_opts, Opts}, _From, #state{cache = Cache} = State) ->
	[Sizeout, Timeout, Interval, Snapshot] = z_lib:get_values(Opts,
		[{cache_size, 0}, {cache_time, 0}, {interval, 0}, {snapshot, 0}]),
	OldSize = zm_cache:memory_size(Cache),
	NewCache = zm_cache:reset(Cache, Sizeout, Timeout),
	[erlang:start_timer(?CACHE_COLLATE_TIMEOUT, self(), cache_collate) ||
		zm_cache:memory_size(NewCache) > 0 andalso OldSize =< 0],
	{reply, ok, State#state{cache = NewCache,
		interval = Interval, snapshot = Snapshot}, ?HIBERNATE_TIMEOUT};

handle_call(get_argument, _From, State) ->
	{reply, State, State, ?HIBERNATE_TIMEOUT};

handle_call(clear, _From, #state{ets = Ets, index = IndexEts, cache = Cache, snapshot_pid = SP1} = State) ->
	case is_snapshot_work(SP1) of
		true ->
			error_logger:error_report({?MODULE, clear, {error, snapshoting}}),
			{reply, 0, State};
		false ->
			C = ets:info(Ets, size),
			ets:delete_all_objects(Ets),
			case IndexEts of
				0 ->
					ignore;
				_ ->
					ets:delete_all_objects(IndexEts)
			end,
			zm_cache:clear(Cache),
			{_, State1, _} = handle_info({timeout, none, {snapshot, 0}}, State#state{reads = sb_trees:empty()}),
			{reply, C, State1}
	end;

handle_call({close, Reason}, _From, #state{reads = Reads} = State) ->
	[zm_file:close(Pid, Reason) || {_, Pid} <- sb_trees:to_list(Reads)],
	{stop, Reason, ok, State};

handle_call(get_reads, _From, #state{reads = Reads} = State) ->
	{reply, Reads, State, ?HIBERNATE_TIMEOUT};

handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({backup, Old, {Key, Value, Vsn, Time}}, #state{ets = Ets, index = IndexEts} = State) when IndexEts =/= 0 ->
	ets:delete(IndexEts, {Old, Key}),
	ets:insert(IndexEts, {{Value, Key}}),
	case ets:lookup(Ets, Key) of
		[] ->
			{noreply, backup_write(Key, Value, Vsn, Time, none, State),
			?HIBERNATE_TIMEOUT};
		[{_, Info, _Vsn, _T}] ->
			{noreply, backup_write(Key, Value, Vsn, Time, Info, State),
			?HIBERNATE_TIMEOUT}
	end;
handle_cast({backup_delete, Key, Old, Time}, #state{ets = Ets, index = IndexEts} = State) when IndexEts =/= 0 ->
	ets:delete(IndexEts, {Old, Key}),
	case ets:lookup(Ets, Key) of
		[] ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[{_, Info, _Vsn, _T}] ->
			{noreply, backup_delete(Key, Time, Info, State), ?HIBERNATE_TIMEOUT}
	end;
handle_cast({backup, {Key, Value, Vsn, Time}}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Key) of
		[] ->
			{noreply, backup_write(Key, Value, Vsn, Time, none, State),
			?HIBERNATE_TIMEOUT};
		[{_, Info, _Vsn, _T}] ->
			{noreply, backup_write(Key, Value, Vsn, Time, Info, State),
			?HIBERNATE_TIMEOUT}
	end;
handle_cast({backup_delete, Key, Time}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Key) of
		[] ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[{_, Info, _Vsn, _T}] ->
			{noreply, backup_delete(Key, Time, Info, State), ?HIBERNATE_TIMEOUT}
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
handle_info({read, Key, _Now, From},
	#state{ets = Ets, table = Table, mod = Mod, args = Args, cache = Cache,
	reads = Reads} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					zm_storage_util:read(Value, Vsn, Time, From);
				none ->
					Mod:send_read(Args, Reads, Key, Info, Vsn, Time, From)
			end;
		[] ->
			case zm_db:default(Table) of
				{ok, DefVal} ->
					%如果读时关键字不存在, 则采用当前表的默认值替代
					z_lib:reply(From, {ok, DefVal, 0, 0});
				_ ->
					z_lib:reply(From, {ok, ?NIL, 0, 0})
			end
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({write, Key, {Value, 0}, Now, From}, #state{ets = Ets, cache = Cache} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, _Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Old} ->
					{noreply, storage_write(Key, Value, Old, Vsn, Now, Info, From, State)};
				_ ->
					{noreply, storage_write(Key, Value, Vsn, Now, Info, From, State)}
			end;
		[] ->
			{noreply, storage_write(Key, Value, 1, Now, none, From, State)}
	end;
handle_info({write, Key, {Value, 1}, Now, From}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Key) of
		[] ->
			{noreply, storage_write(Key, Value, 1, Now, none, From, State)};
		[{_, _, Vsned, _Time}] ->
			z_lib:reply(From, {vsn_error, Vsned}),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;
handle_info({write, Key, {Value, Vsn}, Now, From}, #state{ets = Ets, cache = Cache} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, _Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Old} ->
					{noreply, storage_write(Key, Value, Old, Vsn, Now, Info, From, State)};
				_ ->
					{noreply, storage_write(Key, Value, Vsn, Now, Info, From, State)}
			end;
		[{_, _, Vsned, _Time}] ->
			z_lib:reply(From, {vsn_error, Vsned}),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[] ->
			z_lib:reply(From, {vsn_error, 0}),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({delete, Key, 0, Now, From}, #state{ets = Ets, cache = Cache} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, _Vsn, _T}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Old} ->
					{noreply, storage_delete(Key, Old, Now, Info, From, State)};
				_ ->
					{noreply, storage_delete(Key, 0, Now, Info, From, State)}
			end;
		[] ->
			z_lib:reply(From, {not_exist_error, ?NIL}),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;
handle_info({delete, Key, Vsn, Now, From}, #state{ets = Ets, cache = Cache} = State) ->
	case ets:lookup(Ets, Key) of
		[] ->
			z_lib:reply(From, {not_exist_error, ?NIL}),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[{_, Info, Vsn, _T}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Old} ->
					{noreply, storage_delete(Key, Old, Now, Info, From, State)};
				_ ->
					{noreply, storage_delete(Key, 0, Now, Info, From, State)}
			end;
		[{_, _Info, Vsned, _T}] ->
			z_lib:reply(From, {vsn_error, Vsned}),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({select_index, ValueKeyRange, Filter, FilterType, ResultType, NL, Limit, From},
	#state{ets = Ets, index = IndexEts, table = Table} = State) ->
	try zm_storage_util:select_index(IndexEts, Table, ValueKeyRange,
			Filter, FilterType, ResultType, NL, Limit, node(),
			fun(Key, Type) -> storage_index_get(Ets, Key, Type) end) of
			R -> z_lib:reply(From, R)
	catch
		Error:Reason ->
			z_lib:reply(From, {error,
				{Error, Reason, erlang:get_stacktrace()}})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({dyn_read, Key, Fun, _Now, From}, #state{ets = Ets,
	mod = Mod, args = Args, cache = Cache, reads = Reads} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					zm_storage_util:dyn_read(Key, Value, Vsn, Time, Fun, From);
				none ->
					Mod:send_dyn_read(Args, Reads, Key, Info, Vsn, Time, Fun, From)
			end;
		[] ->
			zm_storage_util:dyn_read(Key, ?NIL, 0, 0, Fun, From)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({dyn_write, Key, Fun, Now, From}, #state{ets = Ets,
	mod = Mod, args = Args, cache = Cache, reads = Reads} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					{noreply, dyn_write(Key, Value, 1, 0, Info, Fun, Now, From, State)};
				none ->
					Mod:send_dyn_write(Args, Reads, Key, Info, Vsn, Time, Fun, From),
					{noreply, State, ?HIBERNATE_TIMEOUT}
			end;
		[] ->
			{noreply, dyn_write(Key, ?NIL, 0, 0, none, Fun, Now, From, State)}
	end;

%文件进程返回的消息
handle_info({{read, _Key, _Vsn, _Time, From}, ok, eof}, State) ->
	z_lib:reply(From, {error, eof}),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({{read, Key, Vsn, Time, From}, ok, Bin},
	#state{ets = Ets, mod = Mod, args = Args, cache = Cache, reads = Reads} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, _, Vsn, Time}] ->
			try binary_to_term(Bin) of
				Value ->
					zm_cache:set(Cache, Key, Value),
					zm_storage_util:read(Value, Vsn, Time, From)
			catch
				_:Error ->
					z_lib:reply(From, {error, Error})
			end;
		[{_, Info, Vsn1, Time1}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					zm_storage_util:read(Value, Vsn1, Time1, From);
				none ->
					Mod:send_read(Args, Reads, Key, Info, Vsn1, Time1, From)
			end;
		[] ->
			z_lib:reply(From, {ok, ?NIL, 0, 0})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({{read, _Key, _Vsn, _Time, From}, error, Error}, State) ->
	z_lib:reply(From, {error, Error}),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({{write, _Key, _Vsn, _Time, _From}, ok}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({{write, _Key, _Vsn, _Time, _From}, _Error} = Reason, _State) ->
	exit(Reason);

handle_info({{delete, _Key, _Time, _From}, ok}, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({{delete, _Key, _Time, _From}, _Error} = Reason, _State) ->
	exit(Reason);

handle_info({{dyn_read, Key, Vsn, Time, Fun, From}, ok, Bin},
	#state{ets = Ets, mod = Mod, args = Args, cache = Cache, reads = Reads} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, _, Vsn, Time}] ->
			try binary_to_term(Bin) of
				Value ->
					zm_cache:set(Cache, Key, Value),
					zm_storage_util:dyn_read(Key, Value, Vsn, Time, Fun, From)
			catch
				_:Error ->
					z_lib:reply(From, {error, Error})
			end;
		[{_, Info, Vsn1, Time1}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					zm_storage_util:dyn_read(Key, Value, Vsn1, Time1, Fun, From);
				none ->
					Mod:send_dyn_read(Args, Reads, Key, Info, Vsn1, Time1, Fun, From)
			end;
		[] ->
			zm_storage_util:dyn_read(Key, ?NIL, 0, 0, Fun, From)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({{dyn_read, _Key, _Vsn, _Time, _Fun, _From}, _Error} = Reason, _State) ->
	exit(Reason);

handle_info({{dyn_write, Key, Vsn, Time, Fun, From}, ok, Bin},
	#state{ets = Ets,  mod = Mod, args = Args, cache = Cache, reads = Reads} = State) ->
	State1 = case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, Time}] ->
			try binary_to_term(Bin) of
				Value ->
					zm_cache:set(Cache, Key, Value),
					dyn_write(Key, Value, Vsn, Time, Info,
						Fun, zm_time:now_millisecond(), From, State)
			catch
				_:Error ->
					z_lib:reply(From, {error, Error}),
					State
			end;
		[{_, Info, Vsn1, Time1}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					dyn_write(Key, Value, Vsn1, Time1, Info,
						Fun, zm_time:now_millisecond(), From, State);
				none ->
					Mod:send_dyn_write(Args, Reads, Key, Info, Vsn1, Time1, Fun, From),
					State
			end;
		[] ->
			dyn_write(Key, ?NIL, 0, 0, none,
				Fun, zm_time:now_millisecond(), From, State)
	end,
	{noreply, State1, ?HIBERNATE_TIMEOUT};

handle_info({{dyn_write, _Key, _Vsn, _Time, _Fun, _From}, _Error} = Reason, _State) ->
	exit(Reason);

handle_info({restore, Key, {Value, Vsn}, Time, From}, State) ->
	{noreply, storage_restore(Key, Value, Vsn, Time, none, From, State)};

handle_info({cache_update_time, Cache, Key}, State) ->
	zm_cache:update(Cache, Key),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({timeout, _Ref, cache_collate}, #state{cache = Cache} = State) ->
	zm_cache:collate(Cache),
	erlang:start_timer(?CACHE_COLLATE_TIMEOUT, self(), cache_collate),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({timeout, _Ref, interval_snapshot},
	#state{interval = Interval, snapshot = Snapshot} = State) ->
	add_timer(Interval, Snapshot),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({timeout, _Ref, interval}, #state{path = Path, mod = Mod, args = Args,
	dir = Dir, reads = Reads, write = {File, _Pid}, snapshot_pid = SP} = State) ->
	case is_snapshot_work(SP) of
		true ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		false ->
			File1 = File + 1,
			F = get_filename(Path, Dir, File1),
			case zm_file:start_link(F, rw) of
				{ok, Pid} ->
					Mod:clear(Args),
					{noreply, State#state{reads = sb_trees:insert(File1, Pid, Reads),
						write = {File1, Pid}, wsize = 0}, ?HIBERNATE_TIMEOUT};
				{error, Reason} ->
					error_logger:error_report({?MODULE, interval, F, Reason}),
					{noreply, State, ?HIBERNATE_TIMEOUT}
			end
	end;

handle_info({timeout, _Ref, snapshot}, #state{ets = Ets, path = Path,
	mod = Mod, args = Args, cache = Cache, dir = Dir, reads = Reads, write = {File, _Pid}} = State) ->
	Dir1 = File + 1,
	File1 = File + 2,
	F = get_filename(Path, Dir, File1),
	case zm_file:start_link(F, rw) of
		{ok, Pid} ->
			Mod:clear(Args),
			SP = spawn(?MODULE, snapshot,
				[Ets, Path, Mod, Args, Cache, Reads, self(), Dir1]),
			{noreply, State#state{reads = sb_trees:insert(File1, Pid, Reads),
				write = {File1, Pid}, wsize = 0, snapshot_pid = SP},
				?HIBERNATE_TIMEOUT};
		{error, Reason} ->
			error_logger:error_report({?MODULE, snapshot, F, Reason}),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({timeout, _Ref, {snapshot, SleepTime}}, #state{ets = Ets, path = Path,
	mod = Mod, args = Args, cache = Cache, dir = Dir, reads = Reads, write = {File, _Pid}} = State) ->
	Dir1 = File + 1,
	File1 = File + 2,
	F = get_filename(Path, Dir, File1),
	case zm_file:start_link(F, rw) of
		{ok, Pid} ->
			Mod:clear(Args),
			SP = spawn(?MODULE, snapshot,
				[Ets, Path, Mod, Args, Cache, Reads, self(), Dir1, SleepTime]),
			{noreply, State#state{reads = sb_trees:insert(File1, Pid, Reads),
				write = {File1, Pid}, wsize = 0, snapshot_pid = SP},
				?HIBERNATE_TIMEOUT};
		{error, Reason} ->
			error_logger:error_report({?MODULE, snapshot, F, Reason}),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info(timeout, State) ->
	{noreply, State, hibernate};

handle_info({'ETS-TRANSFER', TempTable, _From, {snapshot_ok, Dir, TPid}},
	#state{ets = Ets, path = Path, reads = Reads, write = {File, Pid} = Write} = State) ->
	case zm_file:rename(TPid, get_filename(Path, Dir, Dir), r) of
		ok ->
			%重命名临时文件进程，链接文件进程
			link(TPid),
			%重命名写文件进程
			ok = zm_file:rename(Pid, get_filename(Path, Dir, File), rw),
			%重置表中数据的进程、文件和位置
			z_lib:ets_select(TempTable, fun reset_info/2, {Ets, Dir}),
			ets:delete(TempTable),
			%修改当前目录，重置读进程表，关闭原来的读进程
			{noreply, State#state{dir = Dir,
				reads = reset_reads(Reads, {Dir, TPid}, Write)},
				?HIBERNATE_TIMEOUT};
		_E ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

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
% 获得文件名
get_filename(Path, N) ->
	filename:join(Path, z_lib:integer_to_list(N, ?ZEROS_COUNT)).

% 获得文件名
get_filename(Path, N, C) ->
	filename:join(Path, filename:join(z_lib:integer_to_list(N, ?ZEROS_COUNT),
		z_lib:integer_to_list(C, ?ZEROS_COUNT))).

% 获得最大的数字路径，和路径中的数字文件（读文件，由小到大排列），及最大的数字文件（写文件），
get_number_file(Path, [], []) ->
	ok = z_lib:make_dir(get_filename(Path, 1)),
	{1, [], 1};
get_number_file(Path, [], L) ->
	C = lists:max(L),
	Dir = filename:join(Path, z_lib:integer_to_list(C, ?ZEROS_COUNT)),
	{ok, FL} = file:list_dir(Dir),
	number_file(C, FL, []);
get_number_file(Path, [F | T], L) ->
	try list_to_integer(F) of
		I -> get_number_file(Path, T, [I | L])
	catch
		_:_ ->
			get_number_file(Path, T, L)
	end.

% 获得最大的数字路径，和路径中的数字文件（读文件，由小到大排列），及最大的数字文件（写文件），
number_file(C, [], []) ->
	{C, [], 1};
number_file(C, [], [I]) ->
	{C, [], I};
number_file(C, [], L) ->
	Max = lists:max(L),
	{C, lists:sort(lists:delete(Max, L)), Max};
number_file(C, [F | T], L) ->
	try list_to_integer(F) of
		I -> number_file(C, T, [I | L])
	catch
		_:_ ->
			number_file(C, T, L)
	end.

% 加载文件中的数据
load_file(Path, Mod, Args, Cache, DirCount, FileCount, IgnoreError) ->
	File = get_filename(Path, DirCount, FileCount),
	Type = case Cache of
		{false, _} -> r;
		_ -> rw
	end,
	case zm_file:start_link(File, Type) of
		{ok, Pid} ->
			Size = Mod:load(Args, Cache, Pid, FileCount, IgnoreError),
			% 每个打开的文件端口中读内存缓冲都是独立的，
			% 为了防止打开太多文件造成内存占用过大的问题，重新打开一次
			ok = zm_file:rename(Pid, File, Type),
			{Pid, Size};
		{error, Reason} ->
			erlang:error(Reason)
	end.

%根据小时时间增加操作定时
add_timer(Interval, Snapshot) ->
	erlang:start_timer(?TIMER_TIME, self(), interval_snapshot),
	Second = zm_time:now_second() + 3600,
	Time = erlang:universaltime_to_localtime(z_lib:second_to_datetime(Second)),
	case match_timer(Time, Snapshot) of
		false ->
			case match_timer(Time, Interval) of
				false ->
					false;
				I ->
					erlang:start_timer(I * 1000, self(), interval)
			end;
		I ->
			erlang:start_timer(I * 1000, self(), snapshot)
	end.

%匹配指定的定时时间
match_timer(_Time, none) ->
	false;
match_timer({Date, {H, M, S}}, {D, Hour}) ->
	case zm_dtimer:match_date(Date, D) of
		true ->
			case zm_dtimer:match_number(H, Hour) of
				true ->
					3600 - 60 * M - S;
				false ->
					false
			end;
		false ->
			false
	end.

% 写入数据，并回应
storage_write(Key, Value, Vsn, Time, Info, From, #state{index = IndexEts} = State) ->
	case storage_write_(Key, Value, Vsn + 1, Time, Info, State) of
		{ok, State1} ->
			case IndexEts of
				0 -> ok;
				_ -> ets:insert(IndexEts, {{Value, Key}})
			end,
			z_lib:reply(From, ok),
			State1;
		{error, _Reason} = E ->
			z_lib:reply(From, E),
			State;
		E ->
			z_lib:reply(From, {error, E}),
			State
	end.

% 写入数据
storage_write_(Key, Value, Vsn, Time, Info,
	#state{name = Name, ets = Ets, index = IndexEts, table = Table, mod = Mod, args = Args,
	cache = Cache, write = Write, wsize = WSize} = State) ->
	case Mod:send_write(Args, Key, Value, Vsn, Time, Info, self(), Write, WSize) of
		{Size, Info1} ->
			ets:insert(Ets, {Key, Info1, Vsn, Time}),
			zm_cache:set(Cache, Key, Value),
			L = zm_db:active_dht_node(Table, Key, node()),
			case IndexEts of
				0 ->
					gen_server:abcast(L, Name, {backup, {Key, Value, Vsn, Time}});
				_ ->
					gen_server:abcast(L, Name, {backup, 0, {Key, Value, Vsn, Time}})
			end,
			Action = if
				Vsn =:= 2 -> insert;
				true -> update
			end,
			zm_event:notify(Table, {zm_storage, Action}, {Key, Value, Vsn, Time}),
			if
				Size =:= WSize ->
					{ok, State};
				true ->
					{ok, State#state{wsize = Size}}
			end;
		E ->
			{error, E}
	end.

% 写入数据，并回应
storage_write(Key, Value, Old, Vsn, Time, Info, From, #state{index = IndexEts} = State) ->
	case storage_write_(Old, Key, Value, Vsn + 1, Time, Info, State) of
		{ok, State1} ->
			case IndexEts of
				0 -> ok;
				_ ->
					ets:delete(IndexEts, {Old, Key}),
					ets:insert(IndexEts, {{Value, Key}})
			end,
			z_lib:reply(From, ok),
			State1;
		{error, _Reason} = E ->
			z_lib:reply(From, E),
			State;
		E ->
			z_lib:reply(From, {error, E}),
			State
	end.

% 写入数据
storage_write_(Old, Key, Value, Vsn, Time, Info,
	#state{name = Name, ets = Ets, index = IndexEts, table = Table, mod = Mod, args = Args,
	cache = Cache, write = Write, wsize = WSize} = State) ->
	case Mod:send_write(Args, Key, Value, Vsn, Time, Info, self(), Write, WSize) of
		{Size, Info1} ->
			ets:insert(Ets, {Key, Info1, Vsn, Time}),
			zm_cache:set(Cache, Key, Value),
			L = zm_db:active_dht_node(Table, Key, node()),
			case IndexEts of
				0 ->
					gen_server:abcast(L, Name, {backup, {Key, Value, Vsn, Time}});
				_ ->
					gen_server:abcast(L, Name, {backup, Old, {Key, Value, Vsn, Time}})
			end,
			Action = if
				Vsn =:= 2 -> insert;
				true -> update
			end,
			zm_event:notify(Table, {zm_storage, Action}, {Key, Value, Vsn, Time}),
			if
				Size =:= WSize ->
					{ok, State};
				true ->
					{ok, State#state{wsize = Size}}
			end;
		E ->
			{error, E}
	end.

% 写入数据，并回应
storage_restore(Key, Value, Vsn, Time, Info, From, #state{index = IndexEts} = State) ->
	case storage_write_(Key, Value, Vsn, Time, Info, State) of
		{ok, State1} ->
			case IndexEts of
				0 -> ok;
				_ -> ets:insert(IndexEts, {{Value, Key}})
			end,
			z_lib:reply(From, ok),
			State1;
		{error, _Reason} = E ->
			z_lib:reply(From, E),
			State;
		E ->
			z_lib:reply(From, {error, E}),
			State
	end.

% 删除数据，并回应
storage_delete(Key, Old, Time, Info, From, State) ->
	State1 = storage_delete(Key, Old, Time, Info, State),
	z_lib:reply(From, ok),
	State1.

% 删除数据
storage_delete(Key, Old, Time, Info, #state{name = Name,
	ets = Ets, index = IndexEts, table = Table, mod = Mod, args = Args,
	cache = Cache, write = Write, wsize = WSize} = State) ->
	Size = Mod:send_delete(Args, Key, Time, Info, self(), Write, WSize),
	ets:delete(Ets, Key),
	zm_cache:delete(Cache, Key),
	L = zm_db:active_dht_node(Table, Key, node()),
	case IndexEts of
		0 ->
			gen_server:abcast(L, Name, {backup_delete, Key, Time});
		_ ->
			case IndexEts of
				0 -> ok;
				_ -> ets:delete(IndexEts, {Old, Key})
			end,
			gen_server:abcast(L, Name, {backup_delete, Key, Old, Time})
	end,
	zm_event:notify(Table, {zm_storage, delete}, {Key, Time}),
	if
		Size =:= WSize ->
			State;
		true ->
			State#state{wsize = Size}
	end.

% 动态函数处理的数据
dyn_write(Key, Value, Vsn, Time, Info, Fun, Now, From,
	#state{table = Table, cache = Cache} = State) ->
	case zm_storage_util:dyn_write(Key, Value, Vsn, Time, Fun) of
		ok ->
			State;
		{reply, Value1} ->
			z_lib:reply(From, {ok, Value1, Vsn, Time}),
			State;
		delete when Vsn > 1 ->
			case zm_cache:get(Cache, Key) of
				{ok, Old} ->
					storage_delete(Key, Old, Now, Info, From, State);
				_ ->
					storage_delete(Key, 0, Now, Info, From, State)
			end;
		delete ->
			State;
		{delete_reply, Value1} when Vsn > 1 ->
			State1 = storage_delete(Key, 0, Now, Info, State),
			z_lib:reply(From, {ok, Value1, Vsn, Time}),
			State1;
		{delete_reply, Value1} ->
			z_lib:reply(From, {ok, Value1, Vsn, Time}),
			State;
		{update, Value1} ->
			case storage_write(Key, Value1, Vsn + 1, Now, Info, none, State) of
				{ok, State1} ->
					State1;
				E ->
					error_logger:error_report({"dyn_write, ", E,
						{Table, Key, Value, Vsn, Time, Fun, From}}),
					State
			end;
		{update_reply, Value1, Value2} ->
			case storage_write(Key, Value1, Vsn + 1, Now, Info, none, State) of
				{ok, State1} ->
					z_lib:reply(From, {ok, Value2, Vsn + 1, Now}),
					State1;
				{error, _Reason} = E ->
					z_lib:reply(From, E),
					State;
				E ->
					z_lib:reply(From, {error, E}),
					State
			end;
		{error, _Reason} = E ->
			z_lib:reply(From, E),
			State;
		E ->
			z_lib:reply(From, {error, E}),
			State
	end.

% 备份写入数据
backup_write(Key, Value, Vsn, Time, Info,
	#state{ets = Ets, mod = Mod, args = Args,
	write = Write, wsize = WSize} = State) ->
	case Mod:send_write(Args, Key, Value, Vsn, Time, Info, none, Write, WSize) of
		{Size, Info1} ->
			ets:insert(Ets, {Key, Info1, Vsn, Time}),
			if
				Size =:= WSize ->
					State;
				true ->
					State#state{wsize = Size}
			end;
		E ->
			error_logger:error_report({?MODULE, backup_write,
				{Key, Value, Vsn, Time, Info, Write, WSize}, E}),
			State
	end.

% 备份删除数据
backup_delete(Key, Time, Info,
	#state{ets = Ets, mod = Mod, args = Args,
	cache = Cache, write = Write, wsize = WSize} = State) ->
	Size =  Mod:send_delete(Args, Key, Time, Info, none, Write, WSize),
	ets:delete(Ets, Key),
	zm_cache:delete(Cache, Key),
	if
		Size =:= WSize ->
			State;
		true ->
			State#state{wsize = Size}
	end.

%用指定的键获得值和版本时间
storage_get(_Pid, Ets, _Mod, _Args, _Cache, Key, attr) ->
	case ets:lookup(Ets, Key) of
		[{_, _, Vsn, Time}] ->
			{Key, Vsn, Time};
		[] ->
			none
	end;
storage_get(Pid, Ets, Mod, Args, Cache, Key, value) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, _Vsn, _Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					{Key, Value};
				none ->
					{Key, lookup_call(Pid, Mod, Args, Info)}
			end;
		[] ->
			none
	end;
storage_get(Pid, Ets, Mod, Args, Cache, Key, all) ->
	case ets:lookup(Ets, Key) of
		[{_, Info, Vsn, Time}] ->
			case zm_cache:get(Cache, Key) of
				{ok, Value} ->
					{Key, Value, Vsn, Time};
				none ->
					{Key, lookup_call(Pid, Mod, Args, Info), Vsn, Time}
			end;
		[] ->
			none
	end.

%用指定的键获得值和版本时间
storage_index_get(Ets, {Value, Key}, all) ->
	case ets:lookup(Ets, Key) of
		[] ->
			none;
		[{_, _Value, Vsn, Time}] ->
			{Value, Key, Vsn, Time}
	end.

%用指定的键获得值和版本时间
lookup_call(Parent, Mod, Args, {File, _Loc, _KSize, _VSize} = Info) ->
	Pid = get_read_pid(Parent, File),
	try Mod:read(Args, Pid, Info) of
		Bin ->
			binary_to_term(Bin)
	catch
		exit:Reason ->
			case is_process_alive(Pid) of
				true ->
					exit(Reason);
				false ->
					%如果进程关闭了，表示被快照关闭，重新获得Pid
					Pid1 = get_read_pid(Parent, File),
					Bin = Mod:read(Args, Pid1, Info),
					binary_to_term(Bin)
			end
	end.

%获得指定的文件的读取进程
get_read_pid(Parent, File) ->
	case get(storage_file_reads) of
		Reads when is_tuple(Reads) ->
			case sb_trees:get(File, Reads, none) of
				Pid when is_pid(Pid) ->
					case is_process_alive(Pid) of
						true ->
							Pid;
						false ->
							%如果进程关闭了，表示被快照关闭，重新获得Read表
							Reads1 = gen_server:call(Parent, get_reads),
							Pid1 = sb_trees:get(File, Reads1),
							put(storage_file_reads, Reads1),
							Pid1
					end;
				none ->
					%如果没有找到，表示正在做分割或快照，重新获得Read表
					Reads1 = gen_server:call(Parent, get_reads),
					Pid = sb_trees:get(File, Reads1),
					put(storage_file_reads, Reads1),
					Pid
			end;
		_ ->
			Reads1 = gen_server:call(Parent, get_reads),
			Pid = sb_trees:get(File, Reads1),
			put(storage_file_reads, Reads1),
			Pid
	end.

% 判断快照进程是否工作
is_snapshot_work(Pid) when is_pid(Pid) ->
	is_process_alive(Pid);
is_snapshot_work(_) ->
	false.

%给该键值做快照
snapshot_next(_Ets, '$end_of_table', _, _, _, _, _, _, _, Temp) ->
	Temp;
snapshot_next(Ets, Key, Mod, Args, Cache, Dir, Reads, Write, WSize, Temp) ->
	case ets:lookup(Ets, Key) of
		[{_, {F, _Loc, _KSize, _VSize} = Info, Vsn, Time}] when F < Dir ->
			case sb_trees:get(F, Reads, none) of
				Pid when is_pid(Pid) ->
					Bin = case zm_cache:get(Cache, Key) of
						{ok, Value} ->
							term_to_binary(Value, [{minor_version, 1}]);
						none ->
							Mod:read(Args, Pid, Info)
					end,
					% 同步写入
					Size = Mod:write(Args, Key, Vsn, Time, Bin, Write, WSize),
					ets:insert(Temp, {Key, {Size, WSize}}),
					snapshot_next(Ets, ets:next(Ets, Key),
						Mod, Args, Cache, Dir, Reads, Write, WSize + Size, Temp);
				none ->
					snapshot_next(Ets, ets:next(Ets, Key),
						Mod, Args, Cache, Dir, Reads, Write, WSize, Temp)
			end;
		_ ->
			snapshot_next(Ets, ets:next(Ets, Key),
				Mod, Args, Cache, Dir, Reads, Write, WSize, Temp)
	end.

%重置表中数据的进程、文件和位置
reset_info({Ets, File} = A, {Key, Loc}) ->
	case ets:lookup(Ets, Key) of
		[{_, {F, _Loc, _KSize, _VSize}, _Vsn, _Time}] when F >= File ->
			{ok, A};
		[{_, {_F, _Loc, KSize, VSize}, Vsn, Time}] ->
			ets:insert(Ets, {Key, {File, Loc, KSize, VSize}, Vsn, Time}),
			{ok, A};
		[] ->
			{ok, A}
	end.

%重置读进程表，关闭不使用的文件进程
reset_reads(Reads, {RFile, RPid}, {WFile, WPid}) ->
	L = sb_trees:to_list(Reads),
	[zm_file:close(Pid, normal) || {File, Pid} <- L, File =/= WFile],
	sb_trees:insert(WFile, WPid, sb_trees:insert(RFile, RPid, sb_trees:empty())).
