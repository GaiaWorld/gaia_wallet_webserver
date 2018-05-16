%%% 基于内存的存储模块
%%% 数据表的格式为：{Key, Value, Version, Time}


-module(zm_storage_memory).

-description("data base storage by memory").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/5]).
-export([set_opts/2, get_argument/1, get_lasttime/1, count/1, clear/1, size/1, memory/1, close/2]).
-export([lock/5, read/5, write/5, delete/5, dyn_read/5, dyn_write/5, backup/2]).
-export([select/8, select/7, select_index/8]).
-export([restore/5]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(NIL, '$nil').

-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {pid, name, ets, index, table, path, lasttime}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/5
%% Description: Starts a memory storage.
%% Returns: {ok, Pid} | {error, Reason}
%% -----------------------------------------------------------------
start_link(Name, Table, Opts, Path, IgnoreError) ->
	gen_server:start_link({local, Name},
		?MODULE, {Name, Table, Opts, Path, IgnoreError}, []).

%% -----------------------------------------------------------------
%%@doc  set_opts
%% @spec  set_opts(any(), any()) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
set_opts(_, _) ->
	ok.

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
%% @spec  get_lasttime(State::state()) -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
get_lasttime(#state{lasttime = Time}) ->
	Time.

%% -----------------------------------------------------------------
%%@doc  count
%% @spec  count(State::state()) -> return()
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
%% @spec  read(State::state(), Key, Any, Now, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
read(#state{pid = Pid}, Key, _, Now, From) ->
	Pid ! {read, Key, Now, From}.

%% -----------------------------------------------------------------
%%@doc  write
%% @spec  write(State::state(), Key, ValueVsn, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
write(#state{pid = Pid}, Key, ValueVsn, Now, From) ->
	Pid ! {write, Key, ValueVsn, Now, From}.

%% -----------------------------------------------------------------
%%@doc  delete
%% @spec  delete(State::state(), Key, Vsn, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
delete(#state{pid = Pid}, Key, Vsn, Now, From) ->
	Pid ! {delete, Key, Vsn, Now, From}.

%% -----------------------------------------------------------------
%%@doc  dyn_read
%% @spec  dyn_read(State::state(), Key, Fun, Now, From) -> return()
%% where
%%  return() =  argument
%%@end
%% -----------------------------------------------------------------
dyn_read(#state{pid = Pid}, Key, Fun, Now, From) ->
	Pid ! {dyn_read, Key, Fun, Now, From}.

%% -----------------------------------------------------------------
%%@doc  dyn_write
%% @spec  dyn_write(State::state(), Key, FunVsn, Now, From) -> return()
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
%% @spec  select(State::state(), KeyRange, Filter, FilterType, ResultType, NL, Limit, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
select(#state{ets = Ets, table = Table}, KeyRange, Filter, FilterType, ResultType, NL, Limit, From) ->
	spawn(fun() ->
		try zm_storage_util:select(Ets, Table, KeyRange,
				Filter, FilterType, ResultType, NL, Limit, node(),
				fun(Key, Type) -> storage_get(Ets, Key, Type) end) of
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
select(#state{ets = Ets, table = Table}, KeyRange, Filter, FilterType, ResultType, Limit, From) ->
	spawn(fun() ->
		try zm_storage_util:select(Ets, Table, KeyRange,
				Filter, FilterType, ResultType, Limit,
				fun(Key, Type) -> storage_get(Ets, Key, Type) end) of
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
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Name, Table, Opts, Path, _IgnoreError}) ->
	Index = case z_lib:get_value(Opts, index, false) of
		true -> ets:new(z_lib:to_atom(Name, "$index"), [ordered_set, protected]);
		_ -> 0
	end,
	{ok, #state{pid = self(), name = Name,
		ets = ets:new(Name, [ordered_set, protected]), index = Index,
		table = Table, path = Path, lasttime = 0}, ?HIBERNATE_TIMEOUT}.

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
handle_call(get_argument, _From, State) ->
	{reply, State, State};

handle_call(clear, _From, #state{ets = Ets, index = IndexEts} = State) ->
	C = ets:info(Ets, size),
	ets:delete_all_objects(Ets),
	ets:delete_all_objects(IndexEts),
	{reply, C, State};

handle_call({close, Reason}, _From, State) ->
	{stop, Reason, ok, State};

handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({backup, {Key, Value, _, _} = KV}, #state{ets = Ets, index = IndexEts} = State) when IndexEts =/= 0 ->
	case ets:lookup(Ets, Key) of
		[{_, Old, _, _}] ->
			ets:delete(IndexEts, {Old, Key}),
			ets:insert(IndexEts, {{Value, Key}});
		_ -> ok
	end,
	ets:insert(Ets, KV),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_cast({backup_delete, Key, _Time}, #state{ets = Ets, index = IndexEts} = State) when IndexEts =/= 0 ->
	case ets:lookup(Ets, Key) of
		[{_, Old, _, _}] ->
			ets:delete(IndexEts, {Old, Key});
		_ -> ok
	end,
	ets:delete(Ets, Key),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_cast({backup, {_, _, _, _} = KV}, #state{ets = Ets} = State) ->
	ets:insert(Ets, KV),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_cast({backup_delete, Key, _Time}, #state{ets = Ets} = State) ->
	ets:delete(Ets, Key),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({read ,Key, _Now, From}, #state{ets = Ets, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[] ->
			case zm_db:default(Table) of
				{ok, DefVal} ->
					%如果读时关键字不存在, 则采用当前表的默认值替代
					z_lib:reply(From, {ok, DefVal, 0, 0});
				_ ->
					z_lib:reply(From, {ok, ?NIL, 0, 0})
			end;
		[{Key, Value, Vsn, Time}] ->
			zm_storage_util:read(Value, Vsn, Time, From)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({write, Key, {Value, 0}, Now, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Old, Vsn, _Time}] ->
			storage_write(Name, Ets, IndexEts, Table, Key, Value, Old, Vsn, Now, From);
		[] ->
			storage_write(Name, Ets, IndexEts, Table, Key, Value, 1, Now, From)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({write, Key, {Value, 1}, Now, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[] ->
			storage_write(Name, Ets, IndexEts, Table, Key, Value, 1, Now, From);
		[{_, _, Vsned, _Time}] ->
			z_lib:reply(From, {vsn_error, Vsned})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({write, Key, {Value, Vsn}, Now, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Old, Vsn, _Time}] ->
			storage_write(Name, Ets, IndexEts, Table, Key, Value, Old, Vsn, Now, From);
		[{_, _, Vsned, _Time}] ->
			z_lib:reply(From, {vsn_error, Vsned});
		[] ->
			z_lib:reply(From, {vsn_error, 0})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({delete, Key, 0, Now, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Old, _Vsn, _T}] ->
			storage_delete(Name, Ets, IndexEts, Table, Key, Old, Now, From, ok);
		[] ->
			z_lib:reply(From, {not_exist_error, ?NIL})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({delete, Key, Vsn, Now, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Old, Vsn, _T}] ->
			storage_delete(Name, Ets, IndexEts, Table, Key, Old, Now, From, ok);
		[{_, _V, Vsned, _T}] ->
			z_lib:reply(From, {vsn_error, Vsned});
		[] ->
			z_lib:reply(From, {not_exist_error, ?NIL})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

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

handle_info({dyn_read, Key, Fun, _Now, From}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Value, Vsn, Time}] ->
			zm_storage_util:dyn_read(Key, Value, Vsn, Time, Fun, From);
		[] ->
			zm_storage_util:dyn_read(Key, ?NIL, 0, 0, Fun, From)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({dyn_write, Key, Fun, Now, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	case ets:lookup(Ets, Key) of
		[{_, Value, Vsn, Time}] ->
			dyn_write(Name, Ets, IndexEts, Table, Key, Value, Vsn, Time, Fun, Now, From);
		[] ->
			dyn_write(Name, Ets, IndexEts, Table, Key, ?NIL, 0, 0, Fun, Now, From)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({restore, Key, {Value, Vsn}, Time, From},
	#state{name = Name, ets = Ets, index = IndexEts, table = Table} = State) ->
	storage_restore(Name, Ets, IndexEts, Table, Key, Value, Vsn, Time, From),
	{noreply, State, ?HIBERNATE_TIMEOUT};

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
% 插入和更新数据
storage_write(Name, Ets, IndexEts, Table, Key, Value, Vsn, Time, From) ->
	try
		storage_write(Name, Ets, Table, Key, Value, Vsn + 1, Time),
		case IndexEts of
			0 -> ok;
			_ -> ets:insert(IndexEts, {{Value, Key}})
		end,
		z_lib:reply(From, ok)
	catch
		_:Reason ->
			z_lib:reply(From, {error, Reason})
	end.

% 插入和更新数据
storage_write(Name, Ets, IndexEts, Table, Key, Value, Old, Vsn, Time, From) ->
	try
		storage_write(Name, Ets, Table, Key, Value, Vsn + 1, Time),
		case IndexEts of
			0 -> ok;
			_ ->
				ets:delete(IndexEts, {Old, Key}),
				ets:insert(IndexEts, {{Value, Key}})
		end,
		z_lib:reply(From, ok)
	catch
		_:Reason ->
			z_lib:reply(From, {error, Reason})
	end.

% 恢复数据
storage_restore(Name, Ets, IndexEts, Table, Key, Value, Vsn, Time, From) ->
	try
		storage_write(Name, Ets, Table, Key, Value, Vsn, Time),
		case IndexEts of
			0 -> ok;
			_ -> ets:insert(IndexEts, {{Value, Key}})
		end,
		z_lib:reply(From, ok)
	catch
		_:Reason ->
			z_lib:reply(From, {error, Reason})
	end.

% 写入数据
storage_write(Name, Ets, Table, Key, Value, Vsn, Time) ->
	KV = {Key, Value, Vsn, Time},
	ets:insert(Ets, KV),
	L = zm_db:active_dht_node(Table, Key, node()),
	gen_server:abcast(L, Name, {backup, KV}),
	Action = if
		Vsn =:= 2 -> insert;
		true -> update
	end,
	zm_event:notify(Table, {zm_storage, Action}, {Key, Value, Vsn, Time}).

% 删除数据
storage_delete(Name, Ets, IndexEts, Table, Key, Old, Time, From, Reply) ->
	case IndexEts of
		0 -> ok;
		_ -> ets:delete(IndexEts, {Old, Key})
	end,
	ets:delete(Ets, Key),
	L = zm_db:active_dht_node(Table, Key, node()),
	gen_server:abcast(L, Name, {backup_delete, Key, Time}),
	z_lib:reply(From, Reply),
	zm_event:notify(Table, {zm_storage, delete}, {Key, Time}).

% 动态函数处理的数据
dyn_write(Name, Ets, IndexEts, Table, Key, Value, Vsn, Time, Fun, Now, From) ->
	case zm_storage_util:dyn_write(Key, Value, Vsn, Time, Fun) of
		ok ->
			ok;
		{reply, Value1} ->
			z_lib:reply(From, {ok, Value1, Vsn, Time});
		delete when Vsn > 1 ->
			storage_delete(Name, Ets, IndexEts, Table, Key, Value, Now, From, ok);
		delete ->
			ok;
		{delete_reply, Value1} when Vsn > 1 ->
			storage_delete(Name, Ets, IndexEts, Table, Key, Value, Time,
				From, {ok, Value1, Vsn, Time});
		{delete_reply, Value1} ->
			z_lib:reply(From, {ok, Value1, Vsn, Time});
		{update, Value1} ->
			storage_write(Name, Ets, IndexEts, Table, Key, Value1, Value, Vsn, Now, From);
		{update_reply, Value1, Value2} ->
			storage_write(Name, Ets, IndexEts, Table, Key, Value1, Value, Vsn + 1, Now),
			z_lib:reply(From, {ok, Value2, Vsn + 1, Now});
		{error, _Reason} = E ->
			z_lib:reply(From, E);
		E ->
			z_lib:reply(From, {error, E})
	end.

%用指定的键获得值和版本时间
storage_get(Ets, Key, attr) ->
	case ets:lookup(Ets, Key) of
		[] ->
			none;
		[{_, _, Vsn, Time}] ->
			{Key, Vsn, Time}
	end;
storage_get(Ets, Key, value) ->
	case ets:lookup(Ets, Key) of
		[] ->
			none;
		[{_, Value, _Vsn, _Time}] ->
			{Key, Value}
	end;
storage_get(Ets, Key, all) ->
	case ets:lookup(Ets, Key) of
		[] ->
			none;
		[{_, Value, Vsn, Time}] ->
			{Key, Value, Vsn, Time}
	end.

%用指定的键获得值和版本时间
storage_index_get(Ets, {Value, Key}, all) ->
	case ets:lookup(Ets, Key) of
		[] ->
			none;
		[{_, _Value, Vsn, Time}] ->
			{Value, Key, Vsn, Time}
	end.
