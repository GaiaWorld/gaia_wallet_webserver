%%
%%@doc	基于B树的定长存储管理器
%%

-module(btsm).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("btsm.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/2, load/4, load_node/7, set/4, stop/1]).
-compile(export_all). %临时使用，调试完成后删除

%%
%% 启动基于B树的存储管理器
%% 
start_link(Args, Name) when is_atom(Name) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, {Args, Name}, []).

%%
%%加载超级块
%%
load(BTSM, Args, Name, {DatRootPoint, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid}) when is_pid(BTSM) ->
	gen_server:call(BTSM, {load, Args, Name, DatRootPoint, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid}, 30000);
load(BTSM, Args, Name, ?EMPTY) when is_pid(BTSM) ->
	gen_server:call(BTSM, {load, Args, Name, ?EMPTY, [], ?EMPTY, [], 0}, 30000).

%%
%% 异步加载指定的节点数据
%%
load_node(BTSM, Reader, Src, From, Type, Idx, Point) ->
	BTSM ! {load_node, Src, From, Reader, Type, Idx, Point},
	?ASYN_WAIT.

%%
%%设置相关进程
%%
set(BTSM, BTQM, Logger, BTPM) when is_pid(BTQM), is_pid(Logger), is_pid(BTPM) ->
	gen_server:call(BTSM, {set, BTQM, Logger, BTPM}).

%%
%% 关闭基于B树的存储管理器
%%
stop(BTSM) when is_pid(BTSM) ->
	gen_server:call(BTSM, stop, 30000).

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
init({Args, Name}) ->
	Self=self(),
	Path=z_lib:get_value(Args, ?BTS_PATH, ?DEFAULT_BTS_PATH(Name)),
	IsCheckSum=z_lib:get_value(Args, ?IS_CHECKSUM, ?DEFAULT_CHECKSUM),
	CollateTimeout=z_lib:get_value(Args, ?COLLATE_TIMEOUT, ?DEFALUT_COLLATE_TIMEOUT),
	Capacity=z_lib:get_value(Args, ?CACHE_CAPACITY, ?DEFAULT_CACHE_CAPACITY) div erlang:system_info(wordsize),
	PersistentFactor=z_lib:get_value(Args, ?PERSISTENT_CACHE_FACTOR, ?DEFAULT_PERSISTENT_CACHE_FACTOR),
	PersistentTimeout=z_lib:get_value(Args, ?PERSISTENT_CACHE_TIMEOUT, ?DEFAULT_PERSISTENT_CACHE_TIMEOUT),
	{ok, #btsm_context{self = Self, path = Path, checksum = IsCheckSum, 
						persistence_id = ?INIT_UID, idx_persistence_id = ?INIT_UID, 
						buffer = ?EMPTY, write_queue = queue:new(), wait_frees = sb_trees:empty(), 
						cache_capacity = Capacity, persistence_factor = PersistentFactor, 
						wait_frees_timer = erlang:start_timer(?DEFAULT_WAIT_FREE_TIMEOUT, Self, free), 
					    persistence_timeout = PersistentTimeout, persistence_timer = erlang:start_timer(PersistentTimeout, Self, persistence), 
						collate_timeout = CollateTimeout, collate_timer = erlang:start_timer(CollateTimeout, Self, collate)}}.

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
handle_call({write, Type, Key, Value, WriteTime}, From, State) ->
	{noreply, handle_write(From, Type, Key, Value, WriteTime, State), ?BTSM_HIBERNATE_TIMEOUT};
handle_call({load, Args, Name, DatRootPoint, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid}, _, State) ->
	case load(Args, Name, DatRootPoint, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid, State) of
		{ok, DatHandle, IdxHandle, NewState} ->
			{reply, {ok, DatHandle, IdxHandle}, NewState, ?BTSM_HIBERNATE_TIMEOUT};
		E ->
			{stop, E, E, State}
	end;
handle_call(get_btqm, _, State) ->
	{reply, State#btsm_context.btqm, State, ?BTSM_HIBERNATE_TIMEOUT};
handle_call({set, BTQM, Logger, BTPM}, _, #btsm_context{self = Self} = State) ->
	{reply, ok, State#btsm_context{wait_frees_timer = erlang:start_timer(?DEFAULT_WAIT_FREE_TIMEOUT, Self, free), 
								   btqm = BTQM, logger = Logger, btpm = BTPM}, ?BTSM_HIBERNATE_TIMEOUT};
handle_call(stop, {Bts, _}, State) ->
	unlink(Bts),
	{stop, normal, ok, State};
handle_call(_, _, State) ->
    {noreply, State, ?BTSM_HIBERNATE_TIMEOUT}.


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
handle_cast({From, write, Type, Key, Value, WriteTime}, State) ->
	{noreply, handle_write(From, Type, Key, Value, WriteTime, State), ?BTSM_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?BTSM_HIBERNATE_TIMEOUT}.


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
handle_info({load_node, BTQM, From, Reader, Type, Idx, Point}, #btsm_context{self = Self, btqm = BTQM} = State) ->
	bts_reader:asyn_read(Reader, Self, From, {read, Type, Idx}, Point),
	{noreply, State, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({_, read_data, {From, {read, Type, Idx}, Point, {ok, Node}}}, #btsm_context{self = Self, btqm = BTQM} = State) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			{M, Cache}=b_trees:get_cache(Handle),
			case M:load(Cache, Point, Node) of
				true ->
					BTQM ! {Self, load_node_ok, From, Type, Idx, Point};
				{error, Reason} ->
					BTQM ! {Self, load_node_error, Reason, From, Type, Idx, Point};
				false ->
					BTQM ! {Self, load_node_error, load_failed, From, Type, Idx, Point}
			end;
		undefined ->
			erlang:error({error, {idx_not_exist, Idx}})
	end,
	{noreply, State, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({load_node, Self, From, Reader, Type, Idx, Point}, #btsm_context{self = Self} = State) ->
	bts_reader:asyn_read(Reader, Self, From, {write, Type, Idx}, Point),
	{noreply, State, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({_, read_data, {From, {write, Type, Idx}, Point, {ok, Node}}}, #btsm_context{self = Self} = State) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			{M, Cache}=b_trees:get_cache(Handle),
			case M:load(Cache, Point, Node) of
				true ->
					{noreply, handle_wait_load(From, Self, Type, Idx, Handle, Point, State), ?BTSM_HIBERNATE_TIMEOUT};
				{error, Reason} ->
					reply(From, write, {error, Reason}),
					{noreply, State, ?BTSM_HIBERNATE_TIMEOUT};
				false ->
					reply(From, write, {error, {load_node_error, Point}}),
					{noreply, State, ?BTSM_HIBERNATE_TIMEOUT}
			end;
		undefined ->
			erlang:error({error, {idx_not_exist, Idx}})
	end;
handle_info({Logger, write_log_ok}, #btsm_context{self = Self, buffer = {continue_write_ok, LastHandles, Resets, CacheL, FreeL}, logger = Logger} = State) ->
	State#btsm_context.btqm ! {Self, reset_handle, Resets},
	{noreply, State#btsm_context{buffer = {write_log_ok, LastHandles, CacheL, FreeL}}, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({BTQM, reset_handle_ok}, #btsm_context{buffer = {write_log_ok, _, _, FreeL}, btqm = BTQM} = State) ->
	case get(?WRITE_LOCK) of
		{From, {Type, _, _, _}} ->
			reply(From, Type, ok),
			{noreply, continue_handle_write(get_refs(FreeL, persistent(State), [])), ?BTSM_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({error, {write_lock_not_exist, reset_handle_ok}})
	end;
handle_info({BTQM, get_ref_ok, Refs}, #btsm_context{btqm = BTQM} = State) ->
	{noreply, filter_frees(Refs, State), ?BTSM_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, free}, #btsm_context{self = Self, wait_frees = WaitFrees, wait_frees_timer = TimerRef} = State) ->
	time_get_refs(get_frees(WaitFrees, ?DEFAULT_TIME_FREE_COUNT), State, []),
	{noreply, State#btsm_context{wait_frees_timer = erlang:start_timer(?DEFAULT_WAIT_FREE_TIMEOUT, Self, free)}, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({BTPM, commit_ok, DatPoints, IdxPoints}, #btsm_context{btpm = BTPM} = State) ->
	case get({?INDEX, ?KEY_INDEX}) of
		{DatHandle, _} ->
			b_trees:set_commited(DatHandle, DatPoints),
			case get({?INDEX, ?KEY_INDEX}) of
				{IdxHandle, _} ->
					b_trees:set_commited(IdxHandle, IdxPoints);
				undefined ->
					continue
			end,
			{noreply, State, ?BTSM_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({error, {idx_not_exist, ?KEY_INDEX}})
	end;
handle_info({timeout, TimerRef, persistence}, #btsm_context{self = Self, persistence_timeout = PersistenceTimeout, persistence_timer = TimerRef} = State) ->
	NewState=case get({?INDEX, ?KEY_INDEX}) of
		{DatHandle, _} ->
			{DatM, DatCache}=b_trees:get_cache(DatHandle),
			persistent(b_trees:get_root(DatHandle), DatM, DatCache, State);
		undefined ->
			State
	end,
	{noreply, NewState#btsm_context{persistence_timer = erlang:start_timer(PersistenceTimeout, Self, persistence)}, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, collate}, #btsm_context{self = Self, collate_timeout = CollateTimeout, collate_timer = TimerRef} = State) ->
	collate(),
	{noreply, State#btsm_context{collate_timer = erlang:start_timer(CollateTimeout, Self, collate)}, ?BTSM_HIBERNATE_TIMEOUT};
handle_info({Logger, {write_log_error, _} = Reason}, #btsm_context{buffer = {continue_write_ok, LastHandles, _, CacheL, FreeL}, logger = Logger} = State) ->
	case get(?WRITE_LOCK) of
		{From, {Type, _, _, _}} ->
			reply(From, Type, {error, Reason}),
			rollback_continue_write(LastHandles, CacheL, FreeL),
			{noreply, continue_handle_write(State), ?BTSM_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({error, {write_lock_not_exist, Reason}})
	end;
handle_info({BTQM, {reset_handle_error, _} = Reason}, #btsm_context{buffer = {write_log_ok, LastHandles, CacheL, FreeL}, btqm = BTQM} = State) ->
	case get(?WRITE_LOCK) of
		{From, {Type, _, _, _}} ->
			reply(From, Type, {error, Reason}),
			rollback_continue_write(LastHandles, CacheL, FreeL),
			{noreply, continue_handle_write(State), ?BTSM_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({error, {write_lock_not_exist, Reason}})
	end;
handle_info({'EXIT', Pid, Why}, State) ->
	{stop, {process_exit, Pid, Why}, State};
handle_info(_, State) ->
    {noreply, State, ?BTSM_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(normal, #btsm_context{dat_reader = DatReader, idx_reader = IdxReader, wait_frees_timer = WaitFreesTimer, collate_timer = CollateTimer}) ->
	erlang:cancel_timer(WaitFreesTimer),
	erlang:cancel_timer(CollateTimer),
	bts_reader:stop(IdxReader),
	bts_reader:stop(DatReader);
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

init_dat(Args, Name, DatRootPoint, DatFreeBlocks, #btsm_context{path = Path, checksum = IsCheckSum} = State) ->
	DatFileName=filename:join(Path, lists:concat([Name, ?DAT_EXT])),
	case bts_reader:start_link(Args, DatFileName, DatFileName, IsCheckSum) of
		{ok, DatReader} ->
			case z_lib:get_value(Args, ?DAT_ENTRY_BYTE_SIZE, []) of
				EntrySize when is_integer(EntrySize), EntrySize > 0 ->
					{CacheMod, CacheType, Capacity, Sizeout, Timeout}=z_lib:get_value(Args, ?DAT_CACHE_ARGS, ?DEFAULT_CACHE_ARGS),
					case DatRootPoint of
						{_, _} ->
							case bts_reader:read(DatReader, ?EMPTY, ?EMPTY, DatRootPoint, 3000) of
								{_, _, _, {ok, DatRootNode}} ->
									{ok, b_trees:new({?MODULE, load_node, DatReader}, {CacheMod, CacheType}, EntrySize, Capacity, Sizeout, 
													 {DatRootPoint, DatRootNode}, DatFreeBlocks, Timeout), State#btsm_context{dat_reader = DatReader}};
								{error, Reason} ->
									{init_dat_error, {load_root_error, Reason}}
							end;
						?EMPTY ->
							{ok, b_trees:new({?MODULE, load_node, DatReader}, {CacheMod, CacheType}, EntrySize, Capacity, Sizeout, 
													 DatRootPoint, DatFreeBlocks, Timeout), State#btsm_context{dat_reader = DatReader}}
					end;
				_ ->
					{init_dat_error, invalid_dat_entry_size}
			end;
		{_, Reason} ->
			{init_dat_error, Reason}
	end.

init_idx(Args, Name, IdxRootPoint, IdxFreeBlocks, #btsm_context{path = Path, checksum = IsCheckSum} = State) ->
	case z_lib:get_value(Args, ?IS_IDX, ?DEFAULT_IS_IDX) of
		true ->
			IdxFileName=filename:join(Path, lists:concat([Name, ?IDX_EXT])),
			case bts_reader:start_link(Args, IdxFileName, IdxFileName, IsCheckSum) of
				{ok, IdxReader} ->
					case z_lib:get_value(Args, ?IDX_ENTRY_BYTE_SIZE, ?DEFAULT_IDX_ENTRY_BYTE_SIZE) of
						IdxEntrySize when is_integer(IdxEntrySize), IdxEntrySize >= 0 ->
							{CacheMod, CacheType, Capacity, Sizeout, Timeout}=z_lib:get_value(Args, ?IDX_CACHE_ARGS, ?DEFAULT_CACHE_ARGS),
							case IdxRootPoint of
								{_, _} ->
									case bts_reader:read(IdxReader, ?EMPTY, ?EMPTY, IdxRootPoint, 3000) of
										{_, _, _, {ok, IdxRootNode}} ->
											{ok, b_trees:new({?MODULE, load_node, IdxReader}, {CacheMod, CacheType}, IdxEntrySize, Capacity, Sizeout, 
															 {IdxRootPoint, IdxRootNode}, IdxFreeBlocks, Timeout), State#btsm_context{idx_reader = IdxReader}};
										{error, Reason} ->
											{init_dat_error, {load_root_error, Reason}}
									end;
								?EMPTY ->
									{ok, b_trees:new({?MODULE, load_node, IdxReader}, {CacheMod, CacheType}, IdxEntrySize, Capacity, Sizeout, 
															 IdxRootPoint, IdxFreeBlocks, Timeout), State#btsm_context{idx_reader = IdxReader}}
							end;
						_ ->
							{init_idx_error, invalid_idx_entry_size}
					end;
				{_, Reason} ->
					{init_idx_error, Reason}
			end;
		false ->
			{ok, ?EMPTY, State}
	end.

load(Args, Name, DatRootPoint, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid, State) ->
	case init_dat(Args, Name, DatRootPoint, DatFreeBlocks, State) of
		{ok, DatHandle, DatState} ->
			case init_idx(Args, Name, IdxRootPoint, IdxFreeBlocks, DatState) of
				{ok, IdxHandle, NewState} ->
					put({?INDEX, ?KEY_INDEX}, {DatHandle, ?INIT_VSN}),
					[put({?INDEX, ?VALUE_INDEX}, {IdxHandle, ?INIT_VSN}) || is_tuple(IdxHandle)],
					{ok, DatHandle, IdxHandle, NewState#btsm_context{commit_uid = CommitUid}};
				E ->
					E
			end;
		E ->
			E
	end.

handle_wait_load(From, Self, Type, Idx, Handle, Point, State) ->
	case b_trees:continue(From, Idx, Handle, Point) of
		{{?ASYN_WAIT, Reader}, Req, _, _} ->
			Reader(Self, Self, From, Type, ?KEY_INDEX, Req),
			State;
		{{ok, R}, {Type, Key, Value, IsCheckSum, WriteTime}, _, _} ->
			case Idx of
				?KEY_INDEX ->
					continue_write1(Handle, R, From, Self, Type, Key, Value, WriteTime, IsCheckSum, State);
				?VALUE_INDEX ->
					case get({?INDEX, ?KEY_INDEX}) of
						{DatHandle, _} ->
							continue_write1(DatHandle, erase({?ASYN_WAIT, ?VALUE_INDEX, From}), From, Self, Type, Key, Value, WriteTime, IsCheckSum, State);
						undefined ->
							reply(From, Type, {error, {idx_not_exist, ?KEY_INDEX}}),
							State
					end
			end;
		{Reason, _, _, _} ->
			reply(From, Type, {error, Reason}),
			State
	end.

continue_handle_write(State) ->
	NewState=State#btsm_context{buffer = ?EMPTY},
	erase(?WRITE_LOCK),
	case queue:out(State#btsm_context.write_queue) of
		{empty, _} ->
			NewState;
		{{value, {LastFrom, {LastType, LastKey, LastValue, LastWriteTime}} = Last}, NewQueue} ->
			put(?WRITE_LOCK, Last),
			continue_write(LastFrom, LastType, LastKey, LastValue, LastWriteTime, NewState#btsm_context{write_queue = NewQueue})
	end.

handle_write(From, Type, Key, Value, WriteTime, #btsm_context{write_queue = Queue} = State) ->
	Req={Type, Key, Value, WriteTime},
	case get(?WRITE_LOCK) of
		undefined ->
			case queue:out(Queue) of
				{empty, _} ->
					put(?WRITE_LOCK, {From, Req}),
					continue_write(From, Type, Key, Value, WriteTime, State);
				{{value, {LastFrom, {LastType, LastKey, LastValue, LastWriteTime}} = Last}, NewQueue} ->
					put(?WRITE_LOCK, Last),
					continue_write(LastFrom, LastType, LastKey, LastValue, LastWriteTime, State#btsm_context{write_queue = queue:in({From, Req}, NewQueue)})
			end;
		_ ->
			State#btsm_context{write_queue = queue:in({From, Req}, Queue)}
	end.

continue_write(From, Type, Key, Value, WriteTime, #btsm_context{self = Self, checksum = IsCheckSum} = State) ->
	case get({?INDEX, ?KEY_INDEX}) of
		{Handle, _} ->
			case write_dat(Type, Handle, Key, Value, IsCheckSum) of
				{?ASYN_WAIT, Reader} ->
					Reader(Self, Self, From, Type, ?KEY_INDEX, {Type, Key, Value, IsCheckSum, WriteTime}),
		    		State;
				{ok, R} ->
					continue_write1(Handle, R, From, Self, Type, Key, Value, WriteTime, IsCheckSum, State);
				Reason ->
					reply(From, Type, {error, Reason}),
					State
			end;
		undefined ->
			reply(From, Type, {error, {idx_not_exist, ?KEY_INDEX}}),
			State
	end.

continue_write1(Handle, R, From, Self, Type, Key, Value, WriteTime, IsCheckSum, State) ->
	case write_idx(Type, From, Self, Handle, Key, Value, IsCheckSum, WriteTime, R) of
		{ok, L} ->
			{Resets, CacheL, FreeL}=save(L, [], [], []),
			bts_logger:write_log(State#btsm_context.logger, new_write_log(Type, Key, Value, IsCheckSum, WriteTime)),
			State#btsm_context{buffer = {continue_write_ok, reset_handle(Resets, []), Resets, CacheL, FreeL}};
		?ASYN_WAIT ->
			State;
		Reason ->
			reply(From, Type, {error, Reason}),
			State
	end.

write_dat(insert, Handle, Key, Value, IsCheckSum) ->
	b_trees:insert(Handle, Key, Value, IsCheckSum);
write_dat(update, Handle, Key, Value, IsCheckSum) ->
	b_trees:update(Handle, Key, Value, IsCheckSum);
write_dat(delete, Handle, Key, _, IsCheckSum) ->
	b_trees:delete(Handle, Key, IsCheckSum).

write_idx(insert, From, Self, Handle, Key, Value, IsCheckSum, WriteTime, R) ->
	insert_idx(From, Self, Key, Value, IsCheckSum, WriteTime, [{?KEY_INDEX, Handle, R}]);
write_idx(update, From, Self, Handle, Key, Value, IsCheckSum, WriteTime, R) ->
	update_idx(From, Self, Key, Value, IsCheckSum, WriteTime, [{?KEY_INDEX, Handle, R}]);
write_idx(delete, From, Self, Handle, Key, Value, IsCheckSum, WriteTime, R) ->
	delete_idx(From, Self, Key, Value, IsCheckSum, WriteTime, [{?KEY_INDEX, Handle, R}]).
	
save_cache([{_, {Point, _} = N, _}|T], Handle, L1, L2) ->
	save_cache(T, Handle, [N|L1], [Point|L2]);
save_cache([], Handle, L1, L2) ->
	true=b_trees:save(Handle, L1),
	L2.

save([{Idx, Handle, {NewRootPoint, Frees, Writes}}|T], L1, L2, L3) ->
	OldRootPoint=b_trees:get_root(Handle),
	save(T, [{Idx, NewRootPoint}|L1], [{Idx, save_cache(Writes, Handle, [], [])}|L2], [{Idx, OldRootPoint, Frees}|L3]);
save([], L1, L2, L3) ->
	{L1, L2, L3}.

rollback_continue_write(LastHandles, CacheL, FreeL) ->
	rollback_reset_handle(LastHandles),
	rollback_save(CacheL),
	rollback_free(FreeL).

rollback_save([{Idx, Points}|T]) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			{M, Cache}=b_trees:get_cache(Handle),
			rollback_save_cache(Points, M, Cache),
			rollback_save(T);
		undefined ->
			erlang:error({error, {rollback_save, {idx_not_exist, Idx}}})
	end;
rollback_save([]) ->
	ok.
			
rollback_save_cache([Point|T], M, Cache) ->
	M:delete(Cache, Point),
	rollback_save_cache(T, M, Cache);
rollback_save_cache([], _, _) ->
	ok.	

rollback_free([{Idx, _, Points}|T]) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			{M, Cache}=b_trees:get_cache(Handle),
			rollback_free_cache(Points, M, Cache),
			rollback_free(T);
		undefined ->
			erlang:error({error, {rollback_free, {idx_not_exist, Idx}}})
	end;
rollback_free([]) ->
	ok.

rollback_free_cache([Point|T], M, Cache) ->
	M:uncopy(Cache, Point),
	rollback_free_cache(T, M, Cache);
rollback_free_cache([], _, _) ->
	ok.

reset_handle([{Idx, NewRootPoint}|T], L) ->
	case get({?INDEX, Idx}) of
		{Handle, Vsn} ->
			reset_handle(T, [{Idx, Handle, Vsn, NewRootPoint}|L]);
		undefined ->
			{reset_handle_error, {idx_not_exist, Idx}}
	end;
reset_handle([], L) ->
	{ok, [{Idx, Handle, Vsn} || {Idx, Handle, Vsn, NewRootPoint} <- L, put({?INDEX, Idx}, {b_trees:set_root(Handle, NewRootPoint), Vsn + 1}) =/= undefined]}.

rollback_reset_handle(L) ->
	[put({?INDEX, Idx}, {LastHandle, LastVsn}) || {Idx, LastHandle, LastVsn} <- L].

insert_idx(From, Self, Key, Value, IsCheckSum, WriteTime, L) ->
	case get({?INDEX, ?VALUE_INDEX}) of
		{IdxHandle, _} ->
			case b_trees:insert(IdxHandle, {Value, Key}, ?EMPTY, IsCheckSum) of
				{?ASYN_WAIT, IdxReader} ->
					IdxReader(Self, Self, From, insert_idx, ?VALUE_INDEX, {insert, Key, Value, IsCheckSum, WriteTime}),
					[{_, _, R}]=L,
					put({?ASYN_WAIT, ?VALUE_INDEX, From}, R),
					?ASYN_WAIT;
				{ok, R} ->
					{ok, [{?VALUE_INDEX, IdxHandle, R}|L]};
				E ->
					E
			end;
		undefined ->
			{ok, L}
	end.

update_idx(From, Self, Key, Value, IsCheckSum, WriteTime, L) ->
	case get({?INDEX, ?VALUE_INDEX}) of
		{IdxHandle, _} ->
			case b_trees:update(IdxHandle, {Value, Key}, ?EMPTY, IsCheckSum) of
				{?ASYN_WAIT, IdxReader} ->
					IdxReader(Self, Self, From, update_idx, ?VALUE_INDEX, {update, Key, Value, IsCheckSum, WriteTime}),
					[{_, _, R}]=L,
					put({?ASYN_WAIT, ?VALUE_INDEX, From}, R),
					?ASYN_WAIT;
				{ok, R} ->
					{ok, [{?VALUE_INDEX, IdxHandle, R}|L]};
				E ->
					E
			end;
		undefined ->
			{ok, L}
	end.

delete_idx(From, Self, Key, Value, IsCheckSum, WriteTime, L) ->
	case get({?INDEX, ?VALUE_INDEX}) of
		{IdxHandle, _} ->
			case b_trees:delete(IdxHandle, {Value, Key}, IsCheckSum) of
				{?ASYN_WAIT, IdxReader} ->
					IdxReader(Self, Self, From, delete_idx, ?VALUE_INDEX, {delete, Key, Value, IsCheckSum, WriteTime}),
					[{_, _, R}]=L,
					put({?ASYN_WAIT, ?VALUE_INDEX, From}, R),
					?ASYN_WAIT;
				{ok, R} ->
					{ok, [{?VALUE_INDEX, IdxHandle, R}|L]};
				E ->
					E
			end;
		undefined ->
			{ok, L}
	end.

new_write_log(delete, Key, _, IsCheckSum, WriteTime) ->
	{WriteTime, delete, Key, IsCheckSum};
new_write_log(Type, Key, Value, IsCheckSum, WriteTime) ->
	{WriteTime, Type, Key, Value, IsCheckSum}.

reply({_, _} = From, _, Msg) ->
	z_lib:reply(From, Msg);
reply(From, Type, Msg) ->
	From ! {self(), Type, Msg}.

get_refs([{_, ?EMPTY, _}|T], State, L) ->
	get_refs(T, State, L);
get_refs([{Idx, OldRootPoint, Frees}|T], #btsm_context{wait_frees = WaitFrees} = State, L) ->
	get_refs(T, State#btsm_context{wait_frees = sb_trees:insert({Idx, OldRootPoint}, Frees, WaitFrees)}, [{Idx, OldRootPoint}|L]);
get_refs([], #btsm_context{self = Self, btqm = BTQM} = State, L) ->
	BTQM ! {Self, get_ref, L},
	State.

time_get_refs([{Idx, OldRootPoint, _}|T], State, L) ->
	time_get_refs(T, State, [{Idx, OldRootPoint}|L]);
time_get_refs([], #btsm_context{btqm = ?EMPTY}, _) ->
	ignore;
time_get_refs([], #btsm_context{self = Self, btqm = BTQM}, L) ->
	BTQM ! {Self, get_ref, L}.

filter_frees([{Idx, OldRootPoint, ?DEREF}|T], #btsm_context{wait_frees = WaitFrees} = State) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			Key={Idx, OldRootPoint},
			{M, Cache}=b_trees:get_cache(Handle),
			case sb_trees:lookup(Key, WaitFrees) of
				{_, Frees} ->
					frees(Frees, M, Cache, b_trees:get_alloter(Handle)),
					filter_frees(T, State#btsm_context{wait_frees = sb_trees:delete(Key, WaitFrees)});
				none ->
					filter_frees(T, State)
			end;
		undefined ->
			erlang:error({filter_frees_error, {idx_not_exist, Idx}})
	end;
filter_frees([{_, _, _}|T], State) ->
	filter_frees(T, State);
filter_frees([], State) ->
	State.

frees([{_, Offset} = Point|T], M, Cache, Alloter) ->
	M:delete(Cache, Point),
	{ok, _}=bs_alloter:free(Alloter, {bs_alloter:pos(Point), Offset}),
	frees(T, M, Cache, Alloter);
frees([?EMPTY|T], M, Cache, Alloter) ->
	frees(T, M, Cache, Alloter);
frees([], _, _, _) ->
	ok.

get_frees(WaitFrees, Count) ->
	get_frees(Count, sb_trees:iterator(WaitFrees), []).
	
get_frees(0, _, L) ->
	L;
get_frees(Count, Iterator, L) ->
	case sb_trees:next(Iterator) of
		{{Idx, OldRootPoint}, Frees, NewIterator} ->
			get_frees(Count - 1, NewIterator, [{Idx, OldRootPoint, Frees}|L]);
		none ->
			L
	end.

persistent(#btsm_context{cache_capacity = Capacity, persistence_factor = Factor} = State) ->
	case get({?INDEX, ?KEY_INDEX}) of
		{DatHandle, _} ->
			DatRootPoint=b_trees:get_root(DatHandle),
			{DatM, DatCache}=b_trees:get_cache(DatHandle),
			DatCacheSize=DatM:memory_size(DatCache),
			if
				DatCacheSize =< (Capacity * Factor) ->
					State;
				true ->
					persistent(DatRootPoint, DatM, DatCache, State)
			end;
		undefined ->
			erlang:error({error, {persistent_error, {idx_not_exist, ?KEY_INDEX}}})
	end.

persistent(DatRootPoint, DatM, DatCache, State) ->
	Size=DatM:size(DatCache, 1),
	if
		Size > 0 ->
			persistent(DatRootPoint, State);
		(Size =< 0) and (DatRootPoint =:= ?EMPTY) ->
			persistent(DatRootPoint, State);
		true ->
			State
	end.

persistent(DatRootPoint, #btsm_context{logger = Logger} = State) ->
	case get({?INDEX, ?VALUE_INDEX}) of
		{IdxHandle, _} ->
			IdxRootPoint=b_trees:get_root(IdxHandle),
			persistent(Logger, [{?DAT_INDEX, DatRootPoint}, {?IDX_INDEX, IdxRootPoint}], State);
		undefined ->
			persistent(Logger, [{?DAT_INDEX, DatRootPoint}, {?IDX_INDEX, ?EMPTY}], State)
	end.

persistent(Logger, L, #btsm_context{commit_uid = CommitUid} = State) ->
	NewCommitUid = CommitUid + 1,
	io:format("!!!!!!NewCommitUid:~p~n", [NewCommitUid]),
	case bts_logger:persistent(Logger, NewCommitUid, L) of
		ok ->
			State#btsm_context{commit_uid = NewCommitUid};
		{error, busy} ->
			State;
		{error, {repeat_commit_uid, NowCommitUid}} ->
			io:format("!!!!!!NowCommitUid:~p, NewCommitUid:~p~n", [NowCommitUid, NewCommitUid]),
			persistent(Logger, L, State#btsm_context{commit_uid = NowCommitUid});
		{error, _} ->
			State
	end.
	
collate() ->
	case get({?INDEX, ?KEY_INDEX}) of
		{DatHandle, _} ->
			b_trees:collate(DatHandle),
			case get({?INDEX, ?VALUE_INDEX}) of
				{IdxHandle, _} ->
					b_trees:collate(IdxHandle);
				undefined ->
					continue
			end;
		undefined ->
			erlang:error({collate_error, {idx_not_exist, ?KEY_INDEX}})
	end.









