%%
%%@doc	基于B树的变长存储管理器
%%


-module(btsmv).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("btsmv.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/2, load/4, load_idx/7, load_data/10, set/4, stop/1]).
-compile(export_all). %临时使用，调试完成后删除

%%
%% 启动基于B树的存储管理器
%% 
start_link(Args, Name) when is_atom(Name) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, {Args, Name}, []).

%%
%%加载超级块
%%
load(BTSMV, Args, Name, {_, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid}) when is_pid(BTSMV) ->
	gen_server:call(BTSMV, {load, Args, Name, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid}, 30000);
load(BTSMV, Args, Name, ?EMPTY) when is_pid(BTSMV) ->
	gen_server:call(BTSMV, {load, Args, Name, [], ?EMPTY, [], 0}, 30000).

%%
%% 异步加载指定的外部索引
%%
load_idx(BTSMV, Reader, Src, From, Type, Idx, IdxPoint) ->
	BTSMV ! {load_idx, Src, From, Reader, Type, Idx, IdxPoint},
	?ASYN_WAIT.

%%
%% 异步加载指定的数据
%%
load_data(BTSMV, Reader, Src, From, Type, Idx, DatPoint, Key, Vsn, Time) ->
	BTSMV ! {load_data, Src, From, Reader, Type, Idx, DatPoint, Key, Vsn, Time},
	?DATA_ASYN_WAIT.

%%
%%设置相关进程
%%
set(BTSMV, BTQMV, Logger, BTPM) when is_pid(BTQMV), is_pid(Logger), is_pid(BTPM) ->
	gen_server:call(BTSMV, {set, BTQMV, Logger, BTPM}).

%%
%% 关闭基于B树的存储管理器
%%
stop(BTSMV) when is_pid(BTSMV) ->
	gen_server:call(BTSMV, stop, 30000).

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
	{ok, #btsmv_context{self = Self, path = Path, checksum = IsCheckSum,
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
	{noreply, handle_write(From, Type, Key, Value, WriteTime, State), ?BTSMV_HIBERNATE_TIMEOUT};
handle_call({load, Args, Name, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid}, _, State) ->
	case load(Args, Name, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid, State) of
		{ok, DatHandle, IdxHandle, NewState} ->
			{reply, {ok, DatHandle, IdxHandle}, NewState, ?BTSMV_HIBERNATE_TIMEOUT};
		E ->
			{stop, E, E, State}
	end;
handle_call({set, BTQMV, Logger, BTPM}, _, #btsmv_context{self = Self} = State) ->
	{reply, ok, State#btsmv_context{wait_frees_timer = erlang:start_timer(?DEFAULT_WAIT_FREE_TIMEOUT, Self, free), 
								   btqmv = BTQMV, logger = Logger, btpm = BTPM}, ?BTSMV_HIBERNATE_TIMEOUT};
handle_call(stop, {Bts, _}, State) ->
	unlink(Bts),
	{stop, normal, ok, State};
handle_call(_, _, State) ->
    {noreply, State, ?BTSMV_HIBERNATE_TIMEOUT}.


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
	{noreply, handle_write(From, Type, Key, Value, WriteTime, State), ?BTSMV_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?BTSMV_HIBERNATE_TIMEOUT}.


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
handle_info({load_idx, BTQMV, From, Reader, Type, Idx, IdxPoint}, #btsmv_context{self = Self, btqmv = BTQMV} = State) ->
	bts_reader:asyn_read(Reader, Self, From, {idx_by_read, Type, Idx}, IdxPoint),
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({_, read_data, {From, {idx_by_read, Type, Idx}, IdxPoint, {ok, Node}}}, #btsmv_context{self = Self, btqmv = BTQMV} = State) ->
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	{M, Cache}=b_trees:get_cache(IdxHandle),
	case M:load(Cache, IdxPoint, Node) of
		true ->
			BTQMV ! {Self, load_idx_ok, From, Type, Idx, IdxPoint};
		{error, Reason} ->
			BTQMV ! {Self, load_idx_error, Reason, From, Type, Idx, IdxPoint};
		false ->
			BTQMV ! {Self, load_idx_error, load_failed, From, Type, Idx, IdxPoint}
	end,
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({load_data, BTQMV, From, Reader, Type, Idx, DatPoint, Key, Vsn, Time}, #btsmv_context{self = Self, btqmv = BTQMV} = State) ->
	bts_reader:asyn_read(Reader, Self, From, {data_by_read, Type, Idx, Key, Vsn, Time}, DatPoint),
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({_, read_data, {From, {data_by_read, Type, Idx, Key, Vsn, Time}, DatPoint, {ok, Value}}}, #btsmv_context{self = Self, btqmv = BTQMV} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{M, Cache}=bs_variable:get_cache(DatHandle),
	case M:load(Cache, Key, {DatPoint, Value, Vsn, Time}) of
		true ->
			BTQMV ! {Self, load_data_ok, From, Type, Idx, DatPoint};
		{error, Reason} ->
			BTQMV ! {Self, load_data_error, Reason, From, Type, Idx, DatPoint};
		false ->
			BTQMV ! {Self, load_data_error, load_failed, From, Type, Idx, DatPoint}
	end,
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({load_idx, Self, From, Reader, Type, Idx, IdxPoint}, #btsmv_context{self = Self} = State) ->
	bts_reader:asyn_read(Reader, Self, From, {idx_by_write, Type, Idx}, IdxPoint),
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({_, read_data, {From, {idx_by_write, Type, ?KEY_INDEX}, IdxPoint, {ok, Node}}}, #btsmv_context{self = Self} = State) ->
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	{M, Cache}=b_trees:get_cache(IdxHandle),
	case M:load(Cache, IdxPoint, Node) of
		true ->
			{noreply, handle_wait_load_idx_by_write(From, Self, Type, IdxHandle, IdxPoint, State), ?BTSMV_HIBERNATE_TIMEOUT};
		{error, Reason} ->
			reply(From, write, {error, Reason}),
			{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT};
		false ->
			reply(From, write, {error, {load_idx_error, IdxPoint}}),
			{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT}
	end;
handle_info({load_data, Self, From, Reader, Type, Idx, DatPoint, Key, Vsn, Time}, #btsmv_context{self = Self} = State) ->
	bts_reader:asyn_read(Reader, Self, From, {data_by_write, Type, Idx, Key, Vsn, Time}, DatPoint),
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({_, read_data, {From, {data_by_write, Type, ?VALUE_INDEX, Key, Vsn, Time}, DatPoint, {ok, Value}}}, State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{M, Cache}=b_trees:get_cache(DatHandle),
	case M:load(Cache, Key, {DatPoint, Value, Vsn, Time}) of
		true ->
			{noreply, handle_wait_load_data_by_write(From, Type, DatHandle, DatPoint, Key, Value, State), ?BTSMV_HIBERNATE_TIMEOUT};
		{error, Reason} ->
			reply(From, write, {error, Reason}),
			{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT};
		false ->
			reply(From, write, {error, {load_data_error, DatPoint}}),
			{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT}
	end;
handle_info({Logger, write_log_ok}, #btsmv_context{self = Self, buffer = {continue_write_ok, LastIdxHandle, IdxReset, IdxCacheL, IdxFreeL, DatResult}, logger = Logger} = State) ->
	State#btsmv_context.btqmv ! {Self, reset_handle, IdxReset},
	{noreply, State#btsmv_context{buffer = {write_log_ok, LastIdxHandle, IdxCacheL, IdxFreeL, DatResult}}, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({BTQMV, reset_handle_ok}, #btsmv_context{buffer = {write_log_ok, LastIdxHandle, IdxCacheL, IdxFreeL, DatResult}, btqmv = BTQMV} = State) ->
	case get(?WRITE_LOCK) of
		{From, {Type, _, _, _}} ->
			case save_data(DatResult) of
				{true, OldDatPoint, _} ->
					reply(From, Type, ok),
					{noreply, continue_handle_write(get_refs(IdxFreeL, OldDatPoint, persistent(State))), ?BTSMV_HIBERNATE_TIMEOUT};
				{false, _, NewDatPoint} ->
					reply(From, Type, {error, {point_conflict, ?VALUE_INDEX, NewDatPoint}}),
					rollback_continue_write(LastIdxHandle, IdxCacheL, IdxFreeL, NewDatPoint),
					{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT};
				{{error, Reason}, _, NewDatPoint} ->
					reply(From, Type, {error, {Reason, ?VALUE_INDEX, NewDatPoint}}),
					rollback_continue_write(LastIdxHandle, IdxCacheL, IdxFreeL, NewDatPoint),
					{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT}
			end;
		undefined ->
			erlang:error({error, {write_lock_not_exist, reset_handle_ok}})
	end;
handle_info({BTQMV, get_ref_ok, Ref}, #btsmv_context{btqmv = BTQMV} = State) ->
	{noreply, filter_frees(Ref, State), ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, free}, #btsmv_context{self = Self, wait_frees = WaitFrees, wait_frees_timer = TimerRef} = State) ->
	time_get_refs(get_frees(WaitFrees, ?DEFAULT_TIME_FREE_COUNT), State, []),
	{noreply, State#btsmv_context{wait_frees_timer = erlang:start_timer(?DEFAULT_WAIT_FREE_TIMEOUT, Self, free)}, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({BTPM, commit_ok, DatKeys, IdxPoints}, #btsmv_context{btpm = BTPM} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	bs_variable:set_commited(DatHandle, DatKeys),
	b_trees:set_commited(IdxHandle, IdxPoints),
	{noreply, State, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, persistence}, #btsmv_context{self = Self, persistence_timeout = PersistenceTimeout, persistence_timer = TimerRef} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	{DatM, DatCache}=bs_variable:get_cache(DatHandle),
	{IdxM, IdxCache}=b_trees:get_cache(IdxHandle),
	NewState=persistent(b_trees:get_root(IdxHandle), IdxM, IdxCache, DatM, DatCache, State),
	{noreply, NewState#btsmv_context{persistence_timer = erlang:start_timer(PersistenceTimeout, Self, persistence)}, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, collate}, #btsmv_context{self = Self, collate_timeout = CollateTimeout, collate_timer = TimerRef} = State) ->
	collate(),
	{noreply, State#btsmv_context{collate_timer = erlang:start_timer(CollateTimeout, Self, collate)}, ?BTSMV_HIBERNATE_TIMEOUT};
handle_info({Logger, {write_log_error, _} = Reason}, #btsmv_context{buffer = {continue_write_ok, LastIdxHandle, _, IdxCacheL, IdxFreeL, {?VALUE_INDEX, _, {DatPoint, _}}}, logger = Logger} = State) ->
	case get(?WRITE_LOCK) of
		{From, {Type, _, _, _}} ->
			reply(From, Type, {error, Reason}),
			rollback_continue_write(LastIdxHandle, IdxCacheL, IdxFreeL, DatPoint),
			{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({error, {write_lock_not_exist, Reason}})
	end;
handle_info({BTQMV, {reset_handle_error, _} = Reason}, #btsmv_context{buffer = {write_log_ok, LastIdxHandle, IdxCacheL, IdxFreeL, {?VALUE_INDEX, _, {DatPoint, _}}}, btqmv = BTQMV} = State) ->
	case get(?WRITE_LOCK) of
		{From, {Type, _, _, _}} ->
			reply(From, Type, {error, Reason}),
			rollback_continue_write(LastIdxHandle, IdxCacheL, IdxFreeL, DatPoint),
			{noreply, continue_handle_write(State), ?BTSMV_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({error, {write_lock_not_exist, Reason}})
	end;
handle_info({'EXIT', Pid, Why}, State) ->
	{stop, {process_exit, Pid, Why}, State};
handle_info(_, State) ->
    {noreply, State, ?BTSMV_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(normal, #btsmv_context{dat_reader = DatReader, idx_reader = IdxReader, wait_frees_timer = WaitFreesTimer, collate_timer = CollateTimer}) ->
	%TODO 关闭存储管理器时的处理...
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

init_dat(Args, Name, DatFreeBlocks, #btsmv_context{path = Path, checksum = IsCheckSum} = State) ->
	ExtName=lists:concat([Name, ?DAT_EXT]),
	DatFileName=filename:join(Path, ExtName),
	case bts_reader:start_link(Args, ExtName, DatFileName, IsCheckSum) of
		{ok, DatReader} ->
			case z_lib:get_value(Args, ?DAT_ENTRY_BYTE_SIZE, 0) of
				0 ->
					{CacheMod, CacheType, Capacity, Sizeout, Timeout}=z_lib:get_value(Args, ?DAT_CACHE_ARGS, ?DEFAULT_DAT_CACHE_ARGS),
					{ok, bs_variable:new({?MODULE, load_data, DatReader}, {CacheMod, CacheType}, Capacity, Sizeout, DatFreeBlocks, Timeout), State#btsmv_context{dat_reader = DatReader}};
				_ ->
					{init_dat_error, invalid_dat_entry_size}
			end;
		{_, Reason} ->
			{init_dat_error, Reason}
	end.

init_idx(Args, Name, IdxRootPoint, IdxFreeBlocks, #btsmv_context{path = Path, checksum = IsCheckSum} = State) ->
	ExtName=lists:concat([Name, ?IDX_EXT]),
	IdxFileName=filename:join(Path, ExtName),
	case bts_reader:start_link(Args, ExtName, IdxFileName, IsCheckSum) of
		{ok, IdxReader} ->
			case z_lib:get_value(Args, ?IDX_ENTRY_BYTE_SIZE, ?DEFAULT_IDX_ENTRY_BYTE_SIZE) of
				IdxEntrySize when is_integer(IdxEntrySize), IdxEntrySize >= 0 ->
					{CacheMod, CacheType, Capacity, Sizeout, Timeout}=z_lib:get_value(Args, ?IDX_CACHE_ARGS, ?DEFAULT_IDX_CACHE_ARGS),
					case IdxRootPoint of
						{_, _} ->
							case bts_reader:read(IdxReader, ?EMPTY, ?EMPTY, IdxRootPoint, 3000) of
								{_, _, _, {ok, IdxRootNode}} ->
									{ok, b_trees:new({?MODULE, load_idx, IdxReader}, {CacheMod, CacheType}, IdxEntrySize, Capacity, Sizeout, 
													 {IdxRootPoint, IdxRootNode}, IdxFreeBlocks, Timeout), State#btsmv_context{idx_reader = IdxReader}};
								{error, Reason} ->
									{init_dat_error, {load_root_error, Reason}}
							end;
						?EMPTY ->
							{ok, b_trees:new({?MODULE, load_idex, IdxReader}, {CacheMod, CacheType}, IdxEntrySize, Capacity, Sizeout, 
													 IdxRootPoint, IdxFreeBlocks, Timeout), State#btsmv_context{idx_reader = IdxReader}}
					end;
				_ ->
					{init_idx_error, invalid_idx_entry_size}
			end;
		{_, Reason} ->
			{init_idx_error, Reason}
	end.

load(Args, Name, DatFreeBlocks, IdxRootPoint, IdxFreeBlocks, CommitUid, State) ->
	case init_dat(Args, Name, DatFreeBlocks, State) of
		{ok, DatHandle, DatState} ->
			case init_idx(Args, Name, IdxRootPoint, IdxFreeBlocks, DatState) of
				{ok, IdxHandle, NewState} ->
					put({?INDEX, ?KEY_INDEX}, {IdxHandle, ?INIT_VSN}),
					[put({?INDEX, ?VALUE_INDEX}, {DatHandle, ?INIT_VSN}) || is_tuple(IdxHandle)],
					{ok, DatHandle, IdxHandle, NewState#btsmv_context{commit_uid = CommitUid}};
				E ->
					E
			end;
		E ->
			E
	end.

handle_wait_load_idx_by_write(From, Self, Type, Handle, IdxPoint, State) ->
	case b_trees:continue(From, ?KEY_INDEX, Handle, IdxPoint) of
		{{?ASYN_WAIT, Reader}, Req, _, _} ->
			Reader(Self, Self, From, Type, ?KEY_INDEX, Req),
			State;
		{{ok, _}, {Type, Key, Value, _, WriteTime}, _, _} ->
			continue_write(From, Type, Key, Value, WriteTime, State);
		{Reason, _, _, _} ->
			reply(From, Type, {error, Reason}),
			State
	end.

handle_wait_load_data_by_write(From, Type, Handle, DatPoint, Key, Value, State) ->
	case bs_variable:continue(From, ?VALUE_INDEX, Handle, DatPoint) of
		{{ok, _}, {Type, _, WriteTime}, _, _} ->
			continue_write(From, Type, Key, Value, WriteTime, State);
		{Reason, _} ->
			reply(From, Type, {error, Reason}),
			State
	end.

continue_handle_write(State) ->
	NewState=State#btsmv_context{buffer = ?EMPTY},
	erase(?WRITE_LOCK),
	case queue:out(State#btsmv_context.write_queue) of
		{empty, _} ->
			NewState;
		{{value, {LastFrom, {LastType, LastKey, LastValue, LastWriteTime}} = Last}, NewQueue} ->
			put(?WRITE_LOCK, Last),
			continue_write(LastFrom, LastType, LastKey, LastValue, LastWriteTime, NewState#btsmv_context{write_queue = NewQueue})
	end.

handle_write(From, Type, Key, Value, WriteTime, #btsmv_context{write_queue = Queue} = State) ->
	Req={Type, Key, Value, WriteTime},
	case get(?WRITE_LOCK) of
		undefined ->
			case queue:out(Queue) of
				{empty, _} ->
					put(?WRITE_LOCK, {From, Req}),
					continue_write(From, Type, Key, Value, WriteTime, State);
				{{value, {LastFrom, {LastType, LastKey, LastValue, LastWriteTime}} = Last}, NewQueue} ->
					put(?WRITE_LOCK, Last),
					continue_write(LastFrom, LastType, LastKey, LastValue, LastWriteTime, State#btsmv_context{write_queue = queue:in({From, Req}, NewQueue)})
			end;
		_ ->
			State#btsmv_context{write_queue = queue:in({From, Req}, Queue)}
	end.

continue_write(From, Type, Key, Value, WriteTime, #btsmv_context{self = Self, checksum = IsCheckSum} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	case write_dat(Type, DatHandle, Key, Value, IsCheckSum) of
		{?DATA_ASYN_WAIT, FA} ->
			{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
			case b_trees:lookup(IdxHandle, Key) of
				{ok, {DatPoint, Vsn, Time}} ->
					(bs_variable:new_data_reader(DatHandle, DatPoint, FA))(Self, Self, From, Type, ?VALUE_INDEX, Key, Vsn, Time, {Type, IsCheckSum, WriteTime});
				{?ASYN_WAIT, Reader} ->
					Reader(Self, Self, From, Type, ?KEY_INDEX, {Type, Key, Value, IsCheckSum, WriteTime})
			end,
    		State;
		{ok, R} ->
			{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
			continue_write1(IdxHandle, DatHandle, R, From, Self, Type, Key, Value, WriteTime, IsCheckSum, State);
		Reason ->
			reply(From, Type, {error, Reason}),
			State
	end.

continue_write1(IdxHandle, DatHandle, R, From, Self, Type, Key, Value, WriteTime, IsCheckSum, State) ->
	case write_idx(Type, From, Self, IdxHandle, DatHandle, Key, IsCheckSum, WriteTime, R) of
		{ok, IdxResult, DatResult} ->		
			{IdxReset, IdxCacheL, IdxFreeL}=save_idx(IdxResult),
			bts_logger:write_log(State#btsmv_context.logger, new_write_log(Type, Key, Value, IsCheckSum, WriteTime)), %异步将写请求追加到写操作日志
			State#btsmv_context{buffer = {continue_write_ok, reset_handle(IdxReset), IdxReset, IdxCacheL, IdxFreeL, DatResult}}; %重置存储管理器的当前外部索引句柄
		?ASYN_WAIT ->
			State;
		Reason ->
			reply(From, Type, {error, Reason}),
			State
	end.

write_dat(insert, DatHandle, Key, Value, IsCheckSum) ->
	bs_variable:insert(DatHandle, Key, Value, IsCheckSum);
write_dat(update, DatHandle, Key, Value, IsCheckSum) ->
	bs_variable:update(DatHandle, Key, Value, IsCheckSum);
write_dat(delete, DatHandle, Key, _, _) ->
	bs_variable:delete(DatHandle, Key).

write_idx(insert, From, Self, IdxHandle, DatHandle, Key, IsCheckSum, WriteTime, {_, NewDatPoint, _} = R) ->
	insert_idx(IdxHandle, From, Self, Key, NewDatPoint, IsCheckSum, WriteTime, {?VALUE_INDEX, DatHandle, R});
write_idx(update, From, Self, IdxHandle, DatHandle, Key, IsCheckSum, WriteTime, {_, NewDatPoint, _} = R) ->
	update_idx(IdxHandle, From, Self, Key, NewDatPoint, IsCheckSum, WriteTime, {?VALUE_INDEX, DatHandle, R});
write_idx(delete, From, Self, IdxHandle, DatHandle, Key, IsCheckSum, WriteTime, {_, NewDatPoint, _} = R) ->
	delete_idx(IdxHandle, From, Self, Key, NewDatPoint, IsCheckSum, WriteTime, {?VALUE_INDEX, DatHandle, R}).
	
save_idx_cache([{_, {Point, _} = N, _}|T], IdxHandle, L1, L2) ->
	save_idx_cache(T, IdxHandle, [N|L1], [Point|L2]);
save_idx_cache([], IdxHandle, L1, L2) ->
	true=b_trees:save(IdxHandle, L1),
	L2.

save_idx({?KEY_INDEX, IdxHandle, {NewRootPoint, Frees, Writes}}) ->
	OldRootPoint=b_trees:get_root(IdxHandle),
	{{?KEY_INDEX, NewRootPoint}, {?KEY_INDEX, save_idx_cache(Writes, IdxHandle, [], [])}, {?KEY_INDEX, OldRootPoint, Frees}}.

save_data({?VALUE_INDEX, _, {?EMPTY, ?EMPTY, {Key, Value}}}) ->
	erlang:error({save_data_failed, Key, Value});
save_data({?VALUE_INDEX, DatHandle, {OldPoint, ?EMPTY, Key}}) ->
	{bs_variable:unsave(DatHandle, Key), OldPoint, ?EMPTY};
save_data({?VALUE_INDEX, DatHandle, {OldPoint, NewPoint, {Key, Value}}}) ->
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	{ok, {_, Vsn, Time}}=b_trees:lookup(IdxHandle, Key),
	case OldPoint of
		?EMPTY ->
			{bs_variable:save(DatHandle, Key, {NewPoint, Value, Vsn, Time}), OldPoint, NewPoint};
		_ ->
			{bs_variable:unsave(DatHandle, Key) and bs_variable:save(DatHandle, Key, {NewPoint, Value, Vsn, Time}), OldPoint, NewPoint}
	end.

insert_idx(IdxHandle, From, Self, Key, DatPoint, IsCheckSum, WriteTime, DatResult) ->
	case b_trees:insert(IdxHandle, Key, DatPoint, IsCheckSum) of
		{?ASYN_WAIT, IdxReader} ->
			IdxReader(Self, Self, From, insert_idx, ?KEY_INDEX, {insert, Key, DatPoint, IsCheckSum, WriteTime}),
			{_, _, Data}=DatResult,
			put({?ASYN_WAIT, ?KEY_INDEX, From}, Data),
			?ASYN_WAIT;
		{ok, R} ->
			{ok, {?KEY_INDEX, IdxHandle, R}, DatResult};
		E ->
			E
	end.

update_idx(IdxHandle, From, Self, Key, DatPoint, IsCheckSum, WriteTime, DatResult) ->
	case b_trees:update(IdxHandle, Key, DatPoint, IsCheckSum) of
		{?ASYN_WAIT, IdxReader} ->
			IdxReader(Self, Self, From, update_idx, ?KEY_INDEX, {update, Key, DatPoint, IsCheckSum, WriteTime}),
			{_, _, Data}=DatResult,
			put({?ASYN_WAIT, ?KEY_INDEX, From}, Data),
			?ASYN_WAIT;
		{ok, R} ->
			{ok, {?KEY_INDEX, IdxHandle, R}, DatResult};
		E ->
			E
	end.

delete_idx(IdxHandle, From, Self, Key, ?EMPTY, IsCheckSum, WriteTime, DatResult) ->
	case b_trees:delete(IdxHandle, Key, IsCheckSum) of
		{?ASYN_WAIT, IdxReader} ->
			IdxReader(Self, Self, From, delete_idx, ?KEY_INDEX, {delete, Key, ?EMPTY, IsCheckSum, WriteTime}),
			{_, _, Data}=DatResult,
			put({?ASYN_WAIT, ?KEY_INDEX, From}, Data),
			?ASYN_WAIT;
		{ok, R} ->
			{ok, {?KEY_INDEX, IdxHandle, R}, DatResult};
		E ->
			E
	end.

rollback_continue_write(LastIdxHandle, IdxCacheL, IdxFreeL, DatPoint) ->
	rollback_reset_handle(LastIdxHandle),
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	rollback_save(IdxCacheL, IdxHandle),
	rollback_free([DatPoint|IdxFreeL]).

rollback_reset_handle({?KEY_INDEX, LastIdxHandle, LastVsn}) ->
	put({?INDEX, ?KEY_INDEX}, {LastIdxHandle, LastVsn}).

rollback_save({_Idx, Points}, IdxHandle) ->
	{IdxM, IdxCache}=b_trees:get_cache(IdxHandle),
	rollback_save_cache(Points, IdxM, IdxCache).
			
rollback_save_cache([Point|T], M, Cache) ->
	M:delete(Cache, Point),
	rollback_save_cache(T, M, Cache);
rollback_save_cache([], _, _) ->
	ok.	

rollback_free([{?KEY_INDEX, _, Points}|T]) ->
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	{IdxM, IdxCache}=b_trees:get_cache(IdxHandle),
	rollback_free_cache(Points, IdxM, IdxCache),
	rollback_free(T);
rollback_free([Point|T]) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{DatM, DatCache}=bs_variable:get_cache(DatHandle),
	rollback_free_cache(Point, DatM, DatCache),
	rollback_free(T);
rollback_free([]) ->
	ok.

rollback_free_cache([Point|T], M, Cache) ->
	M:uncopy(Cache, Point),
	rollback_free_cache(T, M, Cache);
rollback_free_cache([], _, _) ->
	ok;
rollback_free_cache(Point, M, Cache) ->
	M:uncopy(Cache, Point),
	ok.

%%构建一个写操作日志
new_write_log(delete, Key, _, IsCheckSum, WriteTime) ->
	{WriteTime, delete, Key, IsCheckSum};
new_write_log(Type, Key, Value, IsCheckSum, WriteTime) ->
	{WriteTime, Type, Key, Value, IsCheckSum}.

reset_handle({?KEY_INDEX, NewRootPoint}) ->
	{IdxHandle, Vsn}=get({?INDEX, ?KEY_INDEX}),
	put({?INDEX, ?KEY_INDEX}, {b_trees:set_root(IdxHandle, NewRootPoint), Vsn + 1}),
	{?KEY_INDEX, IdxHandle, Vsn}.

reply({_, _} = From, _, Msg) ->
	z_lib:reply(From, Msg);
reply(From, Type, Msg) ->
	From ! {self(), Type, Msg}.

get_refs({_, ?EMPTY, _}, _, #btsmv_context{self = Self, btqmv = BTQMV} = State) ->
	BTQMV ! {Self, get_ref, []},
	State;
get_refs({Idx, OldRootPoint, IdxFrees}, DatFree, #btsmv_context{self = Self, wait_frees = WaitFrees, btqmv = BTQMV} = State) ->
	BTQMV ! {Self, get_ref, [{Idx, OldRootPoint}]},
	State#btsmv_context{wait_frees = sb_trees:insert({Idx, OldRootPoint}, {IdxFrees, DatFree}, WaitFrees)}.

time_get_refs([{Idx, OldRootPoint}|T], State, L) ->
	time_get_refs(T, State, [{Idx, OldRootPoint}|L]);
time_get_refs([], #btsmv_context{btqmv = ?EMPTY}, _) ->
	ignore;
time_get_refs([], #btsmv_context{self = Self, btqmv = BTQMV}, L) ->
	BTQMV ! {Self, get_ref, L}.

filter_frees([{?KEY_INDEX, OldRootPoint, ?DEREF}|T], #btsmv_context{wait_frees = WaitFrees} = State) ->
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	Key={?KEY_INDEX, OldRootPoint},
	{IdxM, IdxCache}=b_trees:get_cache(IdxHandle),
	case sb_trees:lookup(Key, WaitFrees) of
		{_, {IdxFrees, DatFree}} ->
			frees_idx(IdxFrees, IdxM, IdxCache, b_trees:get_alloter(IdxHandle)),
			{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
			{DatM, DatCache}=bs_variable:get_cache(DatHandle),
			free_data(DatFree, DatM, DatCache, bs_variable:get_alloter(DatHandle)),
			filter_frees(T, State#btsmv_context{wait_frees = sb_trees:delete(Key, WaitFrees)});
		none ->
			filter_frees(T, State)
	end;
filter_frees([{?KEY_INDEX, _, _}|T], State) ->
	filter_frees(T, State);
filter_frees([], State) ->
	State.

frees_idx([{_, Offset} = IdxPoint|T], IdxM, IdxCache, Alloter) ->
	IdxM:delete(IdxCache, IdxPoint),
	{ok, _}=bs_alloter:free(Alloter, {bs_alloter:pos(IdxPoint), Offset}),
	frees_idx(T, IdxM, IdxCache, Alloter);
frees_idx([?EMPTY|T], IdxM, IdxCache, Alloter) ->
	frees_idx(T, IdxM, IdxCache, Alloter);
frees_idx([], _, _, _) ->
	ok.

free_data({_, Offset} = DatPoint, DatM, DatCache, Alloter) ->
	DatM:delete(DatCache, DatPoint),
	{ok, _}=bs_alloter:free(Alloter, {bs_alloter:pos(DatPoint), Offset}),
	ok;
free_data(?EMPTY, _, _, _) ->
	ok.

get_frees(WaitFrees, Count) ->
	get_frees(Count, sb_trees:iterator(WaitFrees), []).
	
get_frees(0, _, L) ->
	L;
get_frees(Count, Iterator, L) ->
	case sb_trees:next(Iterator) of
		{{Idx, OldRootPoint}, _, NewIterator} ->
			get_frees(Count - 1, NewIterator, [{Idx, OldRootPoint}|L]);
		none ->
			L
	end.

persistent(#btsmv_context{cache_capacity = Capacity, persistence_factor = Factor} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{DatM, DatCache}=bs_variable:get_cache(DatHandle),
	DatCacheSize=DatM:memory_size(DatCache),
	if
		DatCacheSize =< (Capacity * Factor) ->
			case get({?INDEX, ?KEY_INDEX}) of
				{IdxHandle, _} ->
					{IdxM, IdxCache}=b_trees:get_cache(IdxHandle),
					IdxCacheSize=IdxM:memory_size(IdxCache),
					if
						IdxCacheSize =< (Capacity * Factor) ->
							State;
						true ->
							persistent(b_trees:get_root(IdxHandle), IdxM, IdxCache, DatM, DatCache, State)
					end;
				undefined ->
					erlang:error({error, {persistent_error, {idx_not_exist, ?KEY_INDEX}}})
			end,
			State;
		true ->
			case get({?INDEX, ?KEY_INDEX}) of
				{IdxHandle, _} ->
					{IdxM, IdxCache}=b_trees:get_cache(IdxHandle),
					persistent(b_trees:get_root(IdxHandle), IdxM, IdxCache, DatM, DatCache, State);
				undefined ->
					erlang:error({error, {persistent_error, {idx_not_exist, ?KEY_INDEX}}})
			end
	end.

persistent(IdxRootPoint, IdxM, IdxCache, DatM, DatCache, #btsmv_context{logger = Logger} = State) ->
	DatSize=DatM:size(DatCache, 1),
	IdxSize=IdxM:size(IdxCache, 1),
	if
		(IdxSize =< 0) and (IdxRootPoint =:= ?EMPTY) ->
			persistent(Logger, [{?DAT_INDEX, ?EMPTY}, {?IDX_INDEX, IdxRootPoint}], State);
		(DatSize > 0) or (IdxSize > 0) ->
			persistent(Logger, [{?DAT_INDEX, ?EMPTY}, {?IDX_INDEX, IdxRootPoint}], State);
		true ->
			State
	end.

persistent(Logger, L, #btsmv_context{commit_uid = CommitUid} = State) ->
	NewCommitUid = CommitUid + 1,
	io:format("!!!!!!NewCommitUid:~p~n", [NewCommitUid]),
	case bts_logger:persistent(Logger, NewCommitUid, L) of
		ok ->
			State#btsmv_context{commit_uid = NewCommitUid};
		{error, busy} ->
			State;
		{error, {repeat_commit_uid, NowCommitUid}} ->
			io:format("!!!!!!NowCommitUid:~p, NewCommitUid:~p~n", [NowCommitUid, NewCommitUid]),
			persistent(Logger, L, State#btsmv_context{commit_uid = NowCommitUid});
		{error, _} ->
			State
	end.

collate() ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	bs_variable:collate(DatHandle),
	b_trees:collate(IdxHandle).

