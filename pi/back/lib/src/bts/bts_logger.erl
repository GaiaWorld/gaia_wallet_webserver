%% 
%% @doc 写操作记录器
%%


-module(bts_logger).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("bts_logger.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/3, check/2, restore/4, restore/3, write_log/2, persistent/3, serialize/2, set/6, stop/1]).
-compile(export_all). %临时使用，调试完成后删除

%%
%% 启动写操作日志记录器
%% 
start_link(Args, Name, Path) when is_atom(Name) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, {Args, Name, Path}, []).

%%
%% 自检写操作日志记录器
%%
check(Logger, Timeout) ->
	gen_server:call(Logger, check, Timeout).

%%
%% 恢复写操作日志记录器
%%
restore(Logger, MFA, RestoreLogFile, Timeout) ->
	gen_server:call(Logger, {restore, MFA, RestoreLogFile}, Timeout).

%%
%% 通过写操作日志进行恢复
%%
restore(BTSM, From, Fd) ->
	spawn(fun() ->
				  try
					  load_restore_log(From, BTSM, Fd, 0, 0)
				  catch
					  _:Reason ->
						  From ! {self(), restore_error, {Reason, erlang:get_stacktrace()}}
				  end
		  end).

%%
%% 异步发送待插入的写操作日志 
%%
write_log(Logger, WriteLog) ->
	Logger ! {write_log, WriteLog},
	?ASYN_WAIT.

%%
%% 异步发送持久化的信息
%%
persistent(Logger, NewCommitUid, L) ->
	try
		gen_server:call(Logger, {persistent, NewCommitUid, L}, 30000)
	catch
		_:Reason ->
			{error, Reason}
	end.

%%
%% 默认的序列化
%%
serialize(_, WriteLog) ->
	Bin=term_to_binary(WriteLog),
	{ok, erlang:adler32(Bin), Bin}.

%%
%%设置相关进程
%%
set(Logger, DatCache, IdxCache, {_, _, _, _, CommitUid}, BTSM, BTPM) when is_pid(Logger), is_pid(BTSM), is_pid(BTPM) ->
	gen_server:call(Logger, {set, DatCache, IdxCache, CommitUid, BTSM, BTPM});
set(Logger, DatCache, IdxCache, ?EMPTY, BTSM, BTPM) when is_pid(Logger), is_pid(BTSM), is_pid(BTPM) ->
	gen_server:call(Logger, {set, DatCache, IdxCache, 0, BTSM, BTPM}).

%%
%% 关闭写操作日志记录器
%%
stop(Logger) when is_pid(Logger) ->
	gen_server:call(Logger, stop, 30000).

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
init({Args, Name, Path}) ->
	Self=self(),
	SnapshootFileName=filename:join(Path, lists:concat([Name, ?SNAPSHOOT_EXT])),
	LogFileName=filename:join(Path, lists:concat([Name, ?LOG_EXT])),
	LogSerializer=z_lib:get_value(Args, ?LOG_SERIALIZER, ?DEFAULT_LOG_SERIALIZER),
	SyncLevel=z_lib:get_value(Args, ?SYNC_LEVEL, ?DEFAULT_SYNC_LEVEL),
	SyncTimerRef=case SyncLevel of
		{_, Timeout} ->
			erlang:start_timer(Timeout, Self, sync);
		_ ->
			?EMPTY
	end,
	MaxErrorTimes=z_lib:get_value(Args, ?MAX_ERROR_TIMES, ?DEFAULT_MAX_ERROR_TIMES),
	{ok, LogFd}=file:open(LogFileName, [append, raw, binary]),
    {ok, #bts_logger_context{self = Self, sync_request = ?EMPTY, log_file = LogFileName, snapshoot_file = SnapshootFileName,
							 log_fd = LogFd, snapshoot_fd = ?EMPTY_FD, 
							 serializer = LogSerializer, sync_level = SyncLevel, buffer_size = 0, 
							 sync_timer = SyncTimerRef, max_error_times = MaxErrorTimes, error_count = 0}}.

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
handle_call({persistent, NewCommitUid, _}, _, #bts_logger_context{commit_uid = CommitUid} = State) when NewCommitUid =< CommitUid ->
	{reply, {error, {repeat_commit_uid, CommitUid}}, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_call({persistent, NewCommitUid, L}, From, State) ->
	{noreply, persistent(L, From, NewCommitUid, State), ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_call({set, DatCache, IdxCache, CommitUid, BTSM, BTPM}, _, State) ->
	{reply, ok, State#bts_logger_context{dat_cache = DatCache, idx_cache = IdxCache, btsm = BTSM, btpm = BTPM, commit_uid = CommitUid}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_call(check, _, #bts_logger_context{log_file = LogFile} = State) ->
	RestoreLogFile=lists:concat([LogFile, ?RESTORE_EXT]),
	case check_write_log(LogFile, RestoreLogFile) of
		ok ->
			{reply, ok, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
		restore ->
			{reply, {restore, RestoreLogFile}, init_restore(RestoreLogFile, State), ?BTS_LOGGER_HIBERNATE_TIMEOUT};
		continue_restore ->
			{reply, {restore, RestoreLogFile}, init_restore(?EMPTY, State), ?BTS_LOGGER_HIBERNATE_TIMEOUT};
		{error, Reason} ->
			{stop, self_check_falied, {error, {self_check_falied, LogFile, Reason}}, State}
	end;
handle_call({restore, Restorer, RestoreLogFile}, From, #bts_logger_context{sync_request = Request} = State) ->
	case Request of
		?EMPTY ->
			{noreply, handle_restore(From, Restorer, RestoreLogFile, State), ?BTS_LOGGER_HIBERNATE_TIMEOUT};
		{_, _, _} ->
			{reply, {error, busy}, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT}
	end;
handle_call(stop, {Bts, _}, State) ->
	unlink(Bts),
	{stop, normal, ok, State};
handle_call(_, _, State) ->
    {noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT}.


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
handle_info({write_log, WriteLog}, #bts_logger_context{self = Self, log_fd = LogFd, serializer = LogSerializer, 
															sync_level = {Sizeout, _}, buffer_size = BufferSize, max_error_times = MaxErrorAmount, error_count = Count, btsm = BTSM} = State) ->
	Bytes=log_to_bytes(LogSerializer, WriteLog),
	case prim_file:write(LogFd, Bytes) of
		ok ->
			NewBufferSize=BufferSize + size(Bytes),
			if
				Sizeout > NewBufferSize ->
					BTSM ! {Self, write_log_ok},
					{noreply, State#bts_logger_context{buffer_size = NewBufferSize}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
				true ->
					case file:datasync(LogFd) of
						ok ->
							BTSM ! {Self, write_log_ok},
							{noreply, State#bts_logger_context{buffer_size = 0, error_count = 0}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
						{error, Reason} ->
							BTSM ! {Self, {write_log_error, {sync_log_error, Reason}}},
							if
								MaxErrorAmount > Count ->
									{noreply, State#bts_logger_context{error_count = Count + 1}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
								true ->
									erlang:error({error, {sync_log_error, Reason}})
							end
					end
			end;
		{error, Reason} ->
			BTSM ! {Self, {write_log_error, Reason}},
			{noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT}
	end;
handle_info({write_log, WriteLog}, #bts_logger_context{self = Self, log_fd = LogFd, serializer = LogSerializer, 
															sync_level = ?ERVER_SYNC_LEVEL, max_error_times = MaxErrorAmount, error_count = Count, btsm = BTSM} = State) ->
	case prim_file:write(LogFd, log_to_bytes(LogSerializer, WriteLog)) of
		ok ->
			case file:datasync(LogFd) of
				ok ->
					BTSM ! {Self, write_log_ok},
					{noreply, State#bts_logger_context{error_count = 0}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
				{error, Reason} ->
					BTSM ! {Self, {write_log_error, {sync_log_error, Reason}}},
					if
						MaxErrorAmount > Count ->
							{noreply, State#bts_logger_context{error_count = Count + 1}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
						true ->
							erlang:error({error, {sync_log_error, Reason}})
					end
			end;
		{error, Reason} ->
			BTSM ! {Self, {write_log_error, Reason}},
			{noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT}
	end;
handle_info({write_log, WriteLog}, #bts_logger_context{self = Self, log_fd = LogFd, serializer = LogSerializer, sync_level = ?AUTO_SYNC_LEVEL, btsm = BTSM} = State) ->
	case prim_file:write(LogFd, log_to_bytes(LogSerializer, WriteLog)) of
		ok ->
			BTSM ! {Self, write_log_ok};
		{error, Reason} ->
			BTSM ! {Self, {write_log_error, Reason}}
	end,
	{noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_info({BTPM, free, NewCommitUid}, #bts_logger_context{btpm = BTPM} = State) ->
	{noreply, free(NewCommitUid, State), ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, sync}, #bts_logger_context{self = Self, log_fd = LogFd, sync_level = {_, Timeout}, 
														   buffer_size = BufferSize, sync_timer = TimerRef, max_error_times = MaxErrorAmount, error_count = Count} = State) ->
	if
		BufferSize > 0 ->
			case file:datasync(LogFd) of
				ok ->
					{noreply, State#bts_logger_context{buffer_size = 0, sync_timer = erlang:start_timer(Timeout, Self, sync), error_count = 0}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
				{error, Reason} ->
					if
						MaxErrorAmount > Count ->
							{noreply, State#bts_logger_context{sync_timer = erlang:start_timer(Timeout, Self, sync), error_count = Count + 1}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
						true ->
							erlang:error({error, {sync_log_error, Reason}})
					end
			end;
		true ->
			{noreply, State#bts_logger_context{sync_timer = erlang:start_timer(Timeout, Self, sync)}, ?BTS_LOGGER_HIBERNATE_TIMEOUT}
	end;
handle_info({BTPM, persistent_ok, From}, #bts_logger_context{btpm = BTPM} = State) ->
	z_lib:reply(From, ok),
	{noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_info({Pid, restore_ok, Count}, #bts_logger_context{sync_request = {From, Pid, Fd}} = State) ->
	z_lib:reply(From, {ok, Count}),
	ok=file:close(Fd),
	{noreply, State#bts_logger_context{sync_request = ?EMPTY}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_info({Pid, restore_error, Reason}, #bts_logger_context{sync_request = {From, Pid, Fd}} = State) ->
	z_lib:reply(From, {error, Reason}),
	ok=file:close(Fd),
	{noreply, State#bts_logger_context{sync_request = ?EMPTY}, ?BTS_LOGGER_HIBERNATE_TIMEOUT};
handle_info({'EXIT', Pid, Why}, State) ->
	{stop, {process_exit, Pid, Why}, State};
handle_info(_, State) ->
    {noreply, State, ?BTS_LOGGER_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(normal, #bts_logger_context{log_fd = LogFd, snapshoot_fd = SnapFd, sync_timer = SyncTimer}) ->
	%TODO 关闭存储管理器时的处理...
	erlang:cancel_timer(SyncTimer),
	file:close(LogFd),
	file:close(SnapFd);
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

persistent(List, From, NewCommitUid, #bts_logger_context{btpm = BTPM} = State) ->
	case swap_log(State) of
		{ok, NewState} ->
			%io:format("!!!!!!swap log ok~n"),
			btpm:persistent(BTPM, From, NewCommitUid, List),
			NewState;
		{swap_log_error, snapshoot_log_exist} ->
			z_lib:reply(From, {error, busy}),
			State;
		Reason ->
			erlang:error({persistent_error, Reason})
	end.

swap_log(#bts_logger_context{log_file = LogFile, snapshoot_file = SnapFile, log_fd = LogFd} = State) ->
	case filelib:is_regular(SnapFile) of
		false ->
			case file:datasync(LogFd) of
				ok ->
					case file:close(LogFd) of
						ok ->
							case file:rename(LogFile, SnapFile) of
								ok ->
									case file:open(LogFile, [append, raw, binary]) of
										{ok, NewLogFd} ->
											{ok, State#bts_logger_context{log_fd = NewLogFd}};
										{error, Reason} ->
											{swap_log_error, {new_log_error, Reason}}
									end;
								{error, Reason} ->
									{swap_log_error, {rename_log_error, Reason}}
							end;
						{error, Reason} ->
							{swap_log_error, {close_snapshoot_error, Reason}}
					end;
				Reason ->
					{swap_log_error, {sync_log_error, Reason}}
			end;
		true ->
			{swap_log_error, snapshoot_log_exist}
	end.
	
log_to_bytes({M, A}, WriteLog) ->
	case M:serialize(A, WriteLog) of
		{ok, CheckSum, Bin} ->
			ByteSize=size(Bin),
			<<ByteSize:32, CheckSum:32, Bin/binary>>;
		E ->
			erlang:error({error, E})
	end. 

free(NewCommitUid, #bts_logger_context{snapshoot_file = SnapFile} = State) ->
	case file:rename(SnapFile, lists:concat([SnapFile, ".", NewCommitUid])) of
		ok ->
			State#bts_logger_context{snapshoot_fd = ?EMPTY_FD, commit_uid = NewCommitUid};
		{error, Reason} ->
			erlang:error({free_logger_error, {rename_snap_error, SnapFile, Reason}})
	end.

check_write_log(LogFile, RestoreLogFile) ->
	case filelib:is_regular(LogFile) of
		true ->
			case file:open(LogFile, [read, binary, raw]) of
				{ok, Fd} ->
					case file:position(Fd, eof) of
						{ok, 0} ->
							check_restore_log(RestoreLogFile);
						{ok, _} ->
							case file:close(Fd) of
								ok ->
									restore;
								E ->
									E
							end;
						E ->
							E
					end;
				E ->
					E
			end;
		false ->
			ok
	end.

check_restore_log(RestoreLogFile) ->
	case filelib:is_regular(RestoreLogFile) of
		true ->
			continue_restore;
		false ->
			ok
	end.

init_restore(RestoreLogFile, #bts_logger_context{log_file = LogFile, log_fd = LogFd} = State) ->
	case RestoreLogFile of
		?EMPTY ->
			State;
		_ ->
			case file:close(LogFd) of
				ok ->
					case file:rename(LogFile, RestoreLogFile) of
						ok ->
							case file:open(LogFile, [append, raw, binary]) of
								{ok, NewLogFd} ->
									State#bts_logger_context{log_fd = NewLogFd};
								{error, Reason} ->
									erlang:error(Reason)
							end;
						{error, Reason} ->
							erlang:error(Reason)
					end;
				{error, Reason} ->
					erlang:error(Reason)
			end
	end.

handle_restore(From, {M, F, A}, RestoreLogFile, #bts_logger_context{self = Self} = State) ->
	case file:open(RestoreLogFile, [read, binary]) of
		{ok, Fd} ->
			Pid=try
				M:F(A, Self, Fd)
			catch
				_:Reason ->
					ok=file:close(Fd),
					z_lib:reply(From, {error, {restore_failed, RestoreLogFile, Reason, erlang:get_stacktrace()}})
			end,
			State#bts_logger_context{sync_request = {From, Pid, Fd}};
		{error, Reason} ->
			z_lib:reply(From, {error, {restore_failed, RestoreLogFile, Reason}}),
			State
	end.

load_restore_log(From, BTSM, Fd, Start, Count) ->
	case file:pread(Fd, Start, 8) of
		{ok, <<Size:32, CheckSum:32>>} ->
			Loc=Start + 8,
			case file:pread(Fd, Loc, Size) of
				{ok, <<Bin/binary>>} ->
					case erlang:adler32(Bin) of
						CheckSum ->
							case
								case binary_to_term(Bin) of
									{WriteTime, insert, Key, Value, _} ->
										gen_server:call(BTSM, {write, insert, Key, Value, WriteTime});
									{WriteTime, update, Key, Value, _} ->
										gen_server:call(BTSM, {write, update, Key, Value, WriteTime});
									{WriteTime, delete, Key, _} ->
										gen_server:call(BTSM, {write, delete, Key, ?EMPTY, WriteTime})
								end
							of
								ok ->
									load_restore_log(From, BTSM, Fd, Loc + Size, Count + 1);
								Reason ->
									From ! {self(), restore_error, {restore_failed, Reason}}
							end;
						_ ->
							From ! {self(), restore_error, {restore_failed, invalid_checksum}}
					end;
				eof ->
					From ! {self(), restore_error, {restore_failed, invalid_body, Start, Size}};
				{error, Reason} ->
					From ! {self(), restore_error, {restore_failed, Reason}}
			end;
		eof ->
			From ! {self(), restore_ok, Count};
		{error, Reason} ->
			From ! {self(), restore_error, {restore_failed, Reason}}
	end.
