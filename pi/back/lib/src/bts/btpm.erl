%% 
%% @doc 持久化管理器，用于持久化批量的写操作
%%


-module(btpm).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("btpm.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/3, load/1, persistent/4, serialize/3, deserialize/4, set/7]).
-compile(export_all). %临时使用，调试完成后删除

%%
%% 启动持久化管理器
%% 
start_link(Args, Name, Path) when is_atom(Name) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, {Args, Name, Path}, []).

%%
%%加载超级块
%%
load(BTPM) when is_pid(BTPM) ->
	gen_server:call(BTPM, load, 30000).

%%
%% 持久化指定的快照缓存数据
%%
persistent(BTPM, From, NewCommitUid, L) ->
	BTPM ! {persistent, From, NewCommitUid, L},
	?ASYN_WAIT.

%%
%%序列化
%%
serialize(_, Block, true) ->
	block_to_bytes_by_checksum(Block);
serialize(_, Block, false) ->
	block_to_bytes(Block).

%%
%%反序列化
%%
deserialize(_, Bin, Size, false) ->
	bytes_to_block(Bin, Size);
deserialize(_, Bin, Size, CheckSum) ->
	bytes_to_block_by_checksum(Bin, Size, CheckSum).

%%
%%设置相关进程
%%
set(BTPM, DatCache, DatAlloter, IdxCache, IdxAlloter, BTSM, Logger) when is_pid(BTPM), is_pid(BTSM), is_pid(Logger) ->
	gen_server:call(BTPM, {set, DatCache, DatAlloter, IdxCache, IdxAlloter, BTSM, Logger}).

%%
%% 关闭持久化管理器
%%
stop(BTPM) when is_pid(BTPM) ->
	gen_server:call(BTPM, stop, 30000).

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
	IsCheckSum=z_lib:get_value(Args, ?IS_CHECKSUM, ?DEFAULT_CHECKSUM),
	Serializer=z_lib:get_value(Args, ?SERIALIZER, ?DEFAULT_SERIALIZER),
	Deserializer=z_lib:get_value(Args, ?DESERIALIZE, ?DEFAULT_DESERIALIZE),
	SupFile=filename:join(Path, lists:concat([Name, ?SUP_EXT])),
	case init_sup(SupFile) of
		{ok, SupFd} ->
			case init_dat(Path, Name) of
				{ok, DatFd} ->
					case init_idx(Args, Path, Name) of
						{ok, IdxFd} ->
		    				{ok, #btpm_context{self = Self, sup_file = SupFile, checksum = IsCheckSum,
											   serialize = Serializer, deserialize = Deserializer, 
											   sup_fd = SupFd, dat_fd = DatFd, idx_fd = IdxFd}};
						E ->
							{stop, E}
					end;
				E ->
					{stop, E}
			end;
		E ->
			{stop, E}
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
handle_call(load, _, State) ->
	case load_sup(State) of
		{ok, SupBlock} ->
			{reply, {ok, SupBlock}, State, ?BTPM_HIBERNATE_TIMEOUT};
		Reason ->
			{stop, Reason, Reason, State}
	end;
handle_call({set, DatCache, DatAlloter, IdxCache, IdxAlloter, BTSM, Logger}, _, State) ->
	{reply, ok, State#btpm_context{dat_cache = DatCache, dat_alloter = DatAlloter, idx_cache = IdxCache, 
								   idx_alloter = IdxAlloter, logger = Logger, btsm = BTSM}, ?BTPM_HIBERNATE_TIMEOUT};
handle_call(stop, {Bts, _}, State) ->
	unlink(Bts),
	{stop, normal, ok, State};
handle_call(_, _, State) ->
    {noreply, State, ?BTPM_HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?BTPM_HIBERNATE_TIMEOUT}.


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
handle_info({persistent, From, NewCommitUid, L}, State) ->
	{noreply, commit(write(L, From, State), NewCommitUid, State), ?BTPM_HIBERNATE_TIMEOUT};
handle_info({'EXIT', Pid, Why}, State) ->
	{stop, {process_exit, Pid, Why}, State};
handle_info(_, State) ->
    {noreply, State, ?BTPM_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(normal, #btpm_context{sup_fd = SupFd, dat_fd = DatFd, idx_fd = IdxFd}) ->
	%TODO 关闭存储管理器时的处理...
	file:close(IdxFd),
	file:close(DatFd),
	file:close(SupFd);
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

init_sup(SupFile) ->
	file:open(SupFile, [read, write, raw, binary]).
	
init_dat(Path, Name) ->
	file:open(filename:join(Path, lists:concat([Name, ?DAT_EXT])), [read, write, raw, binary]).

init_idx(Args, Path, Name) ->
	case z_lib:get_value(Args, ?IS_IDX, ?DEFAULT_IS_IDX) of
		true ->
			file:open(filename:join(Path, lists:concat([Name, ?IDX_EXT])), [read, write, raw, binary]);
		false ->
			{ok, ?EMPTY}
	end.

load_sup(#btpm_context{checksum = IsCheckSum, deserialize = {M, A}, sup_fd =SupFd}) ->
	case file:position(SupFd, eof) of
		{ok, 0} ->
			{ok, ?EMPTY};
		{ok, Eof} ->
			R=case prim_file:pread(SupFd, 0, Eof) of
				{ok, <<Size:32, CheckSum:32, Part/binary>>} when IsCheckSum ->
					<<Bin:Size/binary, _/binary>>=Part,
					M:deserialize(A, Bin, Size, CheckSum);
				{ok, <<Size:32, Part/binary>>} ->
					<<Bin:Size/binary, _/binary>>=Part,
					M:deserialize(A, Bin, Size, false);
				{error, Reason} ->
					{init_sup_error, {read_sup_error, Reason}};
				Reason ->
					{init_sup_error, {read_sup_error, Reason}}
			end,
			{ok, R}
	end.

persistent_data([{Index, RootPoint}|T], From, State, L1, L2) ->
	case 
		case Index of
					   ?DAT_INDEX ->
						   {State#btpm_context.dat_cache, State#btpm_context.dat_alloter, State#btpm_context.dat_fd};
					   ?IDX_INDEX ->
						   {State#btpm_context.idx_cache, State#btpm_context.idx_alloter, State#btpm_context.idx_fd}
		end
	of
		{{M, Cache}, Alloter, Fd} ->
			FreeBlocks=bs_alloter:to_free(Alloter),
			{NS, Points}=persistent_data1(M:keys(Cache, 1), M, Cache, [], []),
			persistent_data(T, From, State, [{Fd, NS}|L1], [{Index, RootPoint, FreeBlocks, Points}|L2]);
		{_, _, Fd} ->
			persistent_data(T, From, State, [{Fd, []}|L1], [{Index, RootPoint, [], []}|L2])
	end;
persistent_data([], From, #btpm_context{self = Self, logger = Logger}, L1, L2) ->
	Logger ! {Self, persistent_ok, From},
	{lists:reverse(L1), lists:reverse(L2)}.

persistent_data1([Key|T], M, Cache, L1, L2) ->
	case M:get(Cache, Key) of
		{ok, _, {{_, _} = DatPoint, Value, _, _}} ->
			persistent_data1(T, M, Cache, [{DatPoint, Value}|L1], [Key|L2]);
		{ok, _, Node} ->
			persistent_data1(T, M, Cache, [{Key, Node}|L1], [Key|L2]);
		false ->
			erlang:error({persistent_data_error, {key_not_exist, Key}})
	end;
persistent_data1([], _, _, L1, L2) ->
	{L1, L2}.

block_to_bytes_by_checksum(Block) ->
	Bin=term_to_binary(Block),
	Size=size(Bin),
	CheckSum=erlang:adler32(Bin),
	<<Size:32, CheckSum:32, Bin/binary>>.

block_to_bytes(Block) ->
	Bin=term_to_binary(Block),
	Size=size(Bin),
	<<Size:32, Bin/binary>>.

bytes_to_block_by_checksum(Bytes, Size, CheckSum) ->
	<<Bin:Size/binary, _/binary>>=Bytes,
	case erlang:adler32(Bin) of
		CheckSum ->
			binary_to_term(Bin);
		E ->
			erlang:error({bytes_to_block_error, {checksum_error, CheckSum, E}})
	end.

bytes_to_block(Bytes, Size) ->
	<<Bin:Size/binary, _/binary>>=Bytes,
	binary_to_term(Bin).

write([{Fd, NS}|T], M, A, IsCheckSum) ->
	write1(NS, Fd, M, A, IsCheckSum),
	write(T, M, A, IsCheckSum);
write([], _, _, _) ->
	ok.

write1([{{_, Offset}, Block}|T], Fd, M, A, IsCheckSum) ->
	case prim_file:pwrite(Fd, Offset, M:serialize(A, Block, IsCheckSum)) of
		ok ->
			write1(T, Fd, M, A, IsCheckSum);
		{error, Reason} ->
			erlang:error({error, {write_block_error, Reason}})
	end;
write1([], ?EMPTY, _, _, _) ->
	ok;
write1([], Fd, _, _, _) ->
	case file:datasync(Fd) of
		ok ->
			ok;
		{error, Reason} ->
			erlang:error({error, {write_sync_error, Reason}})
	end.

write(L, From, #btpm_context{checksum = IsCheckSum, serialize = {M, A}} = State) ->
	{DataL, SupL}=persistent_data(L, From, State, [],[]),
	ok=write(DataL, M, A, IsCheckSum),
	SupL.

commit([{?DAT_INDEX, DatNewRootPoint, DatFreeBlocks, DatPoints}, {?IDX_INDEX, IdxNewRootPoint, IdxFreeBlocks, IdxPoints}], NewCommitUid, #btpm_context{self = Self, sup_file = SupFile, checksum = IsCheckSum, 
																					   serialize = {M, A}, sup_fd = SupFd, logger = Logger, 
																					   btsm = BTSM} = State) ->
	case file:datasync(SupFd) of
		ok ->
			case file:close(SupFd) of
				ok ->
					case file:rename(SupFile, lists:concat([SupFile, ".", NewCommitUid])) of
						ok ->
							case init_sup(SupFile) of
								{ok, NewSupFd} ->
									case prim_file:write(NewSupFd, M:serialize(A, {DatNewRootPoint, DatFreeBlocks, IdxNewRootPoint, IdxFreeBlocks, NewCommitUid}, IsCheckSum)) of
										ok ->
											%io:format("!!!!!!commit ok~n"),
											Logger ! {Self, free, NewCommitUid},
											BTSM ! {Self, commit_ok, DatPoints, IdxPoints},
											State#btpm_context{sup_fd = NewSupFd};	
										{error, Reason} ->
											erlang:error({commit_error, {write_sup_error, Reason}})
									end;
								{error, Reason} ->
									erlang:error({commit_error, {new_sup_error, Reason}})
							end;
						{error, Reason} ->
							erlang:error({commit_error, {rename_sup_error, Reason}})
					end;
				{error, Reason} ->
					erlang:error({commit_error, {close_sup_error, Reason}})
			end;
		{error, Reason} ->
			erlang:error({commit_error, {sync_sup_error, Reason}})
	end;
commit(_, _, State) ->
	State.