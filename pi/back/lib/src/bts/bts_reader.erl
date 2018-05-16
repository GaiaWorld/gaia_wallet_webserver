%% 
%% @doc 为bts提供随机读文件服务
%%


-module(bts_reader).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("bts_reader.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/4, read/5, asyn_read/5, deserialize/3, stop/1]).
-compile(export_all). %临时使用，调试完成后删除

%%
%% 启动文件读取器
%% 
start_link(Args, Name, File, IsCheckSum) when is_list(File) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, {Args, File, IsCheckSum}, []).

%%
%% 异步阻塞的随机读数据
%%
read(Pid, From, Args, {Size, Offset}, Timeout) ->
	gen_server:call(Pid, {read, From, Args, Offset, Size}, Timeout).

%%
%% 异步非阻塞的随机读数据
%%
asyn_read(Pid, Src, From, Args, {Size, Offset}) ->
	Pid ! {read, Src, From, Args, Offset, Size},
	?ASYN_WAIT.
	
%%
%%反序列化
%%
deserialize(_, Bin, true) ->
	bytes_to_block_by_checksum(Bin);
deserialize(_, Bin, false) ->
	bytes_to_block(Bin).

%%
%%关闭文件读取器
%%
stop(Reader) when is_pid(Reader) ->
	gen_server:call(Reader, stop, 30000).

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
init({Args, File, IsCheckSum}) ->
	Self=self(),
	Deserializer=z_lib:get_value(Args, ?DESERIALIZER, ?DEFAULT_DESERIALIZER),
	Sizeout=z_lib:get_value(Args, ?READ_BUFFER_SIZEOUT, ?DEFAULT_READ_BUFFER_SIZEOUT),
	Timeout=z_lib:get_value(Args, ?READ_BUFFER_TIMEOUT, ?DEFAULT_READ_BUFFER_TIMEOUT),
	case file:open(File, [read, raw, binary]) of
		{ok, Fd} ->
			{ok, #bts_reader_context{self = Self, checksum = IsCheckSum, deserializer = Deserializer, 
									 file = File, fd = Fd, buffer = queue:new(), 
									 buffer_sizeout = Sizeout, buffer_timeout = Timeout, 
									 timer_ref = erlang:start_timer(Timeout, Self, read)}, ?BTS_READER_HIBERNATE_TIMEOUT};
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
handle_call({read, From, Args, Offset, Size}, Src, State) ->
	{noreply, handle_read(Src, From, Args, Offset, Size, State), ?BTS_READER_HIBERNATE_TIMEOUT};
handle_call(stop, {Pid, _}, #bts_reader_context{fd = Fd} = State) ->
	case file:close(Fd) of
		ok ->
			unlink(Pid),
			{stop, normal, ok, State};
		{error, _} = E ->
			{reply, E, State, ?BTS_READER_HIBERNATE_TIMEOUT}
	end;
handle_call(_, _, State) ->
    {noreply, State, ?BTS_READER_HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?BTS_READER_HIBERNATE_TIMEOUT}.


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
handle_info({read, Src, From, Args, Offset, Size}, State) ->
	{noreply, handle_read(Src, From, Args, Offset, Size, State), ?BTS_READER_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, read}, #bts_reader_context{timer_ref = TimerRef} = State) ->
	{noreply, handle_read(State)};
handle_info(_, State) ->
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
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

handle_read(Src, From, Args, Offset, Size, #bts_reader_context{self = Self, checksum = IsCheckSum, deserializer = {M, A}, fd = Fd, buffer = Buffer, buffer_sizeout = Sizeout} = State) ->
	Len=queue:len(Buffer),
	if
		Sizeout > Len ->
			State#bts_reader_context{buffer = queue:in({Src, From, Args, Offset, Size}, Buffer)};
		true ->
			read_entrys(Self, Fd, queue:to_list(queue:in({Src, From, Args, Offset, Size}, Buffer)), M, A, IsCheckSum),
			State#bts_reader_context{buffer = queue:drop(Buffer)}
	end.

handle_read(#bts_reader_context{self = Self, checksum = IsCheckSum, deserializer = {M, A}, fd = Fd, buffer = Buffer, buffer_timeout = Timeout} = State) ->
	Len=queue:len(Buffer),
	if
		Len > 0 ->
			read_entrys(Self, Fd, queue:to_list(Buffer), M, A, IsCheckSum),
			State#bts_reader_context{buffer = queue:drop(Buffer), timer_ref = erlang:start_timer(Timeout, Self, read)};
		true ->
			State#bts_reader_context{timer_ref = erlang:start_timer(Timeout, Self, read)}
	end.
			
bytes_to_block_by_checksum(<<Size:32, CheckSum:32, Bin/binary>>) ->
	<<Block:Size/binary, _/binary>> = Bin,
	case erlang:adler32(Block) of
		CheckSum ->
			{ok, binary_to_term(Block)};
		BlockCheckSum ->
			{bytes_to_block_error, {invalid_checksum, CheckSum, BlockCheckSum}}
	end.

bytes_to_block(<<Size:32, Bin/binary>>) ->
	<<Block:Size/binary, _/binary>> = Bin,
	{ok, binary_to_term(Block)}.

read_entrys(Self, Fd, L, M, A, IsCheckSum) ->
	case prim_file:pread(Fd, [{Offset, Size} || {_, _, _, Offset, Size} <- L]) of
		{ok, BinL} ->
			read_ok(L, BinL, Self, M, A, IsCheckSum);
		{error, Reason} ->
			read_error(L, Self, {read_entrys_error, Reason})
	end.

read_ok([{Src, From, Args, Offset, Size}|T1], [eof|T2], Self, M, A, IsCheckSum) ->
	reply(Src, Self, {From, Args, {Size, Offset}, {error, eof}}),
	read_ok(T1, T2, Self, M, A, IsCheckSum);
read_ok([{Src, From, Args, Offset, Size}|T1], [Bin|T2], Self, M, A, IsCheckSum) ->
	reply(Src, Self, {From, Args, {Size, Offset}, M:deserialize(A, Bin, IsCheckSum)}),
	read_ok(T1, T2, Self, M, A, IsCheckSum);
read_ok([], _, _, _, _, _) ->
	ok.

read_error([{Src, From, Args, Offset, Size}|T], Self, E) ->
	reply(Src, Self, {From, Args, {Size, Offset}, {error, E}}),
	read_error(T, Self, E);
read_error([], _, _) ->
	ok.

reply({_, _} = From, _, Msg) ->
	z_lib:reply(From, Msg);
reply(From, Self, Msg) ->
	From ! {Self, read_data, Msg}.

