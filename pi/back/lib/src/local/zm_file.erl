%%@doc
%%% 文件进程
%%@end


-module(zm_file).

-description("file process").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/2, info/1, read/3, write/3, rename/3, flush/1, close/2]).
-export([send_read/5, send_write/5]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {file, type, io_device, read = [], write = []}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/2
%% Description: Starts a file store
%% Returns: {ok, Pid} | {error, Reason}
%% -----------------------------------------------------------------
start_link(File, Type) ->
	gen_server:start_link(?MODULE, {File, Type}, []).

%% -----------------------------------------------------------------
%%@doc 同步获取文件元信息
%% @spec info(Pid::pid()) -> return()
%% where
%% return() =  {ok, Data} | eof | {error, Reason}
%%@end
%% -----------------------------------------------------------------
info(Pid) when is_pid(Pid) ->
	gen_server:call(Pid, info).

%% -----------------------------------------------------------------
%%@doc 同步从文件中读指定位置和长度的内容
%% @spec read(Pid::pid(),Loc::integer(),Length::integer()) -> return()
%% where
%% return() =  {ok, Data} | eof | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read(Pid, Loc, Length) ->
	gen_server:call(Pid, {read, Loc, Length}).

%% -----------------------------------------------------------------
%%@doc 同步从文件的指定位置写指定的内容
%% @spec write(Pid::pid(),Loc::integer(),Data) -> return()
%% where
%% return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
write(Pid, Loc, Data) ->
	gen_server:call(Pid, {write, Loc, Data}).

%% -----------------------------------------------------------------
%%@doc 同步修改指定文件的文件名和文件操作类型
%% @spec rename(Pid::pid(),File::list(),Type) -> return()
%% where
%% return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
rename(Pid, File, Type) ->
	gen_server:call(Pid, {rename, File, Type}).

%% -----------------------------------------------------------------
%%@doc 将缓存在读写列表中的数据写入到文件
%% @spec flush(Pid::pid()) -> return()
%% where
%% return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
flush(Pid) ->
	gen_server:call(Pid, flush).

%% -----------------------------------------------------------------
%%@doc 关闭文件
%% @spec close(Pid::pid(),Reason::term()) -> return()
%% where
%% return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
close(Pid, Reason) ->
	gen_server:cast(Pid, {close, Reason}).

%% -----------------------------------------------------------------
%%@doc 异步从文件的指定位置写指定的内容
%% @spec send_read(Pid::pid(), Loc::integer(), Size::integer(), From::pid(), Ref) -> ok
%%@end
%% -----------------------------------------------------------------
send_read(Pid, Loc, Size, From, Ref) ->
	Pid ! {Loc, Size, From, Ref}.

%% -----------------------------------------------------------------
%%@doc 异步从文件的指定位置写指定的内容
%% @spec send_write(Pid::pid(), Loc::integer(), Data, From::pid(), Ref) -> ok
%%@end
%% -----------------------------------------------------------------
send_write(Pid, Loc, Data, From, Ref) ->
	Pid ! {Loc, Data, From, Ref}.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({File, Type}) ->
	case file:open(File, open_type(Type)) of
		{ok, I} ->
			{ok, #state{file = File, type = Type, io_device = I},
			?HIBERNATE_TIMEOUT};
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
handle_call({read, Loc, Length}, _From, #state{io_device = IO} = State) ->
	{reply, file:pread(IO, Loc, Length), State, timeout(State)};

handle_call({write, Loc, Data}, _From, #state{io_device = IO} = State) ->
	{reply, file:pwrite(IO, Loc, Data), State, timeout(State)};

handle_call({rename, File, Type}, _From, #state{file = OldFile,
	io_device = IO, read = Read, write = Write} = State) ->
	[write_entrys(IO, Write) || Write =/= []],
	[read_entrys(IO, Read) || Read =/= []],
	try
		I = rename(OldFile, IO, File, open_type(Type)),
		{reply, ok, State#state{file = File, type = Type,
			io_device = I, read = [], write = []}, ?HIBERNATE_TIMEOUT}
	catch
		E ->
			{stop, E, E, State}
	end;

handle_call(flush, _From, #state{io_device = IO, read = Read, write = Write} = State) ->
	[write_entrys(IO, Write) || Write =/= []],
	[read_entrys(IO, Read) || Read =/= []],
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call(info, _From, #state{file = File} = State) ->
	{reply, file:read_file_info(File), State, timeout(State)};

handle_call(_Request, _From, State) ->
	{noreply, State, timeout(State)}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({close, Reason}, #state{io_device = IO, read = Read, write = Write} = State) ->
	[write_entrys(IO, Write) || Write =/= []],
	[read_entrys(IO, Read) || Read =/= []],
	file:close(IO),
	{stop, Reason, State};

handle_cast(_Msg, State) ->
	{noreply, State, timeout(State)}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({_Loc, Length, _From, _Ref} = Req,
	#state{read = Read, write = []} = State) when is_integer(Length) ->
	{noreply, State#state{read = [Req | Read]}, 0};
handle_info({_Loc, Length, _From, _Ref} = Req,
	#state{io_device = IO, read = Read, write = Write} = State) when is_integer(Length) ->
	write_entrys(IO, Write),
	{noreply, State#state{read = [Req | Read], write = []}, 0};

handle_info({_Loc, _Data, _From, _Ref} = Req, #state{read = [], write = Write} = State) ->
	{noreply, State#state{write = [Req | Write]}, 0};
handle_info({_Loc, _Data, _From, _Ref} = Req,
	#state{io_device = IO, read = Read, write = Write} = State) ->
	read_entrys(IO, Read),
	{noreply, State#state{read = [], write = [Req | Write]}, 0};

handle_info({close, Reason}, #state{io_device = IO, read = Read, write = Write} = State) ->
	[write_entrys(IO, Write) || Write =/= []],
	[read_entrys(IO, Read) || Read =/= []],
	file:close(IO),
	{stop, Reason, State};

handle_info(timeout, #state{read = [], write = []} = State) ->
	{noreply, State, hibernate};

handle_info(timeout, #state{io_device = IO, read = Read, write = Write} = State) ->
	[write_entrys(IO, Write) || Write =/= []],
	[read_entrys(IO, Read) || Read =/= []],
	{noreply, State#state{read = [], write = []}, ?HIBERNATE_TIMEOUT};

handle_info(_Info, State) ->
	{noreply, State, timeout(State)}.

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
% 获得超时时间
timeout(#state{read = [], write = []}) ->
	?HIBERNATE_TIMEOUT;
timeout(_) ->
	0.

% 获得文件打开类型的对应参数
open_type(r) ->
	[read, raw, binary];
open_type(rw) ->
	[read, write, raw, binary].

% 重命名文件
rename(OldFile, IO, NewFile, Type) ->
	R1 = file:close(IO),
	[erlang:error({"rename close fail", OldFile, R1}) || R1 =/= ok],
	if
		OldFile =/= NewFile ->
			R2 = z_lib:make_dir(filename:dirname(NewFile)),
			[erlang:error({"rename make_dir fail", NewFile, R2}) || R2 =/= ok],
			R3 = file:rename(OldFile, NewFile),
			[erlang:error({"rename fail", OldFile, NewFile, R3}) || R3 =/= ok];
		true ->
			ok
	end,
	case file:open(NewFile, Type) of
		{ok, I} ->
			I;
		E ->
			erlang:error({"rename open fail", NewFile, E})
	end.

% 读取条目的值
read_entrys(IO, Read) ->
	case file:pread(IO, [{Loc, Length} || {Loc, Length, _From, _Ref} <- Read]) of
		{ok, BinL} ->
			read_ok(Read, BinL);
		E ->
			read_error(Read, E)
	end.

% 读取结果的发送
read_ok([{_Loc, _Length, From, Ref} | T], [Bin | R]) ->
	[From ! {Ref, ok, Bin} || is_pid(From)],
	read_ok(T, R);
read_ok([], _Result) ->
	ok.

% 读取错误的发送
read_error([{_Loc, _Size, From, Ref} | T], R) ->
	[From ! {Ref, error, R} || is_pid(From)],
	read_error(T, R);
read_error([], _R) ->
	ok.

% 写入条目
write_entrys(IO, Write) ->
	R = file:pwrite(IO, loc_data_entrys(Write, [])),
	[From ! {Ref, R} || {_Loc, _Data, From, Ref} <- Write, is_pid(From)].

% 获得吸入条目的位置和数据
loc_data_entrys([{Loc, Data, _From, _Ref} | T], L) ->
	loc_data_entrys(T, [{Loc, Data} | L]);
loc_data_entrys([], L) ->
	L.
