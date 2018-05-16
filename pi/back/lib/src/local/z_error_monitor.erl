%% @author lenovo
%% @doc 错误监听器


-module(z_error_monitor).
-behaviour(gen_event).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
%错误信息类型
-define(ERROR_MSG_TYPE, error_msg).
%错误报告类型
-define(ERROR_REPORT_TYPE, error_report).
%堆溢出错误类型
-define(OUT_OF_HEAP_TYPE, out_of_heap).
%未知错误类型
-define(UNKNOWN_TYPE, unknown).
%所有进程
-define(ALL_PROCESS, all).
%模拟器进程
-define(EMULATOR_PROCESS, emulator).
%非本地进程
-define(UN_LOCAL_PROCESS, unlocal).
%默认的缓冲超时时长，单位ms
-define(DEFAULT_BUFFER_TIMEOUT, 1000).
%默认的最大堆大小
-define(DEFAULT_MAX_HEAP_SIZE, 32 * 1024 * 1024).


%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0, start_link/1, listen_msg/4, listen_report/4]).

%%启动错误监听器
start_link() ->
	?MODULE:start_link(?DEFAULT_BUFFER_TIMEOUT).

%%启动错误监听器
start_link(BufferTimeout) ->
	error_logger:add_report_handler(?MODULE, BufferTimeout).

%%监听指定类型的进程的指定错误信息
listen_msg(PType, ErrType, MFA, Timeout) ->
	gen_event:call(error_logger, ?MODULE, {listen_msg, PType, ErrType, MFA, Timeout}).

%%监听指定类型的进程的指定错误报告
listen_report(PType, ErrType, MFA, Timeout) ->
	gen_event:call(error_logger, ?MODULE, {listen_report, PType, ErrType, MFA, Timeout}).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {src, types, buffer, buffer_timeout}).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:init-1">gen_event:init/1</a>
-spec init(InitArgs) -> Result when
	InitArgs :: Args | {Args, Term :: term()},
	Args :: term(),
	Result :: {ok, State}
			| {ok, State, hibernate}
			| {error, Reason :: term()},
	State :: term().
%% ====================================================================
init(BufferTimeout) ->
	z_process:local_process(?MODULE, ?DEFAULT_MAX_HEAP_SIZE, false, true),
    {ok, #state{src = {self(), ?MODULE}, types = #{}, buffer = #{}, buffer_timeout = BufferTimeout}}.


%% handle_event/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_event-2">gen_event:handle_event/2</a>
-spec handle_event(Event :: term(), State :: term()) -> Result when
	Result :: {ok, NewState}
			| {ok, NewState, hibernate}
			| {swap_handlers, Args1, NewState, Handler2, Args2}
			| remove_handler,
	NewState :: term(), Args1 :: term(), Args2 :: term(),
	Handler2 :: Module2 | {Module2, Id :: term()},
	Module2 :: atom().
%% ====================================================================
handle_event({error, Gleader, {?EMULATOR_PROCESS, Format, [ErrPid|_] = Data}}, #state{src = Src, types = Types, buffer = Buffer, 
																					  buffer_timeout = BufferTimeout} = State) ->
	case buffer_filter(ErrPid, Buffer, BufferTimeout) of
		{true, NewBuffer} ->
			{ok, State#state{buffer = NewBuffer}};
		{false, NewBuffer} ->
			case string:rstr(Format, "maximum heap size reached") > 0 of
				true ->
					filter_notify(Types, Src, ?EMULATOR_PROCESS, ?OUT_OF_HEAP_TYPE, ?ERROR_MSG_TYPE, Gleader, Data);
				false ->
					filter_notify(Types, Src, ?EMULATOR_PROCESS, ?UNKNOWN_TYPE, ?ERROR_MSG_TYPE, Gleader, Data)
			end,
			{ok, State#state{buffer = NewBuffer}}
	end;
handle_event({error, Gleader, {ErrPid, _, Data}}, #state{src = Src, types = Types, buffer = Buffer, 
														 buffer_timeout = BufferTimeout} = State) ->
	case buffer_filter(ErrPid, Buffer, BufferTimeout) of
		{true, NewBuffer} ->
			{ok, State#state{buffer = NewBuffer}};
		{false, NewBuffer} ->
			filter_notify(Types, Src, ErrPid, ?UNKNOWN_TYPE, ?ERROR_MSG_TYPE, Gleader, Data),
			{ok, State#state{buffer = NewBuffer}}
	end;
handle_event({error_report, Gleader, {ErrPid, Type, Report}}, #state{src = Src, types = Types, buffer = Buffer, 
																	 buffer_timeout = BufferTimeout} = State) ->
	case buffer_filter(ErrPid, Buffer, BufferTimeout) of
		{true, NewBuffer} ->
			{ok, State#state{buffer = NewBuffer}};
		{false, NewBuffer} ->
			filter_notify(Types, Src, ErrPid, Type, ?ERROR_REPORT_TYPE, Gleader, Report),
			{ok, State#state{buffer = NewBuffer}}
	end;
handle_event(_, State) ->
    {ok, State}.


%% handle_call/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_call-2">gen_event:handle_call/2</a>
-spec handle_call(Request :: term(), State :: term()) -> Result when
	Result :: {ok, Reply, NewState}
			| {ok, Reply, NewState, hibernate}
			| {swap_handler, Reply, Args1, NewState, Handler2, Args2}
			| {remove_handler, Reply},
	Reply :: term(),
	NewState :: term(), Args1 :: term(), Args2 :: term(),
	Handler2 :: Module2 | {Module2, Id :: term()},
	Module2 :: atom().
%% ====================================================================
handle_call({listen_msg, PType, ErrType, MFA, Timeout}, #state{src = Src, types = Types} = State) ->
    {ok, ok, State#state{types = listen_process(Src, PType, ErrType, ?ERROR_MSG_TYPE, MFA, Timeout, Types)}};
handle_call({listen_report, PType, ErrType, MFA, Timeout}, #state{src = Src, types = Types} = State) ->
	{ok, ok, State#state{types = listen_process(Src, PType, ErrType, ?ERROR_REPORT_TYPE, MFA, Timeout, Types)}};
handle_call(_, State) ->
	{ok, invalid, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_info-2">gen_event:handle_info/2</a>
-spec handle_info(Info :: term(), State :: term()) -> Result when
	Result :: {ok, NewState}
			| {ok, NewState, hibernate}
			| {swap_handler, Args1, NewState, Handler2, Args2}
			| remove_handler,
	NewState :: term(), Args1 :: term(), Args2 :: term(),
	Handler2 :: Module2 | {Module2, Id :: term()},
	Module2 :: atom().
%% ====================================================================
handle_info(_, State) ->
    {ok, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:terminate-2">gen_event:terminate/2</a>
-spec terminate(Arg, State :: term()) -> term() when
	Arg :: Args
		| {stop, Reason}
		| stop
		| remove_handler
		| {error, {'EXIT', Reason}}
		| {error, Term :: term()},
	Args :: term(), Reason :: term().
%% ====================================================================
terminate(_Arg, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:code_change-3">gen_event:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> {ok, NewState :: term()} when
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%%监听指定类型的进程
listen_process(Src, PType, ErrType, InfoType, MFA, Timeout, Types) ->
	case maps:find({PType, ErrType, InfoType}, Types) of
		{ok, _} ->
			Types;
		_ ->
			zm_event:set({Src, InfoType}, MFA, Timeout),
			maps:put({PType, ErrType, InfoType}, {MFA, Timeout}, Types)
	end.

%%事件缓冲过滤
buffer_filter(ErrPid, Buffer, BufferTimeout) ->
	NowTime=z_lib:now_millisecond(),
	case maps:find(ErrPid, Buffer) of
		{ok, Timeout} when Timeout > NowTime ->
			%相同进程的上次事件通知还未超时，则过滤事件
			{true, Buffer};
		{ok, _} ->
			{false, maps:update(ErrPid, z_lib:now_millisecond() + BufferTimeout, Buffer)};
		_ ->
			{false, maps:put(ErrPid, z_lib:now_millisecond() + BufferTimeout, Buffer)}
	end.		
			
%%将监听事件通知到监听器
filter_notify(Types, Src, Pid, ErrType, InfoType, Gleader, Data) ->
	PType=if
			is_pid(Pid) ->
				case z_process:type(Pid) of
					false ->
						?UN_LOCAL_PROCESS;
					Type ->
						Type
				end;
			true ->
				Pid
		  end,
	case maps:find({?ALL_PROCESS, ErrType, InfoType}, Types) of
		{ok, _} ->
			%监控所有类型的进程
			zm_event:notify(Src, InfoType, {Gleader, Pid, ErrType, Data});
		_ ->
			case maps:find({PType, ErrType, InfoType}, Types) of
				{ok, _} ->
					zm_event:notify(Src, InfoType, {Gleader, Pid, ErrType, Data});
				_ ->
					ignore
			end
	end.
							
