%% @author lenovo
%% @doc 基于fcm的文件监控器


-module(z_file_monitor).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
%进程休眠超时时长
-define(HIBERNATE_TIMEOUT, 5000).
%文件创建事件
-define(FILE_CREATE_EVENT, "CREATE").
%文件修改事件
-define(FILE_MODIFY_EVENT, "MODIFY").
%文件改名事件
-define(FILE_RENAME_EVENT, "RENAME").
%文件移除事件
-define(FILE_REMOVE_EVENT, "REMOVE").
%文件创建类型
-define(FILE_CREATE_TYPE, create).
%文件修改类型
-define(FILE_MODIFY_TYPE, modify).
%文件改名类型
-define(FILE_RENAME_TYPE, frename).
%目录改名类型
-define(DIR_RENAME_TYPE, drename).
%文件移除类型
-define(FILE_REMOVE_TYPE, remove).

%相同事件最大重复次数
-define(MAX_SAME_EVENT, 3).
%事件缓冲超时时长
-define(EVENT_BUFFER_TIMEOUT, event_buffer_timeout).
%默认事件缓冲超时时长，单位ms
-define(DEFAULT_EVENT_BUFFER_TIMEOUT, 500).

%默认的最大堆大小
-define(DEFAULT_MAX_HEAP_SIZE, 128 * 1024 * 1024).

%根据操作系统获取文件监控模块
-define(GET_FCM_NAME(), case os:type() of
							{win32, _} ->
								"fcm";
							_ ->
								"libfcm"
						end).
-define(FCM_NAME, ?GET_FCM_NAME()).
-define(FCM_MOD, list_to_atom(?FCM_NAME)).

%编译时生成的临时文件扩展名
-define(TEMP_EXT, ".bea#").
%编译时生成的文件扩展名
-define(BEAM_EXT, ".beam").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, monitor/3, unmonitor/1]).

%%启动文件监控器
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%监控指定目录的所有事件
monitor(Dir, {M, F, _} = MFA, Timeout) when is_list(Dir), is_atom(M), is_atom(F), is_integer(Timeout), Timeout > 0 ->
	case filelib:is_dir(Dir) of
		true ->
			gen_server:call(?MODULE, {monitor, Dir, MFA, Timeout});
		false ->
			{error, invalid_path}
	end.

%%移除指定目录所有类型的监控
unmonitor(Dir) when is_list(Dir) ->
	gen_server:call(?MODULE, {unmonitor, Dir}).


%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {handler, monitors, buffer, rename_buffer, buffer_timeout, rename_timer, timer}).

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
init(Args) ->
	z_process:local_process(?MODULE, ?DEFAULT_MAX_HEAP_SIZE, false, true),
	case
		case lib_npc:start_link([]) of
			{ok, _} ->
				ok;
			{error,{already_started, _}} ->
				ok;
			E ->
				E
		end
	of
		ok ->
			lib_npc:register(?FCM_NAME, "int64", "new", "", asyn),
			lib_npc:register(?FCM_NAME, "int64", "add_dir", "char#, int64", sync),
			lib_npc:register(?FCM_NAME, "int64", "remove_dir", "char#, int64", sync),
			case (?FCM_MOD):new(self()) of
				{ok, Handler} ->
					BufferTimeout=z_lib:get_value(Args, ?EVENT_BUFFER_TIMEOUT, ?DEFAULT_EVENT_BUFFER_TIMEOUT),
		    		{ok, #state{handler = Handler, monitors = #{}, buffer = #{}, 
								buffer_timeout = BufferTimeout}};
				Reason1 ->
					{stop, Reason1}
			end;
		Reason ->
			{stop, Reason}
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
handle_call({monitor, Dir, MFA, Timeout}, From, #state{handler = Handler, monitors = Monitors} = State) ->
	case exist(Dir, Monitors) of
		true ->
			{reply, {error, already_monitor}, State, ?HIBERNATE_TIMEOUT};
		false ->
			case (?FCM_MOD):add_dir(Dir, Handler) of
				{ok, Watcher} ->
					add_monitor(MFA, Timeout),
					{reply, ok, State#state{monitors = maps:put(Dir, {From, Watcher}, Monitors)}, ?HIBERNATE_TIMEOUT};
				E ->
					{reply, {error, E}, State, ?HIBERNATE_TIMEOUT}
			end
	end;
handle_call({unmonitor, Dir}, _, #state{monitors = Monitors} = State) ->
	case exist(Dir, Monitors) of
		true ->
			case (?FCM_MOD):remove_dir(Dir, element(2, maps:get(Dir, Monitors))) of
				{ok, 0} ->
					{reply, {error, unmonitor_failed}, State, ?HIBERNATE_TIMEOUT};
				{ok, 1} ->
					remove_monitor(),
					{reply, ok, State#state{monitors = maps:remove(Dir, Monitors)}, ?HIBERNATE_TIMEOUT}
			end;
		false ->
			{reply, ok, State, ?HIBERNATE_TIMEOUT}
	end;
handle_call(_, _, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?HIBERNATE_TIMEOUT}.


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
handle_info([$<, $<, A, B, C, D, E, F, $>, $>, $ |File], State) ->
	%处理文件改变事件
	{noreply, buffer_handle([A, B, C, D, E, F], File, State), ?HIBERNATE_TIMEOUT};
handle_info({timeout, _TimerRef, {TimeEvent, File}}, State) ->
	%清理缓冲区事件
	{noreply, buffer_handle(TimeEvent, File, State), ?HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.


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

%%判断指定目录是否已加入监控
exist(Dir, Map) ->
	maps:is_key(Dir, Map).

%%增加监控文件指定事件的监听器
add_monitor(Type, MFA, Timeout) ->
	zm_event:set({?MODULE, Type}, MFA, Timeout).

%%增加监控文件所有事件的监听器
add_monitor(MFA, Timeout) ->
	add_monitor(?FILE_CREATE_TYPE, MFA, Timeout),
	add_monitor(?FILE_MODIFY_TYPE, MFA, Timeout),
	add_monitor(?DIR_RENAME_TYPE, MFA, Timeout),
	add_monitor(?FILE_RENAME_TYPE, MFA, Timeout),
	add_monitor(?FILE_REMOVE_TYPE, MFA, Timeout).

%%移除监控文件指定事件的监听器
remove_monitor(Type) ->
	zm_event:delete(?MODULE, Type).

%%移除监控文件所有事件的监听器
remove_monitor() ->
	remove_monitor(?FILE_CREATE_TYPE),
	remove_monitor(?FILE_MODIFY_TYPE),
	remove_monitor(?DIR_RENAME_TYPE),
	remove_monitor(?FILE_RENAME_TYPE),
	remove_monitor(?FILE_REMOVE_TYPE).

%%将事件加入缓冲区，并进行处理
buffer_handle(?FILE_CREATE_EVENT, File, State) ->
	case filelib:is_dir(File) of
		true ->
			State;
		false ->
			zm_event:notify(?MODULE, ?FILE_CREATE_TYPE, to_unicode(File)),
			State
	end;
buffer_handle(?FILE_REMOVE_EVENT, File, State) ->
	case filelib:is_dir(File) of
		true ->
			State;
		false ->
			zm_event:notify(?MODULE, ?FILE_REMOVE_TYPE, to_unicode(File)),
			State
	end;
buffer_handle({flush, ?FILE_RENAME_EVENT}, File, #state{rename_buffer = Buffer} = State) ->
	case Buffer of
		{OldFile, _} ->
			case filelib:is_dir(File) of
				true ->
					%缓冲超时，则发送目录改名事件，清空对应缓冲
					zm_event:notify(?MODULE, ?DIR_RENAME_TYPE, {to_unicode(OldFile), to_unicode(File)});
				false ->
					%缓冲超时，则发送文件改名事件，清空对应缓冲
					zm_event:notify(?MODULE, ?FILE_RENAME_TYPE, {to_unicode(OldFile), to_unicode(File)})
			end,
			State#state{rename_buffer = undefined, rename_timer = undefined};
		_ ->
			%其它则忽略
			State#state{rename_timer = undefined}
	end;
buffer_handle(?FILE_RENAME_EVENT, File, #state{rename_buffer = Buffer} = State) ->
	case Buffer of
		undefined ->
			%上次事件为空，则缓冲事件，并设置一个守护定时器
			BufferTimeout=State#state.buffer_timeout,
			State#state{rename_buffer = {File, z_lib:now_millisecond() + BufferTimeout}, 
						rename_timer = erlang:start_timer(BufferTimeout, self(), {{flush, ?FILE_RENAME_EVENT}, File})};
		{OldFile, _} ->
			%在缓冲超时前有后继事件，则继续处理
			buffer_handle(?FILE_RENAME_EVENT, File, State#state{rename_buffer = OldFile});
		OldFile ->
			case filelib:is_dir(File) of
				true ->
					%目录改名事件，清空对应缓冲
					zm_event:notify(?MODULE, ?DIR_RENAME_TYPE, {to_unicode(OldFile), to_unicode(File)}),
					State#state{rename_buffer = undefined};
				false ->
					%文件改名事件，清空对应缓冲
					zm_event:notify(?MODULE, ?FILE_RENAME_TYPE, {to_unicode(OldFile), to_unicode(File)}),
					State#state{rename_buffer = undefined}
			end
	end;
buffer_handle({flush, Event}, File, #state{buffer = Buffer} = State) ->
	case maps:find(File, Buffer) of
		{ok, {Event, _}} ->
			case filelib:is_dir(File) of
				false ->
					%缓冲超时，则发送文件修改事件，并清空对应缓冲
					zm_event:notify(?MODULE, ?FILE_MODIFY_TYPE, to_unicode(File)),
					State#state{buffer = maps:remove(File, Buffer), timer = undefined};
				true ->
					%目录修改事件则忽略
					State#state{timer = undefined}
			end;
		_ ->
			%其它则忽略
			State#state{timer = undefined}
	end;
buffer_handle(Event, File, #state{buffer = Buffer} = State) ->
	case maps:find(File, Buffer) of
		{ok, {Event, Count}} when Count =< ?MAX_SAME_EVENT ->
			State#state{buffer = maps:put(File, {Event, Count + 1}, Buffer)};
		{ok, {Event, _}} ->
			%在缓冲超时前有指定数量的相同后继事件，则立即处理
			buffer_handle(Event, File, State#state{buffer = maps:put(File, Event, Buffer)});
		{ok, Event} ->
			case filelib:is_dir(File) of
				false ->
					%文件修改事件，清空对应缓冲
					zm_event:notify(?MODULE, ?FILE_MODIFY_TYPE, to_unicode(File)),
					State#state{buffer = maps:remove(File, Buffer)};
				true ->
					%目录修改事件则忽略
					State
			end;
		{ok, _} ->
			%忽略无效缓冲事件，缓冲超时后直动重置无效缓冲事件
			State;
		error ->
			%上次事件为空，则缓冲事件，并设置一个守护定时器
			BufferTimeout=State#state.buffer_timeout,
			State#state{buffer = maps:put(File, {Event, 0}, Buffer),
						timer = erlang:start_timer(BufferTimeout, self(), {{flush, Event}, File})}
	end.

%%将二进制文件名转换为字符串文件名，并处理bea#
to_unicode(File) ->
	FileName=unicode:characters_to_list(list_to_binary(File)),
	case filename:extension(FileName) of
		?TEMP_EXT ->
			lists:concat([filename:join([filename:dirname(FileName), filename:basename(FileName, ?TEMP_EXT)]), ?BEAM_EXT]);
		_ ->
			FileName
	end.

	