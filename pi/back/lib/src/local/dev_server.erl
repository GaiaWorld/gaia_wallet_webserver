%% @author zuon
%% @doc 开发服务器


-module(dev_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-define(IP_KEY, ip).
-define(IP, {127, 0, 0, 1}).
-define(SRC, htdocs).
-define(RES, ".res").
-define(RES_CHANGED, "res_changed").
-define(SEQ_KEY, "").
-define(PATH_KEY, "path").
-define(ADD_KEY, "add").
-define(MODIFY_KEY, "modify").
-define(REMOVE_KEY, "remove").
-define(ADD_DIR_KEY, "add_dir").
-define(REMOVE_DIR_KEY, "remove_dir").
-define(INIT_SEQ, 0).
-define(UNDEFINED, undefined).
-define(REMOVE_EVENT, -1).
-define(ADD_EVENT, 0).
-define(MODIFY_EVENT, 1).
-define(NODE, "node").
-define(INIT_CMD, "init").
-define(BUILD_CMD, "exec").
-define(BUILD_INIT_TIME, 0).
-define(DEV_SERVER_HIBERNATE_TIMEOUT, 3000).
-define(DEFAULT_MAX_HEAP_SIZE, 8 * 1024 * 1024).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/2, target_monitor/2, start_monitor/4]).

%%
%%启动开发服务器
%%
start_link(Name, Target) when is_atom(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, Target, []).

%%
%%目标文件监控
%%
target_monitor(Pid, {monitors, _, _, _, _, _, _} = R) ->
	Pid ! {target, R};
target_monitor(_, _) ->
	ignore.

%%
%%目标监控连接启动事件
%%
start_monitor(Name, ?SRC, {ws_con, init}, {Session, Addr}) ->
	case lists:keyfind(?IP_KEY, 1, Addr) of
		{?IP_KEY, ?IP} ->
			gen_server:call(Name, {start_monitor, element(2, Session)});
		_ ->
			ignore
	end.

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {cmd, source_path, spid, source_args, buffer, wait, target_path, tpid, target_args, ws_con, seq, build_time}).

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
init(Target) ->
	z_process:local_process(?MODULE, ?DEFAULT_MAX_HEAP_SIZE, false, true),
	State=#state{wait = ?UNDEFINED, seq = ?INIT_SEQ},
	case Target of
		none ->
			{ok, State};
		{TargetPath, TargetArgs} ->
			case filelib:ensure_dir(lists:concat([TargetPath, "/"])) of
				ok ->
				    {ok, State#state{target_path = TargetPath, target_args = TargetArgs}};
				{_, Reason} ->
					{stop, Reason}
			end
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
handle_call({start_monitor, Con}, _, State) ->
	%开始资源文件监控
	{reply, ok, State#state{ws_con = Con, seq = ?INIT_SEQ}, ?DEV_SERVER_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {noreply, State, ?DEV_SERVER_HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?DEV_SERVER_HIBERNATE_TIMEOUT}.


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
handle_info({target, {monitors, Adds, Removes, Modifys, AddsPath, RemovesPath, ModifyPath}}, 
			#state{target_path = Path, target_args = Args, ws_con = Con, seq = Seq} = State) ->
	%处理目标文件改动事件
	NewSeq=notify_changed(Path, Args, Con, Seq, Adds, Modifys, Removes, AddsPath, RemovesPath, ModifyPath),
	{noreply, State#state{seq = NewSeq}, ?DEV_SERVER_HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State, ?DEV_SERVER_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, _) ->
	io:format("======>dev server crash!~n"),
	io:format("~p~n", [Reason]),
	io:format("<======~n").


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_, State, _) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%%通知前端资源文件改变
notify_changed(Path, Args, {ws_con, _} = Con, Seq, Add, Modify, Remove, AddsPath, RemovesPath, _ModifyPath) ->
	notify_changed1(Path, Args, Con, Seq, filter(Add, Args, []), filter(Modify, Args, []), filter(Remove, Args, []), AddsPath, RemovesPath),
	Seq + 1;
notify_changed(_, _, _, Seq, _, _, _,_,_,_) ->
	Seq.
	
notify_changed1(_Path, _Args, _Con, _Seq, [], [], [], _, _) ->
	ignore;
notify_changed1(Path, _Args, Con, Seq, _Files, [], [], _, _) ->
	%ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?ADD_KEY, lists:flatten(zm_json:encode(Files))}]});
	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?PATH_KEY, Path}]});
notify_changed1(Path, _Args, Con, Seq, [], _Files, [], _, _) ->
	%ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?MODIFY_KEY, lists:flatten(zm_json:encode(Files))}]});
	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?PATH_KEY, Path}]});
notify_changed1(Path, _Args, Con, Seq, [], [], _Files, _, _) ->
	%ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?REMOVE_KEY, lists:flatten(zm_json:encode(Files))}]}).
	ws_con:send(Con, {?RES_CHANGED, [{?SEQ_KEY, integer_to_list(Seq)}, {?PATH_KEY, Path}]}).

%%过滤资源文件
filter([File|T], Args, L) ->
	case filter1(Args, filename:extension(File), false) of
		true ->
			filter(T, Args, [File|L]);
		false ->
			[]
	end;
filter([], _, L) ->
	lists:reverse(L).

%%判断是否是需要过滤的扩展名
filter1([[$-|Ext]|_], Ext, _) ->
	false;
filter1(["-*"|_], _, _) ->
	false;
filter1([[$-|_]|T], Ext, _) ->
	filter1(T, Ext, true);
filter1([Ext|_], Ext, _) ->
	true;
filter1(["*"|_], _, _) ->
	true;
filter1([_|T], Ext, Bool) ->
	filter1(T, Ext, Bool);
filter1([], _, Bool) ->
	Bool.



