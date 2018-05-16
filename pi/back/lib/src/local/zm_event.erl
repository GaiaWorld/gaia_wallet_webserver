%%@doc 单节点上的事件广播表
%%```
%%%配置表的格式为：{{事件源, 事件类型}, MFA和同步异步列表, 超时（0为同步，大于0为异步）}（{{Src, Type}, [{M ,F, A, Timeout}]}）
%%'''
%%@end


-module(zm_event).

-description("event broadcast table").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0]).
-export([notify/3]).
-export([exist/1, get/2, set/3, unset/3, delete/2, delete/3]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================INLINE=======================
-compile({inline, [ handle/3, handle_/3, pid_expire/2, kill_pid/3]}).
-compile({inline_size, 32}).

%%%=======================RECORD=======================
-record(state, {ets, asyn}).

%%%=======================DEFINE=======================
-define(TIMEOUT, 1000).

-define(HIBERNATE_TIMEOUT, 3000).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc Starts a event broadcast table
%%  @spec start_link() -> return()
%% where
%%      return() = {ok, Pid}
%%@end
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% -----------------------------------------------------------------
%%@doc 通知事件
%% @spec notify(Src::atom(), Type::atom(), Args::term()) -> return()
%% where
%%      return() = [] | [{Error, {Why, {M, F, A}, StackTrace}}]
%%@end
%% -----------------------------------------------------------------
notify(Src, Type, Args) ->
	SrcType = {Src, Type},
	try ets:lookup(?MODULE, SrcType) of
		[{_, L}] ->
			[E || I <- L, (E = handle(SrcType, Args, I)) =/= ok];
		[] ->
			[]
	catch
		Error:Reason ->
			{Error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc check process type
%%```
%%  Pid:进程号
%%'''
%% @spec exist(Pid::pid()) -> return()
%% where
%%      return() = true | false
%%@end
%% -----------------------------------------------------------------
exist(Pid) when is_pid(Pid) ->
	case whereis(?MODULE) of
		Pid ->
			true;
		_ ->
			gen_server:call(?MODULE, {exist, Pid}, 10000)
	end.

%% -----------------------------------------------------------------
%%@doc get handler
%% @spec get(Src::atom(), Type::atom()) -> return()
%% where
%%      return() = none | {Type, [{M ,F, A, Opt}]}
%%@end
%% -----------------------------------------------------------------
get(Src, Type) ->
	case ets:lookup(?MODULE, {Src, Type}) of
		[T] -> T;
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc set handler
%% @spec set(Type::{Src::atom(),Type::atom()},MFA::{M::atom(),F::atom(),A::term()},Timeout::integer()) -> ok
%%@end
%% -----------------------------------------------------------------
set({_Src, _Type} = SrcType, {M, F, _A} = MFA, Timeout)
	when is_atom(M), is_atom(F), is_integer(Timeout) ->
	gen_server:call(?MODULE, {set, SrcType, MFA, Timeout}).

%% -----------------------------------------------------------------
%%@doc unset handler
%% @spec unset(Type::{Src::atom(),Type::atom()},any(),any()) -> ok
%%@end
%% -----------------------------------------------------------------
unset(Type, MFA, _) ->
	gen_server:call(?MODULE, {delete, Type, MFA}).

%% -----------------------------------------------------------------
%%@doc delete handler
%% @spec delete(Src::atom(),Type::atom()) -> return()
%% where
%% return() =  none | ok
%%@end
%% -----------------------------------------------------------------
delete(Src, Type) ->
	gen_server:call(?MODULE, {delete, {Src, Type}}).

%% -----------------------------------------------------------------
%%@doc delete handler
%% @spec (Src::atom(),Type::atom(),M::atom()) -> return()
%%        |(Src::atom(),Type::atom(),MFA::{M::atom(),F::atom(),A::term()}) -> return()
%% where
%% return() =  none | ok | {Type, [{M ,F, A, Opt}]}
%%@end
%% -----------------------------------------------------------------
delete(Src, Type, M) when is_atom(M) ->
	gen_server:call(?MODULE, {delete, {Src, Type}, M});
delete(Src, Type, {M, F, _A} = MFA) when is_atom(M), is_atom(F) ->
	gen_server:call(?MODULE, {delete, {Src, Type}, MFA}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Ets) ->
	{ok, #state{ets = ets:new(Ets, [named_table, {read_concurrency, true}]), asyn = []}, ?HIBERNATE_TIMEOUT}.

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
handle_call({exist, Pid}, _From, #state{asyn = Pids} = State) ->
	{reply, lists:keymember(Pid, 1, Pids), State};

handle_call({set, SrcType, MFA, Timeout}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, SrcType) of
		[{_, L}] ->
			ets:insert(Ets, {SrcType, [{MFA, Timeout} | lists:keydelete(MFA, 1, L)]});
		[] ->
			ets:insert(Ets, {SrcType, [{MFA, Timeout}]})
	end,
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({delete, SrcType}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, SrcType) of
		[{_, _L}] ->
			ets:delete(Ets, SrcType),
			{reply, ok, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, none, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({delete, SrcType, M}, _From, #state{ets = Ets} = State) when is_atom(M) ->
	R = case ets:lookup(Ets, SrcType) of
		[{_, L}] ->
			case [E || E = {{Mod, _, _}, _} <- L, Mod =/= M] of
				L ->
					none;
				[] ->
					ets:delete(Ets, SrcType),
					ok;
				List ->
					ets:insert(Ets, T = {SrcType, List}),
					T
			end;
		[] ->
			none
	end,
	{reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call({delete, SrcType, MFA}, _From, #state{ets = Ets} = State) ->
	R = case ets:lookup(Ets, SrcType) of
		[{_, L}] ->
			case lists:keydelete(MFA, 1, L) of
				L ->
					none;
				[] ->
					ets:delete(Ets, SrcType),
					ok;
				List ->
					ets:insert(Ets, T = {SrcType, List}),
					T
			end;
		[] ->
			none
	end,
	{reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({handle, Pid, Exprie, MF, SrcType}, #state{asyn = []} = State) ->
	erlang:start_timer(?TIMEOUT, self(), none),
	{noreply, State#state{asyn = [{Pid, Exprie, MF, SrcType}]}};
handle_info({handle, Pid, Exprie, MF, SrcType}, #state{asyn = L} = State) ->
	{noreply, State#state{asyn = [{Pid, Exprie, MF, SrcType} | L]}};

handle_info({timeout, _Ref, none}, #state{asyn = L} = State) ->
	Now = zm_time:now_millisecond(),
	case [E || E <- L, pid_expire(E, Now)] of
		[] ->
			{noreply, State#state{asyn = []}, ?HIBERNATE_TIMEOUT};
		LL ->
			erlang:start_timer(?TIMEOUT, self(), none),
			{noreply, State#state{asyn = LL}}
	end;

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
% 事件处理函数
handle(SrcType, Args, {MFA, Timeout}) when Timeout < 1 ->
	put('$log', SrcType),
	handle_(SrcType, Args, MFA);
handle(SrcType, Args, {{M, F, _}=MFA, Timeout}) ->
	spawn(
		fun() ->
			?MODULE ! {handle, self(), Timeout + zm_time:now_millisecond(),
				{M, F}, SrcType},
			put('$log', SrcType),
			handle_(SrcType, Args, MFA)
		end),
	ok.

% 事件处理函数
handle_({Src, Type}, Args, {M, F, A} = MFA) ->
	try
		R = M:F(A, Src, Type, Args),
		zm_log:debug(?MODULE, handle, Src, Type,
			[{mfa, MFA}, {args, Args}, {result, R}]),
		ok
	catch
		Error:Reason ->
			ST = erlang:get_stacktrace(),
			zm_log:warn(?MODULE, {handle_error, Error}, Src, Type,
				[{mfa, MFA}, {args, Args}, {error, Reason}, {stacktrace, ST}]),
			{Error, {Reason, {M, F, [A, Args]}, ST}}
	end.

% 异步进程超期
pid_expire({Pid, Expire, MF, SrcType}, Time) ->
	case is_process_alive(Pid) of
		true when Time > Expire ->
			kill_pid(Pid, MF, SrcType),
			false;
		R ->
			R
	end.

% 杀掉指定进程
kill_pid(Pid, MF, {Src, Type}) ->
	CurFun = process_info(Pid, current_function),
	exit(Pid, kill),
	zm_log:warn(?MODULE, kill_pid, Src, Type,
		[{mf, MF}, {cur_fun, CurFun}]).
