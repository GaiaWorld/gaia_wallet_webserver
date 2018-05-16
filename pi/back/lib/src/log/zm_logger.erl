%%%@doc 日志节点上指定名称的日志服务
%%```
%%% 负责缓存一定时间和一定量的日志，调用特定的模块进行存储
%%'''
%%@end

-module(zm_logger).

-description("logger").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1, start_link/3]).
-export([get/1, set/3, unset/3]).
-export([get_timeout/1, set_timeout/2, get_size/1, set_size/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(TIMEOUT, 1000).
-define(SIZE, 20).

-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {mod, args, timeout, size, list, len}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts a logger.
%% -----------------------------------------------------------------
start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, {?TIMEOUT, ?SIZE}, []).

%% -----------------------------------------------------------------
%% Function: start_link/3
%% Description: Starts a logger.
%% -----------------------------------------------------------------
start_link(Name, Timeout, Size) ->
	gen_server:start_link({local, Name}, ?MODULE, {Timeout, Size}, []).

%% -----------------------------------------------------------------
%%@doc 获取指定名称的日志处理器
%% @spec get(Name::atom()) -> return()
%% where
%% return() = {Mod, Args}
%%@end
%% -----------------------------------------------------------------
get(Name) ->
	gen_server:call(Name, get).

%% -----------------------------------------------------------------
%%@doc 设置指定名称的日志处理器
%% @spec set(Name,Mod,Args) -> return()
%% where
%% return() =  tuple()
%%@end
%% -----------------------------------------------------------------
set(Name, Mod, Args) ->
	gen_server:call(Name, {set, Mod, Args}).

%% -----------------------------------------------------------------
%%@doc 删除指定名称的日志处理器
%% @spec unset(Name,Mod,Args) -> return()
%% where
%% return() =  tuple()
%%@end
%% -----------------------------------------------------------------
unset(Name, Mod, Args) ->
	gen_server:call(Name, {unset, Mod, Args}).

%% -----------------------------------------------------------------
%%@doc 获取缓存日志的超时时长
%%```
%%  Timeout::integer()
%%'''
%% @spec get_timeout(Name) -> TimeOut
%%@end
%% -----------------------------------------------------------------
get_timeout(Name) ->
	gen_server:call(Name, get_timeout).

%% -----------------------------------------------------------------
%%@doc 设置缓存日志的超时时长
%%```
%%  OldTimeout::integer()
%%'''
%% @spec set_timeout(Name,Timeout::integer()) -> OldTimeout
%%@end
%% -----------------------------------------------------------------
set_timeout(Name, Timeout) ->
	gen_server:call(Name, {set_timeout, Timeout}).

%% -----------------------------------------------------------------
%%@doc 获取缓存日志的大小
%% @spec get_size(Name) -> Size
%%@end
%% -----------------------------------------------------------------
get_size(Name) ->
	gen_server:call(Name, get_size).

%% -----------------------------------------------------------------
%%@doc 设置缓存日志的大小
%% @spec set_size(Name,Size) -> OldSize
%%@end
%% -----------------------------------------------------------------
set_size(Name, Size) ->
	gen_server:call(Name, {set_size, Size}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Timeout, Size}) ->
	{ok, #state{timeout = Timeout, size = Size, list = [], len = 0}, ?HIBERNATE_TIMEOUT}.

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
handle_call(get, _From, #state{mod = M, args = A} = State) ->
	{reply, {M, A}, State, timeout(State)};
handle_call({set, Mod, Args}, _From, #state{mod = undefined} = State) ->
	{reply, {undefined, undefined}, State#state{mod = Mod, args = Mod:init(Args)},
		?HIBERNATE_TIMEOUT};
handle_call({set, Mod, Args}, _From, #state{mod = M, args = A, list = L} = State) ->
	M:close(M:log(A, lists:reverse(L))),
	{reply, {M, A}, State#state{mod = Mod, args = Mod:init(Args), list = []},
		?HIBERNATE_TIMEOUT};
handle_call({unset, _Mod, _Args}, _From, #state{mod = undefined} = State) ->
	{reply, {undefined, undefined}, State, ?HIBERNATE_TIMEOUT};
handle_call({unset, _Mod, _Args}, _From, #state{mod = M, args = A, list = L} = State) ->
	M:close(M:log(A, lists:reverse(L))),
	{reply, {M, A}, State#state{mod = undefined, args = undefined, list = []},
		?HIBERNATE_TIMEOUT};

handle_call(get_timeout, _From, #state{timeout = Timeout} = State) ->
	{reply, Timeout, State, timeout(State)};
handle_call({set_timeout, Timeout}, _From, #state{timeout = T} = State) ->
	{reply, T, State#state{timeout = Timeout}, Timeout};
handle_call(get_size, _From, #state{size = Size} = State) ->
	{reply, Size, State, timeout(State)};
handle_call({set_size, Size}, _From, #state{size = S} = State) ->
	{reply, S, State#state{size = Size}, timeout(State)};

handle_call({stop, Reason}, _From, #state{mod = undefined} = State) ->
	{stop, Reason, ok, State};
handle_call({stop, Reason}, _From, #state{mod = M, args = A, list = L} = State) ->
	A1 = M:close(M:log(A, lists:reverse(L))),
	{stop, Reason, ok, State#state{args = A1, list = [], len = 0}};

handle_call(_Request, _From, State) ->
	{noreply, State, timeout(State)}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({stop, Reason}, #state{mod = undefined} = State) ->
	{stop, Reason, State};
handle_cast({stop, Reason}, #state{mod = M, args = A, list = L} = State) ->
	A1 = M:close(M:log(A, lists:reverse(L))),
	{stop, Reason, State#state{args = A1, list = [], len = 0}};

handle_cast(_Msg, State) ->
	{noreply, State, timeout(State)}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info(timeout, #state{list = []} = State) ->
	{noreply, State, hibernate};

handle_info(timeout, #state{mod = undefined} = State) ->
	{noreply, State#state{list = [], len = 1}, ?HIBERNATE_TIMEOUT};
handle_info(timeout, #state{mod = M, args = A, list = L} = State) ->
	{noreply, State#state{args = M:log(A, lists:reverse(L)),
		list = [], len = 1}, ?HIBERNATE_TIMEOUT};

handle_info(_Msg, #state{mod = undefined} = State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_info({_Time, _N, _Pid, _Mod, _Code, _Src, _Type, _Data} = Msg,
	#state{mod = M, args = A, timeout = T, size = S, list = L, len = Len} = State) ->
	case S > Len of
		true ->
			{noreply, State#state{list = [Msg | L], len = Len + 1}, T};
		false ->
			{noreply, State#state{args = M:log(A, lists:reverse([Msg | L])),
				list = [], len = 1}, ?HIBERNATE_TIMEOUT}
	end;

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
timeout(#state{list = []}) ->
	?HIBERNATE_TIMEOUT;
timeout(#state{timeout = Timeout}) ->
	Timeout.
