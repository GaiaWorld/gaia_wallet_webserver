%%%@doc 通讯会话管理器
%%```
%%% 支持同步通讯的会话池和异步通讯的单会话
%%%配置表的格式为：{应用协议名字-自定义, M, A, 同步或异步（0为异步，大于0为同步和最大数量）, Pid}
%%% M必须实现open/1, is_alive/1, close/1, asyn_request/4或sync_request/4 方法
%%'''
%%@end


-module(zm_communicator).

-description("Communicator").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0]).
-export([request/3, request/4]).
-export([get/1, set/4, unset/4, delete/1]).

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
-record(state, {ets}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts a communicator server.
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% -----------------------------------------------------------------
%%@doc 通过代理向指定名字的地址发送请求
%% @spec request(Name, Type, Msg) -> return()
%% where
%% return() = {ok, term()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
request(Name, Type, Msg) ->
	request(Name, Type, Msg, ?TIMEOUT).

%% -----------------------------------------------------------------
%%@doc 通过代理向指定名字的地址发送请求
%% @spec request(Name, Type, Msg, Timeout::integer()) -> return()
%% where
%% return() = {ok, term()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
request(Name, Type, Msg, Timeout) ->
	case ets:lookup(?MODULE, Name) of
		[{_, _, _, Count, none}] ->
			case gen_server:call(?MODULE, {start, Name}) of
				{ok, Pid} ->
					request(Pid, Type, Msg, Timeout, Count);
				E ->
					E
			end;
		[{_, _, _, Count, Pid}] ->
			request(Pid, Type, Msg, Timeout, Count);
		[] ->
			{error, handler_not_found}
	end.

request(Pid, Type, Msg, Timeout, 0) ->
	zm_communicator_handler:asyn_request(Pid, Type, Msg, Timeout);
request(Pid, Type, Msg, Timeout, _) ->
	zm_communicator_handler:sync_request(Pid, Type, Msg, Timeout).

%% -----------------------------------------------------------------
%%@doc 获取指定Name的通讯处理器
%% @spec get(Name) -> return()
%% where
%% return() =  none | {Name, M, A, Count, Pid}
%%@end
%% -----------------------------------------------------------------
get(Name) ->
	case ets:lookup(?MODULE, Name) of
		[R] -> R;
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc 设置指定Name的通讯处理器
%% @spec set(Name,M::atom(),A,Count::integer()) -> return()
%% where
%% return() = none | {Name, M, A, Count, Pid}
%%@end
%% -----------------------------------------------------------------
set(Name, M, A, Count) when is_atom(M), is_integer(Count) ->
	gen_server:call(?MODULE, {set, Name, M, A, Count}).

%% -----------------------------------------------------------------
%%@doc 删除指定Name的通讯处理器
%% @spec unset(Name,any(),any(),any()) -> return()
%% where
%% return() =  none | {Name, M, A, Count, Pid}
%%@end
%% -----------------------------------------------------------------
unset(Name, _, _, _) ->
	gen_server:call(?MODULE, {delete, Name}).

%% -----------------------------------------------------------------
%%@doc 删除指定Name的通讯处理器
%% @spec delete(Name) -> return()
%% where
%% return() =  none | {Name, M, A, Count, Pid}
%%@end
%% -----------------------------------------------------------------
delete(Name) ->
	gen_server:call(?MODULE, {delete, Name}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Ets) ->
	process_flag(trap_exit, true),
	{ok, #state{ets = ets:new(Ets, [named_table])}, ?HIBERNATE_TIMEOUT}.

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
handle_call({set, Name, M, A, Count}, _From, #state{ets = Ets} = State) ->
	Reply = case ets:lookup(Ets, Name) of
		[{_, _M, A, _Count, none} = R] ->
			ets:insert(Ets, {Name, M, A, Count, none}),
			R;
		[{_, _M, A, _Count, Old} = R] ->
			ets:insert(Ets, {Name, M, A, Count, none}),
			gen_server:cast(Old, {stop, "set"}),
			R;
		[] ->
			ets:insert(Ets, {Name, M, A, Count, none}),
			none
	end,
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};

handle_call({start, Name}, _From, #state{ets = Ets} = State) ->
	Reply = case ets:lookup(Ets, Name) of
		[{_, M, A, Count, none}] ->
			case zm_communicator_handler:start_link({M, A, Count}) of
				{ok, Pid} = R ->
					ets:insert(Ets, {Name, M, A, Count, Pid}),
					R;
				E ->
					E
			end;
		[{_, _M, _A, _Count, Pid}] ->
			{ok, Pid};
		[] ->
			none
	end,
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};

handle_call({delete, Name}, _From, #state{ets = Ets} = State) ->
	Reply = case ets:lookup(Ets, Name) of
		[{_, _M, _A, _Count, none} = R] ->
			ets:delete(Ets, Name),
			R;
		[{_, _M, _A, _Count, Pid} = R] ->
			ets:delete(Ets, Name),
			gen_server:cast(Pid, {stop, "delete"}),
			R;
		[] ->
			none
	end,
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};

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
handle_info({'EXIT', Pid, _Reason}, #state{ets = Ets} = State) ->
	del_pid(Ets, Pid),
	{noreply, State, ?HIBERNATE_TIMEOUT};

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
%将Pid从表中删除
del_pid(Ets, Pid) ->
	case ets:select(Ets, [{{'_','_','_','_','$1'},[{'=:=','$1',Pid}],['$_']}]) of
		[] ->
			[];
		L ->
			[ets:insert(Ets, {K, V1, V2, V3, none}) || {K, V1, V2, V3, _} <- L]
	end.
