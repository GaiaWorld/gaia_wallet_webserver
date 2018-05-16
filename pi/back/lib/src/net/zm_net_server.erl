%%%@doc 通用网络服务器，提供统一的启动、关闭、查询和统计的接口。
%%@end



-module(zm_net_server).

-description("general net server").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0, list/0, get/1, set/1, unset/1]).
-export([get_info/1, statistics/1, set_ipfilter/2, close_connects/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {ets}).

%%%=================EXPORTED FUNCTIONS=================
%% -------------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts a generic net server.
%% -------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% -----------------------------------------------------------------
%%@doc 获取当前正在活动的通讯服务进程信息列表
%% @spec list() -> list()
%%@end
%% -----------------------------------------------------------------
list() ->
	L = ets:select(?MODULE, [{{'$1','_'}, [{is_pid,'$1'}], ['$1']}]),
	[{Pid, get_info(Pid)} || Pid <- L].

%% -----------------------------------------------------------------
%%@doc 通过MFA获取对应的通讯服务pid，或者通过通讯服务pid获取对应的MFA
%% @spec get(PidMFA) -> return()
%% where
%% return() =  {ok, MFA | Pid} | none
%%@end
%% -----------------------------------------------------------------
get(PidMFA) ->
	case ets:lookup(?MODULE, PidMFA) of
		[{_, R}] ->
			{ok, R};
		[] ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 设置通讯服务的MFA
%% @spec set(MFA::{M::atom(),F::atom(),A::list()}) -> return()
%% where
%% return() =  {ok, Pid}
%%@end
%% -----------------------------------------------------------------
set({M, F, A} = MFA) when is_atom(M), is_atom(F), is_list(A) ->
	gen_server:call(?MODULE, {set, MFA}).

%% -----------------------------------------------------------------
%%@doc  删除通讯服务的MFA
%% @spec unset(MFA::{M::atom(),F::atom(),A::list()}) -> return()
%% where
%% return() =  {ok, Pid} | none
%%@end
%% -----------------------------------------------------------------
unset(MFA) ->
	gen_server:call(?MODULE, {unset, MFA}).

%% -----------------------------------------------------------------
%%@doc 获得服务器信息
%% @spec get_info(Pid::pid()) -> Info
%%@end
%% -----------------------------------------------------------------
get_info(Pid) ->
	gen_server:call(Pid, get_info).

%% -----------------------------------------------------------------
%%@doc 获得服务器统计数据
%% @spec statistics(Pid::pid()) -> return()
%% where
%% return()  =  {ConCount, [{{Cmd, Type}, {Count, ByteCount}}], Total}
%%@end
%% -----------------------------------------------------------------
statistics(Pid) ->
	gen_server:call(Pid, statistics).

%% -----------------------------------------------------------------
%%@doc 设置指定服务器的ip过滤器
%% @spec set_ipfilter(Pid::pid(),IpFilter) -> ok
%%@end
%% -----------------------------------------------------------------
set_ipfilter(Pid, IpFilter) ->
	gen_server:call(Pid, {set_ipfilter, IpFilter}).

%% -----------------------------------------------------------------
%%@doc 关闭指定服务器所有已存在的连接
%% @spec close_connects(Pid::pid(),Reason::term()) -> Info
%%@end
%% -----------------------------------------------------------------
close_connects(Pid, Reason) ->
	gen_server:call(Pid, {close_connects, Reason}).

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
handle_call({set, MFA}, _From, #state{ets = Ets} = State) ->
	R = case ets:lookup(Ets, MFA) of
		[{_, Pid}] ->
			{ok, Pid};
		_ ->
			{ok, server_apply(Ets, MFA)}
	end,
	{reply, R, State, ?HIBERNATE_TIMEOUT};
handle_call({unset, MFA}, _From, #state{ets = Ets} = State) ->
	R = case ets:lookup(Ets, MFA) of
		[{_, Pid}] ->
			ets:delete(Ets, MFA),
			ets:delete(Ets, Pid),
			exit(Pid, unset),
			{ok, Pid};
		_ ->
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
handle_info({'EXIT', Pid, _Reason}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Pid) of
		[{Pid, MFA}] ->
			ets:delete(Ets, Pid),
			server_apply(Ets, MFA),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info(timeout, State) ->
	{noreply, State, hibernate};

handle_info(_Info, State) ->
	{noreply, State}.

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
% 服务启动，并记录进程号
server_apply(Ets, {M, F, A} = MFA) ->
	{ok, Pid} = apply(M, F, A),
	ets:insert(Ets, {Pid, MFA}),
	ets:insert(Ets, {MFA, Pid}),
	Pid.
