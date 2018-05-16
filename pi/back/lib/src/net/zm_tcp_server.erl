%%%@doc 通用TCP服务器
%%@end


-module(zm_tcp_server).

-description("general tcp server").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/9, get_info/1, statistics/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {src, ports, listen_opts, socket_opts, ssl, acceptor, amount, ipfilter, mfa, tcp, list}).

%%%=================EXPORTED FUNCTIONS=================
%% -------------------------------------------------------------------
%%@doc 启动一个tcp管理服务器
%% @spec start_link(Src::atom(), Ports::list(), LOPts::list(), SOpts::list(), SSL::tuple(), AcceptorCount::integer(), ConAmount::integer(), IpFilter::list(), MFA::tuple()) -> return()
%% where
%%	return() = {ok, Pid} | {error, Reason}
%%@end
%% -------------------------------------------------------------------
-type start_link_return() :: {'ok', pid()} | {'error', term()}.
-spec start_link(atom(), list(), list(), list(),
	tuple() | any(), integer(), integer(), atom() | list(), {module(), atom(), [_]}) ->
	start_link_return().
%% -------------------------------------------------------------------
start_link(Src, Ports, LOpts, SOpts, {_, _, _, Timeout} = SSL,
	AcceptorCount, ConAmount, IpFilter, {_M, _F, _A} = MFA)
	when Timeout > 0, Timeout < 16#7fffffff ->
	case ssl:start(permanent) of
		ok ->
			gen_server:start_link(?MODULE, {Src, Ports, LOpts,
				SOpts, SSL, AcceptorCount, ConAmount, IpFilter, MFA}, []);
		{error, {already_started, ssl}} ->
			gen_server:start_link(?MODULE, {Src, Ports, LOpts,
				SOpts, SSL, AcceptorCount, ConAmount, IpFilter, MFA}, []);
		{error, Reason} ->
			erlang:error(Reason)
	end;
start_link(Src, Ports, LOpts, SOpts, none,
	AcceptorCount, ConAmount, IpFilter, {_M, _F, _A} = MFA) ->
	gen_server:start_link(?MODULE, {Src, Ports, LOpts,
		SOpts, none, AcceptorCount, ConAmount, IpFilter, MFA}, []).

%% -----------------------------------------------------------------
%%@doc 获得服务器信息
%% @spec get_info(Pid::pid()) -> return()
%% where
%% return() = {Src, Ports, LOpts, SOpts, SSL, AcceptorCount, ConAmount, MFA}
%%@end
%% -----------------------------------------------------------------
get_info(Pid) ->
	gen_server:call(Pid, get_info).

%% -----------------------------------------------------------------
%%@doc 获得服务器统计数据
%% @spec statistics(Pid::pid()) -> return()
%% where
%% return() =  any()
%%@end
%% -----------------------------------------------------------------
statistics(Pid) ->
	gen_server:call(Pid, statistics).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Src, Ports, LOpts, SOpts, SSL, AcceptorCount, ConAmount, IpFilter, MFA}) ->
	process_flag(trap_exit, true),
	TCP = tcp_module(LOpts),
	L = [{Port, lists:duplicate(AcceptorCount, Listen)} || Port <- Ports,
		({ok, Listen} = gen_tcp:listen(Port, LOpts)) =/= none],
	List = [accept(Src, Port, Listen, SOpts, SSL, ConAmount, IpFilter, MFA, TCP)
		|| {Port, ListenL} <- L, Listen <- ListenL],
	set_con_tables(List),
	{ok, #state{src = Src, ports = Ports, listen_opts = LOpts,
		socket_opts = SOpts, ssl = SSL, acceptor = AcceptorCount,
		amount = ConAmount, ipfilter = IpFilter, mfa = MFA, tcp = TCP, list = List},
		?HIBERNATE_TIMEOUT}.

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
handle_call(get_info, _From, #state{src = Src, ports = Ports,
	listen_opts = LOpts, socket_opts = SOpts, ssl = SSL,
	acceptor = AcceptorCount, amount = ConAmount, mfa = MFA} = State) ->
	Reply = [{src, Src}, {ports, Ports}, {listen_opts, LOpts}, 
		{socket_opts, SOpts}, {ssl, SSL}, {acceptor, AcceptorCount},
		{amount, ConAmount}, {mfa, MFA}],
	{reply, Reply, State, ?HIBERNATE_TIMEOUT};
handle_call(statistics, _From, #state{list = List} = State) ->
	{reply, merge_statistics(List, [], [], 0), State, ?HIBERNATE_TIMEOUT};
handle_call({set_ipfilter, IpFilter}, _From, #state{list = List} = State) ->
	[gen_server:cast(Pid, {set_ipfilter, IpFilter}) || {Pid, _, _, _} <- List],
	{reply, ok, State#state{ipfilter = IpFilter}, ?HIBERNATE_TIMEOUT};
handle_call({close_connects, Reason}, _From, #state{list = List} = State) ->
	[close_connects(ets:tab2list(Table), Reason) || {_, _, _, {Table, _}} <- List],
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

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
handle_info({'EXIT', Pid, _Reason}, #state{src = Src,
	socket_opts = SOpts, ssl = SSL, amount = ConAmount,
	ipfilter = IpFilter, mfa = MFA, tcp = TCP, list = List} = State) ->
	case lists:keyfind(Pid, 1, List) of
		{_, Port, Listen} ->
			T = accept(Src, Port, Listen, SOpts,
				SSL, ConAmount, IpFilter, MFA, TCP),
			List1 = lists:keyreplace(Pid, 1, List, T),
			set_con_tables(List1),
			{noreply, State#state{list = List1}, ?HIBERNATE_TIMEOUT};
		false ->
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
% 获得tcp 模式
tcp_module([inet | _]) ->
	inet_tcp;
tcp_module([inet6 | _]) ->
	inet6_tcp;
tcp_module([{tcp_module, Mod} | _]) ->
	Mod;
tcp_module([_ | Opts]) ->
	tcp_module(Opts);
tcp_module([]) ->
	inet_db:tcp_module().

% 创建一个接收进程，获取它的信息（连接表和统计表）
accept(Src, Port, Listen, Opts, SSL, ConAmount, IpFilter, MFA, TCP) ->
	{ok, Pid} = zm_tcp_acceptor:start_link({
		Src, Listen, Opts, SSL, ConAmount, IpFilter, MFA, TCP}),
	{Pid, Port, Listen}.

% 设置每个监听进程的全局连接表
set_con_tables(L) ->
	Tables = [Table || {_, _, _, {Table, _}} <- L],
	[gen_server:cast(Pid, {con_tables, Tables}) || {Pid, _, _, _} <- L].

% 合并统计
merge_statistics([{Pid, _, _} | T], L1, L2, Total) ->
	case z_lib:pid_dictionary(Pid) of
		L when is_list(L) ->
			{_, {ConEts, StatisticsEts}} = lists:keyfind(tcp_info, 1, L),
			{_, C} = lists:keyfind(tcp_info_total, 1, L),
			S = try
				ets:tab2list(StatisticsEts)
			catch
				_:_ ->
					[]
			end,
			merge_statistics(T, [ConEts | L1], [S | L2], Total + C);
		_ ->
			merge_statistics(T, L1, L2, Total)
	end;
merge_statistics([], L, [H | T], Total) ->
	{zm_con:con_count(L, 0), merge_statistics(T,
		sb_trees:from_orddict(lists:sort(H))), Total}.

% 合并统计树
merge_statistics([H | T], Tree) ->
	merge_statistics(T, zm_con:statistics(Tree, H));
merge_statistics([], Tree) ->
	Tree.

% 关闭全部连接
close_connects(L, Reason) ->
	[R || {Pid, _, _} <- L, (R = Pid ! {close, Reason}) =/= ok].
