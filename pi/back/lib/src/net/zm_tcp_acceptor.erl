%%%@doc 通用TCP端口接收器
%%@end

-module(zm_tcp_acceptor).

-description("general tcp acceptor").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {src, listen, opts, ssl, amount, ipfilter, mfa, tcp, ref, ets, con_tables, statistics}).

%%%=================EXPORTED FUNCTIONS=================
%% -------------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts a tcp acceptor.
%% Returns: {'ok', pid()} | {'error', term()}.
%% -------------------------------------------------------------------
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Src, Listen, Opts, SSL, ConAmount, IpFilter, MFA, TCP}) ->
	{ok, Ref} = prim_inet:async_accept(Listen, -1),
	Ets = ets:new(?MODULE, []),
	Statistics = ets:new(?MODULE, []),
	put(tcp_info, {Ets, Statistics}),
	put(tcp_info_total, 0),
	{ok, #state{src = Src, listen = Listen,
		opts = Opts, ssl = SSL, amount = ConAmount, ipfilter = IpFilter,
		mfa = MFA, tcp = TCP, ref=Ref, ets = Ets, con_tables = [],
		statistics = Statistics}, ?HIBERNATE_TIMEOUT}.

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
handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({con_tables, ConTables}, State) ->
	{noreply, State#state{con_tables = ConTables}, ?HIBERNATE_TIMEOUT};
handle_cast({set_ipfilter, IpFilter}, State) ->
	{noreply, State#state{ipfilter = IpFilter}, ?HIBERNATE_TIMEOUT};

handle_cast({closed, Pid, L}, #state{ets = Ets, statistics = Table} = State) ->
	ets:delete(Ets, Pid),
	[merge(Table, CmdType, Size) || {CmdType, Size} <- L],
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_cast({statistics, _, L}, #state{statistics = Table} = State) ->
	[merge(Table, CmdType, Size) || {CmdType, Size} <- L],
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({inet_async, Listen, Ref, {ok, Socket}}, #state{src = Src,
	opts = Opts, ssl = SSL, amount = ConAmount, ipfilter = IpFilter,
	mfa = MFA, tcp = TCP, ref = Ref, ets = Ets, con_tables = ConTables} = State) ->
	{ok, {IP, Port}} = inet:peername(Socket),
	[accept_socket(Socket, IP, Port, Src, Opts, SSL, MFA, TCP, Ets) ||
		zm_con:ip_filter(Socket, IP, IpFilter),
		zm_con:max_count(Socket, ConTables, ConAmount)],
	Total = get(tcp_info_total),
	put(tcp_info_total, Total + 1),
	{ok, Ref1} = prim_inet:async_accept(Listen, -1),
	{noreply, State#state{ref = Ref1}, ?HIBERNATE_TIMEOUT};

handle_info({inet_async, _Listen, _Ref, Error}, State) ->
	{stop, Error, State};

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
% 接受socket，创建新进程并转交socket的控制权，注册进程和socket
accept_socket(Socket, IP, Port, Src, Opts, SSL, {M, F, A}, TCP, Ets) ->
	true = inet_db:register_socket(Socket, TCP),
	SSLSocket = ssl_socket(Socket, SSL),
	{ok, Pid} = apply(M, F, [A, self(), Src, SSLSocket]),
	ok = zm_socket:controlling_process(SSLSocket, Pid),
	ok = zm_socket:setopts(SSLSocket, Opts),
	ets:insert(Ets, {Pid, SSLSocket, IP, Port}).

% 将socket变成 ssl socket
ssl_socket(Socket, none) ->
	Socket;
ssl_socket(Socket, {Cacertfile, Certfile, Keyfile, Timeout}) ->
	{ok, SSLSocket} = ssl:ssl_accept(Socket,
		[{cacertfile, Cacertfile}, {certfile, Certfile},
		{keyfile, Keyfile}], Timeout),
	SSLSocket.

% 将统计信息合并进表中
merge(Ets, CmdType, {Count,Size}) ->
	case ets:lookup(Ets, CmdType) of
		[{_, {Amount, DataSize}}] ->
			ets:insert(Ets, {CmdType, {Amount + Count, DataSize + Size}});
		_ ->
			ets:insert(Ets, {CmdType, {Count, Size}})
	end;
merge(Ets, CmdType, Size) ->
	case ets:lookup(Ets, CmdType) of
		[{_, {Amount, DataSize}}] ->
			ets:insert(Ets, {CmdType, {Amount + 1, DataSize + Size}});
		_ ->
			ets:insert(Ets, {CmdType, {1, Size}})
	end.
