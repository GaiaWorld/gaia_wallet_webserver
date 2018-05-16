%% 
%% @doc gossip服务器, 负责处理gossip protocol
%%


-module(gossip_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
%%进程上下文
-record(gossip_state, {
					buddy,
					ping_timer,
					ping_pong_timeout,
					send_buffer,
					receive_buffer,
					send_timer,
					receive_timer
}).
-define(GOSSIP_HIBERNATE_TIMEOUT, 5000).

-record(gossip, {
						id,
						from,
						dest,
						type,
						status,
						digests,
						payload,
						send_time,
						timeout
}).
-define(GOSSIP_PING_TIME, 15000).
-define(GOSSIP_PING_PONG_TIMEOUT, 30000).
-define(GOSSIP_SEND_TIMEOUT, 10000).
-define(GOSSIP_RECEIVE_TIMEOUT, 10000).

-define(GOSSIP_SEND_STATE, send).
-define(GOSSIP_ASYN_HANDLE_STATE, asyn_handle).

-define(GOSSIP_TIMEOUT_EVENT, gossip_timeout).
-define(GOSSIP_BUDDY_TIMEOUT_EVENT, buddy_timeout).
-define(GOSSIP_SYS_EVENTS, [?GOSSIP_TIMEOUT_EVENT, ?GOSSIP_BUDDY_TIMEOUT_EVENT]).

-define(GOSSIP_SYNC_MSG_SECTION, gossip_sync_msg).
-define(GOSSIP_ACK_MSG_SECTION, gossip_ack_msg).
-define(GOSSIP_FINISH_MSG_SECTION, gossip_finish_msg).
-define(GOSSIP_EXIT_MSG_SECTION, gossip_exit_msg).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, register/3, sync/7, ack/8, finish/8, exit/6, stop/1]).

%%
%%启动gossip服务器
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%注册gossip事件异步监听器
%%
register(sys, {_, _, _} = MFA, Timeout) ->
	[zm_event:set({?MODULE, Type}, MFA, Timeout) || Type <- ?GOSSIP_SYS_EVENTS],
	ok;
register(Type, {_, _, _} = MFA, Timeout) ->
	zm_event:set({?MODULE, Type}, MFA, Timeout).

%%
%%发送gossip同步请求消息
%%
sync(Src, Dest, Type, Digests, PayLoad, GossipTimeout, Timeout) when Dest =/= node() ->
	gen_server:call(?MODULE, {?GOSSIP_SYNC_MSG_SECTION, {Src, Dest, Type, Digests, PayLoad, GossipTimeout}}, Timeout).

%%
%%发送gossip同步回应消息
%%
ack(Gossip, Src, Dest, Type, Digests, PayLoad, GossipTimeout, Timeout) when Dest =/= node() ->
	gen_server:call(?MODULE, {?GOSSIP_ACK_MSG_SECTION, {Gossip#gossip.id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}}, Timeout).

%%
%%发送同步完成消息
%%
finish(Gossip, Src, Dest, Type, Digests, PayLoad, GossipTimeout, Timeout) when Dest =/= node() ->
	gen_server:call(?MODULE, {?GOSSIP_FINISH_MSG_SECTION, {Gossip#gossip.id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}}, Timeout).

%%
%%发送退出消息
%%
exit(Gossip, Src, Dest, Type, PayLoad, Timeout) when Dest =/= node() ->
	gen_server:call(?MODULE, {?GOSSIP_EXIT_MSG_SECTION, {Gossip#gossip.id, Src, Dest, Type, [], PayLoad, 0}}, Timeout).

%%
%%关闭gossip服务器
%%
stop(Reason) ->
	gen_server:call(?MODULE, {stop, Reason}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

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
init(_Args) ->
    {ok, #gossip_state{ping_pong_timeout = 0,
				send_buffer = undefined, 
				receive_buffer = undefined,
				send_timer = erlang:start_timer(?GOSSIP_SEND_TIMEOUT, self(), check_send),
				receive_timer = erlang:start_timer(?GOSSIP_RECEIVE_TIMEOUT, self(), check_receive)}}.


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
handle_call({?GOSSIP_SYNC_MSG_SECTION, {Src, Dest, Type, Digests, PayLoad, GossipTimeout}}, From, #gossip_state{send_buffer = undefined, receive_buffer = undefined} = State) ->
	NewState=send_gossip(?GOSSIP_SYNC_MSG_SECTION, From, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State),
	{reply, ok, NewState#gossip_state{buddy = Dest, ping_timer = erlang:start_timer(?GOSSIP_PING_TIME, self(), gossip_ping)}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_call({?GOSSIP_ACK_MSG_SECTION, Info}, _, State) ->
	{reply, ok, handle_send_ack(Info, State), ?GOSSIP_HIBERNATE_TIMEOUT};
handle_call({?GOSSIP_FINISH_MSG_SECTION, Info}, _, State) ->
	{reply, ok, (handle_send_finish(Info, State))#gossip_state{send_buffer = undefined, receive_buffer = undefined}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_call({?GOSSIP_EXIT_MSG_SECTION, {Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}}, _, State) ->
	{reply, ok, reset(false, send_gossip(?GOSSIP_EXIT_MSG_SECTION, Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State)), ?GOSSIP_HIBERNATE_TIMEOUT};
handle_call({stop, Reason}, _, State) ->
	{stop, Reason, ok, reset(false, State)};
handle_call({Section, _}, _, #gossip_state{send_buffer = SendBuffer, receive_buffer = ReceiveBuffer} = State) ->
    {reply, {error, {invalid, Section, SendBuffer, ReceiveBuffer}}, State, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
	{noreply, State, ?GOSSIP_HIBERNATE_TIMEOUT}.


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
handle_cast({?GOSSIP_SYNC_MSG_SECTION, #gossip{from = {From, _}, dest = Dest} = Gossip}, #gossip_state{send_buffer = undefined, receive_buffer = undefined} = State) 
  when Dest =:= node() ->
	NewState=receive_gossip(?GOSSIP_SYNC_MSG_SECTION, Gossip, State),
	{noreply, NewState#gossip_state{buddy = From}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_cast({?GOSSIP_ACK_MSG_SECTION, #gossip{dest = Dest} = Gossip}, State) when Dest =:= node() ->
	{noreply, handle_receive_ack(Gossip, State), ?GOSSIP_HIBERNATE_TIMEOUT};
handle_cast({?GOSSIP_FINISH_MSG_SECTION, #gossip{dest = Dest} = Gossip}, State) when Dest =:= node() ->
	{noreply, (handle_receive_finish(Gossip, State))#gossip_state{send_buffer = undefined, receive_buffer = undefined}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_cast({?GOSSIP_EXIT_MSG_SECTION, #gossip{dest = Dest} = Gossip}, State) 
  when Dest =:= node() ->
	zm_event:notify(?MODULE, {?GOSSIP_EXIT_MSG_SECTION, Gossip#gossip.type}, Gossip),
	{noreply, reset(false, State), ?GOSSIP_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?GOSSIP_HIBERNATE_TIMEOUT}.


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
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info({timeout, TimerRef, check_send}, #gossip_state{send_timer = TimerRef} = State) ->
	{noreply, (check_send(State))#gossip_state{send_timer = erlang:start_timer(?GOSSIP_SEND_TIMEOUT, self(), check_send)}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, check_receive}, #gossip_state{receive_timer = TimerRef} = State) ->
	{noreply, (check_receive(State))#gossip_state{receive_timer = erlang:start_timer(?GOSSIP_RECEIVE_TIMEOUT, self(), check_receive)}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_info({timeout, TimerRef, gossip_ping}, #gossip_state{buddy = Buddy, ping_timer = TimerRef} = State) ->
	{noreply, (ping(Buddy, State))#gossip_state{ping_timer = erlang:start_timer(?GOSSIP_PING_TIME, self(), gossip_ping)}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_info({ping, Buddy}, #gossip_state{buddy = Buddy} = State) ->
	%io:format("!!!!!!received ping, from:~p~n", [Buddy]),
	{noreply, pong(Buddy, State), ?GOSSIP_HIBERNATE_TIMEOUT};
handle_info({pong, Buddy}, #gossip_state{buddy = Buddy} = State) ->
	%io:format("!!!!!!received pong, from:~p~n", [Buddy]),
	{noreply, State#gossip_state{ping_pong_timeout = 0}, ?GOSSIP_HIBERNATE_TIMEOUT};
handle_info(_, State) ->
    {noreply, State, ?GOSSIP_HIBERNATE_TIMEOUT}.


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

new_gossip(Id, Src, Dest, Type, Digests, PayLoad) ->
	Local=node(),
	#gossip{
		id = Id,
		from = {Local, Src},
		dest = Dest,
		type = Type,
		digests = Digests,
		payload = PayLoad
	}.

ping(Buddy, #gossip_state{ping_pong_timeout = 0} = State) ->
	%io:format("!!!!!!start ping, dest:~p~n", [Buddy]),
	{?MODULE, Buddy} ! {ping, node()},
	State#gossip_state{ping_pong_timeout = z_lib:now_millisecond() + ?GOSSIP_PING_PONG_TIMEOUT};
ping(_, State) ->
	State.

pong(Buddy, State) ->
	{?MODULE, Buddy} ! {pong, node()},
	State#gossip_state{ping_pong_timeout = z_lib:now_millisecond() + ?GOSSIP_PING_PONG_TIMEOUT}.

send(Dest, Section, Gossip, GossipTimeout) ->
	Now=z_lib:now_millisecond(),
	GossipMsg={Section, Gossip#gossip{status = ?GOSSIP_SEND_STATE, send_time = Now, timeout = Now + GossipTimeout}},
	gen_server:abcast([Dest], ?MODULE, GossipMsg),
	GossipMsg.

send_gossip(Section, {Pid, Ref}, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State) when is_pid(Pid), is_reference(Ref) ->
	State#gossip_state{send_buffer = send(Dest, Section, new_gossip({erlang:pid_to_list(Pid), erlang:ref_to_list(Ref)}, Src, Dest, Type, Digests, PayLoad), GossipTimeout)};
send_gossip(Section, Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State) ->
	State#gossip_state{send_buffer = send(Dest, Section, new_gossip(Id, Src, Dest, Type, Digests, PayLoad), GossipTimeout)}.

handle_send_ack({Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}, #gossip_state{send_buffer = undefined, 
																				   receive_buffer = {?GOSSIP_SYNC_MSG_SECTION, #gossip{id = Id, status = ?GOSSIP_ASYN_HANDLE_STATE}}} = State) ->
	send_gossip(?GOSSIP_ACK_MSG_SECTION, Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State);
handle_send_ack({Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}, #gossip_state{send_buffer = {SendSection, #gossip{id = Id, status = ?GOSSIP_SEND_STATE}}, 
																				   receive_buffer = {ReceiveSection, #gossip{id = Id, status = ?GOSSIP_ASYN_HANDLE_STATE}}} = State) ->
	case 
		case {SendSection, ReceiveSection} of
			{?GOSSIP_SYNC_MSG_SECTION, ?GOSSIP_ACK_MSG_SECTION} ->
				true;
			{?GOSSIP_ACK_MSG_SECTION, ?GOSSIP_ACK_MSG_SECTION} ->
			 	true;
			_ ->
				false
		end
	of
		true ->
			send_gossip(?GOSSIP_ACK_MSG_SECTION, Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State);
		false ->
			State
	end;
handle_send_ack(_, #gossip_state{send_buffer = SendBuffer, receive_buffer = ReceiveBuffer}) ->
	erlang:error({send_ack_failed, SendBuffer, ReceiveBuffer}).


handle_send_finish({Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}, #gossip_state{send_buffer = undefined, 
																					receive_buffer = {?GOSSIP_SYNC_MSG_SECTION, #gossip{id = Id, status = ?GOSSIP_ASYN_HANDLE_STATE}}} = State) ->
	send_gossip(?GOSSIP_FINISH_MSG_SECTION, Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State);
handle_send_finish({Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout}, #gossip_state{send_buffer = {SendSection, #gossip{id = Id, status = ?GOSSIP_SEND_STATE}}, 
																					receive_buffer = {ReceiveSection, #gossip{id = Id, status = ?GOSSIP_ASYN_HANDLE_STATE}}} = State) ->
	case {SendSection, ReceiveSection} of
		{?GOSSIP_ACK_MSG_SECTION, ?GOSSIP_ACK_MSG_SECTION} ->
		 	send_gossip(?GOSSIP_FINISH_MSG_SECTION, Id, Src, Dest, Type, Digests, PayLoad, GossipTimeout, State);
		_ ->
			State
	end;
handle_send_finish(_, #gossip_state{send_buffer = SendBuffer, receive_buffer = ReceiveBuffer}) ->
	erlang:error({send_finish_failed, SendBuffer, ReceiveBuffer}).

receive_gossip(Section, #gossip{status = ?GOSSIP_SEND_STATE} = Gossip, State) ->
	case zm_event:notify(?MODULE, {Section, Gossip#gossip.type}, Gossip) of
		{error, Reason} ->
			erlang:error({gossip_notify_error, Reason});
		_ ->
			State#gossip_state{receive_buffer = {Section, Gossip#gossip{status = ?GOSSIP_ASYN_HANDLE_STATE}}}
	end;
receive_gossip(_, #gossip{status = _}, State) ->
	State.

handle_receive_ack(#gossip{id = Id} = Gossip, #gossip_state{send_buffer = {?GOSSIP_SYNC_MSG_SECTION, #gossip{id = Id, status = ?GOSSIP_SEND_STATE}}, 
																						 receive_buffer = undefined} = State) ->
	receive_gossip(?GOSSIP_ACK_MSG_SECTION, Gossip, State);
handle_receive_ack(#gossip{id = Id} = Gossip, #gossip_state{send_buffer = {SendSection, #gossip{id = Id, status = ?GOSSIP_SEND_STATE}}, 
																						 receive_buffer = {ReceiveSection, #gossip{id = Id, status = ?GOSSIP_ASYN_HANDLE_STATE}}} = State) ->
	case 
		case {SendSection, ReceiveSection} of
			{?GOSSIP_ACK_MSG_SECTION, ?GOSSIP_SYNC_MSG_SECTION} ->
				true;
			{?GOSSIP_ACK_MSG_SECTION, ?GOSSIP_ACK_MSG_SECTION} ->
				true;
			_ ->
				false
		end
	of
		true ->
			receive_gossip(?GOSSIP_ACK_MSG_SECTION, Gossip, State);
		false ->
			State
	end.

handle_receive_finish(#gossip{id = Id} = Gossip, #gossip_state{send_buffer = {SendSection, #gossip{id = Id, status = ?GOSSIP_SEND_STATE}}, 
																						  receive_buffer = {ReceiveSection, #gossip{id = Id, status = ?GOSSIP_ASYN_HANDLE_STATE}}} = State) ->
	
	case 
		case {SendSection, ReceiveSection} of
			{?GOSSIP_SYNC_MSG_SECTION, ?GOSSIP_ACK_MSG_SECTION} ->
				true;
			{?GOSSIP_ACK_MSG_SECTION, ?GOSSIP_SYNC_MSG_SECTION} ->
				true;
			{?GOSSIP_ACK_MSG_SECTION, ?GOSSIP_ACK_MSG_SECTION} ->
				true;
			_ ->
				false
		end
	of
		true ->
			receive_gossip(?GOSSIP_FINISH_MSG_SECTION, Gossip, State);
		false ->
			State
	end.

check_send(#gossip_state{ping_pong_timeout = Timeout, send_buffer = undefined} = State) ->
	Now=z_lib:now_millisecond(),
	if
		(Timeout > 0) and (Timeout =< Now) ->
			reset(true, State);
		true ->
			State
	end;
check_send(#gossip_state{ping_pong_timeout = Timeout, send_buffer = {Section, Gossip}} = State) ->
	Now=z_lib:now_millisecond(),
	if
		(Timeout > 0) and (Timeout =< Now) ->
			reset(true, State);
		true ->
			if
				Gossip#gossip.timeout > Now ->
					State;
				true ->
					case zm_event:notify(?MODULE, ?GOSSIP_TIMEOUT_EVENT, {Section, Now, Gossip}) of
						{error, Reason} ->
							erlang:error({gossip_notify_error, Reason});
						_ ->
							State#gossip_state{send_buffer = undefined}
					end
			end
	end.

check_receive(#gossip_state{ping_pong_timeout = Timeout, receive_buffer = undefined} = State) ->
	Now=z_lib:now_millisecond(),
	if
		(Timeout > 0) and (Timeout =< Now) ->
			reset(true, State);
		true ->
			State
	end;
check_receive(#gossip_state{ping_pong_timeout = Timeout, receive_buffer = {Section, Gossip}} = State) ->
	Now=z_lib:now_millisecond(),
	if
		(Timeout > 0) and (Timeout =< Now) ->
			reset(true, State);
		true ->
			if
				Gossip#gossip.timeout > Now ->
					State;
				true ->
					case zm_event:notify(?MODULE, ?GOSSIP_TIMEOUT_EVENT, {Section, Now, Gossip}) of
						{error, Reason} ->
							erlang:error({gossip_notify_error, Reason});
						_ ->
							State#gossip_state{receive_buffer = undefined}
					end
			end
	end.

reset(IsNotify, #gossip_state{buddy = Buddy, ping_timer = TimerRef, send_buffer = SendBuffer, receive_buffer = ReceiveBuffer} = State) ->
	if
		IsNotify ->
			zm_event:notify(?MODULE, ?GOSSIP_BUDDY_TIMEOUT_EVENT, {Buddy, SendBuffer, ReceiveBuffer});
		true ->
			continue
	end,
	case TimerRef of
		undefined ->
			continue;
		_ ->
			erlang:cancel_timer(TimerRef)
	end,
	State#gossip_state{buddy = undefined, ping_timer = undefined, ping_pong_timeout = 0, send_buffer = undefined, receive_buffer = undefined}.



