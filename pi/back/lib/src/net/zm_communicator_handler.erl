%%%@doc 通讯执行器
%%@end


-module(zm_communicator_handler).

-description("communicator handler").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1]).
-export([asyn_request/4, sync_request/4]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(EPSILON, 100).

-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {mod, args, count, cons, reqs, runs}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts a logger.
%% -----------------------------------------------------------------
start_link({_M, _A, _Count} = Args) ->
	gen_server:start_link(?MODULE, Args, []).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({M, A, 0}) ->
	{ok, #state{mod = M, args = A, count = 0}, ?HIBERNATE_TIMEOUT};
init({M, A, Count}) ->
	{ok, #state{mod = M, args = A, count = Count,
		cons = queue:new(), reqs = queue:new(), runs = sb_trees:empty()},
		?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%%@doc 通过指定的方式向目标发送异步请求
%% @spec asyn_request(Pid::pid(), Type, Msg, Timeout::integer()) -> return()
%% where
%% return() =  {ok, term()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
asyn_request(Pid, Type, Msg, Timeout) ->
	case gen_server:call(Pid, get_request) of
		{ok, M, A} ->
			M:asyn_request(A, Type, Msg, Timeout);
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 通过指定的方式向目标发送同步请求
%% @spec sync_request(Pid::pid(), Type, Msg, Timeout::integer()) -> return()
%% where
%% return() = {ok, term()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
sync_request(Pid, Type, Msg, Timeout) ->
	Ref = make_ref(),
	Pid ! {request, {self(), Type, Msg, Timeout, Ref}},
	receive
		{Ref, R} ->
			R
		after Timeout ->
			{error, timeout}
	end.

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
handle_call(get_request, _From, #state{mod = M, args = A, count = 0} = State) ->
	try
		Con = M:open(A),
		{reply, {ok, M, Con}, State#state{count = {M, Con}}, ?HIBERNATE_TIMEOUT}
	catch
		Error:Reason ->
			{reply, {Error, Reason}, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call(get_request, _From, #state{mod = M, args = A, count = {M, Con}} = State) ->
	case M:is_alive(Con) of
		true ->
			{reply, {ok, M, Con}, State, ?HIBERNATE_TIMEOUT};
		false ->
			try
				Con1 = M:open(A),
				{reply, {ok, M, Con1}, State#state{count = {M, Con1}},
					?HIBERNATE_TIMEOUT}
			catch
				Error:Reason ->
					{reply, {Error, Reason}, State#state{count = 0},
						?HIBERNATE_TIMEOUT}
			end
	end;

handle_call(_Request, _From, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({stop, Reason}, #state{count = 0} = State) ->
	{stop, Reason, State};

handle_cast({stop, Reason}, #state{mod = M, args = _A, count = {M, Con}} = State) ->
	M:close(Con),
	{stop, Reason, State};

handle_cast({stop, Reason}, #state{mod = M, cons = Cons, runs = Runs} = State) ->
	[M:close(Con) || Con <- queue:to_list(Cons)],
	[M:close(Con) || Con <- sb_trees:values(Runs)],
	{stop, Reason, State};

handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({request, {From, _Type, _Msg, _Timeout, Ref} = Info},
	#state{mod = M, args = A, count = Count, cons = Cons, reqs = Reqs, runs = Runs} = State) ->
	case get_con(M, A, Count, Cons) of
		{ok, Con, C1, Q} ->
			{noreply, State#state{count = C1, cons = Q, reqs = Reqs,
				runs = sb_trees:enter(con_request(M, Con, Info), Con, Runs)},
				?HIBERNATE_TIMEOUT};
		{wait, Q} ->
			{noreply, State#state{count = 0, cons = Q,
				reqs = queue:in({Info, zm_time:now_millisecond()}, Reqs)},
				?HIBERNATE_TIMEOUT};
		E ->
			From ! {Ref, E},
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({put_con, Con, Pid}, #state{mod = M, cons = Cons, reqs = Reqs, runs = Runs} = State) ->
	Runs1 = sb_trees:delete_any(Pid, Runs),
	case get_req(Reqs, zm_time:now_millisecond()) of
		{none, Q} ->
			{noreply, State#state{cons = queue:in(Con, Cons), reqs = Q, runs = Runs1},
				?HIBERNATE_TIMEOUT};
		{Info, Q} ->
			Runs2 = sb_trees:enter(con_request(M, Con, Info), Con, Runs1),
			{noreply, State#state{reqs = Q, runs = Runs2}, ?HIBERNATE_TIMEOUT}
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
%获得可用的连接，返回可用的{ok, M, A, Count, Queue}
%如果当前没有可用连接，则返回 {wait, Queue}
%如果创建连接失败，则返回 {Error, Reason}
get_con(M, A, Count, Queue) ->
	case queue:out(Queue) of
		{{_, Con}, Q} ->
			case M:is_alive(Con) of
				true ->
					{ok, Con, Count, Q};
				false ->
					get_con(M, A, Count + 1, Q)
			end;
		{empty, _} ->
			if
				Count > 0 ->
					try
						{ok, M:open(A), Count - 1, Queue}
					catch
						Error:Reason ->
							{Error, Reason}
					end;
				true ->
					 {wait, Queue}
			end
	end.

%连接请求
con_request(M, Con, {From, Type, Msg, Timeout, Ref}) ->
	Self = self(),
	spawn(
		fun () ->
			try
				{Con1, R} = M:sync_request(Con, Type, Msg, Timeout),
				From ! {Ref, R},
				Self ! {put_con, Con1, self()}
			catch
				Error:Reason ->
					From ! {Ref, {Error, Reason}},
					Self ! {put_con, Con, self()}
			end
		end).

%获得可用的请求，返回可用的{Info, Queue}
get_req(Queue, Time) ->
	case queue:out(Queue) of
		{{_, {{From, Type, Msg, Timeout, Ref}, LastTime}}, Q} ->
			T = Timeout - Time + LastTime,
			if
				T > ?EPSILON ->
					{{From, Type, Msg, T, Ref}, Q};
				true ->
					get_req(Q, Time)
			end;
		{empty, _} ->
			{none, Queue}
	end.
