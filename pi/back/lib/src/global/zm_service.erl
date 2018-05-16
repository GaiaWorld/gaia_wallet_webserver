%%@doc 服务节点表模块
%%```
%%% 每个节点上都有这个模块进程，其中有一个为主模块进程，由它来管理和分发节点服务表
%%% 如果没有主模块进程或主模块进程崩溃，则其余的模块进程竞争出一个主模块进程
%%% 主模块进程注册在global模块中，由底层保证它能被所有节点访问
%%% 如果两个网络合并时，会合并为一个主模块进程，并且服务节点表也会合并
%%% 服务节点表的格式为：{{ServiceName, Node}, ServiceArgs}
%%% 提供带超时的全局锁，由主zm_service负责解决冲突及等待
%%% 提供集群上节点间的时间校对
%%'''
%%@end


-module(zm_service).

-description("service node table").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0, start_link/1]).
-export([register/0, count/0, list/0, get/1, get/2, set/2, set/3, sets/2, sets/3, delete/1, deletes/1, node_member/2]).
-export([lock/4, unlock/2, lock_format/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(SLEEP_TIMEOUT, 300).
-define(HANDLE_TIMEOUT, 1000).
-define(SYNC_TIMEOUT, 600).
-define(REGISTER_TIMEOUT, 200).
-define(REGISTER_TIMEOUT_DELAY, 10).

-define(COLLATE_TIMEOUT, 1000).
-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {ets, election, leader, lock, timer_ref}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts a node service server.
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?MODULE, true}, []).

%% -----------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts a node service server.
%% -----------------------------------------------------------------
start_link(Election) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?MODULE, Election}, []).

%% -----------------------------------------------------------------
%% Function: register/0
%% Description: reregister service name.
%% -----------------------------------------------------------------
register() ->
	gen_server:call(?MODULE, register, 10000).

%% -----------------------------------------------------------------
%%@doc 获得服务表上服务的数量，undefined表示没有启动
%%```
%%  Count::integer()
%%'''
%% @spec count() -> return()
%% where
%%      return() = Count | undefined
%%@end
%% -----------------------------------------------------------------
count() ->
	ets:info(?MODULE, size).

%% -----------------------------------------------------------------
%%@doc 列出服务表上的所有服务
%% @spec list() -> list()
%%@end
%% -----------------------------------------------------------------
list() ->
	ets:tab2list(?MODULE).

%% -----------------------------------------------------------------
%%@doc 获取指定名称的服务
%% @spec get(Name) -> return()
%% where
%%      return() = [] | [{N, A}]
%%@end
%% -----------------------------------------------------------------
get(Name) ->
	get(?MODULE, Name, {Name, <<>>}, []).

%% -----------------------------------------------------------------
%%@doc 获取指定名称的服务
%% @spec get(Name,Node) -> return()
%% where
%%      return() = none | {N, A}
%%@end
%% -----------------------------------------------------------------
get(Name, Node) ->
	case ets:lookup(?MODULE, {Name, Node}) of
		[NA] ->
			NA;
		[] ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 设置指定名称的服务
%% @spec set(Name,Args) -> return()
%% where
%%      return() = ok | timeout | Error
%%@end
%% -----------------------------------------------------------------
set(Name, Args) ->
	gen_server:call(get_global(), {set, {{Name, node()}, Args}}, ?HANDLE_TIMEOUT).

%% -----------------------------------------------------------------
%% Function: set
%% Description: 参数Force表示是否强制等待本地同步
%% Returns: ok
%% -----------------------------------------------------------------
set(Name, Args, false) ->
	gen_server:call(get_global(), {set, {{Name, node()}, Args}}, ?HANDLE_TIMEOUT);
set(Name, Args, _Force) ->
	ok = gen_server:call(get_global(),
		{set, {{Name, node()}, Args}}, ?HANDLE_TIMEOUT),
	gen_server:call(?MODULE, {echo, ok}, ?HANDLE_TIMEOUT).

%% -----------------------------------------------------------------
%% Function: sets
%% Description: set service
%% Returns: ok
%% -----------------------------------------------------------------
sets(Name, Args) ->
	gen_server:call(get_global(), {sets, {Name, Args}}, ?HANDLE_TIMEOUT).

%% -----------------------------------------------------------------
%% Function: sets
%% Description: 参数Force表示是否强制等待本地同步
%% Returns: ok
%% -----------------------------------------------------------------
sets(Name, Args, false) ->
	gen_server:call(get_global(), {sets, {Name, Args}}, ?HANDLE_TIMEOUT);
sets(Name, Args, _Force) ->
	ok = gen_server:call(get_global(), {sets, {Name, Args}}, ?HANDLE_TIMEOUT),
	gen_server:call(?MODULE, {echo, ok}, ?HANDLE_TIMEOUT).

%% -----------------------------------------------------------------
%% Function: delete
%% Description: delete service
%% Returns: ok
%% -----------------------------------------------------------------
delete(Name) ->
	gen_server:call(get_global(), {delete, {Name, node()}}, ?HANDLE_TIMEOUT).

%% -----------------------------------------------------------------
%% Function: deletes
%% Description: deletes service
%% Returns: ok
%% -----------------------------------------------------------------
deletes(Name) ->
	gen_server:call(get_global(), {deletes, Name}, ?HANDLE_TIMEOUT).

%% -----------------------------------------------------------------
%% Function: node_member
%% Description: 判断节点是否在列表中，列表必须是从zm_service中取出的排序列表
%% Returns: true | false
%% -----------------------------------------------------------------
node_member(N, [{Node, _A} | T]) when N > Node ->
	node_member(N, T);
node_member(N, [{N, _A} | _]) ->
	true;
node_member(_N, _) ->
	false.

%% -----------------------------------------------------------------
%% Function: lock
%% Description: 用指定的锁，锁住键指定的时间，LockTime为0表示解锁，Timeout表示锁定超时时间
%% Returns: ok | {error, {invalid_lock, Locked}}
%%	Locked = {Module, Pid} | undefined
%% -----------------------------------------------------------------
lock(Key, Lock, LockTime, Timeout) ->
	gen_server:call(get_global(), {lock, Key, lock_format(Lock), LockTime}, Timeout).

%% -----------------------------------------------------------------
%% Function: unlock
%% Description: 用指定的锁解锁
%% Returns: ok
%% -----------------------------------------------------------------
unlock(Key, Lock) ->
	gen_server:cast(get_global(), {unlock, Key, lock_format(Lock)}).

%% -----------------------------------------------------------------
%% Function: lock_format
%% Description: 将锁格式化，保证不会发生冲突
%% Returns: {any(), pid()}
%% -----------------------------------------------------------------
lock_format(Lock) when is_atom(Lock) ->
	{Lock, self()};
lock_format({_, Pid} = Lock) when is_pid(Pid) ->
	Lock;
lock_format(Lock) when is_function(Lock) ->
	{_, Mod} = erlang:fun_info(Lock, module),
	{_, Name} = erlang:fun_info(Lock, name),
	{{Mod, Name}, self()};
lock_format(Lock) ->
	{Lock, self()}.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Ets, Election}) ->
	%监控节点Down事件
	ok = net_kernel:monitor_nodes(true),
	Ets = ets:new(Ets, [ordered_set, named_table]),
	{ok, #state{ets = Ets, election = Election, leader = wait,
		lock = ets:new(Ets, [ordered_set])}, ?HIBERNATE_TIMEOUT}.

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
handle_call(register, From, #state{leader = wait} = State) ->
	RegisterPid=register_master(self(), State),
	{noreply, State#state{leader = {wait, From, RegisterPid}}, ?HIBERNATE_TIMEOUT};
handle_call(register, _From, State) ->
	{reply, ok, State, ?HIBERNATE_TIMEOUT};
handle_call({lock, Key, Lock, LockTime} = Msg,
	From, #state{leader = Leader, lock = Ets, timer_ref = Ref} = State) ->
	if
		self() =:= Leader ->
			State1 = if
				is_reference(Ref) orelse LockTime =:= 0 ->
					State;
				true ->
					State#state{timer_ref = erlang:start_timer(
					?COLLATE_TIMEOUT, self(), lock_collate)}
			end,
			case lock(Ets, Key, Lock, LockTime,
				From, LockTime + zm_time:now_millisecond()) of
				wait ->
					{noreply, State1};
				R ->
					{reply, R, State1}
			end;
		true ->
			%防止合并时，信息错误的发到非主管理进程上
			call_send(Leader, From, Msg),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call(sync, _From, #state{ets = Ets} = State) ->
	{reply, ets:tab2list(Ets), State, ?HIBERNATE_TIMEOUT};

handle_call({Type, Req} = Msg, From, #state{ets = Ets, leader = Leader} = State) ->
	if
		self() =:= Leader ->
			Hash = handle_table(Ets, Type, Req),
			gen_server:abcast(nodes(), ?MODULE, {Type, Req, Hash, Leader}),
			{reply, ok, State, ?HIBERNATE_TIMEOUT};
		true ->
			%防止合并时，信息错误的发到非主管理进程上
			call_send(Leader, From, Msg),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({echo, R}, _From, State) ->
	{reply, R, State, ?HIBERNATE_TIMEOUT};

handle_call(Request, _From, State) ->
	{reply, {undefined_call, Request}, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_cast({unlock, Key, Lock} = Msg, #state{leader = Leader, lock = Ets} = State) ->
	if
		self() =:= Leader ->
			unlock(Ets, Key, Lock);
		true ->
			%防止合并时，信息错误的发到非主管理进程上
			gen_server:cast(Leader, Msg)
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({combine, L}, #state{ets = Ets, leader = Leader} = State) ->
	[ets:insert(Ets, E) || E <- L],
	gen_server:abcast(nodelist(Leader), ?MODULE, {reset, ets:tab2list(Ets), self()}),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({reset, L, Leader}, #state{ets = Ets} = State) ->
	list2ets(L, Ets),
	erlang:monitor(process, Leader),
	{noreply, State#state{leader = Leader}, ?HIBERNATE_TIMEOUT};

handle_cast({Type, Req, Hash, Leader}, #state{ets = Ets, leader = Leader} = State) ->
	%确保是主管理进程发来的消息
	case handle_table(Ets, Type, Req) of
		Hash ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		_ ->
			sync_leader(Ets, Leader),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({timeout, _Ref, lock_collate}, #state{lock = Ets} = State) ->
	case lock_collate(Ets, ets:first(Ets), zm_time:now_millisecond()) of
		collate ->
			{noreply, State#state{timer_ref = erlang:start_timer(
				?COLLATE_TIMEOUT, self(), lock_collate)}};
		_ ->
			{noreply, State#state{timer_ref = none}, ?HIBERNATE_TIMEOUT}
	end;

handle_info({global_name_conflict, {Type, ?MODULE}, NewLeader}, #state{ets = Ets, leader = Leader} = State) ->
	case check_leader(Type, Leader) of
		true ->
			gen_server:cast(NewLeader, {combine, ets:tab2list(Ets)}),
			{noreply, State#state{leader = NewLeader}, ?HIBERNATE_TIMEOUT};
		false ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;					

handle_info({'ETS-TRANSFER', Ets, RegisterPid, {register_ok, Leader}}, #state{ets = Ets, leader = {wait, From, RegisterPid}} = State) ->
	erlang:monitor(process, Leader),
	reply(From, ok),
	RegisterPid ! {stop, self()},
	{noreply, State#state{leader = Leader}, ?HIBERNATE_TIMEOUT};

handle_info({'ETS-TRANSFER', Ets, RegisterPid, {register_error, 'DOWN'}}, #state{ets = Ets, leader = {wait, From, RegisterPid}} = State) ->
	NewRegisterPid=register_master(self(), State),
	{noreply, State#state{leader = {wait, From, NewRegisterPid}}, ?HIBERNATE_TIMEOUT};
handle_info({'DOWN', _Ref, process, Leader, _Info}, #state{leader = Leader} = State) ->
	RegisterPid=register_master(self(), State),
	{noreply, State#state{leader = {wait, Leader, RegisterPid}}, ?HIBERNATE_TIMEOUT};

handle_info({nodedown, Node}, #state{ets = Ets, leader = Leader} = State) when is_pid(Leader) ->
	ets:select_delete(Ets, [{{{'_','$1'},'_'},[{'=:=','$1',Node}],[true]}]),
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
% 获得指定服务所在的节点和参数列表
get(Ets, Name, Key, L) ->
	case ets:prev(Ets, Key) of
		{Name, N} = K ->
			try ets:lookup_element(Ets, K, 2) of
				A -> get(Ets, Name, K, [{N, A} | L])
			catch
				_:_ ->
					get(Ets, Name, K, L)
			end;
		_ ->
			L
	end.

% 获得指定服务所在的节点
select(Ets, Name, Key, L) ->
	case ets:prev(Ets, Key) of
		{Name, _N} = K ->
			select(Ets, Name, K, [K | L]);
		_ ->
			L
	end.

% 异步注册master
register_master(FromPid, #state{ets = Ets, election = Election}) ->
	FromPid=self(),
	RegisterPid=spawn(fun() -> 
							  ok = net_kernel:monitor_nodes(true),
							  register_loop(FromPid, wait, Ets, Election)
					  end),
	ets:give_away(Ets, RegisterPid, register),
	RegisterPid.

% 异步注册循环
register_loop(FromPid, Pid, Ets, Election) ->
	receive
		{'ETS-TRANSFER', Ets, FromPid, register} -> 
			NewPid=register_master(FromPid, Ets, Election),
			ets:give_away(Ets, FromPid, {register_ok, NewPid}),
			register_loop(FromPid, NewPid, Ets, Election);
		{'DOWN', _Ref, process, _Pid, _Info} ->
			ets:give_away(Ets, FromPid, {register_error, 'DOWN'});
		{nodedown, Node} ->
			ets:select_delete(Ets, [{{{'_','$1'},'_'},[{'=:=','$1',Node}],[true]}]),
			register_loop(FromPid, Pid, Ets, Election);
		{stop, FromPid} ->
			true;
		_ ->
			register_loop(FromPid, Pid, Ets, Election)
	end.

% 如果自己可参与选举，则注册成管理进程，如果管理进程已注册，则同步数据并监听进程
register_master(Self, Ets, Election) ->
	timer:sleep(random:uniform(erlang:phash2(self(),
		?REGISTER_TIMEOUT) + 1) + ?REGISTER_TIMEOUT_DELAY),
	Node=node(),
	case zm_node:get(Node) of
		{_, Layers} ->
			GlobalName=get_global_name(Node, Layers),
			case global:whereis_name(GlobalName) of
				undefined when Election =:= true ->
					case global:register_name(GlobalName, Self, fun notify_name/3) of
						yes ->
							Self;
						no ->
							register_master(Self, Ets, Election)
					end;
				undefined ->
					register_master(Self, Ets, Election);
				Pid ->
					case sync_leader(Ets, Pid) of
						ok ->
							erlang:monitor(process, Pid),
							Pid;
						{error, _} ->
							register_master(Self, Ets, Election)
					end
			end;
		none ->
			whereis(?MODULE)
	end.

% 同步主管理进程
sync_leader(Ets, Leader) ->
	try 
		list2ets(gen_server:call(Leader, sync, ?SYNC_TIMEOUT), Ets),
		ok
	catch
		_:Reason ->
			{error, Reason}
	end.

% 获得全局的Leader进程
get_global() ->
	Node=node(),
	case zm_node:get(Node) of
		{_, Layers} ->
			GlobalName=get_global_name(Node, Layers),
			case global:whereis_name(GlobalName) of
				undefined ->
					timer:sleep(?SLEEP_TIMEOUT),
					case global:whereis_name(GlobalName) of
						undefined ->
							erlang:error(zm_service_global_error);
						Pid ->
							Pid
					end;
				Pid ->
					Pid
			end;
		none ->
			whereis(?MODULE)
	end.

% 获取全局名称
get_global_name(Node, L) ->
	case lists:member(slave, L) of
		true ->
			{slave, ?MODULE};
		false ->
			{Node, ?MODULE}
	end.

% 判断选举的leader是否是合法leader
check_leader(Type, Leader) ->
	case Type of
		slave ->
			{_, Layers}=zm_node:get(node(Leader)),
			lists:member(Type, Layers);
		Node when node() =:= Node ->
			true;
		_ ->
			false
	end.

nodelist(Leader) ->
	case zm_node:get(node(Leader)) of
		{_, Layer} ->
			case lists:member(slave, Layer) of
				true ->
					zm_node:active_nodes(slave);
				false ->
					[]
			end;
		none ->
			[]
	end.

% 返回调用者
reply({_, _} = From, Msg) ->
	z_lib:reply(From, Msg);
reply(_, _) ->
	ignore.

% 设置、删除服务
handle_table(Ets, set, KV) ->
	ets:insert(Ets, KV),
	ets2hash(Ets);
handle_table(Ets, sets, {Name, V}) ->
	[ets:insert(Ets, {K, V}) || K <- select(Ets, Name, {Name, <<>>}, [])],
	ets2hash(Ets);
handle_table(Ets, delete, Key) ->
	ets:delete(Ets, Key),
	ets2hash(Ets);
handle_table(Ets, deletes, Name) ->
	[ets:delete(Ets, Key) || Key <- select(Ets, Name, {Name, <<>>}, [])],
	ets2hash(Ets);
handle_table(Ets, _, _) ->
	ets2hash(Ets).

% 将列表放入清空后的ets表中
list2ets(L, Ets) ->
	ets:delete_all_objects(Ets),
	[ets:insert(Ets, E) || E <- L].

% 获得ets表的哈希值
ets2hash(Ets) ->
	erlang:phash2(ets:tab2list(Ets)).

% 通知进程冲突
notify_name(Name, P1, P2) ->
	{Min, Max} = if
		node(P1) < node(P2) ->
			{P1, P2};
		true ->
			{P2, P1}
	end,
	Max ! {global_name_conflict, Name, Min},
	Min.

% 发送Call数据
call_send(Dest, From, Msg) ->
	Msg1 = {'$gen_call', From, Msg},
	case catch erlang:send(Dest, Msg1, [noconnect]) of
		noconnect ->
			spawn(erlang, send, [Dest, Msg1]);
		Other ->
			Other
	end.

% 执行锁
lock(Ets, Key, Lock, LockTime, From, Expire) ->
	LK = {lock, Key},
	case erlang:get(LK) of
		undefined when LockTime > 0 ->
			ets:insert(Ets, {{Expire, From}, Key}),
			put(LK, queue:in({Expire, From, Lock}, queue:new())),
			ok;
		undefined ->
			{error, {invalid_lock, undefined}};
		Queue when LockTime > 0 ->
			ets:insert(Ets, {{Expire, From}, Key}),
			put(LK, queue:in({Expire, From, Lock}, Queue)),
			wait;
		Queue ->
			queue_lock(Ets, LK, Lock, Queue)
	end.

% 解锁
unlock(Ets, Key, Lock) ->
	LK = {lock, Key},
	case erlang:get(LK) of
		undefined ->
			{error, {invalid_lock, undefined}};
		Queue ->
			queue_lock(Ets, LK, Lock, Queue)
	end.

%队列弹出，继续下一个锁
queue_lock(Ets, LK, Lock, Queue) ->
	case queue:out(Queue) of
		{{value, {Expire, From, Lock}}, Q} ->
			ets:delete(Ets, {Expire, From}),
			case queue:peek(Q) of
				{value, {_Expire, From1, _}} ->
					put(LK, Q),
					z_lib:reply(From1, ok),
					ok;
				_ ->
					erase(LK),
					ok
			end;
		{{value, {_Expire, _From, Locked}}, _Q} ->
			{error, {invalid_lock, Locked}}
	end.

% 锁整理
lock_collate(_Ets, {Expire, _From}, Now) when Expire > Now ->
	collate;
lock_collate(Ets, {Expire, From} = Key, Now) ->
	LK = {lock, ets:lookup_element(Ets, Key, 2)},
	ets:delete(Ets, Key),
	case erlang:get(LK) of
		undefined ->
			error;
		Queue ->
			case queue:out(Queue) of
				{{value, {Expire, From, _Lock}}, Q} ->
					case queue:peek(Q) of
						{value, {Expire1, From1, _}} ->
							put(LK, Q),
							[z_lib:reply(From1, ok) || Expire1 > Now];
						_ ->
							erase(LK)
					end;
				_ ->
					put(LK, queue:filter(
						fun({E, F, _}) ->
							E =/= Expire orelse F =/= From
						end, Queue))
			end
	end,
	lock_collate(Ets, ets:next(Ets, Key), Now);
lock_collate(_Ets, _, _Now) ->
	none.
