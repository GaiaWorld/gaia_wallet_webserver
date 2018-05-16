%%@doc 进程分布管理
%%```
%%% 配置上，应设置指定类型的进程参数
%%% 全局查询进程，如果在本地没有查询到，则向该类型配置的层的所有节点进行查询，查询到则缓存
%%% 创建进程，是在该类型配置的层上选择负荷最小的节点进行创建
%%% 进程必须实现handle_call({burden}, _, _)的协议，以返回负荷
%%'''
%%@end


-module(zm_pid_dist).

-description("pid dist manager").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1]).
-export([list/0, get/1, set/5, unset/5, lookup/3, create/3]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(CONFIG, 'zm_pid_dist:config').

-define(COLLATE_TIME, 60*1000).
-define(HIBERNATE_TIMEOUT, 3000).

-define(LOCK_DELAY, 1000).
-define(BURDEN_TIMEOUT, 1000).

%%%=======================RECORD=======================
-record(state, {ets, collate_time, collate_pid}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts pid dist manager.
%% -----------------------------------------------------------------
start_link(CollateTime) when is_integer(CollateTime), CollateTime > 0 ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?MODULE, CollateTime}, []);
start_link(_) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?MODULE, ?COLLATE_TIME}, []).

%% -----------------------------------------------------------------
%%@doc  获得本地的全部进程
%% @spec  list() -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
list() ->
	ets:select(?MODULE, [{{'_','$1'}, [{is_pid,'$1'},{'=:=',{node,'$1'},node()}], ['$_']}]).

%% -----------------------------------------------------------------
%%@doc  获得指定类型的进程参数
%% @spec  get(Type) -> return()
%% where
%%  return() =  {Type, Layer, Divisor, RandNodeCount, MFA} | none
%%@end
%% -----------------------------------------------------------------
get(Type) ->
	zm_config:get(?CONFIG, Type).

%% -----------------------------------------------------------------
%%@doc 设置指定类型的进程参数
%%```
%%	Layer为所在的层，
%%	Divisor为将负荷除数，将负荷缩小到一定的区间内，
%%	RandNodeCount为随机节点的数量，表示每次创建时，至少用该数量的节点进行随机。
%%	MFA为进程启动M:F(A, {Type, Name})，必须返回{ok, Pid} | {error, Reason}
%%'''
%% @spec set(Type, Layer, Divisor, RandNodeCount, MFA) -> return()
%% where
%%  return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Type, Layer, Divisor, RandNodeCount, {_M, _F, _A} = MFA) ->
	zm_config:set(?CONFIG, {Type, Layer, Divisor, RandNodeCount, MFA}).

%% -----------------------------------------------------------------
%%@doc  取消设置指定类型的进程参数
%% @spec  unset(Type, Layer, Divisor, RandNodeCount, MFA) -> return()
%% where
%%  return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Type, Layer, Divisor, RandNodeCount, MFA) ->
	zm_config:unset(?CONFIG, {Type, Layer, Divisor, RandNodeCount, MFA}).

%% -----------------------------------------------------------------
%%@doc 查询指定Type的Name对应的进程，
%%```
%% Acton: local | global
%%'''
%% @spec lookup(Type, Name, Global::boolean()) -> return()
%% where
%%	return() = undefined | {ok, Pid} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
lookup(Type, Name, Global) ->
	TypeName = {Type, Name},
	case ets:lookup(?MODULE, TypeName) of
		[{_, Pid}] ->
			{ok, Pid};
		[] when Global =:= true ->
			case zm_config:get(?CONFIG, Type) of
				{_, Layer, _Divisor, _RandNodeCount, _MFA} ->
					lookup_global(TypeName, Layer);
				_ ->
					{error, undefined_type}
			end;
		_ ->
			undefined
	end.

%% -----------------------------------------------------------------
%%@doc  创建指定Type的Name对应的进程，
%% @spec  create(Type, Name, Timeout) -> return()
%% where
%%	return() = undefined | {ok, Pid} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
create(Type, Name, Timeout) ->
	TypeName = {Type, Name},
	case ets:lookup(?MODULE, TypeName) of
		[{_, Pid}] ->
			{ok, Pid};
		[] ->
			case zm_config:get(?CONFIG, Type) of
				{_, Layer, Divisor, RandNodeCount, MFA} ->
					create(Type, TypeName, Timeout,
						Layer, Divisor, RandNodeCount, MFA);
				_ ->
					{error, undefined_type}
			end;
		_ ->
			undefined
	end.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Ets, CollateTime}) ->
	zm_service:set(?MODULE, running),
	process_flag(trap_exit, true),
	ok = net_kernel:monitor_nodes(true),
	erlang:start_timer(CollateTime, self(), none),
	erlang:put(burden, sb_trees:empty()),
	{ok, #state{ets = ets:new(Ets, [named_table]), collate_time = CollateTime,
		collate_pid = spawn(fun zm_time:now_second/0)}, ?HIBERNATE_TIMEOUT}.

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
handle_call({lookup, Name, Nodes}, From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Name) of
		[{_, Pid}] ->
			{reply, {ok, Pid}, State, ?HIBERNATE_TIMEOUT};
		[] ->
			find(Name, Nodes, From),
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({create, Name, MFA}, From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Name) of
		[{_, Pid}] ->
			{reply, {ok, Pid}, State, ?HIBERNATE_TIMEOUT};
		[] ->
			% 开新进程加载，本地保留回调
			Id = {create, Name},
			case erlang:get(Id) of
				L when is_list(L) ->
					erlang:put(Id, [From | L]);
				_ ->
					Parent = self(),
					erlang:put(Id, [From]),
					spawn(fun () -> async(Parent, Id, MFA, Name) end)
			end,
			{noreply, State, ?HIBERNATE_TIMEOUT}
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
handle_cast({find, Name, From}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Name) of
		[{_, Pid}] ->
			gen_server:cast(From, {found, Name, Pid});
		[] ->
			gen_server:cast(From, {not_found, Name, node()})
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({found, Name, Pid}, #state{ets = Ets} = State) ->
	found(Name, Pid),
	ets:insert(Ets, {Name, Pid}),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({not_found, Name, Node}, State) ->
	not_found(Name, Node),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({burden, L, Node}, State) ->
	% 接收到其他节点发过来的负荷事件，保存到本地
	BurdenTree = erlang:get(burden),
	erlang:put(burden, sb_trees:from_dict(
		[{{Type, Node}, Burden} || {Type, Burden} <- L], BurdenTree)),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({cache, Name, Pid}, #state{ets = Ets} = State) ->
	ets:insert(Ets, {Name, Pid}),
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_cast({pid_exit, {{Name, _Pid}, _Reason} = Msg}, #state{ets = Ets} = State) ->
	% 接收到其他节点发过来的进程结束事件
	ets:delete(Ets, Name),
	zm_event:notify(?MODULE, pid_exit, Msg),
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
handle_info({timeout, _Ref, none}, #state{
	collate_time = CollateTime, collate_pid = Pid} = State) ->
	erlang:start_timer(CollateTime, self(), none),
	case is_process_alive(Pid) of
		true ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		_ ->
			{noreply, State#state{
				collate_pid = spawn(fun collate/0)}, ?HIBERNATE_TIMEOUT}
	end;

handle_info({{create, Name} = Id, R}, #state{ets = Ets} = State) ->
	R1 = case R of
		{ok, Pid} when is_pid(Pid) ->
			NP = {Name, Pid},
			try
				link(Pid),
				ets:insert(Ets, NP),
				ets:insert(Ets, {Pid, Name}),
				zm_event:notify(?MODULE, create_ok, NP),
				R
			catch
				_:Reason ->
					E = {error, Reason, erlang:get_stacktrace()},
					zm_event:notify(?MODULE, link_error, {NP, E}),
					E
			end;
		_ ->
			zm_event:notify(?MODULE, create_error, {Name, R}),
			R
	end,
	[z_lib:reply(From, R1) || From <- erlang:erase(Id)],
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({'EXIT', Pid, Reason}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Pid) of
		[{_, Name}] ->
			ets:delete(Ets, Pid),
			ets:delete(Ets, Name),
			Node = node(),
			L = [N || {N, running} <- zm_service:get(?MODULE), N =/= Node],
			Msg = {{Name, Pid}, Reason},
			% 向全部的服务节点广播进程结束
			gen_server:abcast(L, ?MODULE, {pid_exit, Msg}),
			zm_event:notify(?MODULE, pid_exit, Msg);
		_ ->
			other
	end,
	{noreply, State, ?HIBERNATE_TIMEOUT};

handle_info({nodedown, Node}, #state{ets = Ets} = State) ->
	% 删除在当掉节点上的进程
	case ets:select(Ets, [{{'_','$1'}, [{is_pid,'$1'},{'=:=',{node,'$1'},Node}], ['$_']}]) of
		[] ->
			{noreply, State, ?HIBERNATE_TIMEOUT};
		L ->
			ets:select_delete(Ets, [{{'_','$1'}, [{is_pid,'$1'},{'=:=',{node,'$1'},Node}], [true]}]),
			zm_event:notify(?MODULE, nodedown, L),
			{noreply, State, ?HIBERNATE_TIMEOUT}
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
% 异步执行MFA
async(Parent, Id, {M, F, A}, Name) ->
	process_flag(trap_exit, true),
	R = try
		M:F(A, Name)
	catch
		Error:Reason ->
			{error, {Error, Reason, erlang:get_stacktrace()}}
	end,
	Parent ! {Id, R}.

% 寻找指定名称的进程，向节点列表上发送广播
lookup_global(Name, Layer) ->
	case zm_node:active_nodes(Layer) of
		L when is_list(L) ->
			case lists:delete(node(), get_active_node(L)) of
				[] ->
					undefined;
				LL ->
					try
						gen_server:call(?MODULE, {lookup, Name, LL})
					catch
						Error:Reason ->
							{error, {Error, Reason, erlang:get_stacktrace()}}
					end
			end;
		E ->
			E
	end.

% 创建进程
create(Type, TypeName, Timeout, Layer, Divisor, RandNodeCount, MFA) ->
	try
		% 调用全局锁
		ok = zm_service:lock(TypeName, ?MODULE,
			Timeout, Timeout + ?LOCK_DELAY),
		% 先全局查询
		R = case lookup_global(TypeName, Layer) of
			undefined ->
				% 没找到则进行创建
				create_global(Type, TypeName,
					Layer, Divisor, RandNodeCount, MFA);
			E ->
				E
		end,
		zm_service:unlock(TypeName, ?MODULE),
		R
	catch
		Error1:Reason1 ->
			{error, {Error1, Reason1, erlang:get_stacktrace()}}
	end.

% 在该类型指定的层上，选择负荷最小的节点，创建进程
create_global(Type, Name, Layer, Divisor, RandNodeCount, MFA) ->
	{_, BurdenTree} = z_lib:pid_dictionary(?MODULE, burden),
	case zm_node:active_nodes(Layer) of
		L when is_list(L) ->
			N = select_node(get_active_node(L), Type,
				BurdenTree, Divisor, RandNodeCount, []),
			try gen_server:call({?MODULE, N}, {create, Name, MFA}) of
				{ok, Pid} = R when node(Pid) =/= node() ->
					% 非本地创建的Pid，需要cache
					gen_server:cast(?MODULE, {cache, Name, Pid}),
					R;
				E ->
					E
			catch
				Error:Reason ->
					{error, {Error, Reason, erlang:get_stacktrace()}}
			end;
		E ->
			E
	end.

% 获得正在运行的活动节点
get_active_node(NL) ->
	get_active_node(NL, zm_service:get(?MODULE), []).

% 获得正在运行的活动节点
get_active_node([N1 | T1], [{N2, _} | _] = T2, L) when N1 < N2 ->
	get_active_node(T1, T2, L);
get_active_node([N1 | _] = T1, [{N2, _} | T2], L) when N1 > N2 ->
	get_active_node(T1, T2, L);
get_active_node([N | T1], [{N, S} | T2], L) ->
	if
		S =:= running ->
			get_active_node(T1, T2, [N | L]);
		true ->
			get_active_node(T1, T2, L)
	end;
get_active_node(_, _, L) ->
	L.

% 选择负荷最小的随机节点
select_node([H | T], Type, Tree, Divisor, Count, L) ->
	Burden = sb_trees:get({Type, H}, Tree, 0),
	select_node(T, Type, Tree, Divisor, Count, [{Burden div Divisor, H} | L]);
select_node([], _, _, _, Count, L) ->
	select_node(lists:sort(L), Count, 0, []).

% 选择节点
select_node([{Burden, N} | T], Count, Limit, L) when Count > 0, Burden =< Limit ->
	select_node(T, Count - 1, Limit, [N | L]);
select_node([{Burden, N} | T], Count, _Limit, L) when Count > 0 ->
	select_node(T, Count - 1, Burden, [N | L]);
select_node([{Burden, N} | T], _Count, Limit, L) when Burden =< Limit ->
	select_node(T, 0, Limit, [N | L]);
select_node(_, _, _, L) ->
	erlang:hd(z_lib:shuffle(L)).

% 寻找指定名称的进程，向节点列表上发送广播
find(Name, Nodes, From) ->
	FN = {find, Name},
	case erlang:get(FN) of
		{NL, FL} ->
			erlang:put(FN, {NL, [From | FL]});
		_ ->
			erlang:put(FN, {Nodes, [From]}),
			gen_server:abcast(Nodes, ?MODULE, {find, Name, self()})
	end.

% 找到指定的进程
found(Name, Pid) ->
	case erlang:erase({find, Name}) of
		{_, FL} ->
			[z_lib:reply(From, {ok, Pid}) || From <- FL];
		_ ->
			ok
	end.

% 在指定的节点上没有找到进程
not_found(Name, Node) ->
	FN = {find, Name},
	case erlang:get(FN) of
		{NL, FL} ->
			case lists:delete(Node, NL) of
				[] ->
					erlang:erase(FN),
					[z_lib:reply(From, undefined) || From <- FL];
				L ->
					erlang:put(FN, {L, FL})
			end;
		_ ->
			ok
	end.

% 整理方法，获取节点上的进程负荷
collate() ->
	case list() of
		[] ->
			none;
		L ->
			Burden = get_burden(lists:sort(L), []),
			gen_server:abcast([N || {N, running} <- zm_service:get(?MODULE)],
				?MODULE, {burden, Burden, node()})
	end.

% 累加所有类型的进程负荷
get_burden([{{Type, _}, Pid} | T], [{Type, Burden} | L]) ->
	get_burden(T, [{Type, get_burden(Pid) + Burden} | L]);
get_burden([{{Type, _}, Pid} | T], L) ->
	get_burden(T, [{Type, get_burden(Pid)} | L]);
get_burden([], L) ->
	L.

% 获取进程的负荷
get_burden(Pid) ->
	try
		gen_server:call(Pid, {burden}, ?BURDEN_TIMEOUT)
	catch
		_:_ ->
		0
	end.
