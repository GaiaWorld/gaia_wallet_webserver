%%@doc 全局节点模块
%%```
%%% 节点的配置：{Node(节点名), [Layer](所在层列表)}
%%% 层的配置：{Layer, [Node](所在节点的排序列表), {Node}, [ActiveNode], {ActiveNode}}
%%'''
%%@end


-module(zm_node).

-description("node module").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0]).
-export([list/0, get/1, set/2, unset/2, sets/1, delete/1, layer/1, stop/1]).
-export([active_nodes/1, active_nodes/2, active_node/1, active_node/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================INLINE=======================
-compile({inline, [add_node/5, add_node_layer/4, delete_node/4, delete_node_layer/4, reset_node/1, reset_node_layer/5, is_active_node/2, active_node_list/2, active_node_list/2, nt_to_list/4]}).
-compile({inline_size, 32}).

%%%=======================RECORD=======================
-record(state, {node_ets, layer_ets, tree}).

%%%=======================DEFINE=======================
-define(NODES, 'zm_node:nodes').
-define(LAYERS, 'zm_node:layers').

-define(HIBERNATE_TIMEOUT, 3000).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: start_link/1
%% Description: Starts a event broadcast table
%% Returns: {ok, Pid}
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?NODES, ?LAYERS}, []).

%% -----------------------------------------------------------------
%%@doc list node
%% @spec list() -> list()
%%@end
%% -----------------------------------------------------------------
list() ->
	ets:tab2list(?NODES).

%% -----------------------------------------------------------------
%%@doc get node
%% @spec get(Node::atom()) -> return()
%% where
%%      return() = none | tuple()
%%@end
%% -----------------------------------------------------------------
get(Node) when is_atom(Node) ->
	case ets:lookup(?NODES, Node) of
		[T] -> T;
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc 设置节点信息，并将节点加入到集群节点访问权限列表中
%% @spec set(Node::atom(), Layers::list()) -> return()
%% where
%%      return() = ok | {}
%%@end
%% -----------------------------------------------------------------
set(Node, Layers) when is_atom(Node), is_list(Layers) ->
	gen_server:call(?MODULE, {set, Node, Layers}).

%% -----------------------------------------------------------------
%%@doc  unset handler
%% @spec  unset(Node::atom(), Layers::list()) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
unset(Node, _Layers) when is_atom(Node) ->
	gen_server:call(?MODULE, {delete, Node}).

%% -----------------------------------------------------------------
%%@doc  设置多个节点信息，并将节点加入到集群节点访问权限列表中
%% @spec  sets(NodeLayersList::list()) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
sets(NodeLayersList) when is_list(NodeLayersList) ->
	gen_server:call(?MODULE, {sets, NodeLayersList}).

%% -----------------------------------------------------------------
%%@doc delete Node
%% @spec delete(Node::atom()) -> return()
%% where
%%      return() = none | ok
%%@end
%% -----------------------------------------------------------------
delete(Node) when is_atom(Node) ->
	gen_server:call(?MODULE, {delete, Node}).

%% -----------------------------------------------------------------
%%@doc  获得指定层（或层列表）的排序节点元组
%% @spec  layer(Layer::atom()) -> return()
%% where
%%  return() =  none | {Layer, NodeList, NodeTuple, ActiveNodeList, ActiveNodeTuple}
%%@end
%% -----------------------------------------------------------------
layer(Layer) when is_atom(Layer) ->
	case ets:lookup(?LAYERS, Layer) of
		[T] -> T;
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc  获得指定层的可用节点（节点按名字已排序）
%% @spec  active_nodes(Layer::atom()) -> return()
%% where
%%  return() =  [Node | T] | {error, Reason}
%%@end
%% -----------------------------------------------------------------
active_nodes(Layer) when is_atom(Layer) ->
	try ets:lookup(?LAYERS, Layer) of
		[{_, _, _, NL, _}] ->
			NL;
		_ ->
			{error, "config not found"}
	catch
		_:_ ->
			{error, "zm_node error"}
	end;
active_nodes([Layer | T]) ->
	try ets:lookup(?LAYERS, Layer) of
		[{_, _, _, NL, _}] ->
			active_node_list(T, [NL]);
		_ ->
			{error, "config not found"}
	catch
		_:_ ->
			{error, "zm_node error"}
	end.

%% -----------------------------------------------------------------
%%@doc  获得指定层指定数量的随机偏移量的可用节点（节点按名字已排序）
%% @spec  active_nodes(Layer, N) -> return()
%% where
%%  return() =  [Node | T] | {incomplete, [Node | T]} |{error, Reason}
%%@end
%% -----------------------------------------------------------------
active_nodes(Layer, N) when is_integer(N), N > 0 ->
	try ets:lookup(?LAYERS, Layer) of
		[{_, _, Tuple, NL, NT}] ->
			case tuple_size(NT) of
				Size when Size > N ->
					{M, S, MS} = os:timestamp(),
					I = ((M + S + MS) rem Size) + 1,
					if
						I + N > Size ->
							nt_to_list(NT, 0, I + N - Size, nt_to_list(NT, I, Size, []));
						true ->
							nt_to_list(NT, I, I + N, [])
					end;
				Size when Size =:= N ->
					NL;
				_ ->
					case tuple_size(Tuple) of
						TS when TS >= N ->
							{incomplete, NL};
						_ ->
							NL
					end
			end;
		_ ->
			{error, "config not found"}
	catch
		_:_ ->
			{error, "zm_node error"}
	end.

%% -----------------------------------------------------------------
%%@doc 获得指定层的随机一个可用节点
%% @spec active_node(Layer::term()) -> return()
%% where
%%      return() = Node | {error, Reason}
%%@end
%% -----------------------------------------------------------------
active_node(Layer) ->
	try ets:lookup(?LAYERS, Layer) of
		[{_, _, _, _, {}}] ->
			{error, "node all down"};
		[{_, _, _, _, NT}] ->
			{M, S, MS} = os:timestamp(),
			element(((M + S + MS) rem tuple_size(NT)) + 1, NT);
		_ ->
			{error, "config not found"}
	catch
		_:_ ->
			{error, "zm_node error"}
	end.

%% -----------------------------------------------------------------
%%@doc 获得指定层的随机一个可用节点，跳过指定节点
%% @spec active_node(Layer::term(),Skip) -> return()
%% where
%%      return() = Node | {error, Reason}
%%@end
%% -----------------------------------------------------------------
active_node(Layer, Skip) ->
	try ets:lookup(?LAYERS, Layer) of
		[{_, _, _, _, {}}] ->
			{error, "node all down"};
		[{_, _, _, _, {Skip}}] ->
			{error, "node is skip"};
		[{_, _, _, _, NT}] ->
			{M, S, MS} = os:timestamp(),
			Size = tuple_size(NT),
			R = ((M + S + MS) rem Size) + 1,
			case element(R, NT) of
				Skip when R =:= Size ->
					element(1, NT);
				Skip ->
					element(R + 2, NT);
				Node ->
					Node
			end;
		_ ->
			{error, "config not found"}
	catch
		_:_ ->
			{error, "zm_node error"}
	end.

%% -----------------------------------------------------------------
%%@doc  关闭指定节点与集群的连接
%% @spec  stop(Node::atom()) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
stop(Node) when is_atom(Node) ->
	rpc:cast(Node, net_kernel, stop, []).
		
%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({NodeEts, LayerEts}) ->
	%监控节点UpDown事件
	ok = net_kernel:monitor_nodes(true),
	{ok, #state{node_ets = ets:new(NodeEts, [named_table]),
		layer_ets = ets:new(LayerEts, [named_table]),
		tree = sb_trees:from_orddict([{N, N} || N <- lists:sort(nodes())])},
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
handle_call({set, Node, Layers}, _From,
	#state{node_ets = NodeEts, layer_ets = LayerEts, tree = Tree} = State) ->
	add_node(Node, Layers, NodeEts, LayerEts, Tree),
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({sets, NodeLayersList}, _From,
	#state{node_ets = NodeEts, layer_ets = LayerEts, tree = Tree} = State) ->
	[add_node(Node, Layers, NodeEts, LayerEts, Tree) || {Node, Layers} <- NodeLayersList],
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({delete, Node}, _From,
	#state{node_ets = NodeEts, layer_ets = LayerEts, tree = Tree} = State) ->
	case ets:lookup(NodeEts, Node) of
		[{_, L}] ->
			ets:delete(NodeEts, Node),
			delete_node(LayerEts, Node, L, Tree),
			{reply, ok, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, none, State, ?HIBERNATE_TIMEOUT}
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
handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({nodedown, Node}, State) ->
	State1 = reset_node(State),
	zm_event:notify(?MODULE, nodedown, Node),
	{noreply, State1, ?HIBERNATE_TIMEOUT};

handle_info({nodeup, Node}, State) ->
	State1 = reset_node(State),
	zm_event:notify(?MODULE, nodeup, Node),
	{noreply, State1, ?HIBERNATE_TIMEOUT};

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
% 添加多个节点
add_node(Node, Layers, NodeEts, LayerEts, Tree) ->
	case ets:lookup(NodeEts, Node) of
		[{_, L}] ->
			ets:insert(NodeEts, {Node, Layers}),
			delete_node(LayerEts, Node, L, Tree);
		[] ->
			ets:insert(NodeEts, {Node, Layers})
	end,
	if
		Node =/= node() ->
			ok=net_kernel:allow([Node]),
			net_kernel:connect_node(Node);
		true ->
			ok=zm_service:register()
	end,
	[add_node_layer(LayerEts, Node, E, Tree) || E <- Layers].

% 添加层中节点
add_node_layer(Ets, Node, Layer, Tree) ->
	L1 = case ets:lookup(Ets, Layer) of
		[{_, L, _, _, _}] ->
			lists:usort([Node | L]);
		[] ->
			[Node]
	end,
	L2 = [N || N <- L1, is_active_node(N, Tree)],
	ets:insert(Ets, {Layer, L1, list_to_tuple(L1), L2, list_to_tuple(L2)}).

% 删除层中节点
delete_node(Ets, Node, Layers, Tree) ->
	[delete_node_layer(Ets, Node, E, Tree) || E <- Layers].

% 删除层中节点
delete_node_layer(Ets, Node, Layer, Tree) ->
	case ets:lookup(Ets, Layer) of
		[{_, L, _, _, _}] ->
			case lists:delete(Node, L) of
				[] ->
					ets:delete(Ets, Layer);
				L1 ->
					L2 = [N || N <- L1, is_active_node(N, Tree)],
					ets:insert(Ets, {Layer, L1, list_to_tuple(L1), L2, list_to_tuple(L2)})
			end;
		[] ->
			false
	end.

% 重置层中的活动节点
reset_node(#state{layer_ets = Ets} = State) ->
	Tree = sb_trees:from_orddict([{N, N} || N <- lists:sort(nodes())]),
	[reset_node_layer(Ets, Layer, L, T, Tree) || {Layer, L, T, _, _} <- ets:tab2list(Ets)],
	State#state{tree = Tree}.

% 重置层中的活动节点
reset_node_layer(Ets, Layer, L, T, Tree) ->
	LL = [N || N <- L, is_active_node(N, Tree)],
	ets:insert(Ets, {Layer, L, T, LL, list_to_tuple(LL)}).

% 判断节点是否活动
is_active_node(Node, _) when Node =:= node() ->
	true;
is_active_node(Node, Tree) ->
	sb_trees:is_defined(Node, Tree).

% 获取层列表对应的活动节点列表
active_node_list([H | T], L) ->
	case ets:lookup(?LAYERS, H) of
		[{_, _, _, NL, _}] ->
			active_node_list(T, [NL | L]);
		_ ->
			active_node_list(T, L)
	end;
active_node_list([], L) ->
	merge_node_list(L, []).

% 合并多个活动节点列表到一个列表
merge_node_list([H | T], L) ->
	merge_node_list(T, lists:reverse(z_lib:merge_order(H, L)));
merge_node_list([], L) ->
	L.

% 从节点元组中获取元素到列表
nt_to_list(T, I, J, L) when J > I ->
	nt_to_list(T, I, J - 1, [element(J, T) | L]);
nt_to_list(_T, _I, _J, L) ->
	L.
