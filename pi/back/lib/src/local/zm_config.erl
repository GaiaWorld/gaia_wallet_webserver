%%@doc 配置模块
%%@end


-module(zm_config).

-description("config table").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0]).
-export([list/0, member/2, get/1, get/2, get/3, range/3, first/1, last/1, next/2, prev/2]).
-export([set/2, set/3, unset/2, unset/3, delete/1, delete/2]).

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
%% -----------------------------------------------------------------
%% Starts a config table server
%%  start_link() -> return()
%% where
%%      return() = {ok, Pid}
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% -----------------------------------------------------------------
%%@doc 列出所有配置表的名称
%% @spec list() -> [{atom()}]
%%@end
%% -----------------------------------------------------------------
list() ->
	ets:tab2list(?MODULE).

%% -----------------------------------------------------------------
%%@doc 判断指定表中是否存在KEY
%%```
%%  Table:表名(源内唯一)
%%'''
%% @spec member(Table::atom(), Key::atome()) -> boolean()
%%@end
%% -----------------------------------------------------------------
member(Table, Key) ->
	try
		ets:member(Table, Key)
	catch
		error:badarg -> false
	end.

%% -----------------------------------------------------------------
%%@doc 获得指定表中的全部元素
%%```
%%  Table:表名(源内唯一)
%%'''
%% @spec get(Table::atom()) -> [tuple()]
%%@end
%% -----------------------------------------------------------------
get(Table) ->
	try
		ets:tab2list(Table)
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc 获得表中指定键的值
%% @spec get(Table::atom(),Key::atom()) -> return()
%% where
%%      return() = none | tuple()
%%@end
%% -----------------------------------------------------------------
get(Table, Key) ->
	try ets:lookup(Table, Key) of
		[T] -> T;
		[] -> none
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc 获得表中指定键的值
%% @spec get(Table::atom(), Key::term(), Default::term()) -> return()
%% where
%%      return() = term()
%%@end
%% -----------------------------------------------------------------
get(Table, Key, Default) ->
	try ets:lookup(Table, Key) of
		[T] -> T;
		[] -> Default
	catch
		error:badarg -> Default
	end.

%% -----------------------------------------------------------------
%%@doc 获得表中指定键范围的值
%%```
%%  闭区间，包含StartKey和EndKey，必须是ordered_set
%%'''
%% @spec range(Table::atom(),StartKey::atom(),EndKey::atom()) -> return()
%% where
%%      return() = none | [tuple()]
%%@end
%% -----------------------------------------------------------------
range(Table, StartKey, EndKey) when StartKey < EndKey ->
	try
		range1(Table, ets:prev(Table, EndKey), StartKey, ets:lookup(Table, EndKey))
	catch
		error:badarg -> none
	end;
range(Table, StartKey, EndKey) when StartKey > EndKey ->
	try
		range2(Table, ets:next(Table, EndKey), StartKey, ets:lookup(Table, EndKey))
	catch
		error:badarg -> none
	end;
range(Table, Key, Key) ->
	ets:lookup(Table, Key).

%% -----------------------------------------------------------------
%%@doc 获得表中第一个键
%%```
%%'''
%% @spec first(Table::atom()) -> return()
%% where
%%      return() = none | '$end_of_table' | any()
%%@end
%% -----------------------------------------------------------------
first(Table) ->
	try
		ets:first(Table)
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc 获得表中最后一个键
%%```
%%'''
%% @spec last(Table::atom()) -> return()
%% where
%%      return() = none | '$end_of_table' | any()
%%@end
%% -----------------------------------------------------------------
last(Table) ->
	try
		ets:last(Table)
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc 获得表中键后面的键
%%```
%%'''
%% @spec next(Table::atom(), Key::any()) -> return()
%% where
%%      return() = none | '$end_of_table' | any()
%%@end
%% -----------------------------------------------------------------
next(Table, Key) ->
	try
		ets:next(Table, Key)
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc 获得表中键前面的键
%%```
%%'''
%% @spec prev(Table::atom(), Key::any()) -> return()
%% where
%%      return() = none | '$end_of_table' | any()
%%@end
%% -----------------------------------------------------------------
prev(Table, Key) ->
	try
		ets:prev(Table, Key)
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc 设置表中的指定元素
%% @spec set(Table::atom(),El::tuple()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Table, El) when is_tuple(El), tuple_size(El) > 0 ->
	gen_server:call(?MODULE, {set, Table, El}).

%% -----------------------------------------------------------------
%%@doc 批量设置表
%% @spec set(Table::atom(),L::list(),Opt::option()) -> ok
%% where
%%      option() = ordered_set | keypos2 | named_table
%%@end
%% -----------------------------------------------------------------
set(Table, L, Opt) when is_list(L) ->
	gen_server:call(?MODULE, {set, Table, L, Opt}).

%% -----------------------------------------------------------------
%%@doc 取消设置表中的指定元素
%% @spec unset(Table::atom(),El::tuple()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Table, El) when is_tuple(El), tuple_size(El) > 0 ->
	gen_server:call(?MODULE, {unset, Table, El}).

%% -----------------------------------------------------------------
%%@doc 取消批量设置表
%% @spec unset(Table::atom(),L::list(),Opt) -> ok
%%@end
%% -----------------------------------------------------------------
unset(Table, L, Opt) when is_list(L) ->
	gen_server:call(?MODULE, {unset, Table, L, Opt}).

%% -----------------------------------------------------------------
%%@doc 删除指定的表
%% @spec delete(Table::atom()) -> boolean()
%%@end
%% -----------------------------------------------------------------
delete(Table) ->
	gen_server:call(?MODULE, {delete, Table}).

%% -----------------------------------------------------------------
%%@doc 删除表中指定键的条目
%% @spec delete(Table::atom(),Key::atom()) -> return()
%% where
%%      return() = none | {ok, Old}
%%@end
%% -----------------------------------------------------------------
delete(Table, Key) ->
	gen_server:call(?MODULE, {delete, Table, Key}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Ets) ->
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
handle_call({set, Table, El}, _From, #state{ets = Ets} = State) when is_tuple(El) ->
	case ets:lookup(Ets, Table) of
		[{Table}] ->
			case ets:lookup(Table, element(1, El)) of
				[Old] ->
					ets:insert(Table, El),
					{reply, {ok, Old}, State, ?HIBERNATE_TIMEOUT};
				[] ->
					ets:insert(Table, El),
					{reply, ok, State, ?HIBERNATE_TIMEOUT}
			end;
		[] ->
			ets:new(Table, [set, protected, named_table]),
			ets:insert(Table, El),
			ets:insert(Ets, {Table}),
			{reply, ok, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({set, Table, L, Opt}, _From, #state{ets = Ets} = State) when is_list(L) ->
	case ets:lookup(Ets, Table) of
		[{Table}] ->
			ets:insert(Table, L);
		[] ->
			Options = case Opt of
				ordered_set ->
					[ordered_set, named_table];
				keypos2 ->
					[{keypos, 2}, named_table];
				Opt when is_list(Opt) ->
					[named_table | Opt];
				_ ->
					[named_table]
			end,
			ets:new(Table, Options),
			ets:insert(Table, L),
			ets:insert(Ets, {Table})
	end,
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({unset, Table, El}, _From, #state{ets = Ets} = State) when is_tuple(El) ->
	case ets:lookup(Ets, Table) of
		[{Table}] ->
			ets:delete(Table, element(1, El));
		[] ->
			 ok
	end,
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({unset, Table, L, _Opt}, _From, #state{ets = Ets} = State) when is_list(L) ->
	case ets:lookup(Ets, Table) of
		[{Table}] ->
			[ets:delete(Table, element(1, El)) || El <- L, is_tuple(El), tuple_size(El) > 0],
			case ets:info(Table, size) of
				0 ->
					ets:delete(Table),
					ets:delete(Ets, Table);
				_ ->
					ok
			end;
		[] ->
			ok
	end,
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({delete, Table}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{Table}] ->
			ets:delete(Table),
			ets:delete(Ets, Table),
			{reply, true, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, false, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({delete, Table, Key}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Table) of
		[{Table}] ->
			case ets:lookup(Table, Key) of
				[Old] ->
					ets:delete(Table, Key),
					{reply, {ok, Old}, State, ?HIBERNATE_TIMEOUT};
				[] ->
					{reply, none, State, ?HIBERNATE_TIMEOUT}
			end;
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
% 获得表中指定键范围的值
range1(_Table, '$end_of_table', _EndKey, L) ->
	L;
range1(Table, Key, EndKey, L) when Key >= EndKey ->
	case ets:lookup(Table, Key) of
		[T] ->
			range1(Table, ets:prev(Table, Key), EndKey, [T | L]);
		[] ->
			range1(Table, ets:prev(Table, Key), EndKey, L)
	end;
range1(_Table, _Key, _EndKey, L) ->
	L.

% 获得表中指定键范围的值
range2(_Table, '$end_of_table', _EndKey, L) ->
	L;
range2(Table, Key, EndKey, L) when Key =< EndKey ->
	case ets:lookup(Table, Key) of
		[T] ->
			range2(Table, ets:next(Table, Key), EndKey, [T | L]);
		[] ->
			range2(Table, ets:next(Table, Key), EndKey, L)
	end;
range2(_Table, _Key, _EndKey, L) ->
	L.
