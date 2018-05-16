%%@doc 唯一整数模块，分成2种使用方式。
%%```
%%% 本地基于内存的唯一整数：每次启动开始累积。
%%%	使用时一般需要配合其他字段，比如节点和初始化时间来保证全局唯一性
%%%	如果需要从指定的数字开始累积，可在配置中设置初始值。
%%% 全局持久保存的唯一整数：提供全集群的唯一整数。
%%%	请求的参数包括表名和键名，提供本地缓冲。使用前必须先有表。
%%%	可在配置中设置初始值，每次缓冲的数量。
%%'''
%%@end


-module(zm_unique_int).

-description("unique integer").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([get/2, set/2, unset/2, get/3, set/4, unset/4]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(START, 1).
-define(CACHE_COUNT, 1000).

-define(TIMEOUT, 10).

-define(LOCK_TIME, 2000).
-define(READ_TIMEOUT, 1000).

-define(HIBERNATE_TIMEOUT, 3000).

-define(NIL, '$nil').

%%%=======================RECORD=======================
-record(state, {table, key, cur, max, count, error}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Function: get/2
%% Description: 获得指定键和数量的递增数字，建议键采用{Src, Type}的结构
%% Returns: {ok, integer()} | {error, Reason}
%% -----------------------------------------------------------------
get(Key, Count) ->
	Name = {?MODULE, Key},
	case zm_pid:lookup(Name) of
		Pid when is_pid(Pid) ->
			gen_server:call(Pid, {unique_int, Count});
		undefined ->
			Start = case zm_config:get(?MODULE, Key) of
				none ->
					?START;
				V ->
					V
			end,
			case zm_pid:create(Name, fun() ->
				gen_server:start_link(?MODULE, Start, []) end) of
				{ok, Pid} ->
					gen_server:call(Pid, {unique_int, Count});
				E ->
					E
			end
	end.

%% -----------------------------------------------------------------
%%@doc 设置指定的键配置(开始的位置)
%% @spec set(Key::atom(), Start::integer()) -> return()
%% where
%%      return() =  ok | {ok, {OldKey,OldStart}}
%%@end
%% -----------------------------------------------------------------
set(Key, Start) when is_integer(Start) ->
	zm_config:set(?MODULE, {Key, Start}).

%% -----------------------------------------------------------------
%%@doc 取消设置指定的键配置
%% @spec unset(Key::atom(), Start::integer()) -> return()
%% where
%%      return() = ok | {ok, {OldKey,OldStart}}
%%@end
%% -----------------------------------------------------------------
unset(Key, Start) ->
	zm_config:unset(?MODULE, {Key, Start}).

%% -----------------------------------------------------------------
%% Function: get/3
%% Description: 获得指定表名和键名及数量的递增数字
%% Returns: {ok, integer()} | {error, Reason}
%% -----------------------------------------------------------------
get(Table, Key, Count) when is_atom(Table) ->
	Name = {?MODULE, Table, Key},
	case zm_pid:lookup(Name) of
		Pid when is_pid(Pid) ->
			gen_server:call(Pid, {unique_int, Count});
		undefined ->
			{_, Start, CacheCount} = case zm_config:get(?MODULE, {Table, Key}) of
				none ->
					{none, ?START, ?CACHE_COUNT};
				V ->
					V
			end,
			case zm_pid:create(Name,fun() ->
				gen_server:start_link(?MODULE, {Table, Key, Start, CacheCount}, []) end) of
				{ok, Pid} ->
					gen_server:call(Pid, {unique_int, Count});
				E ->
					E
			end
	end.

%% -----------------------------------------------------------------
%%@doc 设置指定的键配置
%% @spec set(Table::atom(), Key::atom(), Start::integer(), Range::integer()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Table, Key, Start, CacheCount)
	when is_atom(Table), is_integer(Start), is_integer(CacheCount) ->
	zm_config:set(?MODULE, {{Table, Key}, Start, CacheCount}).

%% -----------------------------------------------------------------
%%@doc 取消设置指定的键配置
%% @spec unset(Table::atom(), Key, Start::integer(), Range::integer()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Table, Key, Start, CacheCount) ->
	zm_config:unset(?MODULE, {{Table, Key}, Start, CacheCount}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Table, Key, Start, CacheCount}) ->
	State = case get_cache_count(Table, Key, Start, CacheCount) of
		{ok, I} ->
			#state{table = Table, key = Key, cur = I, max = I + CacheCount,
				count = CacheCount, error = none};
		E ->
			#state{table = Table, key = Key, cur = Start, max = Start,
				count = CacheCount, error = {zm_time:now_second() + ?TIMEOUT, E}}
	end,
	{ok, State, ?HIBERNATE_TIMEOUT};
init(Start) ->
	{ok, #state{cur = Start, max = 0}, ?HIBERNATE_TIMEOUT}.

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
handle_call({unique_int, Count}, _From, #state{cur = I, max = 0} = State) ->
	{reply, {ok, I}, State#state{cur = I + Count}, ?HIBERNATE_TIMEOUT};

handle_call({unique_int, Count}, _From, #state{cur = I, max = Max} = State) when I < Max ->
	{reply, {ok, I}, State#state{cur = I + Count}, ?HIBERNATE_TIMEOUT};

handle_call({unique_int, Count}, _From, #state{table = Table, key = Key,
	cur = C, count = CacheCount, error = none} = State) ->
	CacheCount1 = if
		CacheCount > Count -> CacheCount;
		true -> Count + CacheCount
	end,
	case get_cache_count(Table, Key, C, CacheCount1) of
		{ok, I} ->
			{reply, {ok, I}, State#state{cur = I + Count, max = I + CacheCount1}, ?HIBERNATE_TIMEOUT};
		E ->
			{reply, E, State#state{error = {zm_time:now_second() + ?TIMEOUT, E}},
			?HIBERNATE_TIMEOUT}
	end;

handle_call({unique_int, Count}, _From, #state{table = Table, key = Key,
	cur = C, count = CacheCount, error = {Time, Error}} = State) ->
	case zm_time:now_second() of
		T when T > Time ->
			CacheCount1 = if
				CacheCount > Count -> CacheCount;
				true -> Count + CacheCount
			end,
			case get_cache_count(Table, Key, C, CacheCount1) of
				{ok, I} = R ->
					{reply, R, State#state{
						cur = I + Count, max = I + CacheCount1, error = none},
						?HIBERNATE_TIMEOUT};
				E ->
					{reply, E, State#state{
						error = {T + ?TIMEOUT, E}},
						?HIBERNATE_TIMEOUT}
			end;
		_ ->
			{reply, Error, State, ?HIBERNATE_TIMEOUT}
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
% 从表的指定键上获得范围内的整数
get_cache_count(Table, Key, Start, CacheCount) ->
	%TODO 以后改写成动态写方法
	case zm_db_client:read(Table, Key, ?MODULE, ?LOCK_TIME, ?READ_TIMEOUT) of
		{ok, ?NIL, _Vsn, _Time} ->
			case zm_db_client:write(Table, Key, Start + CacheCount, 1, ?MODULE, 0) of
				ok ->
					{ok, Start};
				E ->
					E
			end;
		{ok, Value, Vsn, _Time} ->
			case zm_db_client:write(Table, Key, Value + CacheCount, Vsn, ?MODULE, 0) of
				ok ->
					{ok, Value};
				E ->
					E
			end;
		E ->
			E
	end.
