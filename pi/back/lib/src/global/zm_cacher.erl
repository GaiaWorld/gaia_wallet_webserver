%%@doc 缓冲服务模块
%%```
%%% 根据类型，提供本地缓冲，可配置该类型的缓冲数据的加载MFA
%%% 如果没有找到缓冲数据，可通过本模块进程异步加载数据并缓存
%%'''
%%@end


-module(zm_cacher).

-description("cache service").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/1]).
-export([list/0, get/1, set/5, unset/5, delete/1]).
-export([read/2, load/2, cache/3, clear/2]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {ets, collate_time}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc Starts a cacher.
%%```
%%  CollateTime 整理时间
%%  Pid::pid()
%%'''
%% @spec start_link(CollateTime::integer()) -> return()
%% where
%%      return() = {ok, Pid}
%%@end
%% -----------------------------------------------------------------
start_link(CollateTime) when is_integer(CollateTime), CollateTime > 0 ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, CollateTime, []).

%% -----------------------------------------------------------------
%%@doc 列出所有的缓存信息.
%% @spec list() -> tuplelist()
%% where
%%      tuplelist() = [{Type::atom(),Cache,MFA::tuple()}]
%%@end
%% -----------------------------------------------------------------
list() ->
	ets:tab2list(?MODULE).

%% -----------------------------------------------------------------
%%@doc 获得指定缓存中的信息
%% @spec get(Type::term()) -> return()
%% where
%%      return() = none | tuple()
%%@end
%% -----------------------------------------------------------------
get(Type) ->
	try ets:lookup(?MODULE, Type) of
		[T] -> T;
		[] -> none
	catch
		error:badarg -> none
	end.

%% -----------------------------------------------------------------
%%@doc  设置指定类型的缓冲的大小、时间和是否读取时间更新及处理模块
%% @spec  set(Type, Sizeout::integer(), Timeout::integer(), ReadTimeUpdate::boolean(), MFA::tuple()) -> return()
%% where
%%  return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Type, Sizeout, Timeout, ReadTimeUpdate, MFA) when is_integer(Timeout), Timeout > 0 ->
	gen_server:call(?MODULE, {set, Type, Sizeout, Timeout, ReadTimeUpdate, MFA}).

%% -----------------------------------------------------------------
%%@doc  取消设置指定类型的缓冲的大小和时间及处理模块
%% @spec  unset(Type, Sizeout::any(), Timeout::any(), ReadTimeUpdate::any(), MFA::any()) -> return()
%% where
%%  return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Type, _, _, _, _) ->
	gen_server:call(?MODULE, {delete, Type}).

%% -----------------------------------------------------------------
%%@doc 删除指定类型的缓冲.
%% @spec delete(Type::term()) -> return()
%% where
%%      return() = none | ok
%%@end
%% -----------------------------------------------------------------
delete(Type) ->
	gen_server:call(?MODULE, {delete, Type}).

%% -----------------------------------------------------------------
%%@doc  读取指定类型的数据
%% @spec  read(Type, Key) -> return()
%% where
%%  return() =  {ok, Value} | {none, MFA} | none | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read(Type, Key) ->
	try ets:lookup(?MODULE, Type) of
		[{_, Cache, MFA}] ->
			case zm_cache:get(Cache, Key) of
				{ok, _} = R -> R;
				none -> {none, MFA}
			end;
		[] ->
			none
	catch
		error:badarg ->
			{error, cacher_not_started}
	end.

%% -----------------------------------------------------------------
%%@doc  加载指定类型的数据，如果无数据，则等待zm_cacher进程异步加载并缓冲数据
%% @spec  load(Type, Key) -> return()
%% where
%%  return() =  {ok, Value} | none | {error, Reason}
%%@end
%% -----------------------------------------------------------------
load(Type, Key) ->
	try ets:lookup(?MODULE, Type) of
		[{_, Cache, MFA}] ->
			case zm_cache:get(Cache, Key) of
				{ok, _} = R ->
					R;
				none ->
					gen_server:call(?MODULE,
						{load, Type, Key, Cache, MFA}, infinity)
			end;
		[] ->
			none
	catch
		error:badarg ->
			{error, cacher_not_started}
	end.

%% -----------------------------------------------------------------
%%@doc 缓冲指定类型的数据
%%```
%%  Type:指定类型
%%  K:指定主键
%%  V:设置的值
%%'''
%% @spec cache(Type::term(), K::term(), V) -> boolean()
%%@end
%% -----------------------------------------------------------------
cache(Type, K, V) ->
	case ets:lookup(?MODULE, Type) of
		[{_, Cache, _}] ->
			case zm_cache:read_time_update(Cache) of
				true ->
					zm_cache:set(Cache, K, V);
				_ ->
					gen_server:call(?MODULE, {cache, Type, K, V})
			end;
		[] ->
			false
	end.

%% -----------------------------------------------------------------
%%@doc 清除指定类型的指定键缓冲
%% @spec clear(Type::term(), Key::term()) -> boolean()
%%@end
%% -----------------------------------------------------------------
clear(Type, Key) ->
	case ets:lookup(?MODULE, Type) of
		[{_, Cache, _}] ->
			case zm_cache:read_time_update(Cache) of
				true ->
					zm_cache:delete(Cache, Key);
				_ ->
					gen_server:call(?MODULE, {clear, Type, Key})
			end;
		[] ->
			false
	end.

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(CollateTime) ->
	erlang:start_timer(CollateTime, self(), none),
	{ok, #state{ets = ets:new(?MODULE, [set, protected, named_table]),
		collate_time = CollateTime}, ?HIBERNATE_TIMEOUT}.

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
handle_call({set, Type, Sizeout, Timeout, ReadTimeUpdate, MFA}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Type) of
		[{_, Cache, MFA}] ->
			case zm_cache:read_time_update(Cache) of
				ReadTimeUpdate ->
					zm_cache:reset(Cache, Sizeout, Timeout);
				_ ->
					zm_cache:destroy(Cache),
					ets:insert(Ets, {Type, zm_cache:new(
						Sizeout, Timeout, ReadTimeUpdate), MFA})
			end;
		[{_, Cache, _}] ->
			zm_cache:destroy(Cache),
			ets:insert(Ets, {Type, zm_cache:new(Sizeout, Timeout, ReadTimeUpdate), MFA});
		[] ->
			ets:insert(Ets, {Type, zm_cache:new(Sizeout, Timeout, ReadTimeUpdate), MFA})
	end,
	{reply, ok, State, ?HIBERNATE_TIMEOUT};

handle_call({delete, Type}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Type) of
		[{_, Cache, _}] ->
			zm_cache:destroy(Cache),
			{reply, ok, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, none, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({load, Type, Key, Cache, MFA}, From, State) ->
	try zm_cache:get(Cache, Key) of
		{ok, _} = R ->
			{reply, R, State, ?HIBERNATE_TIMEOUT};
		none ->
			% 开新进程加载，本地保留回调
			TypeKey = {load, Type, Key},
			case erlang:get(TypeKey) of
				L when is_list(L) ->
					put(TypeKey, [From | L]);
				_ ->
					Parent = self(),
					put(TypeKey, [From]),
					spawn(fun() -> async(Parent, Type, Key, MFA) end)
			end,
			{noreply, State, ?HIBERNATE_TIMEOUT}
	catch
		error:badarg ->
			{reply, {error, cacher_deleted}, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({cache, Type, K, V}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Type) of
		[{_, Cache, _}] ->
			{reply, zm_cache:set(Cache, K, V), State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, false, State, ?HIBERNATE_TIMEOUT}
	end;

handle_call({clear, Type, Key}, _From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Type) of
		[{_, Cache, _}] ->
			{reply, zm_cache:delete(Cache, Key), State, ?HIBERNATE_TIMEOUT};
		[] ->
			{reply, false, State, ?HIBERNATE_TIMEOUT}
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
handle_info({cache, Type, K, V}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Type) of
		[{_, Cache, _}] ->
			zm_cache:set(Cache, K, V),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({clear, Type, Key}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Type) of
		[{_, Cache, _}] ->
			zm_cache:delete(Cache, Key),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[] ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({{load, Type, K} = TypeKey, R}, #state{ets = Ets} = State) ->
	[z_lib:reply(From, R) || From <- erlang:erase(TypeKey)],
	case R of
		{ok, V} ->
			case ets:lookup(Ets, Type) of
				[{_, Cache, _}] ->
					zm_cache:set(Cache, K, V),
					{noreply, State, ?HIBERNATE_TIMEOUT};
				[] ->
					{noreply, State, ?HIBERNATE_TIMEOUT}
			end;
		_ ->
			{noreply, State, ?HIBERNATE_TIMEOUT}
	end;

handle_info({timeout, _Ref, none}, #state{ets = Ets, collate_time = CollateTime} = State) ->
	[zm_cache:collate(Cache) || {_, Cache, _} <- ets:tab2list(Ets)],
	erlang:start_timer(CollateTime, self(), none),
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
% 异步执行加载的MFA
async(Parent, Type, Key, {M, F, A}) ->
	R = try
		{{load, Type, Key}, M:F(A, Type, Key)}
	catch
		Error:Reason ->
			{{load, Type, Key}, {error,
				{Error, Reason, erlang:get_stacktrace()}}}
	end,
	Parent ! R;
async(Parent, Type, Key, MFA) ->
	Parent ! {{load, Type, Key}, {error, MFA}}.
