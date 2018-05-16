%%@doc http会话表
%%@end


-module(zm_http_session_table).

-description("http session table").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0, start_link/1]).
-export([get/1, create/1]).

%% -----------------------------------------------------------------
%% gen_server callbacks
%% -----------------------------------------------------------------
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=======================DEFINE=======================
-define(TIMEOUT, 30*60*1000).

-define(HIBERNATE_TIMEOUT, 3000).

%%%=======================RECORD=======================
-record(state, {ets, timeout}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 启动一个会话表
%%  @spec start_link() -> return()
%%  where
%%      return() = {ok, Pid}
%%@end
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?MODULE, ?TIMEOUT}, []).

%% -----------------------------------------------------------------
%%@doc 启动一个会话表
%%  @spec start_link(Timeout::integer()) -> return()
%%  where
%%      return() = {ok, Pid}
%%@end
%% -----------------------------------------------------------------
start_link(Timeout) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, {?MODULE, Timeout}, []).

%% -----------------------------------------------------------------
%%@doc 获得会话Pid或ID
%% @spec get(ID_Pid::term()) -> return()
%% where
%%      return() = none | {ID, Pid} | {Pid, ID}
%%@end
%% -----------------------------------------------------------------
get(ID_Pid) ->
	case ets:lookup(?MODULE, ID_Pid) of
		[T] -> T;
		[] -> none
	end.

%% -----------------------------------------------------------------
%%@doc 创建指定地址的会话
%% @spec create(Addr) -> return()
%% where
%%      return() = {Id, Pid}
%%@end
%% -----------------------------------------------------------------
create(Addr) ->
	gen_server:call(?MODULE, {create, Addr}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init({Ets, Timeout}) ->
	process_flag(trap_exit, true),
	{ok, #state{ets = ets:new(Ets, [named_table]), timeout = Timeout},
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
handle_call({create, Addr}, _From, #state{ets = Ets, timeout = Timeout} = State) ->
	IP = z_lib:get_value(Addr, ip, {0, 0, 0, 0}),
	Port = z_lib:get_value(Addr, port, 0),
	{ok, Pid} = zm_http_session:start_link(Addr, Timeout),
	ID = zm_http:new_scid(IP, Port, Pid),
	ets:insert(Ets, {ID, Pid}),
	ets:insert(Ets, {Pid, ID}),
	{reply, {ID, Pid}, State, ?HIBERNATE_TIMEOUT};

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
handle_info({'EXIT', Pid, _Reason}, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Pid) of
		[{_, ID}] ->
			ets:delete(Ets, Pid),
			ets:delete(Ets, ID),
			{noreply, State, ?HIBERNATE_TIMEOUT};
		[] ->
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
