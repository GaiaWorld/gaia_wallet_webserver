%%% 进程本地管理
%%% 提供复杂名称和进程的对应表，负责初始化名称对应的进程


-module(zm_pid).

-description("pid local manager").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([start_link/0]).
-export([list/0, lookup/1, create/2, create/3]).

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
%% Function: start_link/0
%% Description: Starts a unique integer.
%% -----------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% -----------------------------------------------------------------
%%@doc  获得全部的进程
%% @spec  list() -> return()
%% where
%%  return() =  list()
%%@end
%% -----------------------------------------------------------------
list() ->
	ets:select(?MODULE, [{{'_','$1'}, [{is_pid,'$1'}], ['$_']}]).

%% -----------------------------------------------------------------
%%@doc  查询指定Name对应的进程
%% @spec  lookup(Name) -> return()
%% where
%%  return() =  undefined | Pid
%%@end
%% -----------------------------------------------------------------
lookup(Name) ->
	case ets:lookup(?MODULE, Name) of
		[{_, Pid}] -> Pid;
		[] -> undefined
	end.

%% -----------------------------------------------------------------
%%@doc  创建指定Name对应的进程
%% @spec  create(Name, Fun) -> return()
%% where
%%  return() =  {ok, Pid} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
create(Name, Fun) ->
	gen_server:call(?MODULE, {create, Name, Fun}).

%% -----------------------------------------------------------------
%%@doc  在指定节点上创建指定Name对应的进程
%% @spec  create(Node, Name, Fun) -> return()
%% where
%%  return() =  {ok, Pid} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
create(Node, Name, Fun) ->
	gen_server:call({?MODULE, Node}, {create, Name, Fun}).

%% -----------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State} |
%%		{ok, State, Timeout} |
%%		ignore |
%%		{stop, Reason}
%% -----------------------------------------------------------------
init(Ets) ->
	process_flag(trap_exit, true),
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
handle_call({create, Name, Fun}, From, #state{ets = Ets} = State) ->
	case ets:lookup(Ets, Name) of
		[{_, Pid}] ->
			{reply, {ok, Pid}, State, ?HIBERNATE_TIMEOUT};
		[] ->
			% 开新进程加载，本地保留回调
			Id = {create, Name},
			case erlang:get(Id) of
				L when is_list(L) ->
					put(Id, [From | L]);
				_ ->
					Parent = self(),
					put(Id, [From]),
					spawn(fun () -> async(Parent, Id, Fun) end)
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
handle_cast(_Msg, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

%% -----------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State} |
%%		{noreply, State, Timeout} |
%%		{stop, Reason, State}
%% -----------------------------------------------------------------
handle_info({{create, Name} = Id, R}, #state{ets = Ets} = State) ->
	R1 = case R of
		{ok, Pid} when is_pid(Pid) ->
			name_pid(Name, Pid, Ets);
		Pid when is_pid(Pid) ->
			name_pid(Name, Pid, Ets);
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
			zm_event:notify(?MODULE, pid_exit, {{Name, Pid}, Reason});
		_ ->
			other
	end,
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
% 异步执行MFA
async(Parent, Id, Fun) ->
	R = try
		{Id, Fun()}
	catch
		Error:Reason ->
			{Id, {error,
				{Error, Reason, erlang:get_stacktrace()}}}
	end,
	Parent ! R.

% 名字和进程的绑定
name_pid(Name, Pid, Ets) ->
	NP = {Name, Pid},
	try
		link(Pid),
		ets:insert(Ets, NP),
		ets:insert(Ets, {Pid, Name}),
		zm_event:notify(?MODULE, create_ok, NP),
		{ok, Pid}
	catch
		_:Reason ->
			E = {error, Reason, erlang:get_stacktrace()},
			zm_event:notify(?MODULE, link_error, {NP, E}),
			E
	end.
