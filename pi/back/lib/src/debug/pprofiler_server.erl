%% 
%% @doc 进程cpu性能剖析管理器
%%


-module(pprofiler_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-record(state, {self, profilers, index}).

-define(EOF_FLAG, "$eof").
-define(PROFILER_TIMEOUT, 3000).

-define(PROFILER_HIBERNATE_TIMEOUT, 5000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, new/0, set/2, report/1, close/1]).

%%
%%启动剖析管理器
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%构建一个剖析器
%%
new() ->
	gen_server:call(?MODULE, new).

%%
%%设置待剖析进程
%%
set(Profiler, [_|_] = Pids) ->
	gen_server:call(?MODULE, {set, Profiler, Pids}).

%%
%%获取指定剖析器的剖析报表
%%
report(Profiler) ->
	gen_server:call(?MODULE, {report, Profiler}).

%%
%%关闭指定的剖析器
%%
close(Profiler) ->
	gen_server:call(?MODULE, {close, Profiler}).

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
    {ok, #state{self = self(), profilers = sb_trees:empty(), index = sb_trees:empty()}}.


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
handle_call(new, From, #state{self = Self} = State) ->
	new_profiler(Self, From),
	{noreply, State, ?PROFILER_HIBERNATE_TIMEOUT};
handle_call({set, Profiler, Pids}, From, #state{self = Self, profilers = Profilers} = State) ->
	case sb_trees:get(Profiler, Profilers, none) of
		none ->
			{reply, {error, invalid_profiler}, State, ?PROFILER_HIBERNATE_TIMEOUT};
		_ ->
			Profiler ! {Self, set_process, From, Pids},
			{noreply, State, ?PROFILER_HIBERNATE_TIMEOUT}
	end;
handle_call({report, Profiler}, From, #state{self = Self, profilers = Profilers} = State) ->
	case sb_trees:get(Profiler, Profilers, none) of
		none ->
			{reply, {error, invalid_profiler}, State, ?PROFILER_HIBERNATE_TIMEOUT};
		_ ->
			Profiler ! {Self, report, From},
			{noreply, State, ?PROFILER_HIBERNATE_TIMEOUT}
	end;
handle_call({close, Profiler}, From, #state{self = Self} = State) ->
	Profiler ! {Self, stop, From},
	{noreply, State, ?PROFILER_HIBERNATE_TIMEOUT};
handle_call(_, _, State) ->
    {noreply, State, ?PROFILER_HIBERNATE_TIMEOUT}.


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
handle_cast(_, State) ->
    {noreply, State}.


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
handle_info({new_reply, Src, Reply}, #state{self = Self, profilers = Profilers, index = Index} = State) ->
	{NewProfilers, NewIndex}=case Reply of
		{ok, {Profiler, Eprof}} -> 
			z_lib:reply(Src, {ok, Profiler}),
			group_leader(Self, Profiler),
			group_leader(Self, Eprof),
			{sb_trees:insert(Profiler, "", Profilers), sb_trees:insert(Eprof, Profiler, Index)};
		_ ->
			{Profilers, Index}
	end,
	{noreply, State#state{profilers = NewProfilers, index = NewIndex}, ?PROFILER_HIBERNATE_TIMEOUT};
handle_info({set_process_reply, Src, _Profiler, Reply}, State) ->
	case Reply of
		{ok, _Pids} ->
			z_lib:reply(Src, ok);
		E ->
			z_lib:reply(Src, E)
	end,
	{noreply, State, ?PROFILER_HIBERNATE_TIMEOUT};
handle_info({io_request, Eprof, Proxy, Request}, #state{profilers = Profilers, index = Index} = State) ->
	Profiler=sb_trees:get(Eprof, Index),
	Result=case sb_trees:get(Profiler, Profilers) of
		{ok, _} ->
			"";
		{error, _} ->
			"";
		R ->
			R
	end,
	NewProfilers=try handle_output(Request) of
		Output ->
			sb_trees:update(Profiler, [Output|Result], Profilers)
	catch
		_:Reason ->
			sb_trees:update(Profiler, {error, {report_failed, Reason}}, Profilers)
	end,
	Eprof ! {io_reply, Proxy, ok},
	{noreply, State#state{profilers = NewProfilers}, ?PROFILER_HIBERNATE_TIMEOUT};
handle_info({report_ok, Src, Profiler}, #state{profilers = Profilers} = State) ->
	NewProfilers=case sb_trees:get(Profiler, Profilers, none) of
		none ->
			z_lib:reply(Src, {error, invalid_profiler}),
			Profilers;
		{error, _} = E ->
			z_lib:reply(Src, E),
			Profilers;
		Result ->
			z_lib:reply(Src, {ok, lists:flatten(lists:reverse(Result))}),
			sb_trees:update(Profiler, "", Profilers)
	end,
	{noreply, State#state{profilers = NewProfilers}, ?PROFILER_HIBERNATE_TIMEOUT};
handle_info({stop_ok, Src, Profiler}, #state{profilers = Profilers} = State) ->
	z_lib:reply(Src, ok),
	{noreply, State#state{profilers = sb_trees:delete_any(Profiler, Profilers)}, ?PROFILER_HIBERNATE_TIMEOUT};
handle_info(_, State) ->
    {noreply, State, ?PROFILER_HIBERNATE_TIMEOUT}.


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

new_profiler(From, Src) ->
	spawn(fun() ->
		Self=self(),
		case eprof:start() of
			{ok, Pid} ->
				From ! {new_reply, Src, {ok, {Self, Pid}}},
				profiler_loop(From, Self);
			{error, {already_started, Pid}} ->
				From ! {new_reply, Src, {ok, {Self, Pid}}},
				profiler_loop(From, Self);
			{error, _} = E ->
				From ! {new_reply, Src, E}
		end
	end).

profiler_loop(From, Profiler) ->
	receive
		{From, set_process, Src, Pids} ->
			From ! {set_process_reply, Src, Profiler, set_process(Pids)},
			profiler_loop(From, Profiler);
		{From, report, Src} ->
			eprof:stop_profiling(),
			eprof:analyze(),
			From ! {report_ok, Src, Profiler},
			profiler_loop(From, Profiler);
		{From, stop, Src} ->
			eprof:stop(),
			From ! {stop_ok, Src, Profiler}
	end.
	
set_process(Pids) ->
	case eprof:profile(Pids) of
		profiling ->
			{ok, Pids};
		{error, Reason} ->
			{error, {profile_failed, Reason}}
	end.

handle_output({put_chars, _, M, F, A}) ->
	apply(M, F, A);
handle_output(_) ->
	"".

