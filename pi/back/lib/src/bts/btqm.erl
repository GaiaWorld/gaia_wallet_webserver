%%
%%@doc	基于B树的查询管理器
%%

-module(btqm).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("btqm.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/2, set/4, stop/1]).
-compile(export_all). %临时使用，调试完成后删除

%%
%% 启动基于B树的查询管理器
%% 
start_link(Args, Name) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, Args, []).

%%
%%设置相关进程
%%
set(BTQM, BTSM, DatHandle, IdxHandle) when is_pid(BTQM), is_pid(BTSM) ->
	gen_server:call(BTQM, {set, BTSM, DatHandle, IdxHandle}).

%%
%% 关闭基于B树的查询管理器
%%
stop(BTQM) when is_pid(BTQM) ->
	gen_server:call(BTQM, stop, 30000).

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
    {ok, #btqm_context{self = self()}}.


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
handle_call({lookup, Key} = Req, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(?KEY_INDEX) of
		{ok, Handle} ->
			case b_trees:lookup(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, lookup, ?KEY_INDEX, Req),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({prev, Key, Idx}, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:prev(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, prev, Idx, {prev, Key}),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({next, Key, Idx}, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:next(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, next, Idx, {next, Key}),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({frist, Idx}, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:first(Handle) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, first, Idx, first),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({last, Idx}, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:last(Handle) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, last, Idx, last),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({rank, Key, Idx}, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:rank(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, rank, Idx, {rank, Key}),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({by_rank, Rank, Idx}, From, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:by_rank(Handle, Rank) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, by_rank, Idx, {by_rank, Rank}),
		    		{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
				R ->
					deref(Handle),
					{reply, R, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		E ->
			{reply, E, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({get_ref, Index}, _, State) ->
	case get({?INDEX, Index}) of
		{Handle, _} ->
			case get(b_trees:get_root(Handle)) of
				undefined ->
					{reply, {ok, ?DEREF}, State, ?BTQM_HIBERNATE_TIMEOUT};
				Ref ->
					{reply, {ok, Ref}, State, ?BTQM_HIBERNATE_TIMEOUT}
			end;
		undefined ->
			{reply, {error, {idx_not_exist, Index}}, State, ?BTQM_HIBERNATE_TIMEOUT}
	end;
handle_call({set, BTSM, DatHandle, IdxHandle}, _, State) ->
	put({?INDEX, ?KEY_INDEX}, {DatHandle, ?INIT_VSN}),
	[put({?INDEX, ?VALUE_INDEX}, {IdxHandle, ?INIT_VSN}) || is_tuple(IdxHandle)],
	{reply, ok, State#btqm_context{btsm = BTSM}, ?BTQM_HIBERNATE_TIMEOUT};
handle_call(stop, {Bts, _}, State) ->
	unlink(Bts),
	{stop, normal, ok, State};
handle_call(_, _, State) ->
    {noreply, State, ?BTQM_HIBERNATE_TIMEOUT}.


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
handle_cast({From, lookup, Key}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(?KEY_INDEX) of
		{ok, Handle} ->
			case b_trees:lookup(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, lookup, ?KEY_INDEX, {lookup, Key});
				R ->
					deref(Handle),
					reply(From, Self, lookup, R)
			end;
		E ->
			reply(From, Self, lookup, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast({From, prev, Key, Idx}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:prev(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, prev, Idx, {prev, Key});
				R ->
					deref(Handle),
					reply(From, Self, prev, R)
			end;
		E ->
			reply(From, Self, prev, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast({From, next, Key, Idx}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:next(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, next, Idx, {next, Key});
				R ->
					deref(Handle),
					reply(From, Self, next, R)
			end;
		E ->
			reply(From, Self, next, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast({From, first, Idx}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:first(Handle) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, first, Idx, first);
				R ->
					deref(Handle),
					reply(From, Self, first, R)
			end;
		E ->
			reply(From, Self, first, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast({From, last, Idx}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:last(Handle) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, last, Idx, last);
				R ->
					deref(Handle),
					reply(From, Self, last, R)
			end;
		E ->
			reply(From, Self, last, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast({From, rank, Key, Idx}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:rank(Handle, Key) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, rank, Idx, {rank, Key});
				R ->
					deref(Handle),
					reply(From, Self, rank, R)
			end;
		E ->
			reply(From, Self, rank, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast({From, by_rank, Rank, Idx}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} ->
			case b_trees:by_rank(Handle, Rank) of
				{?ASYN_WAIT, Reader} ->
					Reader(BTSM, Self, From, by_rank, Idx, {by_rank, Rank});
				R ->
					deref(Handle),
					reply(From, Self, by_rank, R)
			end;
		E ->
			reply(From, Self, by_rank, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?BTQM_HIBERNATE_TIMEOUT}.


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
handle_info({BTSM, load_node_ok, From, Type, Idx, Point}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			handle_wait_load(From, Self, BTSM, Type, Idx, Handle, Point),
			{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
		undefined ->
			erlang:error({get_idx_error, {idx_not_exist, Idx}})
	end;
handle_info({Src, get_ref, FreeL}, #btqm_context{self = Self} = State) ->
	Src ! {Self, get_ref_ok, get_refs(FreeL, [])},
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_info({Src, reset_handle, Resets}, #btqm_context{self = Self} = State) ->
	Src ! {Self, reset_handle(Resets, [])},
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_info({BTSM, load_node_error, Reason, From, Type, Idx, Point}, #btqm_context{self = Self, btsm = BTSM} = State) ->
	case get_handle(Idx) of
		{ok, Handle} -> 
			deref(Handle),
			reply(From, Self, Type, {error, {load_node_error, {Reason, Idx, Point}}});
		E ->
			reply(From, Self, Type, E)
	end,
	{noreply, State, ?BTQM_HIBERNATE_TIMEOUT};
handle_info({'EXIT', Pid, Why}, State) ->
	{stop, {process_exit, Pid, Why}, State};
handle_info(_, State) ->
    {noreply, State, ?BTQM_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(normal, _) ->
	%TODO 关闭查询管理器时的处理...
	todo;
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

get_handle(Idx) ->
	case get({?INDEX, Idx}) of
		{Handle, _} ->
			RootPoint=b_trees:get_root(Handle),
			case get(RootPoint) of
				undefined ->
					put(RootPoint, ?INIT_REF);
				Ref ->
					put(RootPoint, Ref + 1)
			end,
			{ok, Handle};
		undefined ->
			{error, {idx_not_exist, Idx}}
	end.

handle_wait_load(?EMPTY, _, _, _, _, _, _) ->
	ok;
handle_wait_load(From, Self, BTSM, Type, Idx, Handle, Point) ->
	case b_trees:continue(From, Idx, Handle, Point) of
		{{?ASYN_WAIT, Reader}, Req, NextFrom, NextType} ->
			Reader(BTSM, Self, From, Type, Idx, Req),
			handle_wait_load(NextFrom, Self, BTSM, NextType, Idx, Handle, Point);
		{R, _, NextFrom, NextType} ->
			deref(Handle),
			reply(From, Self, Type, R), %立即返回,以加快读响应
			handle_wait_load(NextFrom, Self, BTSM, NextType, Idx, Handle, Point)
	end.

deref(Handle) ->
	RootPoint=b_trees:get_root(Handle),
	case get(RootPoint) of
		undefined ->
			?EMPTY;
		Ref ->
			put(RootPoint, Ref - 1)
	end.

get_refs([{Idx, RootPoint}|T], L) ->
	case get({Idx, RootPoint}) of
		undefined ->
			get_refs(T, [{Idx, RootPoint, ?DEREF}|L]);
		Ref ->
			get_refs(T, [{Idx, RootPoint, Ref}|L])
	end;
get_refs([], L) ->
	lists:reverse(L).

reset_handle([{Idx, NewRootPoint}|T], L) ->
	case get({?INDEX, Idx}) of
		{Handle, Vsn} ->
			reset_handle(T, [{Idx, Handle, Vsn, NewRootPoint}|L]);
		undefined ->
			{reset_handle_error, {idx_not_exist, Idx}}
	end;
reset_handle([], L) ->
	[put({?INDEX, Idx}, {b_trees:set_root(Handle, NewRootPoint), Vsn + 1}) || {Idx, Handle, Vsn, NewRootPoint} <- L],
	reset_handle_ok.

reply({_, _} = From, _, _, Msg) ->
	z_lib:reply(From, Msg);
reply(From, Self, Type, Msg) ->
	From ! {Self, Type, Msg}.
