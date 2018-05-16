%% @author Administrator
%% @doc 基于B树的变长查询管理器


-module(btqmv).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bts.hrl").
-include("btqmv.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/2, set/4, stop/1]).

%%
%% 启动基于B树的变长查询管理器
%% 
start_link(Args, Name) ->
	gen_server:start_link({local, list_to_atom(lists:concat([Name, "_", ?MODULE]))}, ?MODULE, Args, []).

%%
%%设置相关进程
%%
set(BTQMV, BTSMV, DatHandle, IdxHandle) when is_pid(BTQMV), is_pid(BTSMV) ->
	gen_server:call(BTQMV, {set, BTSMV, DatHandle, IdxHandle}).

%%
%% 关闭基于B树的变长查询管理器
%%
stop(BTQMV) when is_pid(BTQMV) ->
	gen_server:call(BTQMV, stop, 30000).

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
    {ok, #btqmv_context{self = self()}}.


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
handle_call({lookup, Key} = Req, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	case bs_variable:lookup(DatHandle, Key) of
		{?DATA_ASYN_WAIT, FA} ->
			IdxHandle=get_handle(?KEY_INDEX),
			case b_trees:lookup(IdxHandle, Key) of
				{ok, {DatPoint, Vsn, Time}} ->
					(bs_variable:new_data_reader(DatHandle, DatPoint, FA))(BTSMV, Self, From, lookup, ?VALUE_INDEX, Key, Vsn, Time, Req);
				{?ASYN_WAIT, Reader} ->
					Reader(BTSMV, Self, From, lookup, ?KEY_INDEX, {lookup, Key})
			end,
			{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call({prev, Key}, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:prev(IdxHandle, Key) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, prev, ?KEY_INDEX, {prev, Key}),
    		{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			deref(IdxHandle),
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call({next, Key}, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:next(IdxHandle, Key) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, next, ?KEY_INDEX, {next, Key}),
    		{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			deref(IdxHandle),
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call(frist, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:first(IdxHandle) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, first, ?KEY_INDEX, first),
    		{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			deref(IdxHandle),
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call(last, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:last(IdxHandle) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, last, ?KEY_INDEX, last),
    		{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			deref(IdxHandle),
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call({rank, Key}, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:rank(IdxHandle, Key) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, rank, ?KEY_INDEX, {rank, Key}),
    		{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			deref(IdxHandle),
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call({by_rank, Rank}, From, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:by_rank(IdxHandle, Rank) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, by_rank, ?KEY_INDEX, {by_rank, Rank}),
    		{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
		R ->
			deref(IdxHandle),
			{reply, R, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call({get_ref, Index}, _, State) ->
	case get({?INDEX, Index}) of
		{Handle, _} ->
			case get(b_trees:get_root(Handle)) of
				undefined ->
					{reply, {ok, ?DEREF}, State, ?BTQMV_HIBERNATE_TIMEOUT};
				Ref ->
					{reply, {ok, Ref}, State, ?BTQMV_HIBERNATE_TIMEOUT}
			end;
		undefined ->
			{reply, {error, {idx_not_exist, Index}}, State, ?BTQMV_HIBERNATE_TIMEOUT}
	end;
handle_call({set, BTSMV, DatHandle, IdxHandle}, _, State) ->
	put({?INDEX, ?KEY_INDEX}, {IdxHandle, ?INIT_VSN}),
	[put({?INDEX, ?VALUE_INDEX}, {DatHandle, ?INIT_VSN}) || is_tuple(DatHandle)],
	{reply, ok, State#btqmv_context{btsmv = BTSMV}, ?BTQMV_HIBERNATE_TIMEOUT};
handle_call(stop, {Bts, _}, State) ->
	unlink(Bts),
	{stop, normal, ok, State};
handle_call(_, _, State) ->
    {noreply, State, ?BTQMV_HIBERNATE_TIMEOUT}.


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
handle_cast({From, lookup, Key}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	case bs_variable:lookup(DatHandle, Key) of
		{?DATA_ASYN_WAIT, FA} ->
			IdxHandle=get_handle(?KEY_INDEX),
			case b_trees:lookup(IdxHandle, Key) of
				{ok, {DatPoint, Vsn, Time}} ->
					(bs_variable:new_data_reader(DatHandle, DatPoint, FA))(BTSMV, Self, From, lookup, ?VALUE_INDEX, Key, Vsn, Time, {lookup, Key});
				{?ASYN_WAIT, Reader} ->
					Reader(BTSMV, Self, From, lookup, ?KEY_INDEX, {lookup, Key})
			end;
		R ->
			reply(From, Self, lookup, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast({From, prev, Key}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:prev(IdxHandle, Key) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, prev, ?KEY_INDEX, {prev, Key});
		R ->
			deref(IdxHandle),
			reply(From, Self, prev, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast({From, next, Key}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:next(IdxHandle, Key) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, next, ?KEY_INDEX, {next, Key});
		R ->
			deref(IdxHandle),
			reply(From, Self, next, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast({From, first}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:first(IdxHandle) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, first, ?KEY_INDEX, first);
		R ->
			deref(IdxHandle),
			reply(From, Self, first, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast({From, last}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:last(IdxHandle) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, last, ?KEY_INDEX, last);
		R ->
			deref(IdxHandle),
			reply(From, Self, last, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast({From, rank, Key}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:rank(IdxHandle, Key) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, rank, ?KEY_INDEX, {rank, Key});
		R ->
			deref(IdxHandle),
			reply(From, Self, rank, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast({From, by_rank, Rank}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	IdxHandle=get_handle(?KEY_INDEX),
	case b_trees:by_rank(IdxHandle, Rank) of
		{?ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, by_rank, ?KEY_INDEX, {by_rank, Rank});
		R ->
			deref(IdxHandle),
			reply(From, Self, by_rank, R)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?BTQMV_HIBERNATE_TIMEOUT}.


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
handle_info({BTSMV, load_idx_ok, From, Type, Idx, IdxPoint}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	{IdxHandle, _}=get({?INDEX, Idx}),
	handle_wait_load_idx(From, Self, BTSMV, Type, Idx, IdxHandle, IdxPoint),
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_info({BTSMV, load_data_ok, From, Type, Idx, DatPoint}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	handle_wait_load_data(From, Self, BTSMV, Type, Idx, DatPoint),
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_info({Src, get_ref, FreeL}, #btqmv_context{self = Self} = State) ->
	Src ! {Self, get_ref_ok, get_refs(FreeL, [])},
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_info({Src, reset_handle, IdxReset}, #btqmv_context{self = Self} = State) ->
	Src ! {Self, reset_handle(IdxReset)},
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_info({BTSMV, load_idx_error, Reason, From, Type, Idx, Point}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	case get_handle(?KEY_INDEX) of
		{ok, IdxHandle} -> 
			deref(IdxHandle),
			reply(From, Self, Type, {error, {load_idx_error, {Reason, Idx, Point}}});
		E ->
			reply(From, Self, Type, E)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_info({BTSMV, load_data_error, Reason, From, Type, Idx, Point}, #btqmv_context{self = Self, btsmv = BTSMV} = State) ->
	case get_handle(?KEY_INDEX) of
		{ok, IdxHandle} -> 
			deref(IdxHandle),
			reply(From, Self, Type, {error, {load_data_error, {Reason, Idx, Point}}});
		E ->
			reply(From, Self, Type, E)
	end,
	{noreply, State, ?BTQMV_HIBERNATE_TIMEOUT};
handle_info({'EXIT', Pid, Why}, State) ->
	{stop, {process_exit, Pid, Why}, State};
handle_info(_, State) ->
    {noreply, State, ?BTQMV_HIBERNATE_TIMEOUT}.


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
	{Handle, _}=get({?INDEX, Idx}),
	RootPoint=b_trees:get_root(Handle),
	case get(RootPoint) of
		undefined ->
			put(RootPoint, ?INIT_REF);
		Ref ->
			put(RootPoint, Ref + 1)
	end,
	Handle.

read_data(lookup, From, Self, BTSMV, IdxHandle, IdxPoint, _, _, {ok, {DatPoint, Vsn, Time}}) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	case bs_variable:lookup(DatHandle, DatPoint) of
		{?DATA_ASYN_WAIT, Reader} ->
			Reader(BTSMV, Self, From, lookup, ?VALUE_INDEX, {lookup, IdxPoint, Vsn, Time});
		{ok, R} ->
			deref(IdxHandle),
			reply(From, Self, lookup, R);
		Reason ->
			deref(IdxHandle),
			reply(From, Self, lookup, {error, Reason})
	end;
read_data(Type, From, Self, _, IdxHandle, _, _, _, R) ->
	deref(IdxHandle),
	reply(From, Self, Type, R).

handle_wait_load_idx(?EMPTY, _, _, _, _, _, _) ->
	ok;
handle_wait_load_idx(From, Self, BTSMV, Type, Idx, IdxHandle, IdxPoint) ->
	case b_trees:continue(From, Idx, IdxHandle, IdxPoint) of
		{{?ASYN_WAIT, Reader}, Req, NextFrom, NextType} ->
			Reader(BTSMV, Self, From, Type, Idx, Req),
			handle_wait_load_idx(NextFrom, Self, BTSMV, NextType, Idx, IdxHandle, IdxPoint);
		{{ok, {Val, Vsn, Time}}, Req, NextFrom, NextType} ->
			continue_read(From, Self, BTSMV, IdxHandle, IdxPoint, Val, Vsn, Time, Req),
			handle_wait_load_idx(NextFrom, Self, BTSMV, NextType, Idx, IdxHandle, IdxPoint);
		{R, _, NextFrom, NextType} ->
			deref(IdxHandle),
			reply(From, Self, Type, R),
			handle_wait_load_idx(NextFrom, Self, BTSMV, NextType, Idx, IdxHandle, IdxPoint)
	end.

continue_read(From, Self, BTSMV, IdxHandle, IdxPoint, DatPoint, Vsn, Time, {lookup, Key}) ->
	{DatHandle, _}=get({?INDEX, ?VALUE_INDEX}),
	case bs_variable:lookup(DatHandle, Key) of
		{?DATA_ASYN_WAIT, FA} ->
			IdxHandle=get_handle(?KEY_INDEX),
			(bs_variable:new_data_reader(DatHandle, DatPoint, FA))(BTSMV, Self, From, lookup, ?VALUE_INDEX, Key, Vsn, Time, {lookup, IdxPoint});
		{ok, Value} ->
			deref(IdxHandle),
			reply(From, Self, lookup, {ok, Value, Vsn, Time})
	end;
continue_read(From, Self, _, IdxHandle, _, Key, Vsn, Time, Type) ->
	deref(IdxHandle),
	reply(From, Self, Type, {ok, {Key, Vsn, Time}}).

%%处理等待指定指针的数据加载的读请求
handle_wait_load_data(From, Self, BTSMV, Type, Idx, DatPoint) ->
	{IdxHandle, _}=get({?INDEX, ?KEY_INDEX}),
	{DatHandle, _}=get({?INDEX, Idx}),
	case bs_variable:continue(From, Idx, DatHandle, DatPoint) of
		{{ok, R}, {lookup, IdxPoint}, NextFrom, NextType} ->
			deref(IdxHandle),
			reply(From, Self, Type, {ok, R}),
			handle_wait_load_idx(NextFrom, Self, BTSMV, NextType, ?KEY_INDEX, IdxHandle, IdxPoint);
		{Reason, {lookup, IdxPoint}, NextFrom, NextType} ->
			deref(IdxHandle),
			reply(From, Self, Type, {error, Reason}),
			handle_wait_load_idx(NextFrom, Self, BTSMV, NextType, ?KEY_INDEX, IdxHandle, IdxPoint)
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

reset_handle({?KEY_INDEX, NewRootPoint}) ->
	case get({?INDEX, ?KEY_INDEX}) of
		{IdxHandle, Vsn} ->
			put({?INDEX, ?KEY_INDEX}, {b_trees:set_root(IdxHandle, NewRootPoint), Vsn + 1}),
			reset_handle_ok;
		undefined ->
			{reset_handle_error, {idx_not_exist, ?KEY_INDEX}}
	end.

reply({_, _} = From, _, _, Msg) ->
	z_lib:reply(From, Msg);
reply(From, Self, Type, Msg) ->
	From ! {Self, Type, Msg}.
