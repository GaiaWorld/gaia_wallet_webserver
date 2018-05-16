%% 
%% @doc 数据缩放服务器
%%


-module(scale_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Include files
%% ====================================================================
-define(NEXT_TABLE, next_table).

-define(FINISH_I, finish1).
-define(FINISH_II, finish2).
-define(FINISH, finish).

-define(INSERT_INI, insert_ini).
-define(INSERT_ACK_INI, insert_ack_ini).

-define(INSERT_ACC, insert_acc).
-define(INSERT_ACK_ACC, insert_ack_acc).

%%进程上下文
-record(state, {
				table,
				sync_time = 0,
				tail_sync_time = 0,		
				sync_stack,
				insert_timeout,
				insert_ack_timeout,
				call_timeout
}).
-define(SCALE_INITIATOR_HIBERNATE_TIMEOUT, 60000).

-define(DB_LAYER, db).
-define(RANGE, 16#ffff).
-define(SERVICE, zm_service).
-define(DB_SERVER, zm_db_server).

-define(INSERT_TIMEOUT, insert_timeout).
-define(DEFAULT_INSERT_TIMEOUT, 600000).
-define(INSERT_ACK_TIMEOUT, insert_ack_timeout).
-define(DEFAULT_INSERT_ACK_TIMEOUT, 600000).
-define(CALL_TIMEOUT, call_timeout).
-define(DEFAULT_CALL_TIMEOUT, 10000).

-define(READ_TIMEOUT, 5000).
-define(RPC_TIMEOUT, 5000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, expansion/2, shrink/2, stop/1]).

%%
%%启动缩放服务器
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%开始放大指定表
%%
expansion(_ExpNodes, []) ->
	zm_event:notify(?MODULE, expansion, {expansion_ok, {[], node()}});
expansion(ExpNodes, Tables) ->
	{_, NL, _, _, _} = zm_node:layer(?DB_LAYER),
	gen_server:cast(?MODULE, {expansion, list_to_tuple(lists:sort(NL ++ ExpNodes)), NL, ExpNodes, Tables}).

%%
%%开始缩小指定表
%%
shrink(_ShrNodes, []) ->
	zm_event:notify(?MODULE, shrink, {shrink_ok, {[], node()}});
shrink(ShrNodes, Tables) ->
	{_, NL, _, _, _} = zm_node:layer(?DB_LAYER),
	gen_server:cast(?MODULE, {shrink, list_to_tuple(lists:sort(NL -- ShrNodes)), NL, ShrNodes, Tables}).

%%
%%关闭缩放服务器
%%
stop(Reason) ->
	gen_server:call(?MODULE, {stop, Reason}).

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
init(Args) ->
    {ok, reset(#state{
					  insert_timeout = z_lib:get_value(Args, ?INSERT_TIMEOUT, ?DEFAULT_INSERT_TIMEOUT), 
					  insert_ack_timeout = z_lib:get_value(Args, ?INSERT_ACK_TIMEOUT, ?DEFAULT_INSERT_ACK_TIMEOUT), 
					  call_timeout = z_lib:get_value(Args, ?CALL_TIMEOUT, ?DEFAULT_CALL_TIMEOUT)})}.


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
handle_call({stop, Reason}, _, State) ->
	{stop, Reason, ok, reset(State)};
handle_call(_, _, State) ->
    {noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}.


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
handle_cast({expansion, NT, NL, ExpNodes, Tables}, State) ->
	{noreply, start_expansion(Tables, NT, NL, ExpNodes, State#state{sync_time = z_lib:now_millisecond()}), ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
handle_cast({shrink, NT, NL, ShrNodes, Tables}, State) ->
	{noreply, start_shrink(Tables, NT, NL, ShrNodes, State#state{sync_time = z_lib:now_millisecond()}), ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
handle_cast(_, State) ->
    {noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}.


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
handle_info({?INSERT_INI, Remote, Table, RL}, State) ->
	case batch_insert(RL, Table) of
		ok ->
			{?MODULE, Remote} ! {?INSERT_ACK_INI, node(), ok};
		E ->
			{?MODULE, Remote} ! {?INSERT_ACK_INI, node(), E}
	end,
	{noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?INSERT_ACK_INI, Node, Reply}, #state{table = Table, sync_stack = [{?INSERT_ACK_INI, Node, Len, Buffers}|T]} = State) ->
	case Reply of
		ok ->
			zm_event:notify(?MODULE, expansion, {realloc_ok, {Table, node(), Node, Len}}),
			{noreply, realloc(Buffers, State#state{sync_stack = T}), ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
		E ->
			io:format("!!!!!!E:~p~n", [E]),
			zm_event:notify(?MODULE, expansion, {error, Table, {realloc_failed, node(), Node, Len, E}}),
			{noreply, reset(State), ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}
	end;
handle_info({?INSERT_ACC, Remote, Table, RL}, #state{table = Table} = State) ->
	case filter_insert(RL, Table) of
		ok ->
			{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), ok};
		E ->
			{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), E}
	end,
	{noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?INSERT_ACC, Remote, Table, RL}, #state{table = LastTable} = State) ->
	case unlock_write(LastTable) of
		ok ->
			case lock_write(Table) of
				ok ->
					case filter_insert(RL, Table) of
						ok ->
							{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), ok};
						E ->
							{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), E}
					end,
					{noreply, State#state{table = Table}, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
				E ->
					case unlock_write(Table) of
						ok ->
							{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), E};
						E ->
							{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), {error, Table, {unlock_now_table_failed, E}}}
					end,
					{noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}
			end;
		E ->
			{?MODULE, Remote} ! {?INSERT_ACK_ACC, node(), {error, LastTable, {unlock_last_table_failed, E}}},
			{noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}
	end;
handle_info({?INSERT_ACK_ACC, Node, Reply}, #state{table = Table, sync_stack = [{?INSERT_ACK_ACC, Node, Len, Buffers}|T]} = State) ->
	case Reply of
		ok ->
			zm_event:notify(?MODULE, shrink, {over_write_ok, {Table, node(), Node, Len}}),
			{noreply, over_write(Buffers, State#state{sync_stack = T}), ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
		E ->
			io:format("!!!!!!E:~p~n", [E]),
			zm_event:notify(?MODULE, shrink, {error, Table, {over_write_failed, node(), Node, Len, E}}),
			{noreply, reset(State), ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}
	end;
handle_info({?FINISH_I, _}, #state{table = Table} = State) ->
	unlock_write(Table),
	{noreply, State#state{table = undefined}, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State, ?SCALE_INITIATOR_HIBERNATE_TIMEOUT}.


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

reset(State) ->
	State#state{sync_time = 0, tail_sync_time = 0, sync_stack = []}.

is_table(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, running, _}] ->
			true;
		_ ->
			false
	end.

start_expansion([Table|Tables], NT, NL, ExpNodes, State) ->
	case is_table(Table) of
		true ->
			case zm_db_client:count(Table) of
				{ok, Count} ->
					Limit=compare_lib:get_split_num(Count),
					case zm_db_client:iterate(Table, ascending, false) of
						{ok, Iterator} ->
							case iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Limit, State) of
								{ok, NewIterator, Buffers, []} ->
									realloc(Buffers, State#state{table = Table, 
																 sync_stack = [{?INSERT_INI, NewIterator, NT, NL, ExpNodes, Limit}, {?FINISH_I, NT, NL, ExpNodes}, {?NEXT_TABLE, NT, NL, ExpNodes, Tables}]});
								{ok, _, _, Error} ->
									io:format("!!!!!!Table:~p, Error:~p~n", [Table, length(Error)]),
									zm_event:notify(?MODULE, expansion, {error, Table, {start_i_failed, node(), {read_failed, Error}}}),
									reset(State);
								E ->
									io:format("!!!!!!Table:~p, E:~p~n", [Table, E]),
									zm_event:notify(?MODULE, expansion, {error, Table, {start_i_failed, node(), E}}),
									reset(State)
							end;
						E ->
							zm_event:notify(?MODULE, expansion, {error, Table, {start_i_failed, node(), E}}),
							reset(State)
					end;
				{estimate, _} ->
					zm_event:notify(?MODULE, expansion, {error, Table, {start_i_failed, node(), incomplete}}),
					reset(State);
				E ->
					zm_event:notify(?MODULE, expansion, {error, Table, {start_i_failed, node(), E}}),
					reset(State)
			end;
		false ->
			zm_event:notify(?MODULE, expansion, {error, Table, {start_i_failed, node(), table_not_exist}}),
			reset(State)
	end.

start_shrink([Table|Tables], NT, NL, ShrNodes, State) ->
	case is_table(Table) of
		true ->
			Pid=get_table_pid(Table),
			Limit=compare_lib:get_split_num(zm_db_table:size(Pid)),
			case zm_db_table:iterate(Pid, ascending) of
				{ok, Iterator} ->
					case iterator_table_by_shr(Iterator, Pid, Table, NT, NL, ShrNodes, Limit, State) of
						{ok, NewIterator, Buffers, []} ->
							over_write(Buffers, State#state{table = Table, 
														 sync_stack = [{?INSERT_ACC, NewIterator, Pid, NT, NL, ShrNodes, Limit}, ?FINISH_I, {?NEXT_TABLE, NT, NL, ShrNodes, Tables}]});
						{ok, _, _, Error} ->
							io:format("!!!!!!Table:~p, Error:~p~n", [Table, length(Error)]),
							zm_event:notify(?MODULE, shrink, {error, Table, {start_i_failed, node(), {read_failed, Error}}}),
							reset(State);
						E ->
							io:format("!!!!!!Table:~p, E:~p~n", [Table, E]),
							zm_event:notify(?MODULE, shrink, {error, Table, {start_i_failed, node(), E}}),
							reset(State)
					end;
				E ->
					zm_event:notify(?MODULE, shrink, {error, Table, {start_i_failed, node(), E}}),
					reset(State)
			end;
		false ->
			zm_event:notify(?MODULE, shrink, {error, Table, {start_i_failed, node(), table_not_exist}}),
			reset(State)
	end.

dht_node_by_exp(NT, _NL, ExpNodes, Duplication, Key) ->
	[Node || Node <- zm_db:dht_node(Key, ?RANGE, Duplication, NT), lists:member(Node, ExpNodes)].

fill_buffer([Node|T], Record, Buffers) ->
	case lists:keyfind(Node, 1, Buffers) of
		{_, L} ->
			fill_buffer(T, Record, lists:keyreplace(Node, 1, Buffers, {Node, [Record|L]}));
		false ->
			fill_buffer(T, Record, lists:keystore(Node, 1, Buffers, {Node, [Record]}))
	end;
fill_buffer([], _, Buffers) ->
	Buffers.

iterator_table_by_exp(over, _, _, _, _, _, _) ->
	{ok, over, [], []};
iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Limit, #state{sync_time = SyncTime, tail_sync_time = TailSyncTime}) ->
	case zm_config:get(zm_db, Table) of
		{_, Duplication, _} ->
			case TailSyncTime of
				0 ->
					iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Duplication, 0, SyncTime, Limit, [], []);
				_ ->
					iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Duplication, SyncTime, TailSyncTime, Limit, [], [])
			end;
		_ ->
			{ok, Iterator, [], []}
	end.

iterator_table_by_exp(Iterator, _, _, _, _, _, _, _, 0, Buffers, Error) ->
	{ok, Iterator, Buffers, Error};
iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Duplication, StartTime, TailTime, Limit, Buffers, Error) ->
	case zm_db_client:iterate_next(Iterator) of
		{ok, {Key, _, Time}, NewIterator} when Time >= StartTime, Time =< TailTime ->
			case zm_db_client:read(Table, Key, ?READ_TIMEOUT) of
				{ok, Value, Vsn, _} ->
					iterator_table_by_exp(NewIterator, Table, NT, NL, ExpNodes, Duplication, StartTime, TailTime, Limit - 1, 
								   fill_buffer(dht_node_by_exp(NT, NL, ExpNodes, Duplication, Key), {Key, Value, Vsn, Time}, Buffers), Error);
				{error, Reason} ->
					iterator_table_by_exp(NewIterator, Table, NT, NL, ExpNodes, Duplication, StartTime, TailTime, Limit, Buffers, [{Key, Reason}|Error])
			end;
		{ok, _, NewIterator} ->
			iterator_table_by_exp(NewIterator, Table, NT, NL, ExpNodes, Duplication, StartTime, TailTime, Limit, Buffers, Error);
		over ->
			{ok, over, Buffers, Error};
		E ->
			E
	end.

lock_write(undefined) ->
	ok;
lock_write(Table) ->
	lock_write(?SERVICE:get(Table), Table).

lock_write([{{Table, Node}, {Opts, running}}|T], Table) ->
	case rpc:call(Node, ?SERVICE, set, [Table, {Opts, only_read}, true], ?RPC_TIMEOUT) of
		ok ->
			case rpc:call(Node, ?DB_SERVER, lock_write, [Table], ?RPC_TIMEOUT) of
				ok ->
					lock_write(T, Table);
				{error, _} = E ->
					E;
				{badrpc, Reason} ->
					{error, Reason}
			end;
		{badrpc, Reason} ->
			{error, Reason}
	end;
lock_write([{{Table, _}, {_, only_read}}|T], Table) ->
	lock_write(T, Table);
lock_write([{{Table, Node}, {_, Status}}|_], Table) ->
	{error, {invalid_service_status, Node, Status}};
lock_write([_|T], Table) ->
	lock_write(T, Table);
lock_write([], _) ->
	ok.

unlock_write(undefined) ->
	ok;
unlock_write(Table) ->
	unlock_write(?SERVICE:get(Table), Table).

unlock_write([{{Table, Node}, {Opts, only_read}}|T], Table) ->
	case rpc:call(Node, ?SERVICE, set, [Table, {Opts, running}, true], ?RPC_TIMEOUT) of
		ok ->
			case rpc:call(Node, ?DB_SERVER, unlock_write, [Table], ?RPC_TIMEOUT) of
				ok ->
					unlock_write(T, Table);
				{error, _} = E ->
					E;
				{badrpc, Reason} ->
					{error, Reason}
			end;
		{badrpc, Reason} ->
			{error, Reason}
	end;
unlock_write([{{Table, _}, {_, running}}|T], Table) ->
	unlock_write(T, Table);
unlock_write([{{Table, Node}, {_, Status}}|_], Table) ->
	{error, {invalid_service_status, Node, Status}};
unlock_write([_|T], Table) ->
	unlock_write(T, Table);
unlock_write([], _) ->
	ok.

realloc([{Node, RL}|T], #state{table = Table, sync_stack = Stack} = State) ->
	rpc:abcast([Node], ?MODULE, {?INSERT_INI, node(), Table, RL}),
	State#state{sync_stack = [{?INSERT_ACK_INI, Node, length(RL), T}|Stack]};
realloc([], State) ->
	realloc_next(State).

realloc_next(#state{table = Table, sync_stack = [{?INSERT_INI, Iterator, NT, NL, ExpNodes, Limit}|T]} = State) ->
	case iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Limit, State) of
		{ok, _, [], []} ->
			realloc_next(State#state{sync_stack = T});
		{ok, NewIterator, Buffers, []} ->
			realloc(Buffers, State#state{sync_stack = [{?INSERT_INI, NewIterator, NT, NL, ExpNodes, Limit}|T]});
		{ok, _, _, Error} ->
			zm_event:notify(?MODULE, expansion, {error, Table, {realloc_failed, node(), {read_failed, Error}}}),
			reset(State);
		E ->
			zm_event:notify(?MODULE, expansion, {error, Table, {realloc_failed, node(), E}}),
			reset(State)
	end;
realloc_next(#state{table = Table, sync_stack = [{?FINISH_I, NT, NL, ExpNodes}|T]} = State) ->
	case lock_write(Table) of
		ok ->
			TailSyncTime=z_lib:now_millisecond(),
			NewState=State#state{tail_sync_time = TailSyncTime},
			case zm_db_client:count(Table) of
				{ok, Count} ->
					Limit=compare_lib:get_split_num(Count),
					case zm_db_client:iterate(Table, ascending, false) of
						{ok, Iterator} ->
							case iterator_table_by_exp(Iterator, Table, NT, NL, ExpNodes, Limit, NewState) of
								{ok, NewIterator, Buffers, []} ->
									realloc(Buffers, NewState#state{table = Table, sync_stack = [{?INSERT_INI, NewIterator, NT, NL, ExpNodes, Limit}, ?FINISH_II|T]});
								{ok, _, _, Error} ->
									zm_event:notify(?MODULE, expansion, {error, Table, {start_ii_failed, node(), {read_failed, Error}}}),
									reset(NewState);
								E ->
									zm_event:notify(?MODULE, expansion, {error, Table, {start_ii_failed, node(), E}}),
									reset(NewState)
							end;
						E ->
							zm_event:notify(?MODULE, expansion, {error, Table, {start_ii_failed, node(), E}}),
							reset(State)
					end;
				{estimate, _} ->
					zm_event:notify(?MODULE, expansion, {error, Table, {start_ii_failed, node(), incomplete}}),
					reset(NewState);
				E ->
					zm_event:notify(?MODULE, expansion, {error, Table, {start_ii_failed, node(), E}}),
					reset(NewState)
			end;
		E ->

			case unlock_write(Table) of
				ok ->
					zm_event:notify(?MODULE, expansion, {error, Table, {lock_failed, node(), E}}),
					reset(State);
				E ->
					zm_event:notify(?MODULE, expansion, {error, Table, {unlock_failed, node(), E}}),
					reset(State)
			end
	end;
realloc_next(#state{table = Table, sync_stack = [?FINISH_II, {?NEXT_TABLE, _, _, ExpNodes, []}]} = State) ->
	case unlock_write(Table) of
		ok ->
			io:format("!!!!!!expansion_ok, Table:~p~n", [Table]),
			zm_event:notify(?MODULE, expansion, {expansion_ok, {Table, node()}}),
			zm_event:notify(?MODULE, expansion, {expansion_db_ok, {ExpNodes, node()}});
		E ->
			zm_event:notify(?MODULE, expansion, {error, Table, {ii_finish_failed, node(), E}})
	end,
	reset(State);
realloc_next(#state{table = Table, sync_stack = [?FINISH_II, {?NEXT_TABLE, NT, NL, ExpNodes, Tables}]} = State) ->
	case unlock_write(Table) of
		ok ->
			zm_event:notify(?MODULE, expansion, {expansion_ok, {Table, node()}});
		E ->
			zm_event:notify(?MODULE, expansion, {error, Table, {ii_finish_failed, node(), E}})
	end,
	start_expansion(Tables, NT, NL, ExpNodes, (reset(State))#state{sync_time = z_lib:now_millisecond()}).	

batch_insert([], _) ->
	ok;
batch_insert(L, Table) ->
	case ets:lookup(?DB_SERVER, Table) of
		[{Table, _, _, _, Pid}] ->
			zm_db_table:leadin(Pid, L);
		_ ->
			{error, {invalid_table, Table}}
	end.

get_table_pid(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, Status, Pid}] when Status =:= running; Status =:= only_read; Status =:= sync ->
			Pid;
		_ ->
			false
	end.

dht_node_by_shr(NT, Duplication, Key) ->
	zm_db:dht_node(Key, ?RANGE, Duplication, NT).

iterator_table_by_shr(over, _, _, _, _, _, _, _) ->
	{ok, over, [], []};
iterator_table_by_shr(Iterator, Pid, Table, NT, NL, ShrNodes, Limit, #state{sync_time = SyncTime, tail_sync_time = TailSyncTime}) ->
	case zm_config:get(zm_db, Table) of
		{_, Duplication, _} ->
			case TailSyncTime of
				0 ->
					iterator_table_by_shr(Iterator, Pid, NT, NL, ShrNodes, Duplication, 0, SyncTime, Limit, [], []);
				_ ->
					iterator_table_by_shr(Iterator, Pid, NT, NL, ShrNodes, Duplication, SyncTime, TailSyncTime, Limit, [], [])
			end;
		_ ->
			{ok, Iterator, [], []}
	end.

iterator_table_by_shr(Iterator, _, _, _, _, _, _, _, 0, Buffers, Error) ->
	{ok, Iterator, Buffers, Error};
iterator_table_by_shr(Iterator, Pid, NT, NL, ShrNodes, Duplication, StartTime, TailTime, Limit, Buffers, Error) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, _, Time}, NewIterator} when Time >= StartTime, Time =< TailTime ->
			case zm_db_table:read(Pid, Key, ?READ_TIMEOUT) of
				{ok, Value, Vsn, LocalTime} ->
					iterator_table_by_shr(NewIterator, Pid, NT, NL, ShrNodes, Duplication, StartTime, TailTime, Limit - 1, 
								   fill_buffer(dht_node_by_shr(NT, Duplication, Key), {Key, Value, Vsn, LocalTime}, Buffers), Error);
				{error, Reason} ->
					iterator_table_by_shr(NewIterator, Pid, NT, NL, ShrNodes, Duplication, StartTime, TailTime, Limit, Buffers, [{Key, Reason}|Error])
			end;
		{ok, _, NewIterator} ->
			iterator_table_by_shr(NewIterator, Pid, NT, NL, ShrNodes, Duplication, StartTime, TailTime, Limit, Buffers, Error);
		over ->
			{ok, over, Buffers, Error};
		E ->
			E
	end.

over_write([{Node, RL}|T], #state{table = Table, sync_stack = Stack} = State) ->
	rpc:abcast([Node], ?MODULE, {?INSERT_ACC, node(), Table, RL}),
	io:format("!!!!!!over_write part ok, Node:~p, Table:~p, Start:~p, Tail:~p~n", [Node, Table, hd(RL), lists:last(RL)]),
	State#state{sync_stack = [{?INSERT_ACK_ACC, Node, length(RL), T}|Stack]};
over_write([], State) ->
	over_write_next(State).

over_write_next(#state{table = Table, sync_stack = [{?INSERT_ACC, Iterator, Pid, NT, NL, ShrNodes, Limit}|T]} = State) ->
	case iterator_table_by_shr(Iterator, Pid, Table, NT, NL, ShrNodes, Limit, State) of
		{ok, _, [], []} ->
			over_write_next(State#state{sync_stack = T});
		{ok, NewIterator, Buffers, []} ->
			over_write(Buffers, State#state{sync_stack = [{?INSERT_ACC, NewIterator, Pid, NT, NL, ShrNodes, Limit}|T]});
		{ok, _, _, Error} ->
			zm_event:notify(?MODULE, shrink, {error, Table, {over_write_failed, node(), {read_failed, Error}}}),
			reset(State);
		E ->
			zm_event:notify(?MODULE, shrink, {error, Table, {over_write_failed, node(), E}}),
			reset(State)
	end;
over_write_next(#state{table = Table, sync_stack = [?FINISH_I, {?NEXT_TABLE, NT, _, ShrNodes, []}]} = State) ->
	rpc:abcast(tuple_to_list(NT), ?MODULE, {?FINISH_I, node()}),
	io:format("!!!!!!shrink_ok, Table:~p~n", [Table]),
	zm_event:notify(?MODULE, shrink, {shrink_ok, {Table, node()}}),
	zm_event:notify(?MODULE, shrink, {shrink_db_ok, {ShrNodes, node()}}),
	reset(State);
over_write_next(#state{table = Table, sync_stack = [?FINISH_I, {?NEXT_TABLE, NT, NL, ShrNodes, Tables}]} = State) ->
	zm_event:notify(?MODULE, shrink, {shrink_ok, {Table, node()}}),
	start_shrink(Tables, NT, NL, ShrNodes, (reset(State))#state{sync_time = z_lib:now_millisecond()}).

filter_insert([], _) ->
	ok;
filter_insert(L, Table) ->
	case ets:lookup(?DB_SERVER, Table) of
		[{Table, _, _, running, Pid}] ->
			zm_db_table:leadin(Pid, filter_record(L, Pid, []));
		_ ->
			{error, {invalid_table, Table}}
	end.

filter_record([{Key, Value, RemoteVsn, RemoteTime}|T], Pid, L) ->
	case zm_db_table:read(Pid, Key, ?READ_TIMEOUT) of
		{ok, Value, _, _} ->
			filter_record(T, Pid, L);
		{ok, _, LocalVsn, _} when LocalVsn > RemoteVsn ->
			filter_record(T, Pid, L);
		{ok, _, RemoteVsn, LocalTime} when LocalTime >= RemoteTime ->
			filter_record(T, Pid, L);
		{ok, _, _, _} ->
			filter_record(T, Pid, [{Key, Value, RemoteVsn, RemoteTime}|L]);
		_ ->
			filter_record(T, Pid, L)
	end;
filter_record([], _, L) ->
	L.
