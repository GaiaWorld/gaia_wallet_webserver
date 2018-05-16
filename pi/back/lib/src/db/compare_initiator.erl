%% 
%% @doc 数据对比同步发起者
%%


-module(compare_initiator).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, digests/6]).

%% ====================================================================
%% Include files
%% ====================================================================
%%gossip
-record(gossip, {
						id,
						from,
						dest,
						type,
						status,
						digests,
						payload,
						send_time,
						timeout
}).

-define(GOSSIP_CONSULT, consult).
-define(GOSSIP_CONSULT_FINISH, consult_finish).

-define(GOSSIP_START_I_I, start1_1).
-define(GOSSIP_START_I_II, start1_2).
-define(GOSSIP_SPLIT_COMPARE, split_compare).
-define(GOSSIP_SPLIT_COMPARE_ACK, split_compare_ack).
-define(GOSSIP_SPLIT_COMPARE_FINISH, split_compare_finish).
-define(GOSSIP_DIRECT_COMPARE, direct_compare).
-define(GOSSIP_DIRECT_COMPARE_ACK, direct_compare_ack).
-define(GOSSIP_FINISH_I, finish1).
-define(GOSSIP_START_II, start2).
-define(GOSSIP_FINISH_II, finish2).

-define(INSERT_REQUEST, insert_req).
-define(INSERT_REQUEST_ACK, insert_req_ack).
-define(INSERT_INI, insert_ini).
-define(INSERT_ACC, insert_acc).
-define(INSERT_ACK_INI, insert_ack_ini).
-define(INSERT_ACK_ACC, insert_ack_acc).

-define(UPDATE, update).
-define(UPDATE_ACK, update_ack).

-define(COMPARE_SPLIT_NUM, 10).
-define(DIRECT_COMPARE_THRESHOLD, 100).

-record(state, {
				from,
				table,
				sync_time,
				hash_type,			
				sync_stack,
				nodes,
				consult_timeout,
				compare_timeout,
				insert_timeout,
				insert_ack_timeout,
				call_timeout
}).
-define(COMPARER_INITIATOR_HIBERNATE_TIMEOUT, 60000).

-define(DB_SERVER, zm_db_server).
-define(NIL, '$nil').
-define(INF, '$inf').

-define(HASH_TYPE, hash_type).
-define(DEFAULT_HASH_TYPE, md5).
-define(CONSULT_TIMEOUT, consult_timeout).
-define(DEFAULT_CONSULT_TIMEOUT, 5000).
-define(COMPARE_TIMEOUT, compare_timeout).
-define(DEFAULT_COMPARE_TIMEOUT, 600000).
-define(INSERT_TIMEOUT, insert_timeout).
-define(DEFAULT_INSERT_TIMEOUT, 600000).
-define(INSERT_ACK_TIMEOUT, insert_ack_timeout).
-define(DEFAULT_INSERT_ACK_TIMEOUT, 600000).
-define(CALL_TIMEOUT, call_timeout).
-define(DEFAULT_CALL_TIMEOUT, 10000).

-define(READ_TIMEOUT, 5000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, start/1, handle_gossip/4, handle_gossip_exception/4, stop/1]).

%%
%%启动比较发起者
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%开始比较指定表
%%
start(Table) ->
	case is_table(Table) of
		true ->
			try
				gen_server:call(?MODULE, {start, Table}, ?DEFAULT_CALL_TIMEOUT)
			catch
				_:Reason ->
					{error, Reason}
			end;
		false ->
			{error, table_not_exist}
	end.

%%
%%处理gossip消息
%%
handle_gossip(_, Src, EventType, #gossip{from = {From, _}, type = Type, digests = CheckSum, payload = PayLoad} = Gossip) ->
%% 	error_logger:error_msg("!!!!!!From:~p, Type:~p~n", [From, Type]),
	case checksum(PayLoad) of
		CheckSum ->
			case PayLoad of
				{error, Reason} ->
					gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, Reason, Type}, 5000),
					handle_error({?MODULE, Src, EventType, From, Type, Reason});
				_ ->
					?MODULE ! {Type, Gossip}
			end;
		_ ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, invalid_checksum, Type}, 5000),
			handle_error({?MODULE, Src, EventType, From, Type, invalid_checksum})
	end.

%%
%%处理gossip异常
%%
handle_gossip_exception(A, Src, Type, Exception) ->
	error_logger:error_msg("!!!!!!A:~p, Src:~p, Type:~p, Exception:~p~n", [A, Src, Type, Exception]),
	handle_error({?MODULE, Src, Type, Exception}).

%%
%%关闭比较发起者
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
    {ok, #state{
				from = undefined, 
				sync_stack = [], 
				nodes = [], 
				hash_type = z_lib:get_value(Args, ?HASH_TYPE, ?DEFAULT_HASH_TYPE),
				consult_timeout = z_lib:get_value(Args, ?CONSULT_TIMEOUT, ?DEFAULT_CONSULT_TIMEOUT), 
				compare_timeout = z_lib:get_value(Args, ?COMPARE_TIMEOUT, ?DEFAULT_COMPARE_TIMEOUT), 
				insert_timeout = z_lib:get_value(Args, ?INSERT_TIMEOUT, ?DEFAULT_INSERT_TIMEOUT), 
				insert_ack_timeout = z_lib:get_value(Args, ?INSERT_ACK_TIMEOUT, ?DEFAULT_INSERT_ACK_TIMEOUT), 
				call_timeout = z_lib:get_value(Args, ?CALL_TIMEOUT, ?DEFAULT_CALL_TIMEOUT)}}.


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
handle_call({start, Table}, From, #state{consult_timeout = GossipTimeout, call_timeout = Timeout, nodes = Nodes} = State) ->
	case 
		case Nodes of
			[] ->
				get_near(Table);
			_ ->
				Nodes
		end
	of
		[Dest|NewNodes] ->
			SyncTime=z_lib:now_millisecond(),
			PayLoad={Table, SyncTime},
			gossip_server:sync(?MODULE, Dest, ?GOSSIP_CONSULT, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
			{noreply, State#state{from = From, table = Table, sync_time = SyncTime, nodes = NewNodes}, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
		[] ->
			{reply, {error, invalid_relation_node}, State#state{nodes = []}, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT}
	end;
handle_call({stop, Reason}, _, State) ->
	{stop, Reason, ok, reset(State)};
handle_call(_, _, State) ->
    {noreply, State, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT}.


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
handle_info({?GOSSIP_CONSULT_FINISH, #gossip{from = {From, _}, payload = {ok, Table, SyncTime}} = Gossip}, 
			#state{from = Src, table = Table, sync_time = SyncTime, hash_type = HashType, compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	z_lib:reply(Src, ok),
	Limit=zm_db_server:table_size(Table),
	PayLoad=digests(Table, 0, SyncTime, HashType, ?INF, Limit),
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_START_I_I, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
	{noreply, State#state{from = undefined, sync_stack = [{?INF, Limit}]}, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_CONSULT_FINISH, #gossip{from = {From, _}, payload = {error, Reason}}}, #state{from = Src} = State) ->
	z_lib:reply(Src, {error, {Reason, From}}),
	{noreply, State#state{from = undefined, table = undefined}, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_START_I_II, #gossip{from = {From, _}, payload = TailKey} = Gossip}, 
			#state{table = Table, sync_time = SyncTime, hash_type = HashType, sync_stack = [{Start, Limit}], compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	SplitLimit=compare_lib:split_set(zm_db_server:table_size(Table), ?COMPARE_SPLIT_NUM),
	PayLoad={_, _, _, Tail, _, _}=digests(Table, 0, SyncTime, HashType, Start, SplitLimit),
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
	{noreply, State#state{sync_stack = [{Tail, SplitLimit}, {TailKey, Limit}]}, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_SPLIT_COMPARE_ACK, Gossip}, State) ->
	{noreply, handle_split_compare_ack(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_DIRECT_COMPARE, Gossip}, State) ->
	{noreply, handle_direct_compare(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_FINISH_I, #gossip{from = {From, _}} = Gossip}, #state{table = Table, compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	zm_event:notify(?MODULE, ?GOSSIP_FINISH_I, {Table, node(), From}),
	%timer:sleep(30000),
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_START_II, checksum(Table), Table, GossipTimeout, Timeout),
	{noreply, reset(State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_FINISH_II, Gossip}, State) ->
%% 	error_logger:error_msg("!!!!!!second section finish, Node:~p, Gossip:~p, Nodes:~p~n", [node(), Gossip, State#state.nodes]),
	{noreply, handle_next_node(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?INSERT_ACC, Gossip}, State) ->
	{noreply, handle_insert_local(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?INSERT_ACK_ACC, Gossip}, State) ->
	{noreply, handle_insert_ack(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?INSERT_REQUEST, Gossip}, State) ->
	{noreply, handle_insert_request(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info({?UPDATE, Gossip}, State) ->
	{noreply, handle_update(Gossip, State), ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State, ?COMPARER_INITIATOR_HIBERNATE_TIMEOUT}.


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

reset(#state{table = Table} = State) ->
	State#state{from = undefined, sync_stack = []}.

is_table(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, Status, _}] ->
			case Status of
				running ->
					true;
				wait_repair ->
					true;
				continue_repair ->
					true;
				{repairing, _} ->
					true;
				{repair_error, _} ->
					true
			end;
		[] ->
			case ets:lookup(zm_db, Table) of
				[{Table, _, _}] ->
					true;
				[] ->
					false
			end;
		_ ->
			false
	end.

get_near(undefined) ->
	[];
get_near(Table) ->
	case zm_db:active_relation_node(Table, node()) of
		this ->
			[];
		none ->
			[];
		{L, R} ->
			L ++ R;
		L ->
			L
	end.

checksum(Term) ->
	erlang:crc32(term_to_binary(Term)).

get_table_pid(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, Status, Pid}] when Status =:= running; Status =:= only_read; Status =:= sync; Status =:= wait_repair; Status =:= continue_repair ->
			Pid;
		[{Table, _, _, {repairing, _}, Pid}] ->
			Pid;
		_ ->
			false
	end.

handle_next_node(#gossip{from = {From, _}} = Gossip, #state{table = Table, nodes = [Node|NewNodes], compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	gossip_server:exit(Gossip, ?MODULE, From, ?GOSSIP_FINISH_II, normal, 3000),
	zm_event:notify(?MODULE, ?GOSSIP_FINISH_II, {Table, node(), From, Node}),
	SyncTime=z_lib:now_millisecond(),
	PayLoad={Table, SyncTime},
	gossip_server:sync(?MODULE, Node, ?GOSSIP_CONSULT, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
	State#state{table = Table, sync_time = SyncTime, nodes = NewNodes};
handle_next_node(#gossip{from = {From, _}} = Gossip, #state{table = Table} = State) ->
	gossip_server:exit(Gossip, ?MODULE, From, ?GOSSIP_FINISH_II, normal, 3000),
	zm_event:notify(?MODULE, ?GOSSIP_FINISH_II, {Table, node(), From}),
	reset(State).

hash_iterator(Type) ->
	crypto:hash_init(Type).

hash_next(Term, Context) ->
	crypto:hash_update(Context, term_to_binary(Term)).

hash_finish(Context) ->
	crypto:bytes_to_integer(crypto:hash_final(Context)).

new_iterator(Pid, KeyRange) ->
	case zm_db_table:iterate(Pid, KeyRange) of
		{ok, Iterator} ->
			Iterator;
		{error, Reason} ->
			erlang:error(Reason);
		{error, Reason, StackTrace} ->
			erlang:error({Reason, StackTrace})
	end.

digests(Table, StartTime, TailTime, HashType, ?INF, Limit) ->
	Pid=get_table_pid(Table),
	new_digests(update_digests(new_iterator(Pid, ascending), Pid, StartTime, TailTime, ?NIL, ?NIL, 0, hash_iterator(HashType), Limit), ?INF);
digests(Table, StartTime, TailTime, HashType, Start, Limit) ->
	Pid=get_table_pid(Table),
	new_digests(update_digests(new_iterator(Pid, {ascending, open, Start}), Pid, StartTime, TailTime, ?NIL, ?NIL, 0, hash_iterator(HashType), Limit), Start).

new_digests({limit, StartKey, ?NIL, Len, Hash}, Start) ->
	{{open, Start}, {closed, StartKey}, StartKey, StartKey, Len, Hash};
new_digests({limit, StartKey, TailKey, Len, Hash}, Start) ->
	{{open, Start}, {closed, TailKey}, StartKey, TailKey, Len, Hash};
new_digests({over, ?NIL, ?NIL, Len, _}, _) ->
	{{open, ?INF}, {open, ?INF}, ?NIL, ?NIL, Len, 0};
new_digests({over, StartKey, ?NIL, Len, Hash}, Start) ->
	{{open, Start}, {closed, StartKey}, StartKey, StartKey, Len, Hash};
new_digests({over, StartKey, TailKey, Len, Hash}, Start) ->
	{{open, Start}, {closed, TailKey}, StartKey, TailKey, Len, Hash}.

update_digests(_, _, _, _, StartKey, TailKey, RealLen, Hash, 0) ->
	{limit, StartKey, TailKey, RealLen, hash_finish(Hash)};
update_digests(Iterator, Pid, StartTime, TailTime, StartKey, TailKey, RealLen, Hash, Limit) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, Vsn, Time}, NewIterator} when Time >= StartTime, Time =< TailTime ->
			{ok, Value, _, _}=zm_db_table:read(Pid, Key, ?READ_TIMEOUT),
			case StartKey of
				?NIL ->
					update_digests(NewIterator, Pid, StartTime, TailTime, Key, TailKey, RealLen + 1, hash_next({Key, Value, Vsn, Time}, Hash), Limit - 1);
				_ ->
					update_digests(NewIterator, Pid, StartTime, TailTime, StartKey, Key, RealLen + 1, hash_next({Key, Value, Vsn, Time}, Hash), Limit - 1)
			end;
		{ok, _, NewIterator} ->
			update_digests(NewIterator, Pid, StartTime, TailTime, StartKey, TailKey, RealLen, Hash, Limit);
		over ->
			{over, StartKey, TailKey, RealLen, hash_finish(Hash)};
		{error, Reason} ->
			erlang:error(Reason);
		{error, Reason, StackTrace} ->
			erlang:error({Reason, StackTrace})
	end.

first(Table, StartTime, TailTime) ->
	first1(new_iterator(get_table_pid(Table), ascending), StartTime, TailTime).
	
first1(Iterator, StartTime, TailTime) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, _, Time}, _} when Time >= StartTime, Time =< TailTime ->
			{ok, Key};
		{ok, _, NewIterator} ->
			first1(NewIterator, StartTime, TailTime);
		over ->
			over;
		{error, Reason} ->
			erlang:error(Reason);
		{error, Reason, StackTrace} ->
			erlang:error({Reason, StackTrace})
	end.

last([]) ->
	?NIL;
last(L) ->
	element(1, hd(L)).

traverse(Table, StartTime, TailTime, ?INF, ?INF, Limit) ->
	Pid=get_table_pid(Table),
	traverse1(new_iterator(Pid, ascending), Pid, StartTime, TailTime, Limit, []);
traverse(Table, StartTime, TailTime, ?INF, Tail, Limit) ->
	Pid=get_table_pid(Table),
	case first(Table, StartTime, TailTime) of
		{ok, Start} ->
			traverse1(new_iterator(Pid, {closed, Start, closed, Tail}), Pid, StartTime, TailTime, Limit, []);
		over ->
			traverse1(new_iterator(Pid, ascending), Pid, StartTime, TailTime, Limit, [])
	end;
traverse(Table, StartTime, TailTime, Start, ?INF, Limit) ->
	Pid=get_table_pid(Table),
	traverse1(new_iterator(Pid, {ascending, open, Start}), Pid, StartTime, TailTime, Limit, []);
traverse(Table, StartTime, TailTime, Start, Tail, Limit) ->
	Pid=get_table_pid(Table),
	traverse1(new_iterator(Pid, {open, Start, closed, Tail}), Pid, StartTime, TailTime, Limit, []).

traverse1(_, _, _, _, 0, L) ->
	{last(L), lists:reverse(L)};
traverse1(Iterator, Pid, StartTime, TailTime, Limit, L) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, Vsn, Time}, NewIterator} when Time >= StartTime, Time =< TailTime ->
			{ok, Value, _, _}=zm_db_table:read(Pid, Key, ?READ_TIMEOUT),
			traverse1(NewIterator, Pid, StartTime, TailTime, Limit - 1, [{Key, Value, Vsn, Time}|L]);
		{ok, _, NewIterator} ->
			traverse1(NewIterator, Pid, StartTime, TailTime, Limit, L);
		over ->
			{last(L), lists:reverse(L)};
		{error, Reason} ->
			erlang:error(Reason);
		{error, Reason, StackTrace} ->
			erlang:error({Reason, StackTrace})
	end.

handle_split_compare_ack(#gossip{from = {From, _}, payload = {CompareStartKey, TailKey}} = Gossip, 
						 #state{table = Table, sync_time = SyncTime, sync_stack = [{TailKey, Limit}|_] = Stack, compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	SubLimit=compare_lib:split_set(Limit, ?COMPARE_SPLIT_NUM),
	case digests(Table, 0, SyncTime, ?DEFAULT_HASH_TYPE, CompareStartKey, SubLimit) of
		{{open, Tail}, {open, '$inf'}, '$nil', '$nil', 0, 0}  = PayLoad ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
			State#state{sync_stack = [{Tail, SubLimit}|Stack]};
		{_, _, _, Tail, _, _} = PayLoad ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
			State#state{sync_stack = [{Tail, SubLimit}|Stack]}
	end;
handle_split_compare_ack(#gossip{from = {From, _}, payload = TailKey} = Gossip, 
						 #state{table = Table, sync_time = SyncTime, sync_stack = [{TailKey, Limit}|T], compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
%% 	error_logger:error_msg("!!!!!!3, TailKey:~p, Stack:~p~n", [TailKey, [{TailKey, Limit}|T]]),
	case T of
		[] ->
			PayLoad={{open, TailKey}, {open, '$inf'}, '$nil', '$nil', 0, 0},
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
			State#state{sync_stack = [{'$inf', Limit}]};
		[{NextTailKey, NextLimit}|_] ->
			if
				TailKey < NextTailKey ->
					case digests(Table, 0, SyncTime, ?DEFAULT_HASH_TYPE, TailKey, Limit) of
						{{open, Tail}, {open, '$inf'}, '$nil', '$nil', 0, 0}  = PayLoad ->
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
							State#state{sync_stack = [{Tail, Limit}|T]};
						{_, _, _, Tail, _, _} = PayLoad ->
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
							State#state{sync_stack = [{Tail, Limit}|T]}
					end;
				true ->
					case handle_split_compare_ack1(T, TailKey, Limit) of
						{RealTailKey, RealLimit, []} ->
							PayLoad={{open, RealTailKey}, {open, '$inf'}, '$nil', '$nil', 0, 0},
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
							State#state{sync_stack = [{RealTailKey, RealLimit}]};
						{RealTailKey, RealLimit, RealStack} ->
							case digests(Table, 0, SyncTime, ?DEFAULT_HASH_TYPE, RealTailKey, RealLimit) of
								{{open, Tail}, {open, '$inf'}, '$nil', '$nil', 0, 0}  = PayLoad ->
									gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
									State#state{sync_stack = [{Tail, NextLimit}|RealStack]};
								{_, _, _, Tail, _, _} = PayLoad ->
									gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
									State#state{sync_stack = [{Tail, NextLimit}|RealStack]}
							end
					end
			end
	end;
handle_split_compare_ack(#gossip{from = {From, _}, payload = TailKey} = Gossip, 
						 #state{sync_stack = [{_, _}], compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_FINISH, checksum(TailKey), TailKey, GossipTimeout, Timeout),
	State#state{sync_stack = []}.

handle_split_compare_ack1([{NextTailKey, NextTailLimit}|T], TailKey, _) when TailKey >= NextTailKey ->
	handle_split_compare_ack1(T, NextTailKey, NextTailLimit);
handle_split_compare_ack1([{NextTailKey, _}|_] = Stack, TailKey, Limit) when TailKey < NextTailKey ->
	{TailKey, Limit, Stack};
handle_split_compare_ack1([], TailKey, Limit) ->
	{TailKey, Limit, []}.

handle_direct_compare(#gossip{from = {From, _}, type = Type, payload = {_RsLen, RemoteRL, StartKey, TailKey, Len}} = Gossip, 
					  #state{table = Table, sync_time = SyncTime, compare_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	{_, LocalRL} = traverse(Table, 0, SyncTime, StartKey, TailKey, Len),
	{I, RemoteC, LocalC}=compare_lib:get_records(RemoteRL, LocalRL),
	Pid=get_table_pid(Table),
	insert(RemoteC, Pid),
	case direct_sync(I, Pid, 0, SyncTime, []) of
		{sync_failed, R} = Reason ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, direct_sync_error, Type, node(), Table, 0, SyncTime, R}, Timeout),
			handle_error(Reason),
			reset(State);
		NewI ->
			NewRs=NewI ++ LocalC, 
			PayLoad={length(NewRs), NewRs},
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_DIRECT_COMPARE_ACK, checksum(PayLoad), PayLoad, GossipTimeout, Timeout),
			State
	end.

direct_sync([{Key, Value, Vsn, Time} = KVVT|T], Pid, StartTime, TailTime, L) ->
	case zm_db_table:read(Pid, Key, ?READ_TIMEOUT) of
		{ok, Value, Vsn, Time} when Time >= StartTime, Time =< TailTime ->
			direct_sync(T, Pid, StartTime, TailTime, L);
		{ok, LocalValue, LocalVsn, LocalTime} when LocalTime >= StartTime, LocalTime =< TailTime ->
			LocalVsnAbs=erlang:abs(LocalVsn),
			VsnAbs=erlang:abs(Vsn),
			if
				LocalVsnAbs > VsnAbs ->
					direct_sync(T, Pid, StartTime, TailTime, [{Key, LocalValue, LocalVsn, LocalTime}|L]);
				LocalVsnAbs < VsnAbs ->
					case insert([KVVT], Pid) of
						ok ->
							direct_sync(T, Pid, StartTime, TailTime, L);
						{error, Reason} ->
							{sync_failed, {Reason, {remote, {Key, Value, Vsn, Time}}}}
					end;
				true ->
					if
						LocalTime > Time ->
							direct_sync(T, Pid, StartTime, TailTime, [{Key, LocalValue, LocalVsn, LocalTime}|L]);
						true ->
							case insert([KVVT], Pid) of
								ok ->
									direct_sync(T, Pid, StartTime, TailTime, L);
								{error, Reason} ->
									{sync_failed, {Reason, {remote, {Key, Value, Vsn, Time}}}}
							end
					end
			end;
		_ ->
			direct_sync(T, Pid, StartTime, TailTime, L)
	end;
direct_sync([], _, _, _, L) ->
	L.

handle_update(#gossip{from = {From, _}, type = Type, payload = {Len, RL}} = Gossip, 
			  #state{table = Table, insert_ack_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	case insert(RL, get_table_pid(Table)) of
		ok ->
			gossip_server:ack(Gossip, ?MODULE, From, ?UPDATE_ACK, checksum(Len), Len, GossipTimeout, Timeout),
			State;
		{error, Reason} ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, update_error, Reason, Type, node(), Table, Len}, Timeout),
			handle_error({update_failed, Reason}),
			reset(State)
	end.

insert([], _) ->
	{error, invalid_records};
insert(L, Pid) ->
	zm_db_table:leadin(Pid, L).

handle_insert_local(#gossip{from = {From, _}, type = Type, payload = {Len, Rs}} = Gossip, 
					#state{table = Table, insert_ack_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	case insert(Rs, get_table_pid(Table)) of
		ok ->
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACK_INI, checksum(Len), Len, GossipTimeout, Timeout),
			State;
		{error, Reason} ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, insert_error, Reason, Type, node(), Table, Len}, Timeout),
			handle_error({insert_local_failed, Reason}),
			reset(State)
	end.

handle_insert_ack(#gossip{payload = Len} = Gossip, #state{sync_stack = [{?INSERT_ACK_INI, _, Len, ?NIL, _}|T]} = State) ->
	handle_insert_ack_sync_stack(T, Gossip, State);
handle_insert_ack(#gossip{from = {From, _}, payload = Len} = Gossip, #state{table = Table, sync_time = SyncTime, sync_stack = [{?INSERT_ACK_INI, Limit, Len, StartKey, TailKey}|T], 
																   call_timeout = Timeout} = State) ->
	case traverse(Table, 0, SyncTime, StartKey, TailKey, Limit) of
		{?NIL, _} ->
			handle_insert_ack_sync_stack(T, Gossip, State);
		{Tail, RL} ->
			NextLen=length(RL),
			PayLoad={NextLen, RL},
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_INI, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{sync_stack = [{?INSERT_ACK_INI, Limit, NextLen, Tail, TailKey}|T]}
	end.

handle_insert_ack_sync_stack([{?INSERT_REQUEST_ACK, Table}|T], #gossip{from = {From, _}} = Gossip, #state{table = Table, call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST_ACK, checksum(Table), Table, State#state.insert_ack_timeout, Timeout),
	State#state{sync_stack = T};
handle_insert_ack_sync_stack(Any, _, State) ->
	%TODO 其它操作...
%% 	error_logger:error_msg("!!!!!!handle insert ack, Stack:~p~n", [Any]),
	timer:sleep(10000000),
	State.

handle_insert_request(#gossip{from = {From, _}, payload = {StartKey, TailKey}} = Gossip, 
					  #state{table = Table, sync_time = SyncTime, sync_stack = Stack, call_timeout = Timeout} = State) ->
	Limit=compare_lib:get_split_num(zm_db_server:table_size(Table)),
	{Tail, RL}=traverse(Table, 0, SyncTime, StartKey, TailKey, Limit),
	Len=length(RL),
	PayLoad={Len, RL},
	gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_INI, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
	State#state{sync_stack = [{?INSERT_ACK_INI, Limit, Len, Tail, TailKey}, {?INSERT_REQUEST_ACK, Table}|Stack]}.

handle_error(Reason) ->
	erlang:exit(whereis(?MODULE), {?MODULE, Reason}).