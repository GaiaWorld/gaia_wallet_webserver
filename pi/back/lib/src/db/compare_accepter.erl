%% 
%% @doc 数据对比同步接受者
%%


-module(compare_accepter).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, hash/7, digests/5, stop/1]).

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
				table,
				sync_time,
				tail_sync_time,
				hash_type,
				sync_stack,
				compare_timeout,
				insert_timeout,
				insert_ack_timeout,
				finish_timeout,
				call_timeout
}).
-define(COMPARER_ACCEPTER_HIBERNATE_TIMEOUT, 60000).

-define(SERVICE, zm_service).
-define(DB_SERVER, zm_db_server).
-define(NIL, '$nil').
-define(INF, '$inf').

-define(HASH_TYPE, hash_type).
-define(DEFAULT_HASH_TYPE, md5).
-define(COMPARE_TIMEOUT, compare_timeout).
-define(DEFAULT_COMPARE_TIMEOUT, 600000).
-define(INSERT_TIMEOUT, insert_timeout).
-define(DEFAULT_INSERT_TIMEOUT, 600000).
-define(INSERT_ACK_TIMEOUT, insert_ack_timeout).
-define(DEFAULT_INSERT_ACK_TIMEOUT, 600000).
-define(FINISH_TIMEOUT, finish_timeout).
-define(DEFAULT_FINISH_TIMEOUT, 600000).
-define(CALL_TIMEOUT, call_timeout).
-define(DEFAULT_CALL_TIMEOUT, 100).

-define(READ_TIMEOUT, 5000).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, handle_gossip/4, handle_gossip_exception/4]).

%%
%%启动比较接受器
%%
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%%处理gossip消息
%%
handle_gossip(_, _, _, #gossip{from = {From, _}, type = Type, digests = CheckSum, payload = PayLoad} = Gossip) ->
%% 	error_logger:error_msg("!!!!!!From:~p, Type:~p~n", [From, Type]),
	case checksum(PayLoad) of
		CheckSum ->
			?MODULE ! {Type, Gossip};
		_ ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, invalid_checksum, Type}, 5000)
	end.

%%
%%处理gossip异常
%%
handle_gossip_exception(A, Src, Type, Exception) ->
	error_logger:error_msg("!!!!!!A:~p, Src:~p, Type:~p, Exception:~p~n", [A, Src, Type, Exception]),
	ok.

%%
%%关闭比较接受器
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
					  hash_type = z_lib:get_value(Args, ?HASH_TYPE, ?DEFAULT_HASH_TYPE),
					  compare_timeout = z_lib:get_value(Args, ?COMPARE_TIMEOUT, ?DEFAULT_COMPARE_TIMEOUT), 
					  insert_timeout = z_lib:get_value(Args, ?INSERT_TIMEOUT, ?DEFAULT_INSERT_TIMEOUT), 
					  insert_ack_timeout = z_lib:get_value(Args, ?INSERT_ACK_TIMEOUT, ?DEFAULT_INSERT_ACK_TIMEOUT), 
					  finish_timeout = z_lib:get_value(Args, ?FINISH_TIMEOUT, ?DEFAULT_FINISH_TIMEOUT), 
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
    {noreply, State, ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT}.


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
    {noreply, State, ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT}.


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
handle_info({?GOSSIP_CONSULT, #gossip{from = {From, _}, payload = {Table, SyncTime}} = Gossip}, State) ->
	if
		SyncTime > 0 ->
			case is_table(Table) of
				true ->
					PayLoad={ok, Table, SyncTime},
					gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_CONSULT_FINISH, checksum(PayLoad), PayLoad, 180000, 10000),
					{noreply, State#state{table = Table, sync_time = SyncTime}, ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
				false ->
					PayLoad={error, table_not_exist},
					gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_CONSULT_FINISH, checksum(PayLoad), PayLoad, 180000, 10000),
					{noreply, State, ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT}
			end;
		true ->
			PayLoad={error, invalid_sync_time},
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_CONSULT_FINISH, checksum(PayLoad), PayLoad, 180000, 10000),
			{noreply, State, ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT}
	end;
handle_info({?GOSSIP_START_I_I, Gossip}, State) ->
	{noreply, handle_i_i_compare(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_SPLIT_COMPARE, Gossip}, State) ->
	{noreply, handle_i_ii_compare(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_DIRECT_COMPARE_ACK, Gossip}, State) ->
	{noreply, handle_direct_compare_ack(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_SPLIT_COMPARE_FINISH, Gossip}, State) ->
	{noreply, handle_i_ii_compare_finish(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?GOSSIP_START_II, Gossip}, State) ->
	{noreply, handle_ii_compare(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?INSERT_INI, Gossip}, State) ->
	{noreply, handle_insert_local(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?INSERT_ACK_INI, Gossip}, State) ->
	{noreply, handle_insert_ack(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?INSERT_REQUEST_ACK, Gossip}, State) ->
	{noreply, handle_insert_request_ack(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info({?UPDATE_ACK, Gossip}, State) ->
	{noreply, handle_update_ack(Gossip, State), ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_, State) ->
    {noreply, State, ?COMPARER_ACCEPTER_HIBERNATE_TIMEOUT}.


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
	State#state{sync_stack = []}.

is_table(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, running, _}] ->
			true;
		_ ->
			false
	end.

checksum(Term) ->
	erlang:crc32(term_to_binary(Term)).

get_table_pid(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, Status, Pid}] when Status =:= running; Status =:= only_read; Status =:= sync ->
			Pid;
		_ ->
			false
	end.

hash_iterator(Type) ->
	crypto:hash_init(Type).

hash_next(Term, Context) ->
	crypto:hash_update(Context, term_to_binary(Term)).

hash_finish(Context) ->
	crypto:bytes_to_integer(crypto:hash_final(Context)).

hash(Table, StartTime, TailTime, HashType, ?INF, ?INF, Limit) ->
	Pid=get_table_pid(Table),
	hash1(new_iterator(Pid, ascending), Pid, StartTime, TailTime, hash_iterator(HashType), Limit);
hash(Table, StartTime, TailTime, HashType, ?INF, Tail, Limit) ->
	Pid=get_table_pid(Table),
	case first(Table, StartTime, TailTime) of
		{ok, Start} ->
			hash1(new_iterator(Pid, {closed, Start, closed, Tail}), Pid, StartTime, TailTime, hash_iterator(HashType), Limit);
		over ->
			hash1(new_iterator(Pid, ascending), Pid, StartTime, TailTime, hash_iterator(HashType), Limit)
	end;
hash(Table, StartTime, TailTime, HashType, Start, ?INF, Limit) ->
	Pid=get_table_pid(Table),
	hash1(new_iterator(Pid, {ascending, open, Start}), Pid, StartTime, TailTime, hash_iterator(HashType), Limit);
hash(Table, StartTime, TailTime, HashType, Start, Tail, Limit) ->
	Pid=get_table_pid(Table),
	hash1(new_iterator(Pid, {open, Start, closed, Tail}), Pid, StartTime, TailTime, hash_iterator(HashType), Limit).

hash1(_, _, _, _, Hash, 0) ->
	hash_finish(Hash);
hash1(Iterator, Pid, StartTime, TailTime, Hash, Limit) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, Vsn, Time}, NewIterator} when Time >= StartTime, Time =< TailTime ->
			{ok, Value, _, _}=zm_db_table:read(Pid, Key, ?READ_TIMEOUT),
			hash1(NewIterator, Pid, StartTime, TailTime, hash_next({Key, Value, Vsn, Time}, Hash), Limit - 1);
		{ok, _, NewIterator} ->
			hash1(NewIterator, Pid, StartTime, TailTime, Hash, Limit);
		over ->
			hash_finish(Hash);
		{error, Reason} ->
			erlang:error(Reason);
		{error, Reason, StackTrace} ->
			erlang:error({Reason, StackTrace})
	end.

new_iterator(Pid, KeyRange) ->
	case zm_db_table:iterate(Pid, KeyRange) of
		{ok, Iterator} ->
			Iterator;
		{error, Reason} ->
			erlang:error(Reason);
		{error, Reason, StackTrace} ->
			erlang:error({Reason, StackTrace})
	end.

digests(Table, StartTime, TailTime, ?INF, ?INF) ->
	update_digests(new_iterator(get_table_pid(Table), ascending), StartTime, TailTime, ?NIL, ?NIL, 0);
digests(Table, StartTime, TailTime, ?INF, TailKey) ->
	{Start, Tail, Len}=update_digests(new_iterator(get_table_pid(Table), {descending, closed, TailKey}), StartTime, TailTime, ?NIL, TailKey, 0),
	{Tail, Start, Len};
digests(Table, StartTime, TailTime, StartKey, ?INF) ->
	update_digests(new_iterator(get_table_pid(Table), {ascending, open, StartKey}), StartTime, TailTime, StartKey, ?NIL, 0);
digests(Table, StartTime, TailTime, StartKey, TailKey) ->
	update_digests(new_iterator(get_table_pid(Table), {open, StartKey, closed, TailKey}), StartTime, TailTime, StartKey, TailKey, 0).

update_digests(Iterator, StartTime, TailTime, StartKey, TailKey, RealLen) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, _, Time}, NewIterator} when Time >= StartTime, Time =< TailTime ->
			case StartKey of
				?NIL ->
					update_digests(NewIterator, StartTime, TailTime, Key, TailKey, RealLen + 1);
				_ ->
					update_digests(NewIterator, StartTime, TailTime, StartKey, Key, RealLen + 1)
			end;
		{ok, _, NewIterator} ->
			update_digests(NewIterator, StartTime, TailTime, StartKey, TailKey, RealLen);
		over ->
			{StartKey, TailKey, RealLen};
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

last(Table, StartTime, TailTime) ->
	last1(new_iterator(get_table_pid(Table), descending), StartTime, TailTime).

last1(Iterator, StartTime, TailTime) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, _, Time}, _} when Time >= StartTime, Time =< TailTime ->
			{ok, Key};
		{ok, _, NewIterator} ->
			last1(NewIterator, StartTime, TailTime);
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

handle_i_i_compare(#gossip{from = {From, _}, payload = {{open, ?INF}, {open, ?INF}, ?NIL, ?NIL, 0, 0}} = Gossip, 
					  #state{table = Table, sync_time = SyncTime, sync_stack = Stack, call_timeout = Timeout} = State) ->
	Limit=compare_lib:get_split_num(zm_db_server:table_size(Table)),
	case traverse(Table, 0, SyncTime, ?INF, ?INF, Limit) of
		{_, []} ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
			State#state{sync_stack = []};
		{Tail, RL} ->
			Len=length(RL),
			PayLoad={Len, RL},
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACC, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{sync_stack = [{?INSERT_ACK_ACC, Limit, Len, Tail}, {?GOSSIP_FINISH_I, Table}|Stack]}
	end;
handle_i_i_compare(#gossip{from = {From, _}, payload = {{open, CompareStartKey}, _, StartKey, TailKey, Len, Hash}} = Gossip, 
				   #state{table = Table, sync_time = SyncTime, hash_type = HashType, sync_stack = Stack, call_timeout = Timeout} = State) ->
	case zm_db_server:table_size(Table) of
		0 ->
			PayLoad={CompareStartKey, TailKey},
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{sync_stack = [{?GOSSIP_FINISH_I, Table}|Stack]};
		Len ->
			case {first(Table, 0, SyncTime), last(Table, 0, SyncTime)} of
				{{ok, StartKey}, {ok, TailKey}} ->
					case hash(Table, 0, SyncTime, HashType, CompareStartKey, TailKey, Len) of
						Hash ->
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
							reset(State);
						_ ->
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_START_I_II, checksum(TailKey), TailKey, State#state.compare_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_FINISH_I, Table}|Stack]}
					end;
				{{ok, StartKey1}, {ok, TailKey1}} ->
					case compare_lib:compare_by_range({StartKey, TailKey}, {StartKey1, TailKey1}) of
						{_, {}, _} ->
							Limit=compare_lib:get_split_num(zm_db_server:table_size(Table)),
							case traverse(Table, 0, SyncTime, CompareStartKey, TailKey1, Len) of
								{_, []} ->
									PayLoad={CompareStartKey, TailKey},
									gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
									State#state{sync_stack = [{?GOSSIP_FINISH_I, Table}|Stack]};
								{Tail, RL} ->
									NextLen=length(RL),
									PayLoad={NextLen, RL},
									gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACC, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
									State#state{sync_stack = [{?INSERT_ACK_ACC, Limit, NextLen, Tail}, {?INSERT_REQUEST, CompareStartKey, TailKey}, {?GOSSIP_FINISH_I, Table}|Stack]}
							end;
						_ ->
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_START_I_II, checksum(TailKey), TailKey, State#state.compare_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_FINISH_I, Table}|Stack]}
					end
			end;
		_ ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_START_I_II, checksum(TailKey), TailKey, State#state.compare_timeout, Timeout),
			State#state{sync_stack = [{?GOSSIP_FINISH_I, Table}|Stack]}
	end.

is_direct_compare(Len0, Len1) ->
	if
		Len0 =< Len1, Len0 =< ?DIRECT_COMPARE_THRESHOLD ->
			true;
		Len0 > Len1, (Len0 + Len1) =< ?DIRECT_COMPARE_THRESHOLD ->
			true;
		true ->
			false
	end.

handle_i_ii_compare(#gossip{from = {From, _}, payload = {{open, ?INF}, {open, ?INF}, ?NIL, ?NIL, 0, 0}} = Gossip, 
					 #state{table = Table, sync_stack = [{?GOSSIP_FINISH_I, Table}], call_timeout = Timeout} = State) ->
%% 	error_logger:error_msg("!!!!!!0, Prev:~p~n", [?INF]),
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
	reset(State);
handle_i_ii_compare(#gossip{from = {From, _}, payload = {{open, Prev}, {open, ?INF}, ?NIL, ?NIL, 0, 0}} = Gossip, 
					 #state{table = Table, sync_time = SyncTime, sync_stack = [{?GOSSIP_FINISH_I, Table}] = Stack, call_timeout = Timeout} = State) ->
%% 	error_logger:error_msg("!!!!!!1, Prev:~p~n", [Prev]),
	case digests(Table, 0, SyncTime, Prev, ?INF) of
		{_, _, 0} ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
			reset(State);
		{_, Tail, Len} ->
			case traverse(Table, 0, SyncTime, Prev, Tail, Len) of
				{_, []} ->
					gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
					reset(State);
				{Tail, RL} ->
					Limit=compare_lib:get_split_num(zm_db_server:table_size(Table)),
					NextLen=length(RL),
					PayLoad={NextLen, RL},
					gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACC, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
					State#state{sync_stack = [{?INSERT_ACK_ACC, Limit, NextLen, Tail}|Stack]}
			end
	end;
handle_i_ii_compare(#gossip{from = {From, _}, payload = {{open, Prev}, {open, ?INF}, ?NIL, ?NIL, 0, 0}} = Gossip, 
					 #state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, Prev}|T], call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_ACK, checksum(Prev), Prev, State#state.compare_timeout, Timeout),
	State#state{sync_stack = T};
handle_i_ii_compare(#gossip{from = {From, _}, payload = {{open, Prev}, {open, ?INF}, ?NIL, ?NIL, 0, 0}} = Gossip, 
					 #state{table = Table, sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, PrevKey}|_], call_timeout = Timeout} = State) when Prev > PrevKey ->
%% 	error_logger:error_msg("!!!!!!3, Prev:~p, PrevKey:~p~n", [Prev, PrevKey]),
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
	reset(State);
handle_i_ii_compare(#gossip{from = {From, _}, payload = {{open, CompareStartKey}, {closed, CompareTailKey}, StartKey, TailKey, Len, Hash}} = Gossip, 
					 #state{table = Table, sync_time = SyncTime, sync_stack = Stack, call_timeout = Timeout} = State) ->
	case digests(Table, 0, SyncTime, CompareStartKey, TailKey) of
		{_, _, 0} ->
			PayLoad={CompareStartKey, TailKey},
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]};
		{StartKey, TailKey, Len} ->
			case is_direct_compare(Len, Len) of
				false ->
					case hash(Table, 0, SyncTime, ?DEFAULT_HASH_TYPE, CompareStartKey, TailKey, Len) of
						Hash ->
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_ACK, checksum(TailKey), TailKey, State#state.compare_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]};
						_ ->
							PayLoad={CompareStartKey, TailKey},
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_ACK, checksum(PayLoad), PayLoad, State#state.compare_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]}
					end;
				true ->
					case traverse(Table, 0, SyncTime, CompareStartKey, TailKey, Len) of
						{_, []} ->
							PayLoad={CompareStartKey, TailKey},
							gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]};
						{_, RL} ->
							PayLoad={length(RL), RL, CompareStartKey, TailKey, Len},
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_DIRECT_COMPARE, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]}
					end
			end;
		{TmpStartKey, TmpTailKey, TmpLen} ->
			{StartKey1, TailKey1}=case {TmpStartKey, TmpTailKey, TmpLen} of
				{TmpStartKey, ?NIL, 1} when TmpStartKey >= StartKey ->
					{StartKey, TmpStartKey};
				{TmpStartKey, ?NIL, 1} ->
					{TmpStartKey, TmpStartKey};
				_ when TmpStartKey >= StartKey ->
					{StartKey, TmpTailKey};
				_ ->
					{TmpStartKey, TmpTailKey}
			end,
			case is_direct_compare(Len, TmpLen) of
				false ->
					case compare_lib:compare_by_range({StartKey, TailKey}, {StartKey1, TailKey1}) of
						{_, {}, _} ->
							Limit=compare_lib:get_split_num(zm_db_server:table_size(Table)),
							case traverse(Table, 0, SyncTime, CompareStartKey, TailKey1, Limit) of
								{_, []} ->
									PayLoad={CompareStartKey, TailKey},
									gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
									State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]};
								{Tail, RL} ->
									NextLen=length(RL),
									PayLoad={NextLen, RL},
									gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACC, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
									State#state{sync_stack = [{?INSERT_ACK_ACC, Limit, NextLen, Tail}, {?INSERT_REQUEST, CompareStartKey, TailKey}, {?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]}
							end;
						_ ->
							PayLoad={CompareStartKey, TailKey},
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_ACK, checksum(PayLoad), PayLoad, State#state.compare_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]}
					end;
				true ->
					Len1=if
						TmpLen >= Len ->
							TmpLen;
						true ->
							Len
					end,
					case traverse(Table, 0, SyncTime, CompareStartKey, TailKey1, Len1) of
						{_, []} ->
							PayLoad={CompareStartKey, TailKey},
							gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]};
						{_, Rs} ->
							PayLoad={length(Rs), Rs, CompareStartKey, TailKey, Len1},
							gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_DIRECT_COMPARE, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
							State#state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|Stack]}
					end
			end
	end.

handle_direct_compare_ack(#gossip{from = {From, _}, type = Type, payload = {_Len, RemoteRL}} = Gossip, 
						  #state{table = Table, sync_time = SyncTime, sync_stack = Stack, call_timeout = Timeout} = State) ->
	Pid=get_table_pid(Table),
	case direct_sync(RemoteRL, Pid, 0, SyncTime) of
		{sync_failed, R} ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, direct_sync_error, Type, node(), Table, 0, SyncTime, R}, Timeout),
			reset(State);
		ok ->
			{TailKey, NewStack}=handle_direct_compare_ack1(Stack, ?NIL),
%% 			error_logger:error_msg("!!!!!!TailKey:~p, Stack:~p~n", [TailKey, NewStack]),
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_ACK, checksum(TailKey), TailKey, State#state.compare_timeout, Timeout),
			State#state{sync_stack = NewStack}
	end.

handle_direct_compare_ack1([{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|T], ?NIL) ->
	handle_direct_compare_ack1(T, TailKey);
handle_direct_compare_ack1([{?GOSSIP_SPLIT_COMPARE_ACK, NextTailKey}|T] = Stack, TailKey) ->
	if
		TailKey >= NextTailKey ->
			handle_direct_compare_ack1(T, TailKey);
		true ->
			{TailKey, Stack}
	end;
handle_direct_compare_ack1(Stack, TailKey) ->
	{TailKey, Stack}.

direct_sync([{Key, _, _, Time} = KVVT|T], Pid, StartTime, TailTime) ->
	case zm_db_table:read(Pid, Key, ?READ_TIMEOUT) of
		{ok, _, _, LocalTime} when LocalTime >= StartTime, LocalTime =< TailTime ->
			case insert([KVVT], Pid) of
				ok ->
					direct_sync(T, Pid, StartTime, TailTime);
				{error, Reason} ->
					{sync_failed, {Reason, {remote, KVVT}}}
			end;
		[] when Time >= StartTime, Time =< TailTime ->
			case insert([KVVT], Pid) of
				ok ->
					direct_sync(T, Pid, StartTime, TailTime);
				{error, Reason} ->
					{sync_failed, {Reason, {remote, KVVT}}}
			end;
		_ ->
			direct_sync(T, Pid, StartTime, TailTime)
	end;
direct_sync([], _, _, _) ->
	ok.

handle_i_ii_compare_finish(#gossip{from = {From, _}, payload = TailKey} = Gossip, #state{sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|T], call_timeout = Timeout} = State) ->
	case T of
		[{?GOSSIP_FINISH_I, Table}] ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
			reset(State);
		Any ->
			%TODO 其它操作...
%% 			error_logger:error_msg("!!!!!!handle i sub compare finish, Stack:~p~n", [Any]),
			timer:sleep(10000000),
			State
	end;
handle_i_ii_compare_finish(#gossip{from = {From, _}, payload = _TailKey} = Gossip, #state{sync_stack = [{?GOSSIP_FINISH_I, Table}], call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
	reset(State).

lock_write(Table) ->
	case ?SERVICE:get(Table, node()) of
		{_, {Opts, running}} ->
			?SERVICE:set(Table, {Opts, only_read}, true),
			?DB_SERVER:lock_write(Table);
		{_, {_, only_read}} ->
			ok;
		{_, {_, Status}} ->
			{error, {invalid_service_status, Status}};
		none ->
			{error, {invalid_service, {Table, node()}}}
	end.

unlock_write(Table) ->
	case ?SERVICE:get(Table, node()) of
		{_, {Opts, only_read}} ->
			?SERVICE:set(Table, {Opts, running}, true),
			?DB_SERVER:unlock_write(Table);
		{_, {_, running}} ->
			ok;
		{_, {_, Status}} ->
			{error, {invalid_service_status, Status}};
		none ->
			{error, {invalid_service, {Table, node()}}}
	end.

handle_ii_compare(#gossip{from = {From, _}, payload = Table} = Gossip, #state{table = Table} = State) ->
	case lock_write(Table) of
		ok ->
			TailSyncTime=z_lib:now_millisecond(),
			Limit=compare_lib:get_split_num(zm_db_server:table_size(Table)),
			handle_ii_compare(?INF, TailSyncTime, Limit, Gossip, State);
		{error, Reason} ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {section_ii_lock_failed, Table, Reason}, 3000),
			reset(State)
	end.

handle_ii_compare(Start, TailSyncTime, Limit, #gossip{from = {From, _}, payload = _Len} = Gossip, 
				  #state{table = Table, sync_time = StartSyncTime, call_timeout = Timeout} = State) ->
	case traverse(Table, StartSyncTime, TailSyncTime, Start, ?INF, Limit) of
		{?NIL, []} ->
			finish_ii_compare(Table, Gossip, State);
		{Start, _} ->
			finish_ii_compare(Table, Gossip, State);
		{Tail, RL} ->
%% 			error_logger:error_msg("!!!!!!0, Start:~p, Tail:~p, Limit:~p~n", [hd(RL), Tail, Limit]),
			NextLen=length(RL),
			PayLoad={NextLen, RL},
			gossip_server:ack(Gossip, ?MODULE, From, ?UPDATE, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{tail_sync_time = TailSyncTime, sync_stack = [{?UPDATE_ACK, Limit, NextLen, Tail}, {?GOSSIP_FINISH_II, Table}|State#state.sync_stack]}
	end.

handle_update_ack(#gossip{payload = Len} = Gossip, #state{tail_sync_time = TailSyncTime, sync_stack = [{?UPDATE_ACK, Limit, Len, Start}|T]} = State) ->
	handle_update_ack(Start, Limit, TailSyncTime, Gossip, State#state{sync_stack = T}).

handle_update_ack(?NIL, _, _, #gossip{payload = _Len} = Gossip, #state{table = Table, sync_stack = [{?GOSSIP_FINISH_II, Table}]} = State) ->
	finish_ii_compare(Table, Gossip, State);
handle_update_ack(Start, Limit, TailSyncTime, #gossip{from = {From, _}, payload = _Len} = Gossip, 
				  #state{table = Table, sync_time = SyncTime, sync_stack = Stack, call_timeout = Timeout} = State) ->
	case traverse(Table, SyncTime, TailSyncTime, Start, ?INF, Limit) of
		{?NIL, []} ->
			handle_update_ack(?NIL, Limit, TailSyncTime, Gossip, State);
		{Start, _} ->
			handle_update_ack(?NIL, Limit, TailSyncTime, Gossip, State);
		{Tail, RL} ->
%% 			error_logger:error_msg("!!!!!!1, Start:~p, Tail:~p, Limit:~p~n", [hd(RL), Tail, Limit]),
			NextLen=length(RL),
			PayLoad={NextLen, RL},
			gossip_server:ack(Gossip, ?MODULE, From, ?UPDATE, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{tail_sync_time = TailSyncTime, sync_stack = [{?UPDATE_ACK, Limit, NextLen, Tail}|Stack]}
	end.

finish_ii_compare(Table, #gossip{from = {From, _}} = Gossip, #state{insert_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	case unlock_write(Table) of
		ok ->
			gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_II, checksum(Table), Table, GossipTimeout, Timeout),
			reset(State);
		{error, Reason} ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {section_ii_lock_failed, Table, Reason}, 3000),
			reset(State)
	end.

insert([], _) ->
	ok;
insert(L, Pid) ->
	zm_db_table:leadin(Pid, L).

handle_insert_local(#gossip{from = {From, _}, type = Type, payload = {Len, RL}} = Gossip, 
					#state{table = Table, insert_ack_timeout = GossipTimeout, call_timeout = Timeout} = State) ->
	case insert(RL, get_table_pid(Table)) of
		ok ->
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACK_ACC, checksum(Len), Len, GossipTimeout, Timeout),
			State;
		{error, Reason} ->
			gossip_server:exit(Gossip, ?MODULE, From, error, {handle_gossip_failed, insert_error, Reason, Type, node(), Table, Len}, Timeout),
			reset(State)
	end.

handle_insert_ack(#gossip{payload = Len} = Gossip, #state{sync_stack = [{?INSERT_ACK_ACC, _, Len, ?NIL}|T]} = State) ->
	handle_insert_ack_sync_stack(T, Gossip, State);
handle_insert_ack(#gossip{from = {From, _}, payload = Len} = Gossip, #state{table = Table, sync_time = SyncTime, sync_stack = [{?INSERT_ACK_ACC, Limit, Len, Start}|T], 
																   call_timeout = Timeout} = State) ->
	case traverse(Table, 0, SyncTime, Start, ?INF, Limit) of
		{_, []} ->
			handle_insert_ack_sync_stack(T, Gossip, State);
		{Tail, RL} ->
			NextLen=length(RL),
			PayLoad={NextLen, RL},
			gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_ACC, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
			State#state{sync_stack = [{?INSERT_ACK_ACC, Limit, NextLen, Tail}|T]}
	end.

handle_insert_ack_sync_stack([{?INSERT_REQUEST, CompareStartKey, TailKey}|T], #gossip{from = {From, _}} = Gossip, #state{call_timeout = Timeout} = State) ->
	PayLoad={CompareStartKey, TailKey},
	gossip_server:ack(Gossip, ?MODULE, From, ?INSERT_REQUEST, checksum(PayLoad), PayLoad, State#state.insert_timeout, Timeout),
	State#state{sync_stack = T};
handle_insert_ack_sync_stack([{?GOSSIP_FINISH_I, Table}], #gossip{from = {From, _}} = Gossip, #state{table = Table, call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
	reset(State);
handle_insert_ack_sync_stack(Any, _, State) ->
	%TODO 其它操作...
%% 	error_logger:error_msg("!!!!!!handle insert ack, Stack:~p~n", [Any]),
	timer:sleep(10000000),
	State.

handle_insert_request_ack(#gossip{from = {From, _}, payload = Table} = Gossip, #state{table = Table, sync_stack = [{?GOSSIP_SPLIT_COMPARE_ACK, TailKey}|T], call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_SPLIT_COMPARE_ACK, checksum(TailKey), TailKey, State#state.compare_timeout, Timeout),
	State#state{sync_stack = T};
handle_insert_request_ack(#gossip{from = {From, _}, payload = Table} = Gossip, #state{table = Table, sync_stack = [{?GOSSIP_FINISH_I, Table}], call_timeout = Timeout} = State) ->
	gossip_server:ack(Gossip, ?MODULE, From, ?GOSSIP_FINISH_I, checksum(Table), Table, State#state.finish_timeout, Timeout),
	reset(State);
handle_insert_request_ack(_, #state{sync_stack = Any} = State) ->
	%TODO 其它操作...
%% 	error_logger:error_msg("!!!!!!handle insert request ack, Stack:~p~n", [Any]),
	timer:sleep(10000000),
	State.
	
