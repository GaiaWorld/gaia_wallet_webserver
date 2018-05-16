%%% -------------------------------------------------------------------
%%% Author  : Administrator
%%% Description : 索引检索器
%%%
%%% Created : 2012-8-14
%%% -------------------------------------------------------------------
-module(retrievaler).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("retrievaler.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, 
		 retrieval/4, 
		 retrieval_reduce/9,
		 count/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


%% ====================================================================
%% External functions
%% ====================================================================

%%
%% Function: start_link/1
%% Description: 启动索引检索器
%% Arguments: 
%%	Args
%% Returns: {ok,Pid} | ignore | {error,Error}
%% 
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%% Function: retrieval/4
%% Description: 检索函数
%% Arguments: 
%%	TimeRange
%%	BoolExpre
%%	Limit
%%	Timeout
%% Returns: {ok, ResultSet} | {error,Error}
%% 
retrieval(TimeRange, BoolExpre, Limit, Timeout) 
  when is_list(BoolExpre), is_integer(Timeout), Timeout > 0->
	gen_server:call(?MODULE, {?RETRIEVAL_FLAG, self(), to_second(TimeRange), BoolExpre, Limit, Timeout}, Timeout).

%%
%% Function: retrieval_reduce/9
%% Description: 检索的归并函数
%% Arguments: 
%%	TimeRange
%%	From
%%	IndexDirPath
%%	KGRemIndexCount
%%	DictCount
%%	InvertedCount
%%	BoolExpre
%%	Limit
%%	Timeout
%% Returns: ok
%% 
retrieval_reduce(From, TimeRange, IndexDirPath, 
				 KGRemIndexCount, DictCount, InvertedCount, BoolExpre, Limit, Timeout) ->
	Fun=fun(Key) ->
				io:format("!!!!!!Key:~p~n", [Key]),
				file_retrievaler:get(to_week(TimeRange), IndexDirPath, KGRemIndexCount, DictCount, InvertedCount, to_minute(TimeRange), Key)
	end,
	Keys=case BoolExpre of
		[{_, Val}] ->
			[Val];
		_ ->
			get_expre_keys(BoolExpre, [], [])
	end,
	case z_lib:map_reduce(Fun, Keys, Timeout) of
		{ok, R} ->
			ResultSet=case R of
				[[_|_] = Rs] ->
					Rs;
				[_|_] = Rs ->
					lists:flatten(Rs)
			end,
			case Limit of
				none ->
					From ! {?MAPREDUCE_REPLY, self(), retrievaler_util:bool_eval(BoolExpre, Keys, ResultSet)};
				_ ->
					io:format("!!!!!!BoolExpre:~p, Keys:~p, ResultSet:~p~n", [BoolExpre, Keys, ResultSet]),
					From ! {?MAPREDUCE_REPLY, self(), limit_amount(retrievaler_util:bool_eval(BoolExpre, Keys, ResultSet), Limit, 0, [])}
			end;
		timeout ->
			From ! {'ERROR', self(), {error, timeout}};
		{error, _, _} = E ->
			From ! {'ERROR', self(), E}
	end.

%%
%% Function: count/3
%% Description: 检索满足指定布尔表达式的反向索引数量
%% Arguments: 
%%	TimeRange
%%	BoolExpre
%%	Timeout
%% Returns: {ok, ResultSet} | {error,Error}
%% 
count(TimeRange, BoolExpre, Timeout) ->
	case retrieval(TimeRange, BoolExpre, none, Timeout) of
		{ok, ResultSet} ->
			length(ResultSet);
		E ->
			E
	end.

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init(Args) ->
	process_flag(trap_exit, true),
	case z_lib:get_value(Args, ?INDEX_DIR_PATH, "") of
		[_|_] = IndexDirPath ->
			KGremIndexCount=z_lib:get_value(Args, ?KGREM_INDEX_COUNT, ?DEFAULT_KGREM_INDEX_COUNT),
			DictCount=z_lib:get_value(Args, ?DICT_COUNT, ?DEFAULT_DICT_COUNT),
			InvertedCount=z_lib:get_value(Args, ?INVERTED_COUNT, ?DEFAULT_INVERTED_COUNT),
			CollateTime=z_lib:get_value(Args, ?COLLATE_TIME, ?DEFAULT_COLLATE_TIME),
			CacheTimeout=z_lib:get_value(Args, ?CACHE_TIMEOUT, ?DEFAULT_CACHE_TIMEOUT),
			CacheCapacity=z_lib:get_value(Args, ?CACHE_CAPACITY, ?DEFAULT_CACHE_CAPACITY),
			Cache = zm_cache:new(CacheCapacity, CacheTimeout),
			erlang:start_timer(CollateTime, self(), ?CACHE_COLLATE),
		  	{ok, #state{path = IndexDirPath, kgram_index_count = KGremIndexCount, dict_count = DictCount,  
						inverted_count = InvertedCount, receives = sb_trees:empty(), 
						collate_time = CollateTime, cache_capacity = CacheCapacity, cache = Cache}, ?RETRIEVAL_HIBERNATE_TIMEOUT};
		"" ->
		  erlang:error({error, invalid_path})
	end.    

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call({?RETRIEVAL_FLAG, _Src, TimeRange, BoolExpre, Limit, Timeout}, From,
			#state{path = IndexDirPath, kgram_index_count = KGRemIndexCount, dict_count = DictCount, 
				   inverted_count = InvertedCount, receives = Receives} = State) ->
	Pid=spawn_link(?MODULE, retrieval_reduce, 
			   [self(), TimeRange, IndexDirPath, KGRemIndexCount, DictCount, InvertedCount, BoolExpre, Limit, Timeout]),
    {noreply, State#state{receives = sb_trees:insert(Pid, From, Receives)}, ?RETRIEVAL_HIBERNATE_TIMEOUT};
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?RETRIEVAL_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State, ?RETRIEVAL_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({Reply, Pid, Info}, #state{receives = Receives} = State) ->
	case sb_trees:lookup(Pid, Receives) of
		{Pid, From} ->
			%TODO 以后查询优化(如读缓存)操作在此处进行...
			Result=case Reply of
				?MAPREDUCE_REPLY ->
					{ok, Info};
				'ERROR' ->
					Info;
				'EXIT' ->
					{error, Info}
			end,
			z_lib:reply(From, Result),
			{noreply, State#state{receives = sb_trees:delete_any(Pid, Receives)}, ?RETRIEVAL_HIBERNATE_TIMEOUT};
		none ->
			{noreply, State, ?RETRIEVAL_HIBERNATE_TIMEOUT}
	end;
handle_info({timeout, _Ref, ?CACHE_COLLATE}, #state{collate_time = CollateTime, cache = Cache} = State) ->
	zm_cache:collate(Cache),
	erlang:start_timer(CollateTime, self(), ?CACHE_COLLATE),
    {noreply, State, ?RETRIEVAL_HIBERNATE_TIMEOUT};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State, ?RETRIEVAL_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

to_second({{{_, _, _}, {_, _, _}} = StartTime, {{_, _, _}, {_, _, _}} = TailTime}) ->
	{z_lib:localtime_to_second(StartTime), z_lib:localtime_to_second(TailTime)};
to_second({_, _} = TimeRange) ->
	TimeRange.

to_minute({StartTime, TailTime}) 
  when is_integer(StartTime), is_integer(TailTime), StartTime > 0, TailTime > 0 ->
	{utc_to_minute(StartTime), utc_to_minute(TailTime)};
to_minute({{{_, _, _}, {_, _, _}}, {{_, _, _}, {_, _, _}}} = TimeRange) ->
	to_minute(to_second(TimeRange)).

to_week({StartTime, TailTime}) 
  when is_integer(StartTime), is_integer(TailTime), StartTime > 0, TailTime > 0 ->
	{year_week(StartTime), year_week(TailTime)}.

get_expre_keys([{Type, Val}|T], OutStack, InStack) when is_atom(Type) ->
	get_expre_keys(T, OutStack, [Val|InStack]);
get_expre_keys([{MeshSize, OP}|T], [], InStack) ->
	{[Val0, Val1], NewInStack}=if
		length(InStack) >= MeshSize ->
			key_pop(InStack, [], MeshSize);
		true ->
			erlang:error({error, {invalid_mesh_size, OP, MeshSize}})
	end,
	get_expre_keys(T, filter_key(OP, Val0, Val1, []), NewInStack);
get_expre_keys([{_MeshSize, OP}|T], [Val0|NewOutStack], [Val1|NewInStack]) ->
	get_expre_keys(T, filter_key(OP, Val0, Val1, NewOutStack), NewInStack);
get_expre_keys([], OutStack, _InStack) ->
	lists:reverse([filter_tuple(Tmp) || Tmp <- OutStack]).

key_pop(T, Vals, 0) ->
	{Vals, T};
key_pop([Val|T], Vals, MeshSize) ->
	key_pop(T, [Val|Vals], MeshSize - 1).

filter_key('&', Val0, Val1, Stack) ->
	case public_lib:is_lists(Val0) of
		true ->
			[[Val1|Val0]|Stack];
		false when is_tuple(Val0) ->
			[[Val1], Val0|Stack];
		false ->
			[[Val1, Val0]|Stack]
	end;
filter_key(_, Val0, Val1, Stack) ->
	case public_lib:is_lists(Val0) of
		true ->
			[{Val1}, Val0|Stack];
		false when is_tuple(Val0) ->
			[{Val1}, Val0|Stack];
		false ->
			[{Val1}, {Val0}|Stack]
	end.

filter_tuple({X}) ->
	X;
filter_tuple(X) ->
	X.

limit_amount([{_DocId, File, Offset}|T], inf, Length, L) ->
	limit_amount(T, inf, Length + 1, [{File, Offset}|L]);
limit_amount([_|T], {Start, _} = Limit, Length, L) when Length < Start ->
	limit_amount(T, Limit, Length + 1, L);
limit_amount([{_DocId, File, Offset}|T], {_, Tail} = Limit, Length, L) when Length =< Tail ->
	limit_amount(T, Limit, Length + 1, [{File, Offset}|L]);
limit_amount([_|_], {_, Tail}, Length, L) when Length > Tail->
	lists:reverse(L);
limit_amount([{_DocId, File, Offset}|T], Limit, Length, L) when Length < Limit ->
	limit_amount(T, Limit, Length + 1, [{File, Offset}|L]);
limit_amount([_|_], Limit, Limit, L) ->
	lists:reverse(L);
limit_amount([], _Limit, _Length, L) ->
	lists:reverse(L).

utc_to_minute(Second) when is_integer(Second), Second >= 0 ->
	Second div 60.

year_week(Second) when is_integer(Second) ->
	year_week(z_lib:second_to_localtime(Second));
year_week({{_Year, _, _} = Date, {_, _, _}}) ->
	calendar:iso_week_number(Date);
year_week({_Year, _, _} = Date) ->
	calendar:iso_week_number(Date).


