%% @author Administrator
%% @doc 遍历索引构建器

-module(traverse_indexer).
-behaviour(gen_server).

%% ====================================================================
%% Include files
%% ====================================================================
-include("traverse_indexer.hrl").

%% ====================================================================
%% Exported Functions
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, indexing/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%%
%% Function: start_link/1
%% Description: 启动遍历索引构建器
%% Arguments: 
%%	Args参数
%% Returns: {ok,Pid} | ignore | {error,Error}
%% 
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%% Function: indexing/2
%% Description: 生成遍历索引
%% Arguments: 
%%	DocDate
%%	Tokens
%% Returns:  ok | {error, Reason}
%% 
indexing(_, []) ->
	ok;
indexing({_, _, _} = DocDate, Tokens) when is_list(Tokens) ->
	gen_server:cast(?MODULE, {?TRAVERSE_FLAG, DocDate, Tokens}).

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
	process_flag(trap_exit, true),
	IndexDirPath=case z_lib:get_value(Args, ?INDEX_DIR_PATH, "") of
					  [_|_] = Path ->
						  Path;
					  "" ->
						  erlang:error({error, invalid_path})
	end,
	case filelib:ensure_dir(IndexDirPath) of
		ok ->
			IndexCount=z_lib:get_value(Args, ?INDEX_COUNT, ?DEFAULT_INDEX_COUNT),
			TraverseBts=case z_lib:get_value(Args, ?TRAVERSE_NAME, "") of
						"" ->
							erlang:error({error, invalid_traverse_name});
						Name ->
							{ok, Bts}=bts:new(list_to_atom(Name), [{scope, 16#0}, {save_path, IndexDirPath}, {save_file, Name}]),
							Bts
					end,
			CollateTime=z_lib:get_value(Args, ?COLLATE_TIME, ?DEFAULT_COLLATE_TIME),
			erlang:start_timer(CollateTime, self(), ?QUEUE_COLLATE),
		    {ok, #state{path = IndexDirPath, index_count = IndexCount, keys = z_lib:get_value(Args, ?KEYS, []), traverse_storage = TraverseBts, 
						collate_time = CollateTime, wait_queue = queue:new()}, ?TRAVERSE_HIBERNATE_TIMEOUT};
		{error, Reason} ->
			erlang:error({error, {invalid_index_dir_path, Reason}})
	end.


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
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?TRAVERSE_HIBERNATE_TIMEOUT}.


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
handle_cast({?TRAVERSE_FLAG, DocDate, Tokens}, #state{keys = Keys, wait_queue =Queue} = State) ->
	{Items, Other} = lists:unzip(Tokens),
	{DocIds, _, _}=lists:unzip3(Other),
	{noreply, State#state{wait_queue = queue:in({DocDate, filter_kv(Items, DocIds, Keys, [])}, Queue)}, ?TRAVERSE_HIBERNATE_TIMEOUT};
handle_cast(_Msg, State) ->
    {noreply, State, ?TRAVERSE_HIBERNATE_TIMEOUT}.


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
handle_info({timeout, _Ref, ?QUEUE_COLLATE}, #state{traverse_storage=Storage, collate_time = CollateTime, wait_queue = Queue} = State) ->
	Self=self(),
	case queue:out(Queue) of
		{{value, {_DocDate, Tokens}}, NewQueue} ->
			case index_handler(Storage, Tokens) of
				ok ->
					erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
					{noreply, State#state{wait_queue = NewQueue}, ?TRAVERSE_HIBERNATE_TIMEOUT};
				{error, _} = E ->
					erlang:error(E)
			end;
		{empty, _} ->
			erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
			{noreply, State, ?TRAVERSE_HIBERNATE_TIMEOUT}
	end;
handle_info({'EXIT', _Pid, Why}, State) ->
	{stop, Why, State};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State, ?TRAVERSE_HIBERNATE_TIMEOUT}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, _State) ->
	io:format("!!!!!!Reason:~p~n", [Reason]),
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

filter_kv([], _, _, L) ->
	L;
filter_kv([Key|T], [DocId|T0], Keys, L) ->
	case lists:member(Key, Keys) of
		false ->
			filter_kv(T, T0, Keys, L);
		true ->
			[Value|T_]=T,
			[_|T0_]=T0,
			filter_kv(T_, T0_, Keys, [{{Key, Value}, DocId}|L])
	end.

index_handler(Storage, Items) ->
	merge_traverse_file(Items, Storage, []).

%%批量写入遍历索引
batch_insert_traverse([], _) ->
	ok;
batch_insert_traverse([{KV, DocIds}|T], Storage) ->
	case bts:lookup(Storage, KV, 3000) of
		none ->
			bts:insert(Storage, KV, DocIds, 0, 0, 3000);
		{error,root_not_exist} ->
			bts:insert(Storage, KV, DocIds, 0, 0, 3000);
		_ ->
			bts:update(Storage, KV, DocIds, 0, 0, 3000)
	end,
	batch_insert_traverse(T, Storage).

merge_docid({MinDocId, MaxDocId}, DocId) when MinDocId > DocId ->
	{DocId, MaxDocId};
merge_docid({MinDocId, MaxDocId}, DocId) when MaxDocId < DocId ->
	{MinDocId, DocId};
merge_docid({-1, MaxDocId}, DocId) ->
	{DocId, MaxDocId};
merge_docid({MinDocId, -1}, DocId) ->
	{MinDocId, DocId};
merge_docid(DocIds, _) ->
	DocIds.

merge_traverse_file([], Storage, Items) ->
	batch_insert_traverse(Items, Storage);
merge_traverse_file([{KV, DocId} = Item|T], Storage, Items) ->
	case bts:lookup(Storage, KV, 3000) of
		{{KV, OldDocIds}, _, _} ->
			merge_traverse_file(T, Storage, [{KV, merge_docid(OldDocIds, DocId)}|Items]);
		none ->
			merge_traverse_file(T, Storage, [{KV, {-1, DocId}}|Items]);
		{error, root_not_exist} ->
			merge_traverse_file(T, Storage, [{KV, {-1, DocId}}|Items]);
		{merge_k_gram_file_error, _} = E ->
			E
	end.
