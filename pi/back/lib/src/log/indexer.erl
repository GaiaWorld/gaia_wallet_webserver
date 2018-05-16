%%% -------------------------------------------------------------------
%%% Author  : Administrator
%%% Description : 索引构建器
%%%
%%% Created : 2012-8-1
%%% -------------------------------------------------------------------
-module(indexer).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("indexer.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, indexing/3, batch_indexing/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% External functions
%% ====================================================================

%%
%% Function: start_link/1
%% Description: 启动索引构建器
%% Arguments: 
%%	Args
%% Returns: {ok,Pid} | ignore | {error,Error}
%% 
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%% Function: indexing/4
%% Description: 构建索引
%% Arguments: 
%%	File
%%	Offset
%%	Time
%%	Doc
%% Returns: ok
%% 
indexing(File, Offset, Doc) ->
	gen_server:cast(?MODULE, {?INDEX_HANDLER_FLAG, self(), File, Offset, Doc}).

%%
%% Function: batch_indexing/1
%% Description: 批量构建索引
%% Arguments: 
%%	Docs文档列表, 结构:[{File, Offset, Doc}|T]
%% Returns: ok
%% 
batch_indexing(Docs) when is_list(Docs) ->
	gen_server:cast(?MODULE, {?BATCH_INDEX_HANDLER_FLAG, self(), Docs}).

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Arguments: 
%%	TokenHandler
%%	LanguageHandler
%%	IndexHandler索引处理器, {M, F, A}, F(Args, Tokens) | non
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init(Args) ->
	process_flag(trap_exit, true),
	TokenHandler=z_lib:get_value(Args, ?TOKEN_HANDLER, ?DEFAULT_HANDLER), 
	LanguageHandler=z_lib:get_value(Args, ?LANGUAGE_HANDLER, ?DEFAULT_HANDLER), 
	IsKGram=z_lib:get_value(Args, ?IS_K_GRAM, ?DEFAULT_K_GRAM),
	CacheTimeout=z_lib:get_value(Args, ?CACHE_TIMEOUT, ?DEFAULT_CACHE_TIMEOUT),
	CacheCapacity=z_lib:get_value(Args, ?CACHE_CAPACITY, ?DEFAULT_CACHE_CAPACITY),
	Cache=zm_cache:new(CacheCapacity, CacheTimeout),
	if
		IsKGram ->
			case three_gram_indexer:start_link(Args) of
				{ok, _} ->
					ok;
				E ->
					erlang:error({error, {three_gram_indexer_start_failed, E}})
			end;
		true ->
			continue
	end,
	case doc_handler:start_link(TokenHandler, LanguageHandler) of
		{ok, DocHandler} ->
			case indexer_storage:start_link(Args) of
				{ok, _} ->
					Self=self(),
					erlang:start_timer(?CLOCK_FREQUENCY, Self, ?CLOCK),
					erlang:start_timer(CacheTimeout, Self, ?CACHE_COLLATE),
				    {ok, #state{doc_handler = DocHandler, is_k_gram = IsKGram,
								cur = ?UINT32_START, max = ?MAX_UINT32_LENGTH, start_time = z_lib:now_second(), 
								cache_timeout = CacheTimeout, cache_capacity = CacheCapacity, cache = Cache, self = Self}, ?INDEXER_HIBERNATE_TIMEOUT};
				E_ ->
					erlang:error({error, {indexer_storage_start_failed, E_}})
			end;
		E_ ->
			erlang:error({error, {doc_handler_start_failed, E_}})
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
handle_call(_Request, _From, State) ->
    {noreply, State, ?INDEXER_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({?INDEX_HANDLER_FLAG, From, File, Offset, Doc}, #state{doc_handler = DocHandler, cur = I, max = Max, start_time = StartTime} = State) when I =< Max ->
	case doc_handle(From, DocHandler, StartTime, I, File, Offset, Doc) of
		ok ->
			{noreply, State#state{cur = I + 1}, ?INDEXER_HIBERNATE_TIMEOUT};
		{ok, NewStartTime} ->
			{noreply, State#state{cur = I + 1, start_time = NewStartTime}, ?INDEXER_HIBERNATE_TIMEOUT}
	end;
handle_cast({?BATCH_INDEX_HANDLER_FLAG, From, Docs}, #state{doc_handler = DocHandler, cur = I, max = Max, start_time = StartTime} = State) when (I + length(Docs)) =< Max ->
	{ok, NewStartTime, NewI} = batch_doc_handle(Docs, From, DocHandler, StartTime, I),
	{noreply, State#state{cur = NewI, start_time = NewStartTime}, ?INDEXER_HIBERNATE_TIMEOUT};
handle_cast({?INDEX_HANDLER_FLAG, From, _Date, File, Offset, _Doc}, State) ->
	From ! {?REPLY_KEY, {{error, ?ERROR_OUT_OF_DOCID}, File, Offset}},
	{stop, ?ERROR_OUT_OF_DOCID, State};
handle_cast({?BATCH_INDEX_HANDLER_FLAG, From, _Docs}, State) ->
	From ! {?BATCH_REPLY_KEY, {error, ?ERROR_OUT_OF_DOCID}},
	{stop, ?ERROR_OUT_OF_DOCID, State};
handle_cast(Msg, State) ->
	io:format("!!!!!!Msg:~p~n", [Msg]),
    {noreply, State, ?INDEXER_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({?MAP_REPLY, From, DocDate, Tokens, File, Offset}, #state{is_k_gram = IsKGram, cache_capacity = CacheCapacity, cache = Cache} = State) ->
	Reply=cache_commit(CacheCapacity, Cache, DocDate, Tokens, IsKGram, true),
%% 	From ! {?REPLY_KEY, {Reply, File, Offset}},
	{noreply, State, ?INDEXER_HIBERNATE_TIMEOUT};
handle_info({timeout, _Ref, ?CACHE_COLLATE}, #state{is_k_gram = IsKGram, cache_timeout = CacheTimeout, cache_capacity = CacheCapacity, cache = Cache, self = Self} = State) ->
	ok=cache_commit(CacheCapacity, Cache, none, [], IsKGram, false),
	erlang:start_timer(CacheTimeout, Self, ?CACHE_COLLATE),
	{noreply, State, ?INDEXER_HIBERNATE_TIMEOUT};
handle_info({timeout, _Ref, ?CLOCK}, #state{doc_handler = DocHandler, cur = I, max = Max, start_time = StartTime, self = Self} = State) when I =< Max ->
	Time=z_lib:now_millisecond(),
	erlang:start_timer(?CLOCK_FREQUENCY, Self, ?CLOCK),
	case clock_doc_handle(Self, DocHandler, StartTime, I, 0, {0, 0}, [{?DOC_SYS_TIME_KEY, Time}]) of
		ok ->
			{noreply, State#state{cur = I + 1}, ?INDEXER_HIBERNATE_TIMEOUT};
		{ok, NewStartTime} ->
			{noreply, State#state{cur = I + 1, start_time = NewStartTime}, ?INDEXER_HIBERNATE_TIMEOUT}
	end;
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State, ?INDEXER_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(out_of_docid, #state{is_k_gram = IsKGram, cache_capacity = CacheCapacity, cache = Cache}) ->
	ok=cache_commit(CacheCapacity, Cache, none, [], IsKGram, false);
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

doc_handle(From, DocHandler, StartTime, I, File, Offset, Doc) ->
	Requce=self(),
	DocDate=get_doc_date(Doc), 
	case get_uint64(StartTime, I) of
			{ok, DocId} ->
				DocHandler ! {?DOC_HANDLE, Requce, From, File, Offset, DocId, DocDate, Doc},
				ok;
			{ok, DocId, NewStartTime} ->
				DocHandler ! {?DOC_HANDLE, Requce, From, File, Offset, DocId, DocDate, Doc},
				{ok, NewStartTime}
	end.

batch_doc_handle([], _, _, StartTime, I) ->
	{ok, StartTime, I};
batch_doc_handle([{File, Offset, Doc}|T], From, DocHandler, StartTime, I) ->
	case doc_handle(From, DocHandler, StartTime, I, File, Offset, Doc) of
		ok ->
			batch_doc_handle(From, T, DocHandler, StartTime, I + 1);
		{ok, NewStartTime} ->
			batch_doc_handle(From, T, DocHandler, NewStartTime, I + 1)
	end.

clock_doc_handle(From, DocHandler, StartTime, I, File, Offset, Doc) ->
	Requce=self(),
	DocDate=get_doc_date(Doc),
	case get_uint64(StartTime, I) of
			{ok, DocId} ->
				DocHandler ! {?DOC_HANDLE, Requce, From, File, Offset, DocId, DocDate, Doc},
				ok;
			{ok, DocId, NewStartTime} ->
				DocHandler ! {?DOC_HANDLE, Requce, From, File, Offset, DocId, DocDate, Doc},
				{ok, NewStartTime}
	end.
get_doc_date(Doc) ->
	{DocDate, _} = z_lib:second_to_localtime(z_lib:get_value(Doc, ?DOC_SYS_TIME_KEY, "") div 1000),
	DocDate.

get_uint64(OldStartTime, Uid) ->
	NowTime=z_lib:now_second(),
	NowDay=public_lib:day_of_year(NowTime),
	case public_lib:day_of_year(OldStartTime) of
		NowDay ->
			{ok, ((OldStartTime band 16#ffffffff) bsl 32) bor (Uid band 16#ffffffff)};
		_ ->
			{ok, ((NowTime band 16#ffffffff) bsl 32) bor (Uid band 16#ffffffff), NowTime}
	end.
 

cache_commit(CacheCapacity, Cache, DocDate, Tokens, IsKGram, IsCheckCapacity) ->
	TokensSize=tokens_size(Tokens),
	NewCacheSize=zm_cache:memory_size(Cache) * erlang:system_info(wordsize) + TokensSize,
	if
		(CacheCapacity > NewCacheSize) and IsCheckCapacity ->
			update_cache(Cache, DocDate, Tokens);
		true ->
			update_cache(Cache, DocDate, Tokens),
			case lists:keysort(2, zm_cache:get(Cache)) of 
				[{{_, Date} = CacheKey, CacheValue, _}] ->
					true=zm_cache:delete(Cache, CacheKey),
					k_gram_indexing(IsKGram, Date, CacheValue),
					traverse_indexer:indexing(Date, CacheValue),
					indexer_storage:inverting(Date, CacheValue);
				[{{_, Date} = CacheKey, CacheValue, _}, {{_, Date1, _} = CacheKey1, CacheValue1, _}] ->
					true=zm_cache:delete(Cache, CacheKey),
					k_gram_indexing(IsKGram, Date, CacheValue),
					traverse_indexer:indexing(Date, CacheValue),
					indexer_storage:inverting(Date, CacheValue),
					true=zm_cache:delete(Cache, CacheKey1),
					k_gram_indexing(IsKGram, Date, CacheValue1),
					traverse_indexer:indexing(Date1, CacheValue1),
					indexer_storage:inverting(Date1, CacheValue1);
				[] ->
					ok
			end
	end.

tokens_size(Tokens) ->
	erlang:external_size(Tokens).

update_cache(_, _, []) ->
	ok;
update_cache(Cache, DocDate, Tokens) ->
	CacheKey={?MODULE, DocDate},
	case zm_cache:get(Cache, CacheKey) of
		{ok, CacheValue} ->
			zm_cache:set(Cache, CacheKey, CacheValue ++ Tokens);
		none ->
			zm_cache:set(Cache, CacheKey, Tokens)
	end,
	ok.

k_gram_indexing(false, _, _) ->
	ok;
k_gram_indexing(true, Date, CacheValue) ->
	three_gram_indexer:indexing(Date, CacheValue).
