%%% -------------------------------------------------------------------
%%% Author  : Administrator
%%% Description : 三字索引构建器，为词典构建索引

%%% Created : 2012-8-15
%%% -------------------------------------------------------------------
-module(three_gram_indexer).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("three_gram_indexer.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, indexing/2,
		 get_file_path/2, get_k_gram_filename/2, 
		 three_gram/1, exist_wildcard/1, three_gram/3, three_gram_/2, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% External functions
%% ====================================================================

%%
%% Function: start_link/1
%% Description: 启动三字索引构建器
%% Arguments: 
%%	Args
%% Returns: {ok,Pid} | ignore | {error,Error}
%% 
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%% Function: indexing/2
%% Description: 生成三字索引
%% Arguments: 
%%	DocDate
%%	Tokens
%% Returns:  ok | {error, Reason}
%% 
indexing(_, []) ->
	ok;
indexing({_, _, _} = DocDate, Tokens) when is_list(Tokens) ->
	gen_server:cast(?MODULE, {?K_GRAM_FLAG, DocDate, Tokens}).

%%
%% Function: get_file_path/2
%% Description: 获取指定文件名的索引文件路径
%% Arguments: 
%%	IndexDirPath
%%	FileName
%% Returns:  ok | {error, Reason}
%% 
get_file_path(IndexDirPath, FileName) ->
	filename:join(IndexDirPath, FileName).
	
%%
%% Function: get_k_gram_filename/2
%% Description: 获取指定时间所对应的三字索引文件名
%% Arguments: 
%%	Date
%%	Hash
%% Returns:  ok | {error, Reason}
%% 
get_k_gram_filename({_, _, _} = Date, Hash) ->
	{Year, Week}=calendar:iso_week_number(Date),
	Suffix=lists:concat([Year, ?INDEX_FILE_SPLIT_CHAR, Week, ?INDEX_FILE_SPLIT_CHAR, Hash]),
	lists:concat([?K_GRAM_FILE_PREFIX, Suffix]);
get_k_gram_filename({Year, Week}, Hash) ->
	Suffix=lists:concat([Year, ?INDEX_FILE_SPLIT_CHAR, Week, ?INDEX_FILE_SPLIT_CHAR, Hash]),
	lists:concat([?K_GRAM_FILE_PREFIX, Suffix]);
get_k_gram_filename(Second, Hash) when is_integer(Second) ->
	{Date, _}=z_lib:second_to_localtime(Second),
	get_k_gram_filename(Date, Hash).

%%
%% Function: three_gram/2
%% Description: 将有通配符的词条转换为三字词条列表
%% Arguments: 
%%	Token
%% Returns:  Tokens
%% 
three_gram(Token) when is_list(Token) ->
	case exist_wildcard(Token) of
		false ->
			[Token];
		true ->
			{Tokens, _}=lists:unzip(three_gram(three_gram_(Token, []), Token, [])),
			Tokens
	end;
three_gram(Token) when is_number(Token) ->
	[Token].

%%
%% Function: exist_wildcard/1
%% Description: 判断是否包括通配符
%% Arguments: 
%%	Token
%% Returns:  ok | {error, Reason}
%% 
exist_wildcard(Token) ->
	case string:chr(Token, ?RETRIEVALER_WILDCARD) of
		0 ->
			false;
		_ ->
			true
	end.

%%
%%关闭三字索引器
%%
stop(Reason) ->
	gen_server:call(?MODULE, {stop, Reason}).

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
	IndexDirPath=case z_lib:get_value(Args, ?INDEX_DIR_PATH, "") of
					  [_|_] = Path ->
						  Path;
					  "" ->
						  erlang:error(invalid_path)
	end,
	case filelib:ensure_dir(IndexDirPath) of
		ok ->
			IndexCount=z_lib:get_value(Args, ?INDEX_COUNT, ?DEFAULT_INDEX_COUNT),
			CollateTime=z_lib:get_value(Args, ?COLLATE_TIME, ?DEFAULT_COLLATE_TIME),
			erlang:start_timer(CollateTime, self(), ?QUEUE_COLLATE),
		    {ok, #state{path = IndexDirPath, index_count = IndexCount, lock = 0,
						collate_time = CollateTime, wait_queue = queue:new()}, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT};
		{error, Reason} ->
			erlang:error({invalid_index_dir_path, Reason})
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
handle_call({stop, Reason}, _From, State) ->
	{stop, Reason, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({?K_GRAM_FLAG, DocDate, Tokens}, #state{wait_queue =Queue} = State) ->
	{Items, _} = lists:unzip(Tokens),
	{noreply, State#state{wait_queue = queue:in({DocDate, Items}, Queue)}, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT};
handle_cast(_Msg, State) ->
    {noreply, State, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({timeout, _Ref, ?QUEUE_COLLATE}, #state{path = IndexDirPath, index_count = Range, lock = Lock, 
													collate_time = CollateTime, wait_queue = Queue} = State) ->
	Self=self(),
	case Lock of
		?LOCK_UNUSED ->
			case queue:out(Queue) of
				{{value, {DocDate, Tokens}}, NewQueue} ->
					NewLock=index_handler(IndexDirPath, Range, DocDate, Tokens),
					erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
					{noreply, State#state{lock = NewLock, wait_queue = NewQueue}, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT};
				{empty, _} ->
					erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
					{noreply, State, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT}
			end;
		_ ->
			erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
			{noreply, State, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT}
	end;
handle_info({?MAP_REPLY, ok, _Info}, #state{lock = Lock} = State) ->
	{noreply, State#state{lock = Lock - 1}, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT};
handle_info({?MAP_REPLY, _DocDate, Type, Reason}, State) ->
	{stop, {Type, Reason}, State};
handle_info({'EXIT', _Pid, Why}, State) ->
	{stop, Why, State};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State, ?THREE_GRAM_INVERTED_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    close().

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

index_handler(IndexDirPath, Range, DocDate, Items) ->
	From=self(),
	TokensMerged=document_analyzer:k_gram_mapping(Range, item_handler(Items, [])),
	map_handler(TokensMerged, From, IndexDirPath, DocDate, ?LOCK_UNUSED).
	
map_handler([], _, _, _, LockNum) ->
	LockNum;
map_handler([{Hash, Tokens}|T], From, IndexDirPath, DocDate, LockNum) ->
	spawn_link(fun() -> 
					   {Reply, Info}=default_index_handler(IndexDirPath, DocDate, Hash, Tokens),
					   unlink(From),
					   From ! {?MAP_REPLY, Reply, Info}
			   end),
	map_handler(T, From, IndexDirPath, DocDate, LockNum + 1).

default_index_handler(IndexDirPath, DocDate, Hash, TokensGramed) ->
	KGramFileName=get_k_gram_filename(DocDate, Hash),
	KGramFile=get_file_path(IndexDirPath, KGramFileName),
	KGramTable=?K_GRAM_TABLE(Hash),
	case filelib:is_file(KGramFile) of
		false ->
			dets:close(KGramTable);
		true ->
			continue
	end,
	case dets:open_file(KGramTable, [{file, KGramFile}, {auto_save, 1000}]) of
		{ok, _} ->
			merge_k_gram_file(TokensGramed, KGramTable, []);
		{error, Reason} ->
			{open_k_gram_file_error, {KGramFile, Reason}}
	end.

item_handler([], L) ->
	L;
item_handler([Token|T], L) ->
	case z_lib:is_iodata(Token) of
		true ->
			item_handler(T, three_gram(three_gram_(Token, []), Token, L));
		false ->
			item_handler(T, L)
	end.

three_gram([_X, _Y, _Z] = Gram, Token, L) ->
	update_tokens(Gram, Token, L);
three_gram([X|T], Token, L) ->
	case T of
		[Y, Z|_] ->
			three_gram(T, Token, update_tokens([X, Y, Z], Token, L));
		[?RETRIEVALER_WILDCARD] ->
			three_gram(T, Token, L);
		[] ->
			three_gram(T, Token, L)
	end;
three_gram(_, _, L) ->
	L.

update_tokens(Gram, Token, L) ->
	case lists:keyfind(Gram, 1, L) of
		{Gram, Tokens} ->
			lists:keyreplace(Gram, 1, L, {Gram, unique_token(Token, Tokens)});
		false ->
			lists:keystore(Gram, 1, L, {Gram, [Token]})
	end.
	
three_gram_([H|T], L) ->
	three_gram_(T, [H|L]);
three_gram_([], L) ->
	[$\$|lists:reverse([$\$|L])].

merge_k_gram_file([], KGramTable, Items) ->
	case dets:insert(KGramTable, Items) of
		ok ->
			{ok, length(Items)};
		{merge_k_gram_file_error, _} = E ->
			E
	end;
merge_k_gram_file([{Gram, Tokens} = Item|T], KGramTable, Items) ->
	case dets:lookup(KGramTable, Gram) of
		[{Gram, OldTokens}] ->
			merge_k_gram_file(T, KGramTable, [{Gram, unique_tokens(Tokens, OldTokens)}|Items]);
		[] ->
			merge_k_gram_file(T, KGramTable, [Item|Items]);
		{merge_k_gram_file_error, _} = E ->
			E
	end.

unique_token(Token, Tokens) ->
	case lists:member(Token, Tokens) of
		true ->
			Tokens;
		false ->
			[Token|Tokens]
	end.

unique_tokens([], Tokens) ->
	Tokens;
unique_tokens([Token|T], Tokens) ->
	unique_tokens(T, unique_token(Token, Tokens)).

close() ->
	[dets:close(?K_GRAM_TABLE(Num)) || Num <- lists:seq(1, ?DEFAULT_INDEX_COUNT)].
	


