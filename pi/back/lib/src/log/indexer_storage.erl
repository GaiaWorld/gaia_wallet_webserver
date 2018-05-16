%% Author: Administrator
%% Created: 2012-8-1
%% Description: 索引存储器
%%
-module(indexer_storage).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("indexer_storage.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/1, inverting/2, 
		 get_file_path/2, get_inverted_filename/2, get_dict_filename/2,
		 uncompress/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% External functions
%% ====================================================================

%%
%% Function: start_link/1
%% Description: 启动索引存储器
%% Arguments: 
%%	Args参数
%% Returns: {ok,Pid} | ignore | {error,Error}
%% 
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%
%% Function: inverting/2
%% Description: 生成词典和反向索引
%% Arguments: 
%%	DocDate
%%	Tokens
%% Returns:  ok | {error, Reason}
%% 
inverting(_, []) ->
	ok;
inverting({_, _, _} = DocDate, Tokens) when is_list(Tokens) ->
	gen_server:cast(?MODULE, {?STORAGE_FLAG, DocDate, Tokens}).

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
%% Function: get_inverted_filename/2
%% Description: 获取指定时间所对应的反向索引文件名
%% Arguments: 
%%	DocDate
%%	Hash
%% Returns:  ok | {error, Reason}
%% 
get_inverted_filename({_, _, _} = DocDate, Hash) ->
	{Year, Week}=calendar:iso_week_number(DocDate),
	Suffix=lists:concat([Year, ?INDEX_FILE_SPLIT_CHAR, Week, ?INDEX_FILE_SPLIT_CHAR, Hash]),
	lists:concat([?INVERTED_FILE_PREFIX, Suffix]);
get_inverted_filename({Year, Week}, Hash) ->
	Suffix=lists:concat([Year, ?INDEX_FILE_SPLIT_CHAR, Week, ?INDEX_FILE_SPLIT_CHAR, Hash]),
	lists:concat([?INVERTED_FILE_PREFIX, Suffix]);
get_inverted_filename(Second, Hash) when is_integer(Second) ->
	{DocDate, _}=z_lib:second_to_localtime(Second),
	get_inverted_filename(DocDate, Hash).

%%
%% Function: get_dict_filename/2
%% Description: 获取指定时间所对应的词典文件名
%% Arguments: 
%%	DocDate
%%	Hash
%% Returns:  ok | {error, Reason}
%% 
get_dict_filename({_, _, _} = DocDate, Hash) ->
	{Year, Week}=calendar:iso_week_number(DocDate),
	Suffix=lists:concat([Year, ?INDEX_FILE_SPLIT_CHAR, Week, ?INDEX_FILE_SPLIT_CHAR, Hash]),
	lists:concat([?DICT_FILE_PREFIX, Suffix]);
get_dict_filename({Year, Week}, Hash) ->
	Suffix=lists:concat([Year, ?INDEX_FILE_SPLIT_CHAR, Week, ?INDEX_FILE_SPLIT_CHAR, Hash]),
	lists:concat([?DICT_FILE_PREFIX, Suffix]);
get_dict_filename(Second, Hash) when is_integer(Second) ->
	{DocDate, _}=z_lib:second_to_localtime(Second),
	get_dict_filename(DocDate, Hash).

%%
%% Function: uncompress/1
%% Description: 解压缩数据
%% Arguments: 
%%	CompressData
%% Returns:  DocInfoList
%% 
uncompress([<<_/binary>>|_] = CompressData) ->
	binary_to_term(binary:list_to_bin(z_lib:uncompress(CompressData)));
uncompress({_, DocInfoList}) ->
	DocInfoList.

%%
%%关闭索引存储器
%%
stop(Reason) ->
	gen_server:call(?MODULE, {stop, Reason}).

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Arguments: 
%%	IndexDirPath
%%	InvertedHandler
%%	DictHandler
%%	返回ok | {error, Reason}
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
						  erlang:error({error, invalid_path})
	end,
	case filelib:ensure_dir(IndexDirPath) of
		ok ->
			Self=self(),
			InvertedCount=z_lib:get_value(Args, ?INVERTED_COUNT, ?DEFAULT_INVERTED_COUNT),
			MergeMapHandler=z_lib:get_value(Args, ?MERGE_MAP_HANDLER, ?DEFAULT_HANDLER),
			InvertedHandler=z_lib:get_value(Args, ?INVERTED_HANDLER, ?DEFAULT_HANDLER),
			DictCount=z_lib:get_value(Args, ?DICT_COUNT, ?DEFAULT_DICT_COUNT),
			DictMapHanler=z_lib:get_value(Args, ?INVERTED_HANDLER, ?DEFAULT_HANDLER),
			DictHandler=z_lib:get_value(Args, ?DICT_HANDLER, ?DEFAULT_HANDLER),
			CollateTime=z_lib:get_value(Args, ?COLLATE_TIME, ?DEFAULT_COLLATE_TIME),
			erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
			{ok, #state{path = IndexDirPath, inverted_count = InvertedCount, 
						merge_map_handler = MergeMapHandler, inverted_handler = InvertedHandler, 
						dict_count = DictCount, dict_map_handler = DictMapHanler, dict_handler = DictHandler, 
						lock = ?LOCK_UNUSED, collate_time = CollateTime, wait_queue = queue:new()}, ?INVERTED_HIBERNATE_TIMEOUT};
		{error, Reason} ->
			erlang:error({error, invalid_index_dir_path, Reason})
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
    {noreply, State, ?INVERTED_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({?STORAGE_FLAG, DocDate, Tokens}, #state{wait_queue = Queue} = State) ->
	{noreply, State#state{wait_queue = queue:in({DocDate, Tokens}, Queue)}, ?INVERTED_HIBERNATE_TIMEOUT};
handle_cast(_Msg, State) ->
    {noreply, State, ?INVERTED_HIBERNATE_TIMEOUT}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({timeout, _Ref, ?QUEUE_COLLATE}, #state{path = IndexDirPath, inverted_count = InvertedRange, 
													merge_map_handler = MergeMapHandler, inverted_handler = InvertedHandler, 
													lock = Lock, collate_time = CollateTime, wait_queue = Queue} = State) ->
	Self=self(),
	case Lock of
		?LOCK_UNUSED ->
			case queue:out(Queue) of
				{{value, {DocDate, Tokens}}, NewQueue} ->
					NewLock=inverted_handler(IndexDirPath, MergeMapHandler, InvertedRange, InvertedHandler, DocDate, Tokens),
					erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
					{noreply, State#state{lock = NewLock, wait_queue = NewQueue}, ?INVERTED_HIBERNATE_TIMEOUT};
				{empty, _} ->
					erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
					{noreply, State, ?INVERTED_HIBERNATE_TIMEOUT}
			end;
		_ ->
			erlang:start_timer(CollateTime, Self, ?QUEUE_COLLATE),
			{noreply, State, ?INVERTED_HIBERNATE_TIMEOUT}
	end;
handle_info({?MAP_REPLY, DocDate, ok, DictInfo}, #state{path = IndexDirPath, dict_count = DictRange, 
														dict_map_handler = DictMapHandler, dict_handler = DictHandler, lock = Lock} = State) ->
	case dict_handler(IndexDirPath, DictMapHandler, DictRange, DictHandler, DocDate, DictInfo) of
		ok ->
			{noreply, State#state{lock = Lock - 1}, ?INVERTED_HIBERNATE_TIMEOUT};
		E ->
			{stop, E, State}
	end;
handle_info({?MAP_REPLY, _DocDate, Type, Reason}, State) ->
	{stop, {Type, Reason}, State};
handle_info({'EXIT', _Pid, Why}, State) ->
	{stop, Why, State};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State, ?INVERTED_HIBERNATE_TIMEOUT}.

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

inverted_handler(IndexDirPath, MergeMapHandler, Range, InvertedHandler, DocDate, Tokens) ->
	From=self(),
	TokensMerged=document_analyzer:merge_mapping(MergeMapHandler, Range, Tokens),
	inverted_map_handler(TokensMerged, From, IndexDirPath, InvertedHandler, DocDate, ?LOCK_UNUSED).

inverted_map_handler([], _, _, _, _, LockNum) ->
	LockNum;
inverted_map_handler([{Hash, Tokens}|T], From, IndexDirPath, InvertedHandler, DocDate, LockNum) ->
	case InvertedHandler of
		none ->
			spawn_link(fun() -> 
							   {Reply, Info}=default_inverted_handler(IndexDirPath, DocDate, Hash, Tokens),
							   unlink(From),
							   From ! {?MAP_REPLY, DocDate, Reply, Info}
					   end);
		{M, F, A} ->
			spawn_link(fun() -> 
							   {Reply, Info}=apply(M, F, [A, IndexDirPath, DocDate, Hash, Tokens]),
							   unlink(From),
							   From ! {?MAP_REPLY, DocDate, Reply, Info}
					   end)
	end,
	inverted_map_handler(T, From, IndexDirPath, InvertedHandler, DocDate, LockNum + 1).

default_inverted_handler(IndexDirPath, DocDate, Hash, Tokens) ->
	InvertedFileName=get_inverted_filename(DocDate, Hash),
	InvertedFile=get_file_path(IndexDirPath, InvertedFileName),
	InvertedTable=?INVERTED_TABLE(Hash),
	case filelib:is_file(InvertedFile) of
		false ->
			dets:close(InvertedTable);
		true ->
			continue
	end,
	case dets:open_file(InvertedTable, [{file, InvertedFile}, {auto_save, 1000}]) of
		{ok, _} ->
			merge_inverted_file(Tokens, InvertedTable, Hash, [], []);
		{error, Reason} ->
			{open_inverted_file_error, {InvertedFile, Reason}}
	end.

dict_handler(IndexDirPath, DictMapHandler, Range, DictHandler, DocDate, DictInfo) ->
	DictMaped=document_analyzer:dict_mapping(DictMapHandler, Range, DictInfo),
	dict_map_handler(DictMaped, IndexDirPath, DictHandler, DocDate).

dict_map_handler([], _, _, _) ->
	ok;
dict_map_handler([{Hash, DictInfo}|T], IndexDirPath, DictHandler, DocDate) ->
	case DictHandler of
		none ->
			case default_dict_handler(DictInfo, IndexDirPath, Hash, DocDate) of
				ok ->
					dict_map_handler(T, IndexDirPath, DictHandler, DocDate);
				E ->
					E
			end;
		{M, F, A} ->
			case apply(M, F, [A, IndexDirPath, DocDate, DictInfo]) of
				ok ->
					dict_map_handler(T, IndexDirPath, DictHandler, DocDate);
				E ->
					E
			end
	end.
	
default_dict_handler(DictInfo, IndexDirPath, Hash, DocDate) ->
	DictFileName=get_dict_filename(DocDate, Hash),
	DictFile=get_file_path(IndexDirPath, DictFileName),
	DictTable=?DICT_TABLE(Hash),
	case filelib:is_file(DictFile) of
		false ->
			dets:close(DictTable);
		true ->
			continue
	end,
	case dets:open_file(DictTable, [{file, DictFile}, {auto_save, 1000}]) of
		{ok, _} ->
			merge_dict_file(DictInfo, DictTable, []);
		{error, Reason} ->
			{open_dict_file_error, {DictFile, Reason}}
	end.
	
merge_dict_file([], DictTable, Items) ->
	case dets:insert(DictTable, Items) of
		ok ->
			ok;
		{merge_dict_file_error, _} = E ->
			E
	end;
merge_dict_file([{Token, {Frequency, File}} = Item|T], DictTable, Items) ->
	case dets:lookup(DictTable, Token) of
		[{Token, {TotalFrequency, _}}] ->
			merge_dict_file(T, DictTable, [{Token, {TotalFrequency + Frequency, File}}|Items]);
		[] ->
			merge_dict_file(T, DictTable, [Item|Items]);
		{merge_dict_file_error, _} = E ->
			E
	end.

merge_inverted_file([], InvertedTable, _, Items, DictInfo) ->
	case dets:insert(InvertedTable, Items) of
		ok ->
			{ok, DictInfo};
		{merge_inverted_file_error, _} = E ->
			E
	end;
merge_inverted_file([{Token, DocInfoList}|T], InvertedTable, File, Items, DictInfo) ->
	case dets:lookup(InvertedTable, Token) of
		[{Token, CompressInverted}] ->
			OldDocInfoList=uncompress(CompressInverted),
			case lists:keyfind(Token, 1, DictInfo) of
				{Token, {DocFrequency, File}} ->
					merge_inverted_file(T, InvertedTable, File, [{Token, compress(lists:keysort(1, OldDocInfoList ++ DocInfoList))}|Items], 
										lists:keyreplace(Token, 1, DictInfo, {Token, {DocFrequency + length(DocInfoList), File}}));
				false ->
					merge_inverted_file(T, InvertedTable, File, [{Token, compress(lists:keysort(1, OldDocInfoList ++ DocInfoList))}|Items], 
										[{Token, {length(DocInfoList), File}}|DictInfo])
			end;
		[] ->
			merge_inverted_file(T, InvertedTable, File, [{Token, compress(DocInfoList)}|Items], 
								[{Token, {length(DocInfoList), File}}|DictInfo]);
		{merge_inverted_file_error, _} = E ->
			E
	end.

compress(DocInfoList) ->
	Bin=erlang:external_size(DocInfoList),
	if
		size(Bin) > 16#1f ->
			z_lib:compress(Bin);
		true ->
			{1, DocInfoList}
	end.

close() ->
	[begin ok = dets:close(?DICT_TABLE(Num)) end || Num <- lists:seq(1, ?DEFAULT_DICT_COUNT)],
	[begin ok = dets:close(?INVERTED_TABLE(Num)) end || Num <- lists:seq(1, ?DEFAULT_INVERTED_COUNT)].
