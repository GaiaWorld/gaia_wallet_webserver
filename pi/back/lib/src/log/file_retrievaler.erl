%% Author: Administrator
%% Created: 2012-10-11
%% Description: 文件检索器
-module(file_retrievaler).

%%
%% Include files
%%
-include("retrievaler.hrl").

%%
%% Exported Functions
%%
-export([get/7]).

%%
%% API Functions
%%

%%
%% Function: get/7
%% Description: 获取指定关键字在指定时间范围内的反向索引, 文件只以读方式打开
%% Arguments: 
%%	YearWeekRange
%%	IndexDirPath
%%	KGRemIndexCount
%%	DictCount
%%	InvertedCount
%%	MinuteRange
%%	Key
%% Returns: {Key, Inverteds}
%% 
get({{StartYear, StartWeek}, {TailYear, TailWeek}}, IndexDirPath, KGRemIndexCount, 
													DictCount, InvertedCount, MinuteRange, Key) 
	when is_list(IndexDirPath), is_integer(KGRemIndexCount), is_integer(DictCount), is_integer(InvertedCount) ->
	Self=self(),
	{MinDocId, MaxDocId}=try
							 get_docid(MinuteRange, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self)
						 catch
							 _:Reason ->
								 io:format("!!!!!!Reason:~p, StackTrace:~p~n", [Reason, erlang:get_stacktrace()]),
								 timer:sleep(100000000)
						 end,
	io:format("!!!!!!MinDocId:~p, MaxDocId:~p~n", [MinDocId, MaxDocId]),
	DictInfos=case public_lib:is_lists(Key) of
		true ->
			%与逻辑的关键字列表
			Tokens=query_kgrams(filter_same(Key), StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, []),
			query_dicts(Tokens, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, []);			
		false ->
			{Key, Tokens}=query_kgram(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, Key),
			case query_dict(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, Key, Tokens) of
				[] ->
					[{Key, []}];
				Dicts ->
					Dicts
			end
	end,
	merge_inverted(
	  filter_inverted(
		query_inverted(DictInfos, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Self, []), MinDocId, MaxDocId, []), []).

%%
%% Local Functions
%%

get_docid({StartMinute, TailMinute}, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Owen) ->
	MinDictFileName=indexer_storage:get_dict_filename({StartYear, StartWeek}, document_analyzer:token_hash(StartMinute, DictCount)),
	MinDictFile=indexer_storage:get_file_path(IndexDirPath, MinDictFileName),
	MinDictTable=?DICT_TABLE(Owen, StartWeek, min),
	MaxDictFileName=indexer_storage:get_dict_filename({TailYear, TailWeek}, document_analyzer:token_hash(TailMinute, DictCount)),
	MaxDictFile=indexer_storage:get_file_path(IndexDirPath, MaxDictFileName),
	MaxDictTable=?DICT_TABLE(Owen, TailWeek, max),
	try dets:open_file(MinDictTable, [{file, MinDictFile}, {access, read}]) of
		{ok, _} ->
			MinDocId=case lists:sort(dets:select(MinDictTable, [{{'$1', '$3'}, [{'is_integer', '$1'}, {'>=', '$1', StartMinute}], ['$1']}])) of
				[] ->
					?DEFAULT_NULL_DOCID;
				[StartKey|_] ->
					case dets:lookup(MinDictTable, StartKey) of
						[{_, {_, MinFileHash}}] ->
							MinInvertedFileName=indexer_storage:get_inverted_filename({StartYear, StartWeek}, MinFileHash),
							MinInvertedFile=indexer_storage:get_file_path(IndexDirPath, MinInvertedFileName),
							MinInvertedTable=?INVERTED_TABLE(Owen, StartWeek, MinFileHash),
							try dets:open_file(MinInvertedTable, [{file, MinInvertedFile}, {access, read}]) of
								{ok, _} ->
									case read_inverted(MinInvertedTable, StartKey) of
										[{MinId, _, _}|_] ->
											MinId;
										[] ->
											?DEFAULT_NULL_DOCID
									end;
								{error, Reason} ->
									erlang:error({open_inverted_file_error, {MinInvertedFile, Reason}})
							after
								dets:close(MinInvertedTable)
							end;
						[] ->
							?DEFAULT_NULL_DOCID;
						{error, Reason} ->
							erlang:error({lookup_dict_error, {MinDictTable, StartKey, Reason}})
					end;
				{error, Reason} ->
					erlang:error({select_dict_error, {MinDictTable, StartMinute, Reason}})
			end,
			MaxDocId=try dets:open_file(MaxDictTable, [{file, MaxDictFile}, {access, read}]) of
				{ok, _} ->
					case dets:select(MaxDictTable, [{{'$1', '$3'}, [{'is_integer', '$1'}, {'=<', '$1', TailMinute}], ['$1']}]) of
						{error, Reason1} ->
							erlang:error({select_dict_error, {MaxDictTable, TailMinute, Reason1}});
						[] ->
							?DEFAULT_NULL_DOCID;
						SL ->
							TailKey=lists:last(SL),
							case dets:lookup(MaxDictTable, TailKey) of
								[{_, {_, MaxFileHash}}] ->
									MaxInvertedFileName=indexer_storage:get_inverted_filename({TailYear, TailWeek}, MaxFileHash),
									MaxInvertedFile=indexer_storage:get_file_path(IndexDirPath, MaxInvertedFileName),
									MaxInvertedTable=?INVERTED_TABLE(Owen, TailWeek, MaxFileHash),
									try dets:open_file(MaxInvertedTable, [{file, MaxInvertedFile}, {access, read}]) of
										{ok, _} ->
											case read_inverted(MaxInvertedTable, TailKey) of
												[_|_] = L ->
													{MaxId, _, _}=erlang:hd(lists:reverse(L)),
													MaxId;
												[] ->
													?DEFAULT_NULL_DOCID
											end;
										{error, Reason1} ->
											erlang:error({open_inverted_file_error, {MaxInvertedFile, Reason1}})
									after
										dets:close(MaxInvertedTable)
									end;
								[] ->
									?DEFAULT_NULL_DOCID;
								{error, Reason1} ->
									erlang:error({select_dict_error, {MaxDictTable, TailMinute, Reason1}})
							end
					end;
				{error, Reason1} ->
					erlang:error({open_dict_file_error, {MaxDictFile, Reason1}})
			after
				dets:close(MaxDictTable)
			end,
			{MinDocId, MaxDocId};
		{error, Reason} ->
			erlang:error({open_dict_file_error, {MinDictFile, Reason}})
	after
		dets:close(MinDictTable)
	end.

query_kgrams([Key|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, L) ->
	KGramTokens=query_kgram(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, Key),
	query_kgrams(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, [KGramTokens|L]);
query_kgrams([], _StartYear, _StartWeek, _TailYear, _TailWeek, _IndexDirPath, _KGRemIndexCount, _Self, L) ->
	lists:reverse(L).

query_kgram(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, Key) ->
	Keys=three_gram_indexer:three_gram(Key),
	if
		length(Keys) < 2 ->
			{Key, Keys};
		true ->
			KGramFiles=open_kgram_file(Keys, StartYear, StartWeek, 
							TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Self, []),

			{Key, [Token || Token <- query_kgram_(KGramFiles, Keys, []), is_match_key(string:tokens(Key, "$"), Token)]}
	end.

query_kgram_([Files|T0], [Key|T1], L) ->
	query_kgram_(T0, T1, L ++ query_kgram_files(Files, Key, []));
query_kgram_([], [], L) ->
	L.

query_kgram_files([File|T], Key, L) ->
	case dets:lookup(File, Key) of
		[{Key, Tokens}] ->
			query_kgram_files(T, Key, L ++ Tokens);
		[] ->
			query_kgram_files(T, Key, L)
	end;
query_kgram_files([], _Key, L) ->
	L.

open_kgram_file([Key|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Owen, L) ->
	Files=open_kgram_file_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, document_analyzer:token_hash(Key, KGRemIndexCount), []),
	open_kgram_file(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, KGRemIndexCount, Owen, [Files|L]);
open_kgram_file([], _StartYear, _StartWeek, _TailYear, _TailWeek, _IndexDirPath, _KGRemIndexCount, _Owen, L) ->
	lists:reverse(L).

open_kgram_file_(Year, StartWeek, Year, TailWeek, _IndexDirPath, _Owen, _Hash, L) when StartWeek > TailWeek ->
	L;
open_kgram_file_(StartYear, StartWeek, TailYear, 0, IndexDirPath, Owen, Hash, L) ->
	open_kgram_file_(StartYear, StartWeek, TailYear - 1, ?MAX_WEEK_OF_YEAR, IndexDirPath, Owen, Hash, L);
open_kgram_file_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, Hash, L) ->
	KGramFileName=three_gram_indexer:get_k_gram_filename({TailYear, TailWeek}, Hash),
	KGramFile=three_gram_indexer:get_file_path(IndexDirPath, KGramFileName),
	KGramTable=?K_GRAM_TABLE(Owen, TailWeek, Hash),
	io:format("!!!!!!KGramFileName:~p, KGramFile:~p, KGramTable:~p~n", [KGramFileName, KGramFile, KGramTable]),
	case dets:open_file(KGramTable, [{file, KGramFile}, {access, read}]) of
		{ok, _} ->
			open_kgram_file_(StartYear, StartWeek, TailYear, TailWeek - 1, IndexDirPath, Owen, Hash, [KGramTable|L]);
		{error, Reason} ->
			erlang:error({open_k_gram_file_error, {KGramFile, Reason}})
	end.

query_dicts([{Key, Token}|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, L) ->
	DictInfos=query_dict(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, Key, Token),
	query_dicts(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, [DictInfos|L]);
query_dicts([], _StartYear, _StartWeek, _TailYear, _TailWeek, _IndexDirPath, _DictCount, _Self, L) ->
	lists:flatten(lists:reverse(L)).

query_dict(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, Key, [Key] = Tokens) ->
	DictFiles=open_dict_file(Tokens, StartYear, StartWeek, 
					TailYear, TailWeek, IndexDirPath, DictCount, Self, []),
	query_dict_(DictFiles, Tokens);
query_dict(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Self, Key, Tokens) ->
	DictFiles=open_dict_file(Tokens, StartYear, StartWeek, 
					TailYear, TailWeek, IndexDirPath, DictCount, Self, []),
	query_dict_(DictFiles, Tokens, Key, []).

query_dict_([Files], [Key]) ->
	case query_dict_files(Files, Key, 0, 0) of
		ignore ->
			[];
		R ->
			[{Key, R}]
	end.

query_dict_([Files|T0], [Token|T1], Key, L) ->
	case query_dict_files(Files, Token, 0, 0) of
		ignore ->
			query_dict_(T0, T1, Key, L);
		R ->
			query_dict_(T0, T1, Key, [{{Key, Token}, R}|L])
	end;
query_dict_([], [], _Key, L) ->
	lists:flatten(L).

query_dict_files([File|T], Token, TotalFrequency, InvertedFileHash) ->
	case dets:lookup(File, Token) of
		[{Token, {Frequency, NewInvertedFileHash}}] ->
			query_dict_files(T, Token, TotalFrequency + Frequency, NewInvertedFileHash);
		[] ->
			query_dict_files(T, Token, TotalFrequency, InvertedFileHash)
	end;
query_dict_files([], _Token, 0, 0) ->
	ignore;
query_dict_files([], _Token, TotalFrequency, InvertedFileHash) ->
	{TotalFrequency, InvertedFileHash}.

open_dict_file([Key|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Owen, L) ->
	Files=open_dict_file_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, document_analyzer:token_hash(Key, DictCount), []),
	open_dict_file(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, DictCount, Owen, [Files|L]);
open_dict_file([], _StartYear, _StartWeek, _TailYear, _TailWeek, _IndexDirPath, _DictCount, _Owen, L) ->
	lists:reverse(L).

open_dict_file_(Year, StartWeek, Year, TailWeek, _IndexDirPath, _Owen, _Hash, L) when StartWeek > TailWeek ->
	L;
open_dict_file_(StartYear, StartWeek, TailYear, 0, IndexDirPath, Owen, Hash, L) ->
	open_dict_file_(StartYear, StartWeek, TailYear - 1, ?MAX_WEEK_OF_YEAR, IndexDirPath, Owen, Hash, L);
open_dict_file_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, Hash, L) ->
	DictFileName=indexer_storage:get_dict_filename({TailYear, TailWeek}, Hash),
	DictFile=indexer_storage:get_file_path(IndexDirPath, DictFileName),
	DictTable=?DICT_TABLE(Owen, TailWeek, Hash),
	io:format("!!!!!!DictFileName:~p, DictFile:~p, DictTable:~p~n", [DictFileName, DictFile, DictTable]),
	case dets:open_file(DictTable, [{file, DictFile}, {access, read}]) of
		{ok, _} ->
			open_dict_file_(StartYear, StartWeek, TailYear, TailWeek - 1, IndexDirPath, Owen, Hash, [DictTable|L]);
		{error, Reason} ->
			erlang:error({open_dict_file_error, {DictFile, Reason}})
	end.

query_inverted([{{Key, Token}, {_Frequency, Hash}}|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Owen, L) ->
	InvertesByRange=query_inverted_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, Hash, Token, []),
	query_inverted(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Owen, [{Key, InvertesByRange}|L]);
query_inverted([{Key, {_Frequency, Hash}}|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Owen, L) ->
	InvertesByRange=query_inverted_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, Hash, Key, []),
	query_inverted(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Owen, [{Key, InvertesByRange}|L]);
query_inverted([{_, []} = H|T], StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Owen, L) ->
	query_inverted(T, StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, InvertedCount, Owen, [H|L]);
query_inverted([], _StartYear, _StartWeek, _TailYear, _TailWeek, _IndexDirPath, _InvertedCount, _Owen, L) ->
	lists:reverse(L).
																													
query_inverted_(Year, StartWeek, Year, TailWeek, _IndexDirPath, _Owen, _Hash, _Key, L) when StartWeek > TailWeek ->
	lists:flatten(L);
query_inverted_(StartYear, StartWeek, TailYear, 0, IndexDirPath, Owen, Hash, Key, L) ->
	query_inverted_(StartYear, StartWeek, TailYear - 1, ?MAX_WEEK_OF_YEAR, IndexDirPath, Owen, Hash, Key, L);
query_inverted_(StartYear, StartWeek, TailYear, TailWeek, IndexDirPath, Owen, Hash, Key, L) ->
	InvertedFileName=indexer_storage:get_inverted_filename({TailYear, TailWeek}, Hash),
	InvertedFile=indexer_storage:get_file_path(IndexDirPath, InvertedFileName),
	InvertedTable=?INVERTED_TABLE(Owen, TailWeek, Hash),
	case dets:open_file(InvertedTable, [{file, InvertedFile}, {access, read}]) of
		{ok, _} ->
			Inverteds=read_inverted(InvertedTable, Key),
			query_inverted_(StartYear, StartWeek, TailYear, TailWeek - 1, IndexDirPath, Owen, Hash, Key, [Inverteds|L]);
		{error, Reason} ->
			erlang:error({open_inverted_file_error, {InvertedFile, Reason}})
	end.

read_inverted(InvertedTable, Key) ->
	case dets:lookup(InvertedTable, Key) of
		[{Key, Inverteds}] ->
			indexer_storage:uncompress(Inverteds);
		[] ->
			[]
	end.
	
is_match_key([SubStr|T], Token) ->
	case string:str(Token, SubStr) of
		0 ->
			false;
		_ ->
			is_match_key(T, Token)
	end;
is_match_key([], _Token) ->
	true.
	
filter_inverted(_, ?DEFAULT_NULL_DOCID, _, _) ->
	[];
filter_inverted(_, _, ?DEFAULT_NULL_DOCID, _) ->
	[];
filter_inverted([{Key, Inverteds}|T], MinDocId, MaxDocId, L) ->
	NewInverteds=filter_inverted_(Inverteds, MinDocId, MaxDocId, []),
	filter_inverted(T, MinDocId, MaxDocId, [{Key, NewInverteds}|L]);
filter_inverted([], _MinDocId, _MaxDocId, L) ->
	lists:reverse(L).
	
filter_inverted_([{DocId, _File, _Offset} = DocInfo |T], MinDocId, MaxDocId, L) 
  when DocId >= MinDocId, DocId =< MaxDocId ->
	filter_inverted_(T, MinDocId, MaxDocId, [DocInfo|L]);
filter_inverted_([{_DocId, _File, _Offset} |T], MinDocId, MaxDocId, L) ->
	filter_inverted_(T, MinDocId, MaxDocId, L);
filter_inverted_([], _MinDocId, _MaxDocId, L) ->
	lists:reverse(L).

merge_inverted([{Key, Inverted}|T0], [{Key, Inverteds}|T1]) ->
	merge_inverted(T0, [{Key, filter_same(lists:sort(Inverteds ++ Inverted))}|T1]);
merge_inverted([H|T], L) ->
	merge_inverted(T, [H|L]);
merge_inverted([], L) ->
	lists:reverse(L).

filter_same(L) ->
	filter_same(lists:sort(L), []).

filter_same([H|T], [H|_] = L) ->
	filter_same(T, L);
filter_same([H|T], L) ->
	filter_same(T, [H|L]);
filter_same([], L) ->
	lists:reverse(L).
	
