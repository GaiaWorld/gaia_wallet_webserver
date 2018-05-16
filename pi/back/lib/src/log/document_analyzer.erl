%% Author: Administrator
%% Created: 2012-7-31
%% Description: 文档分析器，用于进行词条化和语言学处理
-module(document_analyzer).

%%
%% Include files
%%
-include("document_analyzer.hrl").

%%
%% Exported Functions
%%
-export([analyze/6, tokenization/5, linguistics/2, dict_mapping/3, merge_mapping/3, k_gram_mapping/2, token_hash/2, inverteds_sort/1]).

%%
%% API Functions
%%

%%
%% Function: analyzer/6
%% Description: 分析文档
%% Arguments: 
%%	TokenHandler
%%	LanguageHandler
%%	File
%%	Offset
%%	DocId
%%	Doc
%% Returns: [{token, docinfo} | T] | []
%% 
analyze(TokenHandler, LanguageHandler, File, Offset, DocId, Doc) ->
	linguistics(LanguageHandler, tokenization(TokenHandler, File, Offset, DocId, Doc)).

%%
%% Function: tokenization/5
%% Description: 词条化文档
%% Arguments: 
%%	TokenHandler
%%	File
%%	Offset
%%	DocId
%%	Doc
%% Returns:  [{token, docinfo} | T] | []
%% 
tokenization(_, _, _, _, []) ->
	[];
tokenization(none, File, Offset, DocId, Doc) when is_list(Doc) ->
	default_tokenization(Doc, DocId, File, Offset, []);
tokenization({M, F, A}, File, Offset, DocId, Doc) 
  when is_atom(M), is_atom(F), is_list(Doc) ->
	apply(M, F, [A, File, Offset, DocId, Doc]).

%%
%% Function: linguistics/2
%% Description: 语言学处理
%% Arguments: 
%%	LanguageHandler
%%	Tokens
%% Returns:  [{token, docinfo} | T] | []
%% 
linguistics(_, []) ->
	[];
linguistics(none, Tokens) when is_list(Tokens) ->
	default_linguistics(none, Tokens);
linguistics({M, F, A}, Tokens) 
  when is_atom(M), is_atom(F), is_list(Tokens) ->
	apply(M, F, [A, Tokens]).

%%
%% Function: merge_mapping/5
%% Description: 词条映射合并
%% Arguments: 
%%	MergeMapHandler
%%	Rang
%%	Tokens
%% Returns: [{Hash, [{token, docinfolist} | T]}] | []
%% 
merge_mapping(none, Range, Tokens) when is_integer(Range), is_list(Tokens), Range > 0 ->
	default_merge_mapping(Tokens, Range);
merge_mapping({M, F, A}, Range, Tokens) 
  when is_atom(M), is_atom(F), is_integer(Range), Range > 0 ->
	apply(M, F, [A, Tokens, Range]).

%%
%% Function: dict_mapping/5
%% Description: 词项映射
%% Arguments: 
%%	DictMapHandler
%%	Rang
%%	Tokens
%% Returns: [{Hash, [{token, docinfolist} | T]}] | []
%% 
dict_mapping(none, Range, Tokens) when is_integer(Range), is_list(Tokens), Range > 0 ->
	default_dict_mapping(Tokens, Range);
dict_mapping({M, F, A}, Range, Tokens) 
  when is_atom(M), is_atom(F), is_integer(Range), Range > 0 ->
	apply(M, F, [A, Tokens, Range]).

%%
%% Function: k_gram_mapping/5
%% Description: Gram词项映射。
%% Arguments: 
%%	Rang
%%	TokensGramed
%% Returns: [{Hash, [{gram, tokenlist} | T]}] | []
%% 
k_gram_mapping(Range, TokensGramed) when is_integer(Range), is_list(TokensGramed), Range > 0 ->
	default_k_gram_mapping(TokensGramed, Range).

%%
%% Function: token_hash/2
%% Description: 词条映射函数
%% Arguments: 
%%	Token
%%	Range
%% Returns: Hash
%% 
token_hash(Token, Range) ->
	erlang:phash(Token, Range).

%%
%% Function: inverteds_sort/1
%% Description: 反向索引列表排序
%% Arguments: 
%%	Inverteds
%% Returns: InvertesSorted | []
%% 
inverteds_sort([]) ->
	[];
inverteds_sort(Invertes) ->
	{_, InvertesSorted} = lists:unzip(lists:keysort(1, inverteds_sort_(Invertes, []))),
	InvertesSorted.

%%
%% Local Functions
%%

default_tokenization([], _, _, _, Tokens) ->
	Tokens;
default_tokenization([{_, _} = KV|T], DocId, File, Offset, Tokens) ->
	DocInfo={DocId, File, Offset},
	case key_filter(KV) of
		{Key, Value} ->
			default_tokenization(T, DocId, File, Offset, [{Key, DocInfo}, {Value, DocInfo}|Tokens]);
		SysKey ->
			default_tokenization(T, DocId, File, Offset, [{SysKey, DocInfo}|Tokens])
	end.
	
key_filter({?SYS_DOC_TIME, Time}) when is_integer(Time), Time > 0 ->
	Time div 60000;
key_filter({?SYS_DOC_TIME, _}) ->
	erlang:error({error, invalid_doc_time});
key_filter({?SYS_DOC_LEVEL, Level}) when is_list(Level), length(Level) > 0 ->
	Level;
key_filter({?SYS_DOC_LEVEL, _}) ->
	erlang:error({error, invalid_doc_level});
key_filter({?SYS_DOC_SRC, Src}) ->
	Src;
key_filter({?SYS_DOC_MOD, Mod}) when is_atom(Mod) ->
	atom_to_list(Mod);
key_filter({?SYS_DOC_MOD, _}) ->
	erlang:error({error, invalid_doc_mod});
key_filter({?SYS_DOC_CODE, Code}) when is_list(Code) ->
	Code;
key_filter({?SYS_DOC_CODE, _}) ->
	erlang:error({error, invalid_doc_code});
key_filter({?SYS_DOC_TYPE, Type}) when is_list(Type) ->
	Type;
key_filter({?SYS_DOC_TYPE, Type}) when is_atom(Type) ->
	atom_to_list(Type);
key_filter({?SYS_DOC_TYPE, _}) ->
	erlang:error({error, invalid_doc_code});
key_filter({Key, Value}) when is_list(Key) ->
	{Key, value_filter(Value)};
key_filter({Key, Value}) when is_atom(Key) ->
	{atom_to_list(Key), value_filter(Value)}.

value_filter(Value) when is_integer(Value) ->
	Value;
value_filter(Value) when is_float(Value) ->
	Value;
value_filter(Value) when is_list(Value) ->
	Value;
value_filter(Value) when is_atom(Value) ->
	atom_to_list(Value);
value_filter(Value) ->
	term_to_string(Value).


term_to_string([]) ->
	[];
term_to_string(Pid) when is_pid(Pid) ->
	erlang:pid_to_list(Pid); 
term_to_string(Port) when is_port(Port) ->
	erlang:port_to_list(Port);
term_to_string(Term) ->
	lists:flatten(io_lib:format("~p", [Term])).

default_linguistics(_Args, Tokens) ->
	Tokens.

default_merge_mapping(Tokens, Range) ->
	merge(mapping(Tokens, Range, []), []).

merge([], TokensMerged) ->
	TokensMerged;
merge([{Hash, Tokens}|T], TokensMerged) ->
	merge(T, [{Hash, sort_merge(Tokens, [])}|TokensMerged]).

sort_merge([], InvertedTable) ->
	InvertedTable;
sort_merge([{Token, DocInfo}|T], InvertedTable) ->
	case lists:keyfind(Token, 1, InvertedTable) of
		{Token, Inverted} ->
			sort_merge(T, lists:keyreplace(Token, 1, InvertedTable, {Token, lists:keysort(1, [DocInfo|Inverted])}));
		false ->
			sort_merge(T, [{Token, [DocInfo]}|InvertedTable])
	end.

default_dict_mapping(Tokens, Range) ->
	mapping(Tokens, Range, []).

default_k_gram_mapping(TokensGramed, Range) ->
	mapping(TokensGramed, Range, []).

mapping([], _, TokensMaped) ->
	TokensMaped;
mapping([{Token, _} = Item|T], Range, TokensMaped) ->
	Hash=token_hash(Token, Range),
	case lists:keyfind(Hash, 1, TokensMaped) of
		{Hash, Val} ->
			mapping(T, Range, lists:keyreplace(Hash, 1, TokensMaped, {Hash, [Item|Val]}));
		false ->
			mapping(T, Range, [{Hash, [Item]}|TokensMaped])
	end.

inverteds_sort_([], L) ->
	L;
inverteds_sort_([Inverted|T], L) ->
	inverteds_sort_(T, [{length(Inverted), Inverted}|L]).
