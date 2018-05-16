%% Author: Administrator
%% Created: 2012-9-28
%% Description: 表达式分析器，支持算术，关系和逻辑运算, 运算符优先级可以由外部指定
-module(expre_parser).

%%
%% Include files
%%
-include("expre_parser.hrl").

%%
%% Exported Functions
%%
-export([text_parse/2, text_parse/1]).

%%
%% API Functions
%%

%%
%% Function: text_parse/2
%% Description: 分析中缀表达式文本，返回后缀表达式词条列表
%% Arguments: 
%%	ExpreText
%%	OPLevelTable
%% Returns: ExpreRPN
%% 
text_parse(ExpreText, OPLevelTable) 
  when is_list(ExpreText), is_list(OPLevelTable) ->
	case erl_scan:string(ExpreText) of
		{ok, Tokens, _} ->
			to_rpn(Tokens, OPLevelTable, queue:new(), []);
		{error, _} = E ->
			erlang:error(E)
	end.

%%
%% Function: text_parse/1
%% Description: 分析中缀表达式文本，使用默认运算符优先级，返回后缀表达式词条列表
%% Arguments: 
%%	ExpreText
%% Returns: ExpreRPN
%% 
text_parse(ExpreText) ->
  text_parse(ExpreText, ?DEFAULT_OP_LEVEL_TABLE).

%%
%% Local Functions
%%

to_rpn([Token|T], OPLevelTable, Queue, Stack) ->
	case parse_token(Token) of
		{_, _} = Item ->
			to_rpn(T, OPLevelTable, queue:in(Item, Queue), Stack);
		OP ->
			{NewQueue, NewStack}=op_push(OP, OPLevelTable, Queue, Stack),
			to_rpn(T, OPLevelTable, NewQueue, NewStack)
	end;
to_rpn([], _OPLevelTable, _Queue, [OP|_]) when OP =:= ?LEFT_BRACKET; OP =:= ?RIGHT_BRACKET ->
	erlang:error({error, no_match_bracket});
to_rpn([], _OPLevelTable, Queue, Stack) ->
	to_rpn1(Stack, Queue).
	
to_rpn1([OP|ST], Queue) ->
	case lists:keyfind(OP, 1, ?DEFAULT_OP_META_TABLE) of
		{_, Meta} ->
			to_rpn1(ST, queue:in({Meta, OP}, Queue));
		false ->
			erlang:error({error, {invalid_meta, OP}})
	end;
to_rpn1([], Queue) ->
	queue:to_list(Queue).

op_push(?LEFT_BRACKET, _OPLevelTable, Queue, Stack) ->
	{Queue, [?LEFT_BRACKET|Stack]};
op_push(?RIGHT_BRACKET, _OPLevelTable, Queue, Stack) ->
	op_push1(Stack, Queue);
op_push(OP, OPLevelTable, Queue, Stack) ->
	case Stack of
		[OP1|ST] ->
			case lists:keyfind(OP1, 1, ?DEFAULT_OP_META_TABLE) of
				{_, Meta} ->
					case op_compare(OP, OP1, OPLevelTable) of
						FLAG when FLAG =:= ?LESS_THAN; FLAG =:= ?EQUAL ->
							{queue:in({Meta, OP1}, Queue), [OP|ST]};
						?GREATER_THAN ->
							{Queue, [OP|Stack]}
					end;
				false ->
					erlang:error({error, {invalid_meta, OP1}})
			end;
		[] ->
			{Queue, [OP|Stack]}
	end.

op_push1([?LEFT_BRACKET|ST], Queue) ->
	{Queue, ST};
op_push1([OP|ST], Queue) ->
	case lists:keyfind(OP, 1, ?DEFAULT_OP_META_TABLE) of
		{_, Meta} ->
			op_push1(ST, queue:in({Meta, OP}, Queue));
		false ->
			erlang:error({error, {invalid_meta, OP}})
	end;
op_push1([], _Queue) ->
	erlang:error({error, no_match_bracket}).
  
parse_token({Type, _, OPNum})->
	if
		Type =:= atom ->
			case lists:member(OPNum, ?OP_LIST) of
				false ->
					{Type, OPNum};
				true ->
					OPNum
			end;
		true ->
			{Type, OPNum}
	end;
parse_token({OP, _}) ->
	OP.

op_compare(OP, OP1, OPLevelTable) ->
	case {lists:keyfind(OP, 1, OPLevelTable), lists:keyfind(OP1, 1, OPLevelTable)} of
		{{_, A}, {_, B}} when A > B ->
			?LESS_THAN;
		{{_, A}, {_, B}} when A < B ->
			?GREATER_THAN;
		{{_, _}, {_, _}} ->
			?EQUAL;
		_ ->
			erlang:error({error, {invalid_op, {OP, OP1}}})
	end.

