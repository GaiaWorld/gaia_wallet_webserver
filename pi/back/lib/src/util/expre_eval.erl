%% Author: Administrator
%% Created: 2012-9-28
%% Description: 表达式执行器，支持算术，关系和逻辑表达式求值, 支持重载运算符
-module(expre_eval).

%%
%% Include files
%%
-include("expre_parser.hrl").
-include("expre_eval.hrl").

%%
%% Exported Functions
%%
-export([eval/2, text_eval/3, text_eval/2]).
-export([
		 add/2, sub/2, mul/2, divi/2, ediv/2, mod/2, pow/2,
		 less_than/2, less_equal_than/2, equal_than/2, greater_than/2, greater_equal_than/2, no_equal_than/2,
		 booland/2, boolor/2, boolnot/1, boolxor/2
		 ]).

%%
%% API Functions
%%

%%
%% Function: eval/2
%% Description: 对后缀表达式词条列表进行求值
%% Arguments: 
%%	ExpreRPN
%%	OPOverLoadTable
%% Returns: Result
%% 
eval(ExpreRPN, OPOverLoadTable) 
  when is_list(ExpreRPN) ->
	eval(ExpreRPN, OPOverLoadTable, sb_trees:empty(), []).

%%
%% Function: text_eval/3
%% Description: 对中缀表达式文本进行求值
%% Arguments: 
%%	ExpreText
%%	OPLevelTable
%%	OPOverLoadTable
%% Returns: Result
%% 
text_eval(ExpreText, OPLevelTable, OPOverLoadTable) ->
	eval(expre_parser:text_parse(ExpreText, OPLevelTable), OPOverLoadTable).

%%
%% Function: text_eval/2
%% Description: 对中缀表达式文本进行求值
%% Arguments: 
%%	ExpreText
%%	OPOverLoadTable
%% Returns: Result
%% 
text_eval(ExpreText, OPOverLoadTable) ->
	eval(expre_parser:text_parse(ExpreText), OPOverLoadTable).

%%
%% Function: add/2
%% Description: 对值进行加法运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
add(Val0, Val1) ->
	case {is_number(Val0), is_number(Val1)} of
		{true, true} ->
			Val0 + Val1;
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_number, Val1}})
	end.

%%
%% Function: sub/2
%% Description: 对值进行减法运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
sub(Val0, Val1) ->
	case {is_number(Val0), is_number(Val1)} of
		{true, true} ->
			Val0 - Val1;
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_number, Val1}})
	end.

%%
%% Function: mul/2
%% Description: 对值进行乘法运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
mul(Val0, Val1) ->
	case {is_number(Val0), is_number(Val1)} of
		{true, true} ->
			Val0 * Val1;
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_number, Val1}})
	end.

%%
%% Function: div/2
%% Description: 对值进行除法运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
divi(Val0, Val1) ->
	case {is_number(Val0), is_number(Val1) and (Val1 =/= 0)} of
		{true, true} ->
			Val0 / Val1;
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_number, Val1}})
	end.

%%
%% Function: ediv/2
%% Description: 对值进行整除运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
ediv(Val0, Val1) ->
	case {is_number(Val0), is_number(Val1) and (Val1 =/= 0)} of
		{true, true} ->
			Val0 div Val1;
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_number, Val1}})
	end.

%%
%% Function: mod/2
%% Description: 对值进行取余运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
mod(Val0, Val1) ->
	case {is_number(Val0), is_integer(Val1)} of
		{true, true} ->
			Val0 rem Val1;
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_integer, Val1}})
	end.

%%
%% Function: pow/2
%% Description: 对值进行幂运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
pow(Val0, Val1) ->
	case {is_number(Val0), is_number(Val1)} of
		{true, true} ->
			math:pow(Val0, Val1);
		{false, _} ->
			erlang:error({error, {invalid_number, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_number, Val1}})
	end.

%%
%% Function: less_than/2
%% Description: 对值进行小于运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
less_than(Val0, Val1) ->
	Val0 < Val1.

%%
%% Function: less_equal_than/2
%% Description: 对值进行小于等于运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
less_equal_than(Val0, Val1) ->
	Val0 =< Val1.

%%
%% Function: equal_than/2
%% Description: 对值进行等于运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
equal_than(Val0, Val1) ->
	Val0 =:= Val1.

%%
%% Function: greater_than/2
%% Description: 对值进行大于运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
greater_than(Val0, Val1) ->
	Val0 > Val1.

%%
%% Function: greater_equal_than/2
%% Description: 对值进行大于等于运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
greater_equal_than(Val0, Val1) ->
	Val0 >= Val1.

%%
%% Function: no_equal_than/2
%% Description: 对值进行不等于运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
no_equal_than(Val0, Val1) ->
	Val0 =/= Val1.

%%
%% Function: booland/2
%% Description: 对值进行与运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
booland(Val0, Val1) ->
	case {is_boolean(Val0), is_boolean(Val1)} of
		{true, true} ->
			Val0 and Val1;
		{false, _} ->
			erlang:error({error, {invalid_boolean, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_boolean, Val1}})
	end.

%%
%% Function: boolor/2
%% Description: 对值进行或运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
boolor(Val0, Val1) ->
	case {is_boolean(Val0), is_boolean(Val1)} of
		{true, true} ->
			Val0 or Val1;
		{false, _} ->
			erlang:error({error, {invalid_boolean, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_boolean, Val1}})
	end.

%%
%% Function: boolnot/1
%% Description: 对值进行非运算
%% Arguments: 
%%	Val
%% Returns: Result
%% 
boolnot(Val) ->
	case is_boolean(Val) of
		true ->
			not Val;
		false ->
			erlang:error({error, {invalid_boolean, Val}})
	end.

%%
%% Function: boolxor/2
%% Description: 对值进行异或运算
%% Arguments: 
%%	Val0
%%	Val1
%% Returns: Result
%% 
boolxor(Val0, Val1) ->
	case {is_boolean(Val0), is_boolean(Val1)} of
		{true, true} ->
			Val0 xor Val1;
		{false, _} ->
			erlang:error({error, {invalid_boolean, Val0}});
		{_, false} ->
			erlang:error({error, {invalid_boolean, Val1}})
	end.

%%
%% Local Functions
%%

eval([{Type, Val}|T], OPOverLoadTable, Context, Stack) when is_atom(Type) ->
	eval(T, OPOverLoadTable, Context, [Val|Stack]);
eval([{Meta, OP}|T], OPOverLoadTable, Context, Stack) ->
	{Vals, NewStack}=if
		length(Stack) >= Meta ->
			val_pop(Stack, [], Meta);
		true ->
			erlang:error({error, {invalid_meta, OP, Meta}})
	end,
	case expre_eval(OP, OPOverLoadTable, Context, Vals) of
		{error, _} = E ->
			erlang:error(E);
		{NewContext, Result} ->
			eval(T, OPOverLoadTable, update_context(NewContext, Context), [Result|NewStack])
	end;
eval([], _OPOverLoadTable, _Context, [Result]) ->
	Result;
eval([], _OPOverLoadTable, _Context, Stack) ->
	erlang:error({error, {invalid_val, Stack}}).
		
val_pop(T, Vals, 0) ->
	{Vals, T};
val_pop([Val|T], Vals, Count) ->
	val_pop(T, [Val|Vals], Count - 1);
val_pop([], _Vals, _Count) ->
	erlang:error({error, invalid_val_amount}).

expre_eval(OP, {M, A}, Context, Vals) ->
	F=to_inner_op(OP),
	apply(M, F, [A, Context|Vals]);
expre_eval(OP, OPOverLoadTable, Context, Vals) ->
	F=to_inner_op(OP),
	case lists:keyfind(OP, 1, OPOverLoadTable) of
		false ->
			{[], apply(?MODULE, F, [Context|Vals])};
		{_, {M, A}} ->
			apply(M, F, [A, Context|Vals])
	end.

to_inner_op(?ADD) -> ?INNER_ADD;
to_inner_op(?SUB) -> ?INNER_SUB;
to_inner_op(?MUL) -> ?INNER_MUL;
to_inner_op(?DIV) -> ?INNER_DIV;
to_inner_op(?EDIV) -> ?INNER_EDIV;
to_inner_op(?REM) -> ?INNER_REM;
to_inner_op(?POW) -> ?INNER_POW;
to_inner_op(?LESS_THAN) -> ?INNER_LESS_THAN;
to_inner_op(?LESS_EQUAL_THAN) -> ?INNER_LESS_EQUAL_THAN;
to_inner_op(?EQUAL) -> ?INNER_EQUAL;
to_inner_op(?GREATER_THAN) -> ?INNER_GREATER_THAN;
to_inner_op(?GREATER_EQUAL_THAN) -> ?INNER_GREATER_EQUAL_THAN;
to_inner_op(?NO_EQUAL) -> ?INNER_NO_EQUAL;
to_inner_op(?AND) -> ?INNER_AND;
to_inner_op(?OR) -> ?INNER_OR;
to_inner_op(?NOT) -> ?INNER_NOT;
to_inner_op(?XOR) -> ?INNER_XOR;
to_inner_op(OP) -> 
	erlang:error({error, {invalid_op, OP}}).

update_context([{Key, Value}|T], Context) ->
	update_context(T, sb_trees:enter(Key, Value, Context));
update_context([], Context) ->
	Context.