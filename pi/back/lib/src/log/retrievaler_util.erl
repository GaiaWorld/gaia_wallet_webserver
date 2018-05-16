%% Author: Administrator
%% Created: 2012-10-17
%% Description: 索引检索的工具模块，主要提供了布尔运算相关的函数
-module(retrievaler_util).

%%
%% Include files
%%
-include("retrievaler.hrl").

%%
%% Exported Functions
%%
-export([bool_eval/3, booland/4, boolor/4, boolxor/4]).

%%
%% API Functions
%%

%%
%% Function: bool_eval/2
%% Description: 执行索引检索的逻辑表达式
%% Arguments: 
%% Returns: ResultSet
%% 
bool_eval([{Type, Key}], [Key], ResultSet) when is_atom(Type) ->
	{_, ReplyResultSet}=lists:keyfind(Key, 1, ResultSet),
	ReplyResultSet;
bool_eval(BoolExpre, Keys, ResultSet) when is_list(ResultSet) ->
	{ReplyResultSet}=expre_eval:eval(BoolExpre, {?MODULE, {Keys, ResultSet}}),
	ReplyResultSet.

%%
%% Function: booland/4
%% Description: 对反向索引进行与运算
%% Arguments: 
%%	Args
%%	Context
%%	Val0
%%	Val1
%% Returns: {NewContext, {Result}} | {error, Reason}
%% 
booland({Keys, ResultSet}, Context, Val0, Val1) ->
	booland1(Keys, ResultSet, Context, Val0, Val1).

%%
%% Function: boolor/4
%% Description: 对反向索引进行或运算
%% Arguments: 
%%	Args
%%	Context
%%	Val0
%%	Val1
%% Returns: {NewContext, {Result}} | {error, Reason}
%% 
boolor({_Keys, ResultSet}, _Context, Val0, Val1) ->
	{[], {boolor1(ResultSet, Val0, Val1)}}.

%%
%% Function: boolxor/4
%% Description: 对反向索引进行排除运算
%% Arguments: 
%%	Args
%%	Context
%%	Val0
%%	Val1
%% Returns: {NewContext, {Result}} | {error, Reason}
%% 
boolxor({_Keys, ResultSet}, _Context, Val0, Val1) ->
	{[], {boolxor1(ResultSet, Val0, Val1)}}.

%%
%% Local Functions
%%

%TODO优化内容:采用更好的数据结构，以简化与运算延时操作的代码复杂度和代码量
booland1(_Keys, _ResultSet, _Context, {L0}, {L1}) ->
	{[{?AND_STACK_KEY, []}], {and_eval_(L0, L1, [])}};
booland1(_Keys, ResultSet, _Context, {L0}, Key1) ->
	L1=get_inverted(ResultSet, Key1),
	{[{?AND_STACK_KEY, []}], {and_eval_(L0, L1, [])}};
booland1(_Keys, ResultSet, _Context, Key0, {L1}) ->
	L0=get_inverted(ResultSet, Key0),
	{[{?AND_STACK_KEY, []}], {and_eval_(L0, L1, [])}};
booland1(Keys, ResultSet, Context, Key0, Key1) ->
	case wait_booland(Keys, Context, Key0, Key1) of
		{?AND_STACK_KEY, _} = Entry ->
			{[Entry], []};
		AndKeys ->
			{[{?AND_STACK_KEY, []}], {and_eval(AndKeys, ResultSet, [])}}
	end.

wait_booland([H|T], Context, [], Key1) ->
	case public_lib:is_lists(H) of
		true when length(H) > 1 ->
			case lists:member(Key1, H) of
				true ->
					fill_booland(H, Context, Key1);
				false ->
					wait_booland(T, Context, [], Key1)
			end;
		_ ->
			wait_booland(T, Context, [], Key1)
	end;
wait_booland([H|T], Context, Key0, []) ->
	case public_lib:is_lists(H) of
		true when length(H) > 1 ->
			case lists:member(Key0, H) of
				true ->
					fill_booland(H, Context, Key0);
				false ->
					wait_booland(T, Context, Key0, [])
			end;
		_ ->
			wait_booland(T, Context, Key0, [])
	end;
wait_booland([H|T], Context, Key0, Key1) ->
	case public_lib:is_lists(H) of
		true when length(H) > 1 ->
			case lists:member(Key0, H) and lists:member(Key1, H) of
				true ->
					fill_booland(H, Context, Key0, Key1);
				false ->
					wait_booland(T, Context, Key0, Key1)
			end;
		_ ->
			wait_booland(T, Context, Key0, Key1)
	end.

fill_booland(H, Context, Key0, Key1) ->
	L=z_lib:get_value(Context, ?AND_STACK_KEY, []),
	case [Key1, Key0|L] of
		H ->
			H;
		_ ->
			{?AND_STACK_KEY, [Key1, Key0|L]} 
	end.

fill_booland(H, Context, Key) ->
	L=z_lib:get_value(Context, ?AND_STACK_KEY, []),
	case [Key|L] of
		H ->
			H;
		_ ->
			{?AND_STACK_KEY, [Key|L]} 
	end.

get_inverted(ResultSet, Key) ->
	case lists:keyfind(Key, 1, ResultSet) of
		{_, Result} ->
			Result;
		false ->
			erlang:error({error, {key_not_exist, Key}})
	end.

and_eval([Key|T], ResultSet, L) ->
	Result=get_inverted(ResultSet, Key),
	and_eval(T, ResultSet, [{length(Result), Result}|L]);
and_eval([], _ResultSet, L) ->
	and_eval1(lists:keysort(1, L), []).

and_eval1([{_Length, H}|T], Result) when Result =/= [] ->
	and_eval1(T, and_eval_(Result, H, []));
and_eval1([{_Length, Result}|T], []) ->
	and_eval1(T, Result);
and_eval1([], Result) ->
	Result.

and_eval_([{DocId, _, _} = H|T0], [{DocId, _, _}|T1], L) ->
	and_eval_(T0, T1, [H|L]);
and_eval_([{DocId0, _, _}|T0] = L0, [{DocId1, _, _}|T1] = L1, L) ->
	if
		DocId0 < DocId1 ->
			and_eval_(T0, L1, L);
		true ->
			and_eval_(L0, T1, L)
	end;
and_eval_([], _, L) ->
	lists:reverse(L);
and_eval_(_, [], L) ->
	lists:reverse(L).

boolor1(_ResultSet, {L0}, {L1}) ->
	or_eval(L0, L1, []);
boolor1(ResultSet, {L0}, Key1) ->
	case lists:keyfind(Key1, 1, ResultSet) of
		{_, L1} ->
			or_eval(L0, L1, []);
		false ->
			erlang:error({error, {key_not_exist, Key1}})
	end;
boolor1(ResultSet, Key0, {L1}) ->
	case lists:keyfind(Key0, 1, ResultSet) of
		{_, L0} ->
			or_eval(L0, L1, []);
		false ->
			erlang:error({error, {key_not_exist, Key0}})
	end;
boolor1(ResultSet, Key0, Key1) ->
	case {lists:keyfind(Key0, 1, ResultSet), lists:keyfind(Key1, 1, ResultSet)} of
		{{_, L0}, {_, L1}} ->
			or_eval(L0, L1, []);
		{false, _} ->
			erlang:error({error, {key_not_exist, Key0}});
		{_, false} ->
			erlang:error({error, {key_not_exist, Key1}})
	end.
	
or_eval([{DocId, _, _} = H|T0], [{DocId, _, _}|T1], L) ->
	or_eval(T0, T1, [H|L]);
or_eval([{DocId0, _, _} = H0|T0], [{DocId1, _, _} = H1|T1], L) ->
	if
		DocId0 =< DocId1 ->
			or_eval(T0, T1, [H1, H0|L]);
		true ->
			or_eval(T0, T1, [H0, H1|L])
	end;
or_eval([], [H|T], L) ->
	or_eval([], T, [H|L]);
or_eval([H|T], [], L) ->
	or_eval(T, [], [H|L]);
or_eval([], [], L) ->
	lists:reverse(L).

boolxor1(_ResultSet, {L0}, {L1}) ->
	xor_eval(lists:keysort(1, L0 ++ L1), []);
boolxor1(ResultSet, {L0}, Key1) ->
	case lists:keyfind(Key1, 1, ResultSet) of
		{_, L1} ->
			xor_eval(lists:keysort(1, L0 ++ L1), []);
		false ->
			erlang:error({error, {key_not_exist, Key1}})
	end;
boolxor1(ResultSet, Key0, {L1}) ->
	case lists:keyfind(Key0, 1, ResultSet) of
		{_, L0} ->
			xor_eval(lists:keysort(1, L0 ++ L1), []);
		false ->
			erlang:error({error, {key_not_exist, Key0}})
	end;
boolxor1(ResultSet, Key0, Key1) ->
	case {lists:keyfind(Key0, 1, ResultSet), lists:keyfind(Key1, 1, ResultSet)} of
		{{_, L0}, {_, L1}} ->
			xor_eval(lists:keysort(1, L0 ++ L1), []);
		{false, _} ->
			erlang:error({error, {key_not_exist, Key0}});
		{_, false} ->
			erlang:error({error, {key_not_exist, Key1}})
	end.
	
xor_eval([{DocId, _, _}|T0], [{DocId, _, _}|T1]) ->
	xor_eval(T0, T1);
xor_eval([H|T], L) ->
	xor_eval(T, [H|L]);
xor_eval([], L) ->
	lists:reverse(L).
