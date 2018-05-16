%%@doc 条件模块
%%```
%%% Condition本身为： [复合条件] | {'not', Condition} | {M, F, A} | {MFA1 | Value1, '>' | '<' | '>=' | '=<' | '=' | '=:=' | '=/=', MFA2 | Value2}
%%% M:F(A, Args), 返回值会参与布尔或比较运算，如果返回其他不可识别的，默认为 false
%%% [Condition1, Condition2, Condition3, ...] 表示多个条件与
%%% ['and', Condition1, Condition2, Condition3, ...] 表示多个条件与
%%% ['or', Condition1, Condition2, Condition3, ...] 表示多个条件或
%%'''
%%	@type mfa() = {m::atom(), f::atom(), a::any()}
%%	@type comp() = '>' | '<' | '>=' | '=<' | '=' | '=:=' | '=/='
%%	@type zm_condition() = [zm_condition()] | {'not', mfa::mfa()} | mfa() | {mfa() | any(), comp(), mfa() | any()}
%%@end

-module(zm_condition).

-description("zm_condition").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([check/2, check/3]).

%%%=======================INLINE=======================
-compile({inline, [check/2, check_or/2, check_and/2,get_value/2]}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  条件检查
%% @spec check(zm_condition(), any()) -> boolean()
%%@end
%% ----------------------------------------------------------------
check(['or' | Condition], Args) ->
	check_or(Condition, Args);
check(['and' | Condition], Args) ->
	check_and(Condition, Args);
check([_ | _] = Condition, Args) ->
	check_and(Condition, Args);
check({A, Comp, B}, Args) ->
	case Comp of
		'>' -> get_value(A, Args) > get_value(B, Args);
		'<' -> get_value(A, Args) < get_value(B, Args);
		'>=' -> get_value(A, Args) >= get_value(B, Args);
		'=<' -> get_value(A, Args) =< get_value(B, Args);
		'=:=' -> get_value(A, Args) =:= get_value(B, Args);
		'=/=' -> get_value(A, Args) =/= get_value(B, Args);
		'==' -> get_value(A, Args) == get_value(B, Args);
		_ -> A:Comp(B, Args)
	end;
check({'not', Condition}, Args) ->
	check(Condition, Args) =/= true;
check(R, _) ->
	R.

%% -----------------------------------------------------------------
%%@doc  条件检查
%% @spec check(any(), atom(), any()) -> boolean()
%%@end
%% ----------------------------------------------------------------
check(A, Comp, B) ->
	case Comp of
		'>' -> A > B;
		'<' -> A < B;
		'>=' -> A >= B;
		'=<' -> A =< B;
		'=:=' -> A =:= B;
		'=/=' -> A =/= B;
		'=' -> A =:= B;
		'!' -> A =/= B;
		'==' -> A == B;
		_ -> A:Comp(B)
	end.

%%%===================LOCAL FUNCTIONS==================
% 检查 or 关系
check_or([H | T], Args) ->
	case check(H, Args) of
		true -> true;
		_ -> check_or(T, Args)
	end;
check_or(_, _) ->
	false.

% 检查 and 关系
check_and([H | T], Args) ->
	case check(H, Args) of
		true -> check_and(T, Args);
		_ -> false
	end;
check_and(_, _) ->
	true.

% 获得值
get_value({M, F, A}, Args) when is_atom(M), is_atom(F) ->
	M:F(A, Args);
get_value(V, _Args) ->
	V.
