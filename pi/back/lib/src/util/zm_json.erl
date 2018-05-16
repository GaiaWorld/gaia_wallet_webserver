%%@doc 一般数据的json编解码模块
%%```
%%% 一般数据指：
%%%	标量为string(), integer(), float()，
%%%	数组为[any(), ...]
%%%	对象为[{Key, Value}, ...]或sb_trees()，Key必须是字符串或原子或utf8的二进制数据
%%%
%%%解码时：
%%%	JSON			Erlang
%%%	------			------
%%%	number			number()
%%%	string			string()
%%%	array			{ArrayLength, ElementList}
%%%	object			sb_trees:{string(), V}
%%%	true, false, null		atoms 'true', 'false', and 'null'
%%'''
%%@end


-module(zm_json).

-description("json encode & decode").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([encode/1, encode/2, decode/1]).

%%%=======================DEFINE=======================
-define(Q, 34).

-define(IS_WHITESPACE(C), (C =:= $\s orelse C =:= $\t orelse C =:= $\r)).
-define(IS_HEX(C), ((C >= $0 andalso C =< $9) orelse (C >= $a andalso C =< $f) orelse (C >= $A andalso C =< $F))).

-define(ERR_INFO, 20).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 将数据编码json格式的iolist()
%% @spec  encode(Any::any()) -> return()
%% where
%%  return() =  iolist()
%%@end
%% -----------------------------------------------------------------
encode(Any) ->
	lists:reverse(encode1(Any, [], utf8)).

%% -----------------------------------------------------------------
%%@doc 将数据编码json格式的iolist()
%% @spec  encode(Any::any(), InEncoding :: utf8 | unicode) -> return()
%% where
%%  return() =  iolist()
%%@end
%% -----------------------------------------------------------------
encode(Any, InEncoding) ->
	lists:reverse(encode1(Any, [], InEncoding)).


%% -----------------------------------------------------------------
%%@doc 将字符串解码成json格式的term
%% @spec  decode(Str::string()) -> return()
%% where
%%  return() =  {more, fun(Str)} | term()
%%@end
%% -----------------------------------------------------------------
decode([C | _] = Str) when C > 0, C < 16#80000000 ->
	decode1(Str, 1, [fun(_, _, Json, _) -> Json end]).

%%%===================LOCAL FUNCTIONS==================
% 编码方法
encode1(true, L, _InEncoding) ->
	[$e, $u, $r, $t | L];
encode1(false, L, _InEncoding) ->
	[$e, $s, $l, $a, $f | L];
encode1(nil, L, _InEncoding) ->
	[$}, ${ | L];
encode1(null, L, _InEncoding) ->
	[$l, $l, $u, $n | L];
encode1(Num, L, _InEncoding) when is_integer(Num) ->
	[integer_to_list(Num) | L];
encode1(Num, L, _InEncoding) when is_float(Num) ->
	[float_to_list(Num) | L];
encode1(Atom, L, _InEncoding) when is_atom(Atom) ->
	[?Q, atom_to_list(Atom), ?Q | L];
encode1(Bin, L, _InEncoding) when is_binary(Bin) ->
	[?Q, base64:encode(Bin), ?Q | L];
encode1([{K, _V} | _] = Obj, L, InEncoding) when is_atom(K) ->
	[$} | encode_object(Obj, [${ | L], InEncoding)];
encode1([{K, _V} | _] = Obj, L, InEncoding) when is_list(K) ->
	[$} | encode_object(Obj, [${ | L], InEncoding)];
encode1([{K, _V} | _] = Obj, L, InEncoding) when is_binary(K) ->
	[$} | encode_object(Obj, [${ | L], InEncoding)];
encode1({_, _, Size, _, _} = Tree, L, InEncoding) when is_integer(Size) ->
	try
		encode1(sb_trees:to_list(Tree), L, InEncoding)
	catch
		_:_ ->
			[?Q, z_lib:any_to_iolist(Tree), ?Q | L]
	end;
encode1([H | _] = List, L, InEncoding) when H > 0, H < 16#80000000 ->
	case encode_text(List, [], InEncoding) of
		false ->
			[$] | encode_array(List, [$[ | L], InEncoding)];
		Str ->
			[?Q, Str, ?Q | L]
	end;
encode1("", L, _InEncoding) ->
	[?Q, ?Q | L];
encode1(List, L, InEncoding) when is_list(List) ->
	[$] | encode_array(List, [$[ | L], InEncoding)];
encode1(Tuple, L, InEncoding) when is_tuple(Tuple) ->
	[$] | encode_array(tuple_to_list(Tuple), [$[ | L], InEncoding)];
encode1(Any, L, _InEncoding) ->
	[?Q, z_lib:any_to_iolist(Any), ?Q | L].

% 编码文字，返回false表示不是文字，否则返回编码后的字符串
encode_text([?Q | T], L, InEncoding) ->
	encode_text(T, [?Q, $\\ | L], InEncoding);
encode_text([$\\ | T], L, InEncoding) ->
	encode_text(T, [$\\, $\\ | L], InEncoding);
encode_text([$\n | T], L, InEncoding) ->
	encode_text(T, [$n, $\\ | L], InEncoding);
encode_text([$\t | T], L, InEncoding) ->
	encode_text(T, [$t, $\\ | L], InEncoding);
encode_text([$\r | T], L, InEncoding) ->
	encode_text(T, [$r, $\\ | L], InEncoding);
encode_text([$\b | T], L, InEncoding) ->
	encode_text(T, [$b, $\\ | L], InEncoding);
encode_text([$\f | T], L, InEncoding) ->
	encode_text(T, [$f, $\\ | L], InEncoding);
encode_text([$\v | T], L, InEncoding) ->
	encode_text(T, [$v, $\\ | L], InEncoding);
encode_text([Ch | T], L, InEncoding) when Ch < 32 ->
	encode_text(T, L, InEncoding);
encode_text([Ch | T], L, InEncoding) when Ch < 127 ->
	encode_text(T, [Ch | L], InEncoding);
encode_text([Ch | T], L, InEncoding) when Ch < 16#80000000 ->
	if
		InEncoding =:= utf8 ->
			encode_text(T, z_lib:char_to_utf8(Ch, L), InEncoding);
		true ->
			encode_text(T, [Ch | L], InEncoding)
	end;
encode_text([_ | _], _L, _InEncoding) ->
	false;
encode_text([], L, _InEncoding) ->
	lists:reverse(L).

% 编码对象
encode_object([{K, V} | T], L, InEncoding) when is_atom(K) ->
	encode_field(T, atom_to_list(K), V, L, InEncoding);
encode_object([{K, V} | T], L, InEncoding) when is_list(K) ->
	encode_field(T, K, V, L, InEncoding);
encode_object([{K, V} | T], L, InEncoding) when is_binary(K) ->
	case unicode:characters_to_list(K, InEncoding) of
		Key when is_list(Key) ->
			encode_field(T, Key, V, L, InEncoding);
		_ ->
			encode_object(T, L, InEncoding)
	end;
encode_object([_ | T], L, InEncoding) ->
	encode_object(T, L, InEncoding);
encode_object([], [$, | L], _InEncoding) ->
	L;
encode_object([], L, _InEncoding) ->
	L.

% 编码对象的域
encode_field(T, Key, Value, L, InEncoding) ->
	case encode_text(Key, [], InEncoding) of
		false ->
			encode_object(T, L, InEncoding);
		Str ->
			encode_object(T, [$, | encode1(Value, [$:, ?Q, Str, ?Q | L], InEncoding)], InEncoding)
	end.

% 编码数组
encode_array([H], L, InEncoding) ->
	encode1(H, L, InEncoding);
encode_array([H | T], L, InEncoding) ->
	encode_array(T, [$, | encode1(H, L, InEncoding)], InEncoding).

%%%===================LOCAL FUNCTIONS==================
% 解码方法
decode1([C | T], Line, FL) when ?IS_WHITESPACE(C) ->
	decode1(T, Line, FL);
decode1([$\n | T], Line, FL) ->
	decode1(T, Line + 1, FL);
decode1([${ | T], Line, FL) ->
	decode_object(T, Line, sb_trees:empty(), 0, FL);
decode1([$[ | T], Line, FL) ->
	case T of
		[$] | T1] ->
			[Fun | _] = FL,
			Fun(T1, Line, [], FL);
		_ ->
			F = fun(Next, Line1, E, NextFL) ->
				decode_array(Next, Line1, 1, [E], NextFL)
			end,
			decode1(T, Line, [F | FL])
	end;
decode1([?Q | T], Line, FL) ->
	decode_string(T, Line, [], FL);
decode1([C | T], Line, FL) when (C >= $0 andalso C =< $9) ->
	decode_number(T, Line, [C], false, FL);
decode1([$- | T], Line, FL) ->
	decode_negative_number(T, Line, FL);
decode1([$t | T], Line, FL) ->
	decode_true(T, Line, FL);
decode1([$f | T], Line, FL) ->
	decode_false(T, Line, FL);
decode1([$n | T], Line, FL) ->
	decode_null(T, Line, FL);
decode1([], Line, FL) ->
	{more, fun(Next) -> decode1(Next, Line, FL) end};
decode1(T, Line, _FL) ->
	erlang:error({invalid_json, lists:sublist(T, ?ERR_INFO), Line}).

% 解码对象方法，可解析无引号的键，Comma为0表示{开头，1表示已经有，2表示需要有
decode_object([?Q | T], Line, Obj, Comma, FL) when Comma =< 1 ->
	F = fun(Next, Line1, K, NextFL) ->
		decode_colon(Next, Line1, Obj, K, NextFL)
	end,
	decode_string(T, Line, [], [F | FL]);
decode_object([C | T], Line, Obj, Comma, FL) when (Comma =< 1 andalso
	((C >= $A andalso C =< $Z) orelse (C >= $a andalso C =< $z))) ->
	F = fun(Next, Line1, K, NextFL) ->
		decode_colon(Next, Line1, Obj, K, NextFL)
	end,
	decode_ss(T, Line, [C], [F | FL]);
decode_object([$, | T], Line, Obj, Comma, FL) when (Comma =:= 0 orelse Comma =:= 2) ->
	decode_object(T, Line, Obj, 1, FL);
decode_object([$} | T], Line, Obj, Comma, [Fun | FL]) when (Comma =:= 0 orelse Comma =:= 2) ->
	Fun(T, Line, Obj, FL);
decode_object([C | T], Line, Obj, Comma, FL) when ?IS_WHITESPACE(C) ->
	decode_object(T, Line, Obj, Comma, FL);
decode_object([$\n | T], Line, Obj, Comma, FL) ->
	decode_object(T, Line + 1, Obj, Comma, FL);
decode_object([], Line, Obj, Comma, FL) ->
	{more, fun(Next) -> decode_object(Next, Line, Obj, Comma, FL) end};
decode_object(T, Line, _Obj, _Comma, _FL) ->
	erlang:error({invalid_obj, lists:sublist(T, ?ERR_INFO), Line}).

% 解码键值中的“:”方法
decode_colon([$: | T], Line, Obj, Key, FL) ->
	F = fun(Next, Line1, Value, NextFL) ->
		decode_object(Next, Line1, sb_trees:enter(Key, Value, Obj), 2, NextFL)
	end,
	decode1(T, Line, [F | FL]);
decode_colon([C | T], Line, Obj, Key, FL) when ?IS_WHITESPACE(C) ->
	decode_colon(T, Line, Obj, Key, FL);
decode_colon([$\n | T], Line, Obj, Key, FL) ->
	decode_colon(T, Line + 1, Obj, Key, FL);
decode_colon([], Line, Obj, Key, FL) ->
	{more, fun(Next) -> decode_colon(Next, Line, Obj, Key, FL) end};
decode_colon(T, Line, _Obj, _Key, _FL) ->
	erlang:error({invalid_colon, lists:sublist(T, ?ERR_INFO), Line}).

% 解码数组方法
decode_array([$, | T], Line, Len, Array, FL) ->
	F = fun(Next, Line1, E, NextFL) ->
		decode_array(Next, Line1, Len + 1, [E | Array], NextFL)
	end,
	decode1(T, Line, [F | FL]);
decode_array([$] | T], Line, Len, Array, [Fun | FL]) ->
	Fun(T, Line, {Len, lists:reverse(Array)}, FL);
decode_array([C | T], Line, Len, Array, FL) when ?IS_WHITESPACE(C) ->
	decode_array(T, Line, Len, Array, FL);
decode_array([$\n | T], Line, Len, Array, FL) ->
	decode_array(T, Line + 1, Len, Array, FL);
decode_array([], Line, Len, Array, FL) ->
	{more, fun(Next) -> decode_array(Next, Line, Len, Array, FL) end};
decode_array(T, Line, _Array, _Len, _FL) ->
	erlang:error({invalid_array, lists:sublist(T, ?ERR_INFO), Line}).

% 解码字符串方法
decode_string([?Q | T], Line, L, [Fun | FL]) ->
	Fun(T, Line, lists:reverse(L), FL);
decode_string([$\\, $u, U1, U2, U3, U4 | T], Line, L, FL)
	when ?IS_HEX(U1) andalso ?IS_HEX(U2) andalso ?IS_HEX(U3) andalso ?IS_HEX(U4) ->
	C = (z_lib:hex_digit(U1) bsl 12) bor (z_lib:hex_digit(U2) bsl 8)
		bor (z_lib:hex_digit(U3) bsl 4) bor z_lib:hex_digit(U4),
	decode_string(T, Line, [C | L], FL);
decode_string([$\\, C | T], Line, L, FL) ->
	decode_string(T, Line, [esc_to_char(C, T, Line) | L], FL);
decode_string([$\\], Line, L, FL) ->
	{more, fun(Next) -> decode_string([$\\ | Next], Line, L, FL) end};
decode_string([C | T], Line, L, FL) when C > 0, C < 16#80000000 ->
	decode_string(T, Line, [C | L], FL);
decode_string([], Line, L, FL) ->
	{more, fun(Next) -> decode_string(Next, Line, L, FL) end};
decode_string(T, Line, _L, _FL) ->
	erlang:error({invalid_str, lists:sublist(T, ?ERR_INFO), Line}).

% 转义符方法
esc_to_char(?Q, _, _) -> ?Q;
esc_to_char($/, _, _) -> $/;
esc_to_char($\\, _, _) -> $\\;
esc_to_char($n, _, _) -> $\n;
esc_to_char($t, _, _) -> $\t;
esc_to_char($r, _, _) -> $\r;
esc_to_char($b, _, _) -> $\b;
esc_to_char($f, _, _) -> $\f;
esc_to_char($v, _, _) -> $\v;
esc_to_char(C, T, Line) ->
	erlang:error({invalid_escape, lists:sublist([$\\, C | T], ?ERR_INFO), Line}).

% 解码简单字符串方法
decode_ss([C | T], Line, L, FL) when (C >= $A andalso C =< $Z) orelse
	(C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9) ->
	decode_ss(T, Line, [C | L], FL);
decode_ss([_ | _] = T, Line, L, [Fun | FL]) ->
	Fun(T, Line, lists:reverse(L), FL);
decode_ss([], Line, L, FL) ->
	{more, fun(Next) -> decode_string(Next, Line, L, FL) end}.

% 解码数字方法
decode_number([C | T], Line, L, Float, FL) when (C >= $0 andalso C =< $9) ->
	decode_number(T, Line, [C | L], Float, FL);
decode_number([$. | T], Line, L, _Float, FL) ->
	decode_number(T, Line, [$. | L], true, FL);
decode_number([C1, C2 | T], Line, L, _Float, FL)
	when (C1 =:= $e orelse C1 =:= $E) andalso (C2 =:= $+ orelse C2 =:= $-) ->
	decode_number(T, Line, [C2, C1 | L], true, FL);
decode_number([], Line, L, Float, [_] = FL) ->
	decode_number1([], Line, L, Float, FL);
decode_number([], Line, L, Float, FL) ->
	{more, fun(Next) -> decode_number(Next, Line, L, Float, FL) end};
decode_number([C | T], Line, L, Float, FL) when ?IS_WHITESPACE(C) ->
	decode_number1(T, Line, L, Float, FL);
decode_number([$\n | T], Line, L, Float, FL) ->
	decode_number1(T, Line + 1, L, Float, FL);
decode_number([C | _] = T, Line, L, Float, FL) when (C =:= $, orelse C =:= $} orelse C =:= $]) ->
	decode_number1(T, Line, L, Float, FL).

% 解码数字方法
decode_number1(T, Line, L, false, [Fun | FL]) ->
	Fun(T, Line, parse_int(L, 1, 0), FL);
decode_number1(T, Line, L, _, [Fun | FL]) ->
	try list_to_float(lists:reverse(L)) of
		F -> Fun(T, Line, F, FL)
	catch
		_:_ ->
			erlang:error({invalid_float,
				lists:sublist(lists:reverse(L, T), ?ERR_INFO), Line})
	end.

% 解析倒排的整数数字方法
parse_int([H], Digit, N) ->
	if
		H =:= $- -> -N;
		true -> (H - $0) * Digit + N
	end;
parse_int([H | T], Digit, N) ->
	parse_int(T, Digit * 10, (H - $0) * Digit + N).

% 解码负数方法
decode_negative_number([C | T], Line, FL) when (C >= $0 andalso C =< $9) ->
	decode_number(T, Line, [C, $-], false, FL);
decode_negative_number([], Line, FL) ->
	{more, fun(Next) -> decode_negative_number(Next, Line, FL) end};
decode_negative_number(T, Line, _FL) ->
	erlang:error({invalid_negative_number, lists:sublist(T, ?ERR_INFO), Line}).

% 解码true方法
decode_true([$r, $u, $e | T], Line, [Fun | FL]) ->
	Fun(T, Line, true, FL);
decode_true([_, _, _ | _] = T, Line, _FL) ->
	erlang:error({invalid_true, lists:sublist(T, ?ERR_INFO), Line});
decode_true(T, Line, FL) ->
	{more, fun(Next) -> decode_true(T ++ Next, Line, FL) end}.

% 解码false方法
decode_false([$a, $l, $s, $e | T], Line, [Fun | FL]) ->
	Fun(T, Line, false, FL);
decode_false([_, _, _, _ | _] = T, Line, _FL) ->
	erlang:error({invalid_false, lists:sublist(T, ?ERR_INFO), Line});
decode_false(T, Line, FL) ->
	{more, fun(Next) -> decode_false(T ++ Next, Line, FL) end}.

% 解码null方法
decode_null([$u, $l, $l | T], Line, [Fun | FL]) ->
	Fun(T, Line, null, FL);
decode_null([_, _, _ | _] = T, Line, _FL) ->
	erlang:error({invalid_null, lists:sublist(T, ?ERR_INFO), Line});
decode_null(T, Line, FL) ->
	{more, fun(Next) -> decode_null(T ++ Next, Line, FL) end}.
