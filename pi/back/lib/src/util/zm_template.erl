%%@doc 模板的编解码模块
%%```
%%% <!--{#name/}--> 表示根对象的name变量，
%%% <!--{#player.name/}--> 表示根对象中player对象的name变量，
%%% <!--{$name/}--> 表示当前对象中的name变量，
%%% <!--{$player.name/}--> 表示当前对象中player对象的name变量，
%%% <!--{$list}--> <BR><!--{$show/}-->&nbsp;<!--{#name/}-->&nbsp;<!--{#group.xyz/}--><BR> <!--{/$list}-->
%%%	表示当前对象中以list为名的列表，并循环该列表，
%%% <!--{$list}--> <BR><!--{$/}--> <!--{$[1]/}--><!--{/$list}-->
%%%	$表示列表中的每个元素，$[1]表示列表中的每个元素（必须是列表）的第1元素
%%% <!--{$list[1]/}--> 表示当前对象中list列表的第一个元素，注意：[]内不能为变量
%%% <!--{$list[1].name/}--> 表示当前对象中list列表的第一个元素name变量，
%%% <!--{length($list)/}--> 调用函数length，取当前对象中list列表的长度，
%%% <!--{tl($list)}--> <BR><!--{$show/}-->&nbsp;<!--{#name/}--><BR> <!--{/tl($list)}-->
%%%	调用函数tl，参数为当前对象中以list为名的列表，返回从list第2个元素开始的新列表
%%% <!--{lists:nthtail(2, $list)}--> <BR><!--{$show/}-->&nbsp;<!--{#name/}--><BR> <!--{/lists:nthtail(2, $list)}-->
%%%	调用函数lists:nthtail，参数2为当前对象中以list为名的列表，返回从list第2个元素开始的新列表
%%% 增加了变量字符串和数字相加的特性，<!-{1+#global.time[2]+#global.date[3]+"@@"/}->，得到变量的值，相邻是数字会相加，之后数字与字符串连接成字符串，当一个变量的值不存在时，整个都不显示。
%%% 增加了字符串解析的特殊化设置，<!-{#reply s=","}->， 在列表解析的开始，可在变量之后空格加上列表解析的特性设置，多个也用空格隔开，暂时有的是用s表示列表解析的分隔符，s的值必须用引号括起，并且当中不能有空格
%%'''
%%@end


-module(zm_template).

-description("template encode & decode").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([encode/1, decode/2]).

%%%=======================DEFINE=======================
-define(PREFIX, <<"<!--{">>).
-define(SUFFIX, <<"}-->">>).
-define(END_CHAR, $/).
-define(ROOT_CHAR, $#).
-define(CUR_CHAR, $$).
-define(FIELD_CHAR, $.).
-define(ELE_CHAR1, $[).
-define(ELE_CHAR2, $]).
-define(ADD_CHAR, $+).
-define(NUM, num).
-define(QUOTATION_CHAR, $").
-define(NUM_BEGIN, 48).
-define(NUM_END, 57).
-define(BLANK_CHAR, <<" ">>).
-define(EQUAL_CHAR, "=").


-define(ERLANG, erlang).
-define(MOD_CHAR, $:).
-define(FUN_CHAR1, $().
-define(FUN_CHAR2, $)).
-define(ARGUMENT_CHAR, $,).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  将数据编码模板
%% @spec  encode(Data) -> return()
%% where
%%  return() =  {Name, Type, list()}
%%@end
%% -----------------------------------------------------------------
encode(Bin) when is_binary(Bin) ->
	encode_prefix(Bin, {["", []], [], []}, [], 1, binary:compile_pattern(<<"\n">>));
encode(S) ->
	erlang:error({"must be binary", S}).

%% -----------------------------------------------------------------
%%@doc  将模板和对象解码成数据
%% @spec  decode({Name, Type, L}, Root) -> return()
%% where
%%  return() =  io_list()
%%@end
%% -----------------------------------------------------------------
decode({_Name, _Type, L}, Root) ->
	decode(L, Root, Root, []).

%%%===================LOCAL FUNCTIONS==================
% 找到前缀进行编码
encode_prefix(Bin, {Name, Type, L} = Template, TL, Line, CP) ->
	case binary:split(Bin, ?PREFIX) of
		[<<>>, Bin2] ->
			encode_suffix(Bin2, Template, TL, Line, CP);
		[Bin1, Bin2] ->
			encode_suffix(Bin2, {Name, Type, [Bin1 | L]}, TL,
						  Line + z_lib:binary_match_count(Bin1, CP), CP);
		[Bin] ->
			case TL of
				[_] ->
					erlang:error({"end without match", {name, Name}, {line, Line}});
				[] ->
					{Name, Type, [Bin | L]}
			end
	end.

% 找到后缀进行编码
encode_suffix(Bin, Template, TL, Line, CP) ->
	case binary:split(Bin, ?SUFFIX) of
		[<<>>, _Bin2] ->
			erlang:error({"var is null", {line, Line}});
		[Bin1, Bin2] ->
			parse(Bin1, Bin2, Template, TL, Line + z_lib:binary_match_count(Bin1, CP), CP);
		[Bin] ->
			erlang:error({"must be end with suffix", {line, Line}})
	end.

% 分析变量的类型
parse(Bin1, Bin2, {[Name, TupleList], Type, L} = Template, TL, Line, CP) ->
	case binary:first(Bin1) of
	% 模板结束
		?END_CHAR ->
			case binary_to_string(Bin1, 2, byte_size(Bin1), Line) of
				Name ->
					case TL of
						[{N1, T1, L1} | T] ->
							encode_prefix(Bin2, {N1, T1, [Template | L1]}, T, Line, CP);
						[] ->
							erlang:error({"root match end", {name, Name}, {line, Line}})
					end;
				Name1 ->
					erlang:error({"list does not match", {Name, Name1}, {line, Line}})
			end;
		_ ->
			case binary:last(Bin1) of
			% 变量
				?END_CHAR ->
					Name1 = binary_to_string(Bin1, 1, byte_size(Bin1) - 1, Line),
					SpliteList = z_lib:split(Name1, ?ADD_CHAR),
					if
						length(SpliteList) > 1 ->
							[NameList, ParseList] = parse_name(SpliteList, [], [], Line),
							encode_prefix(Bin2, {[Name, TupleList], Type, [{NameList, ParseList} | L]}, TL, Line, CP);
						true ->
							encode_prefix(Bin2, {[Name, TupleList], Type, [{Name1, parse_name(Name1, Line)} | L]}, TL, Line, CP)
					end;
			% 列表模板开始
				_ ->
					Name1 = binary_to_list(Bin1),
					case binary:match(Bin1, ?BLANK_CHAR) of
						false ->
							encode_prefix(Bin2, {[Name1, []], parse_name(Name1, Line), []}, [Template | TL], Line, CP);
						_ ->
							[ListName|SpeList] = z_lib:split(Name1, " "),
							SpecialTList = list_to_kv(SpeList, []),
							encode_prefix(Bin2, {[ListName, SpecialTList], parse_name(ListName, Line), []}, [Template | TL], Line, CP)
					end
			end
	end.

list_to_kv([S|T], SpecialTList) ->
	[Key, Value] = z_lib:split(S, ?EQUAL_CHAR),
	case {lists:last(Value), hd(Value)} of
		{?QUOTATION_CHAR, ?QUOTATION_CHAR} ->
			list_to_kv(T, [{list_to_atom(Key), lists:sublist(Value, 2, length(Value) - 2)}|SpecialTList]);
		_ -> erlang:error({"quotation is not match in list analysis", {value, Value}})
	end;
list_to_kv([], SpecialTList) ->
	SpecialTList.

%%分析‘+’拆分而来的列表的变量名称，返回name和type列表
%%  #skill[1]+1+"pass_one_time"+$name[1] 拆分转换之后会得到：
%%例如：name:[#skill[1]，$name[1]]      type：[{$#,["skill","1"]},{num,1},{$","pass_one_time"},{$$,["name","1"]}]
parse_name([F|T], NameList, ParseList, Line) ->
	S = string:strip(F),
	case z_lib:check_list_range(S, ?NUM_BEGIN, ?NUM_END) of
		false ->
			case hd(S) of
				?QUOTATION_CHAR ->
					parse_name(T, NameList, [{?QUOTATION_CHAR, lists:sublist(S, 2, length(S) - 2)}|ParseList], Line);
				_ -> parse_name(T, [S|NameList], [parse_name(S, Line)|ParseList], Line)
			end;
		_ -> parse_name(T, NameList, [{?NUM, list_to_integer(S)}|ParseList], Line)
	end;
parse_name([], NameList, ParseList, _Line) ->
	[lists:reverse(NameList), lists:reverse(ParseList)].


% 分析变量名称
parse_name([?ROOT_CHAR | Name], Line) ->
	{?ROOT_CHAR, parse_name_list(
		z_lib:split(Name, ?FIELD_CHAR), [], Name, Line)};
parse_name([?CUR_CHAR | Name], Line) ->
	{?CUR_CHAR, parse_name_list(
		z_lib:split(Name, ?FIELD_CHAR), [], Name, Line)};
parse_name(Name, Line) ->
	case z_lib:split(Name, {?FUN_CHAR1, ?FUN_CHAR2}) of
		{Left, Middle, Right} ->
			{M, F} = try
				case z_lib:split(Left, ?MOD_CHAR) of
					[Mod, Fun] ->
						{list_to_atom(Mod), list_to_atom(Fun)};
					[Left] ->
						{?ERLANG, list_to_atom(Left)}
				end
					 catch
						 _:_ ->
							 erlang:error({"invalid fun name", {name, Name}, {line, Line}})
					 end,
			{?MOD_CHAR, M, F, parse_args(Middle, Line), parse_name_list(
				z_lib:split(Right, ?FIELD_CHAR), [], Name, Line)};
		end_without_match ->
			case z_lib:scan_parse_term(Name ++ ".") of
				{ok, Term} ->
					{?ARGUMENT_CHAR, Term};
				_E ->
					erlang:error({"invalid term", {name, Name}, {line, Line}})
			end;
		{right_not_match, _} ->
			erlang:error({"fun without match", {name, Name}, {line, Line}})
	end.

% 分析变量名称列表
parse_name_list([H | T], L, Name, Line) ->
	case z_lib:split(H, {?ELE_CHAR1, ?ELE_CHAR2}) of
		{Left, Middle, Right} ->
			N1 = try list_to_integer(Middle) of
					 N -> N
				 catch
					 _:_ ->
						 erlang:error({"index must be integer", {name, Name}, {line, Line}})
				 end,
			L1 = case Left of
					 "" ->
						 L;
					 _ ->
						 [Left | L]
				 end,
			case Right of
				"" ->
					parse_name_list(T, [N1 | L1], Name, Line);
				[$[ | _] ->
					parse_name_list([Right | T], [N1 | L1], Name, Line);
				_ ->
					erlang:error({"invaid char", {name, Name}, {line, Line}})
			end;
		end_without_match when H =:= "" ->
			erlang:error({"zero length string", {name, Name}, {line, Line}});
		end_without_match ->
			parse_name_list(T, [H | L], Name, Line);
		{right_not_match, _} ->
			erlang:error({"element without match", {name, Name}, {line, Line}})
	end;
parse_name_list([], L, _Name, _Line) ->
	lists:reverse(L).

% 分析函数参数
parse_args([], _Line) ->
	[];
parse_args(Args, Line) ->
	L = z_lib:split(Args, ?ARGUMENT_CHAR, {?FUN_CHAR1, ?FUN_CHAR2}),
	[parse_name(string:strip(E), Line) || E <- L].

% 二进制转换成字符串，如果错误，抛出异常
binary_to_string(Bin, Start, Stop, Line) when Start > Stop ->
	erlang:error({"invalid var", {Bin, Start, Stop}, {line, Line}});
binary_to_string(Bin, Start, Stop, _Line) ->
	binary_to_list(Bin, Start, Stop).

% 解码模板到iolist中
decode([H | T], Root, Cur, IOList) when is_binary(H) ->
	decode(T, Root, Cur, [H | IOList]);
decode([H | T], Root, Cur, IOList) when is_list(H) ->
	decode(T, Root, Cur, [H | IOList]);
decode([{_, Type} | T], Root, Cur, IOList) ->
	decode(T, Root, Cur, decode_var(Root, Cur, Type, IOList));
decode([{[_, SpecialTList], Type, L} | T], Root, Cur, IOList) ->
	case get_list(Root, Cur, Type) of
		false ->
			decode(T, Root, Cur, IOList);
		LL ->
			decode(T, Root, Cur, decode_list(
				L, Root, lists:reverse(LL), IOList, SpecialTList))
	end;
decode([], _Root, _Cur, IOList) ->
	IOList.

% 循环列表，解码列表到iolist中
decode_list(L, Root, [Cur | T], IOList, SpecialTList) ->
	if
		length(T) > 0 -> case get_special_value(SpecialTList, s) of
							 false -> decode_list(L, Root, T, decode(L, Root, Cur, IOList), SpecialTList);
							 Sign -> decode_list(L, Root, T, [Sign|decode(L, Root, Cur, IOList)], SpecialTList)
						 end;
		true -> decode_list(L, Root, T, decode(L, Root, Cur, IOList), SpecialTList)
	end;
decode_list(_, _Root, [], IOList, _SpecialTList) ->
	IOList.

%得到列表解析特殊设置的值
get_special_value([{Atom, Value}|_T], Atom) ->
	Value;
get_special_value([_S|T], Atom) ->
	get_special_value(T, Atom);
get_special_value([], _Atom) ->
	false.

% 解码变量所对应的值到iolist中
decode_var(Root, Cur, Type, IOList) ->
	try
		case is_list(Type) of
			true -> [z_lib:any_to_iolist(get_vars(Root, Cur, Type, [])) | IOList];
			_ -> [z_lib:any_to_iolist(get_var(Root, Cur, Type)) | IOList]
		end
	catch
		_:_ ->
			IOList
	end.

% 根据名字，获得对应的列表
get_list(Root, Cur, Type) ->
	try get_var(Root, Cur, Type) of
		V when is_list(V) ->
			V;
		V when is_tuple(V) ->
			tuple_to_list(V);
		_ ->
			false
	catch
		_:_ ->
			false
	end.
%根据名字列表，得到值
get_vars(Root, Cur, [S|T], Result) ->
	case Answer = get_var(Root, Cur, S) of
		[] -> [];
		_ -> get_vars(Root, Cur, T, [Answer|Result])
	end;
get_vars(_Root, _Cur, [], Result) ->
	num_add(Result, 0, [], false).


%取出数字，相加
num_add([S|T], Sum, Result, Sign) ->
	if
		is_integer(S) -> num_add(T, Sum + S, Result, true);
		Sign =:= true -> num_add(T, 0, [S, integer_to_list(Sum), Result], false);
		true -> num_add(T, 0, [S|Result], Sign)
	end;

num_add([], Sum, Result, Sign) ->
	if
		Sign =:= true -> [integer_to_list(Sum)|Result];
		true -> Result
	end.

% 根据名字，获得值
get_var(_Root, _Cur, {?NUM, Number}) ->
	Number;
get_var(_Root, _Cur, {?QUOTATION_CHAR, String}) ->
	String;
get_var(Root, _Cur, {?ROOT_CHAR, Name}) ->
	get_var(Root, Name);
get_var(_Root, Cur, {?CUR_CHAR, Name}) ->
	get_var(Cur, Name);
get_var(Root, Cur, {?MOD_CHAR, M, F, A, Name}) ->
	get_var(apply(M, F, [get_var(Root, Cur, E) || E <- A]), Name);
get_var(_Root, _Cur, {?ARGUMENT_CHAR, Term}) ->
	Term.

% 根据名字，从当前的对象中获得值
get_var(Obj, [N | T]) when is_list(Obj), is_integer(N) ->
	get_var(lists:nth(N, Obj), T);
get_var(Obj, [Name | T]) when is_list(Obj) ->
	get_var(element(2, lists:keyfind(Name, 1, Obj)), T);
get_var(Obj, [N | T]) when is_tuple(Obj), is_integer(N) ->
	get_var(element(N, Obj), T);
get_var(Obj, [Name | T]) when is_tuple(Obj) ->
	get_var(sb_trees:get(Name, Obj, ""), T);
get_var(Obj, []) ->
	Obj.


