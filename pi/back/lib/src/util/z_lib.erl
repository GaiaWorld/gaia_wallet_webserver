%%@doc 基础库函数
%%@end


-module(z_lib).

-description("z lib").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([aes_ecb_encrypt/3, aes_ecb_decrypt/3]).
-export([random/1, random/2, random/3]).
-export([now_second/0, now_millisecond/0]).
-export([second_to_datetime/1, second_to_localtime/1, datetime_to_second/1, localtime_to_second/1]).
-export([while/2, for/4, foreach/3, map_reduce/3]).
-export([get_values/2, get_values/3, get_values/4]).
-export([get_value/3, get_value/4, get_value/5, get_column/2, set_column/3]).
-export([scan_parse_term/1, scan_parse_eval/2, string/1, get_mod_attr/2]).
-export([send/2, reply/2]).
-export([get_stacktrace/0]).
-export([string_stacktrace/1]).
-export([pid_dictionary/1, pid_dictionary/2]).
-export([fapply/2, mapply/3]).
-export([char_filter/2, char_replace/2, char_replace/3]).
-export([split_first/2, split/2, split/3, split_filter/3]).
-export([is_iodata/1, get_list_range/1, check_list_range/3]).
-export([char_to_utf8/1, char_to_utf8/2, characters_to_utf8/1, utf8_to_characters/1, rutf8_to_characters/1]).
-export([digit_hex/1, digit_hex_lower/1, hex_digit/1, bin_to_hex_str/2, hex_str_to_bin/1, bin_to_hex/1]).
-export([get_integer_digit/2, integer_to_list/2]).
-export([to_atom/1, to_atom/2, any_to_iolist/1]).
-export([read_dynamic_length/2, read_dynamic_data/2]).
-export([binary_get_part/4, binary_match_count/2, binary_match_count/4]).
-export([binary_xor/2, binary_xor/4]).
-export([compare/2, half_keyfind/4, half_find/2, half_find/3, half_find/4]).
-export([tuple_foreach/3, tuple_insert/3, tuple_delete/2]).
-export([shuffle/1, shuffle/2]).
-export([member_order/2, delete_order/2]).
-export([merge_order/2, merge_order_desc/2, compare_order/2, compare_order/3]).
-export([list_index/2, list_insert_index/3, list_delete_index/2]).
-export([list_replace/3, list_delete/2, list_deletes/2]).
-export([list_keydeletes/3, list_keydelete/3, list_keyreplace/4, list_merge/2]).
-export([ets_select/3, ets_key_select/3, ets_value_select/4]).
-export([merge_tree/2, merge_tree/4]).
-export([safe_path/1, dir_path/1, make_dir/1, get_file_info/1, list_file/2, list_file/3]).
-export([compress/1, uncompress/1, gzip/1, gunzip/1, zip/1, unzip/1]).
-export([gunzip/2, uncompress/2, unzip/2]).

%%%=======================INLINE=======================
-compile({inline, [random/3, for1/4, for2/4, get_value/5, get_column1/2, set_column_list/3, set_column_list1/3, string1/2, string_text/2, string_array/2, string_atom/2, atom_valid_char/1, string_stacktrace/2, char_filter_1/5, char_filter_2/5, char_replace_1/6, char_replace_2/5, split_first_1/3, split_first_2/3, split_1/4, split_2/4, split_match/2, split_3/5, split_4/6, split_filter_1/5, split_filter_2/5, split_filter_3/5, split_filter_4/5, get_list_range/4, utf8_to_characters/2, rutf8_to_characters/2, digit_hex/1, digit_hex_lower/1, hex_digit/1, bin_to_hex_str1/2, bin_to_hex/2, hex2/1, get_integer_digit/3, padding/2, check_list_range/4, binary_match_count/6, xor_binary/6, xor_binary/5, xor_iolist/4, compare/2, half_keyfind1/6, half_find1/4, half_find1/5, half_find1/6, tuple_foreach/4, shuffle1/3, delete_order/3, merge_order/3, merge_order_desc/3, compare_order/5, compare_order1/6, compare_order2/6, list_index/3, list_insert_index/4, list_delete_index/3, list_replace/5, list_delete/4, list_deletes/5, list_keydeletes/6, list_keydelete/4, list_keyreplace/5, list_merge/3, ets_select1/4, ets_key_select1/4, ets_value_select1/5, merge_tree1/3, merge_tree1/5, safe_path/2, dir_path1/1, traversal_file/6, file_skip/4, collect/4]}).
-compile({inline_size, 32}).
-compile([native, {hipe, [o3]}]).

%%%=======================DEFINE=======================
-define(MAX_POSITIVE_INT32, 16#7fffffff).
-define(MAX_NEGATIVE_INT32, -16#7fffffff).

-define(RAND_A, 16807).
-define(RAND_Q, 127773).
-define(RAND_MASK, 123459876).

-define(RANDOM_SEED, '$RandomSeed').

-define(SECONDS_PER_DAY, 86400).
-define(DAYS_FROM_0_TO_1970, 719528).

-define(Q, 34).
-define(SQ, 39).

-define(MAX_WBITS, 15).
-define(INFLATE, 14).
-define(Z_NO_FLUSH, 0).

%%参数模块的执行方法
-define(MF(MA, F), (element(1, MA)):F(MA).

%%%=======================INCLUDE=======================
-include_lib("kernel/include/file.hrl").

%%%===============EXPORTED FUNCTIONS===============

%%
%%对字符串或iolist进行aes ecb加密
%%
aes_ecb_encrypt(Type, Key, Data) 
  when (is_integer(Type) andalso (Type =:= 16 orelse Type =:= 24 orelse Type =:= 32)) andalso is_binary(Data) ->
		Size=byte_size(Data),
		FillLen=case Type - size(Data) rem Type of
			0 ->
				Type;
			Len ->
				Len
		end,
		FillData=binary:copy(<<FillLen>>, FillLen),
		Bin= <<Data:Size/binary, FillData/binary>>,
		crypto:block_encrypt(aes_ecb, Key, Bin);
aes_ecb_encrypt(Type, Key, List) when is_list(List) ->
	aes_ecb_encrypt(Type, Key, binary:list_to_bin(List)).

%%
%%对aes ecb加密的数据进行解密
%%
aes_ecb_decrypt(Type, Key, Bin) 
  when (is_integer(Type) andalso (Type =:= 16 orelse Type =:= 24 orelse Type =:= 32)) andalso is_binary(Bin) ->
	<<<<X>> || <<X>> <= crypto:block_decrypt(aes_ecb, Key, Bin), X > Type>>.

%% -----------------------------------------------------------------
%% Function: random/1
%% Description: 随机数生成器，采用倍增同余算法，返回正整数范围的随机数
%% Returns: integer()
%% -----------------------------------------------------------------
random({_, _, _} = Seed) ->
	{R, _}=random:uniform_s(?MAX_POSITIVE_INT32, Seed),
	R;
random(Seed) ->
	S=erlang:abs(Seed),
	{R, _}=random:uniform_s(?MAX_POSITIVE_INT32, {S div 1000000000000, S div 1000000 rem 1000000, S rem 1000000}),
	R.

%% -----------------------------------------------------------------
%%@doc 生成指定范围的随机数（随机值可以取到N1和N2）
%% @spec random(N1::integer(),N2::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
random(N1, N2) ->
	Seed = case get(?RANDOM_SEED) of
		{_, _, _} = S ->
			S;
		_ ->
			{M, S, MicroSecs} = os:timestamp(),
			{M, S, MicroSecs + erlang:phash2(self())}
	end,
	{R, S1}=random:uniform_s(?MAX_POSITIVE_INT32, Seed),
	put(?RANDOM_SEED, S1),
	random(R, N1, N2).

%% -----------------------------------------------------------------
%%@doc 用指定的种子，生成指定范围的随机数（随机值可以取到N1和N2）
%% @spec random(Seed::integer(),N1::integer(),N2::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
random(Seed, N1, N2) when N2 > N1 ->
	(Seed rem (N2 - N1 + 1)) + N1;
random(Seed, N1, N2) when N2 < N1 ->
	(Seed rem (N1 - N2 + 1)) + N2;
random(_Seed, N, N) ->
	N.

%% -----------------------------------------------------------------
%%@doc 操作系统当前的秒
%% @spec now_second() -> integer()
%%@end
%% -----------------------------------------------------------------
-spec now_second() ->
	integer().
%% -----------------------------------------------------------------
now_second() ->
	{M, S, _} = os:timestamp(),
	M * 1000000 + S.

%% -----------------------------------------------------------------
%%@doc 操作系统当前的毫秒
%% @spec now_millisecond() -> integer()
%%@end
%% -----------------------------------------------------------------
-spec now_millisecond() ->
	integer().
%% -----------------------------------------------------------------
now_millisecond() ->
	{M, S, MS} = os:timestamp(),
	M * 1000000000 + S * 1000 + MS div 1000.

%% -----------------------------------------------------------------
%%@doc 根据1970年的秒数转成公历时间
%% @spec second_to_datetime(Second::integer()) -> {{Y::integer(),M::integer(),D::integer()},{H::integer(),M::integer(),S::integer()}}
%%@end
%% -----------------------------------------------------------------
-spec second_to_datetime(integer()) ->
	{{integer(), integer(), integer()}, {integer(), integer(), integer()}}.
%% -----------------------------------------------------------------
second_to_datetime(Sec) ->
	{calendar:gregorian_days_to_date(
		Sec div ?SECONDS_PER_DAY + ?DAYS_FROM_0_TO_1970),
		calendar:seconds_to_time(Sec rem ?SECONDS_PER_DAY)}.

%% -----------------------------------------------------------------
%%@doc 根据1970年的秒数转成本地时区时间
%% @spec second_to_localtime(Second::integer()) -> {{Y::integer(),M::integer(),D::integer()},{H::integer(),M::integer(),S::integer()}}
%%@end
%% -----------------------------------------------------------------
-spec second_to_localtime(integer()) ->
	{{integer(), integer(), integer()}, {integer(), integer(), integer()}}.
%% -----------------------------------------------------------------
second_to_localtime(Sec) ->
	erlang:universaltime_to_localtime({calendar:gregorian_days_to_date(
		Sec div ?SECONDS_PER_DAY + ?DAYS_FROM_0_TO_1970),
		calendar:seconds_to_time(Sec rem ?SECONDS_PER_DAY)}).

%% -----------------------------------------------------------------
%%@doc 根据公历时间转成1970年的秒数
%% @spec datetime_to_second({{Y::integer(),M::integer(),D::integer()},{H::integer(),M::integer(),S::integer()}}) -> integer()
%%@end
%% -----------------------------------------------------------------
-spec datetime_to_second({{integer(), integer(), integer()}, {integer(), integer(), integer()}}) ->
	integer().
%% -----------------------------------------------------------------
datetime_to_second({Date, Time}) ->
	?SECONDS_PER_DAY * (calendar:date_to_gregorian_days(Date) - ?DAYS_FROM_0_TO_1970)
	+ calendar:time_to_seconds(Time).

%% -----------------------------------------------------------------
%%@doc 根据本地时区时间转成1970年的秒数
%% @spec localtime_to_second({{Y::integer(),M::integer(),D::integer()},{H::integer(),M::integer(),S::integer()}}) -> integer()
%%@end
%% -----------------------------------------------------------------
-spec localtime_to_second({{integer(), integer(), integer()}, {integer(), integer(), integer()}}) ->
	integer().
%% -----------------------------------------------------------------
localtime_to_second(DateTime) ->
	{Date, Time} = erlang:localtime_to_universaltime(DateTime),
	?SECONDS_PER_DAY * (calendar:date_to_gregorian_days(Date) - ?DAYS_FROM_0_TO_1970)
	+ calendar:time_to_seconds(Time).

%% -----------------------------------------------------------------
%%@doc 循环调用
%%```
%%	Fun Arguments:[Args::term()]
%%	Fun Returns:
%%		{break, Args::term()} | %循环结束
%%		{ok, Args::term()} | %表示继续循环
%%		Args::term() %简化版，表示继续循环
%%'''
%% @spec while(F::function(),Argus::term()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec while(function(), any()) ->
	any().
%% -----------------------------------------------------------------
while(F, Args) ->
	case F(Args) of
		{ok, A} ->
			while(F, A);
		{break, A} ->
			A;
		A ->
			while(F, A)
	end.

%% -----------------------------------------------------------------
%%@doc 循环调用
%%```
%%	Fun Arguments:[Args::term(), I::integer()]
%%	Fun Returns:
%%		{break, Args::term()} | %循环结束
%%		{ok, Args::term()} | %表示继续循环
%%		Args::term() %简化版，表示继续循环
%%'''
%% @spec for(F::function(),Argus::any(),I::integer(),N::integer()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec for(function(), any(), integer(), integer()) ->
	any().
%% -----------------------------------------------------------------
for(F, Args, I, N) when I < N ->
	for1(F, Args, I, N);
for(F, Args, I, N) when I > N ->
	for2(F, Args, I, N);
for(_F, Args, N, N) -> Args.

for1(F, Args, I, N) when I < N ->
	case F(Args, I) of
		{ok, A} ->
			for1(F, A, I + 1, N);
		{break, A} ->
			A;
		A ->
			for1(F, A, I + 1, N)
	end;
for1(_F, Args, _, _) -> Args.

for2(F, Args, I, N) when I > N ->
	case F(Args, I) of
		{ok, A} ->
			for2(F, A, I - 1, N);
		{break, A} ->
			A;
		A ->
			for2(F, A, I - 1, N)
	end;
for2(_F, Args, _, _) -> Args.

%% -----------------------------------------------------------------
%%@doc 循环调用列表上的元素
%%```
%%	Fun Arguments:[Args::term(), I]
%%	Fun Returns:
%%		{break, Args::term()} | %循环结束
%%		{ok, Args::term()} | %表示继续循环
%%		Args::term() %简化版，表示继续循环
%%'''
%% @spec foreach(F::function(),Argus::any(),List::list()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec foreach(function(), any(), list()) ->
	any().
%% -----------------------------------------------------------------
foreach(F, Args, [H | T]) ->
	case F(Args, H) of
		{break, A} ->
			A;
		{ok, A} ->
			foreach(F, A, T);
		A ->
			foreach(F, A, T)
	end;
foreach(_F, Args, []) ->
	Args.

%% -----------------------------------------------------------------
%%@doc 归并映射算法
%% @spec (F::function(),List::list(),Timeout::integer()) -> return()
%%	|({M,F,A},List::list(),Timeout::integer()) -> return()
%%	|(any(),[],any()) -> return()
%% where
%% return() = timeout | {ok, RList::list()} | {error, Reason, StackTrace}
%%@end
%% -----------------------------------------------------------------
-spec map_reduce({atom(), atom(), any()} | function(), list(), integer() | infinity) ->
	timeout | {ok, list()} | {error, any(), list()}.
%% -----------------------------------------------------------------
map_reduce(Fun, [_ | _] = L, Timeout)
	when is_function(Fun), is_integer(Timeout), Timeout > 0 ->
	map_reduce_(Fun, L, Timeout);
map_reduce({M, F, A}, [_ | _] = L, Timeout)
	when is_atom(M), is_atom(F), is_list(A), is_integer(Timeout), Timeout > 0 ->
	map_reduce_(fun (Args) -> M:F(A, Args) end, L, Timeout);
map_reduce(_Fun, [], _) ->
	{ok, []}.

map_reduce_(Fun, [_ | _] = L, Timeout) ->
	Parent = self(),
	Pid = spawn_link(
		fun() ->
			process_flag(trap_exit, true),
			map_reduce_spawn(Parent, self(), Fun, L, 1, [])
		end),
	receive
		{Pid, Result} -> Result
	after Timeout ->
		unlink(Pid),
		Pid ! {'EXIT', Parent, timeout},
		timeout
	end.

map_reduce_spawn(Parent, Pid, Fun, [H | T], N, L) ->
	map_reduce_spawn(Parent, Pid, Fun, T, N + 1, [
		spawn_link(
			fun() ->
				R = try
					{ok, Parent, N, Fun(H)}
				catch
					Error:Reason ->
						{error, Parent, self(), {error, {Error, Reason},
							erlang:get_stacktrace()}}
				end,
				unlink(Pid),
				Pid ! R
			end) | L]);
map_reduce_spawn(Parent, _Pid, _Fun, [], N, L) ->
	map_reduce_receive(Parent, N, list_to_tuple(L)).

map_reduce_receive(Parent, Amount, RT) ->
	receive
		{ok, Parent, N, Result} when Amount > 2 ->
			map_reduce_receive(Parent,
				Amount - 1, setelement(N, RT, Result));
		{ok, Parent, N, Result} ->
			Parent ! {self(), {ok,
				tuple_to_list(setelement(N, RT, Result))}};
		{error, Parent, Pid, Error} ->
			unlink(Parent),
			Parent ! {self(), Error},
			exit({Pid, Error});
		{'EXIT', Parent, Why} ->
			exit({Parent, Why})
	end.

%% -----------------------------------------------------------------
%%@doc 从元组列表或Tree中，获得指定的键值，如果键不存在，使用默认值
%%```
%% KVList默认为[{Key::term(),Value::term()}]结构，如果元组内大于2个元素，则默认第一位为key，第二位为value
%%'''
%% @spec (KVList::list(),KeyDefaultList::[{Key::term(),Value::term()}]) -> [Value::term()]
%%	|(Tree::tuple(),KeyDefaultList::[{Key::term(),Value::term()}]) -> [Value::term()]
%%@end
%% -----------------------------------------------------------------
-spec get_values(list() | tuple(), list()) ->
	list().
%% -----------------------------------------------------------------
get_values(KVList, KeyDefaultList) when is_list(KVList) ->
	[get_value(KVList, K, 1, 2, D) || {K, D} <- KeyDefaultList];
get_values(Tree, KeyDefaultList) when is_tuple(Tree) ->
	[sb_trees:get(K, Tree, D) || {K, D} <- KeyDefaultList].

%% -----------------------------------------------------------------
%%@doc 从元组列表或Tree中，获得指定的键值，如果键不存在，使用默认值
%%```
%% 与get_values/2方法不同处在于，此方法可以指定value的位置
%%'''
%% @spec (KVList::list(),ValuePos::integer(),KeyDefaultList::[{Key::term(),Value::term()}]) -> [Value::term()]
%%	|(Tree::tuple(),ValuePos::integer(),KeyDefaultList::[{Key::term(),Value::term()}]) -> [Value::term()]
%%@end
%% -----------------------------------------------------------------
-spec get_values(list(), integer(), list()) ->
	list().
%% -----------------------------------------------------------------
get_values(KVList, ValuePos, KeyDefaultList) when is_list(KVList) ->
	[get_value(KVList, K, 1, ValuePos, D) || {K, D} <- KeyDefaultList];
get_values(Tree, ValuePos, KeyDefaultList) when is_tuple(Tree) ->
	[get_value1(Tree, K, ValuePos, D) || {K, D} <- KeyDefaultList].

%% -----------------------------------------------------------------
%%@doc 从元组列表或Tree中，获得指定的键值，如果键不存在，使用默认值
%%```
%% 与get_values/2方法不同处在于，此方法key的位置和value的位置都可以指定
%%'''
%% @spec get_values(KVList::list(), KeyPos::integer(), ValuePos::integer(), KeyDefaultList::list()) -> [Value]
%%@end
%% -----------------------------------------------------------------
-spec get_values(list(), integer(), integer(), list()) ->
	list().
%% -----------------------------------------------------------------
get_values(KVList, KeyPos, ValuePos, KeyDefaultList) when is_list(KVList) ->
	[get_value(KVList, K, KeyPos, ValuePos, D) || {K, D} <- KeyDefaultList].

%% -----------------------------------------------------------------
%%@doc 从元组列表或Tree中，获得指定的键值，如果键不存在，使用默认值
%%```
%% KVList默认为[{Key::term(),Value::term()}]结构，如果元组内大于2个元素，则默认第一位为key，第二位为value
%%'''
%% @spec (Tree::tuple(), Key::term(), Default::term()) -> any()
%%	|(KVList::list(), Key::term(), Default::term()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec get_value(list() | tuple(), any(), any()) ->
	any().
%% -----------------------------------------------------------------
get_value(Tree, Key, Default) when is_tuple(Tree) ->
	sb_trees:get(Key, Tree, Default);
get_value(KVList, Key, Default) when is_list(KVList) ->
	get_value(KVList, Key, 1, 2, Default);
get_value(_Tree, _Key, Default) ->
	Default.

%% -----------------------------------------------------------------
%%@doc 从元组列表或Tree中，获得指定的键值，如果键不存在，使用默认值
%%```
%% 与get_value/3方法不同处在于，此方法可以指定value的位置
%%'''
%% @spec (Tree::tuple(), Key::any(), ValuePos::integer(), Default::any()) -> any()
%%	|(KVList::list(), Key::any(), ValuePos::integer(), Default::any()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec get_value(list(), any(), integer(), any()) ->
	any().
%% -----------------------------------------------------------------
get_value(Tree, Key, ValuePos, Default) when is_tuple(Tree) ->
	get_value1(Tree, Key, ValuePos, Default);
get_value(KVList, Key, ValuePos, Default) when is_list(KVList) ->
	get_value(KVList, Key, 1, ValuePos, Default);
get_value(_Tree, _Key, _ValuePos, Default) ->
	Default.

get_value1(Tree, Key, ValuePos, Default) ->
	case sb_sets:get(Key, Tree, none) of
		V when is_tuple(V), tuple_size(V) >= ValuePos ->
			element(ValuePos, V);
		_ ->
			Default
	end.

%% -----------------------------------------------------------------
%%@doc 从元组列表或Tree中，获得指定的键值，如果键不存在，使用默认值
%%```
%% 与get_value/3方法不同处在于，此方法key的位置和value的位置都可以指定
%%'''
%% @spec get_value(KVList::list(), Key::any(), KeyPos::integer(), ValuePos::integer(), Default::any()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec get_value(list(), any(), integer(), integer(), any()) ->
	any().
%% -----------------------------------------------------------------
get_value(KVList, Key, KeyPos, ValuePos, Default) ->
	case lists:keyfind(Key, KeyPos, KVList) of
		T when tuple_size(T) >= ValuePos ->
			element(ValuePos, T);
		_ ->
			Default
	end.


%% -----------------------------------------------------------------
%%@doc  获得值中列域，允许双层数字列表
%% @spec  get_column(Value, Column::integer()) -> return()
%% where
%%  return() = Value
%%@end
%% -----------------------------------------------------------------
get_column(Value, 0) ->
	Value;
get_column(Value, Column) when is_integer(Column) ->
	element(Column, Value);
get_column(Value, Column) ->
	[get_column1(Value, I) || I <- Column].

get_column1(Value, 0) ->
	Value;
get_column1(Value, Column) when is_integer(Column) ->
	element(Column, Value);
get_column1(Value, Column) ->
	[element(I, Value) || I <- Column].

%% -----------------------------------------------------------------
%%@doc  设置值中列域，允许双层数字列表
%% @spec  set_column(OldValue, Column, Value) -> return()
%% where
%%  return() = Value
%%@end
%% -----------------------------------------------------------------
set_column(_OldValue, 0, Value) ->
	Value;
set_column(OldValue, Column, Value) when is_integer(Column) ->
	setelement(Column, OldValue, Value);
set_column(OldValue, Column, Value) ->
	set_column_list(OldValue, Column, Value).

set_column_list(OldValue, [Column | T1], [Value | T2]) when is_integer(Column) ->
	set_column_list(setelement(Column, OldValue, Value), T1, T2);
set_column_list(OldValue, [Column | T1], [Value | T2]) ->
	set_column_list(set_column_list1(OldValue, Column, Value), T1, T2);
set_column_list(OldValue, [], []) ->
	OldValue.

set_column_list1(OldValue, [Column | T1], [Value | T2]) ->
	set_column_list1(setelement(Column, OldValue, Value), T1, T2);
set_column_list1(OldValue, [], []) ->
	OldValue.

%% -----------------------------------------------------------------
%%@doc 将字符串转换成erlang的term，字符串必须以“.”结尾
%% @spec scan_parse_term(Text::string()) -> return()
%% where
%% return() = {ok, Term} | {error, ErrorInfo} | {error, Reason, EndLine}
%%@end
%% -----------------------------------------------------------------
-spec scan_parse_term(string()) ->
	{ok, any()} | {error, any()} | {error, any(), integer()}.
%% -----------------------------------------------------------------
scan_parse_term(Text) ->
	case erl_scan:string(Text) of
		{ok, Tokens, _EndLine} ->
			erl_parse:parse_term(Tokens);
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 动态执行Erlang表达式，字符串必须以“.”结尾
%% @spec scan_parse_eval(Text::string()) -> return()
%% where
%% return() = {value, Value, NewBindings} | {error, ErrorInfo} | {error, Reason, EndLine}
%%@end
%% -----------------------------------------------------------------
-spec scan_parse_eval(string(), [{term(), term()}]) ->
	{value, any(), [{term(), term()}]} | {error, any()} | {error, any(), integer()}.
%% -----------------------------------------------------------------
scan_parse_eval(Text, Bindings) ->
	case erl_scan:string(Text) of
		{ok, Tokens, _EndLine} ->
			case erl_parse:parse_exprs(Tokens) of
				{ok, Expr} ->
					erl_eval:exprs(Expr, Bindings);
				E ->
					E
			end;
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 将数据变成erlang的字符串表达
%% @spec string(Str::any()) -> list()
%%@end
%% -----------------------------------------------------------------
-spec string(any()) ->
	iolist().
%% -----------------------------------------------------------------
string(Any) ->
	lists:reverse(string1(Any, [])).

% 编码方法
string1(Num, L) when is_integer(Num) ->
	[integer_to_list(Num) | L];
string1(Num, L) when is_float(Num) ->
	[float_to_list(Num) | L];
string1(Atom, L) when is_atom(Atom) ->
	string_atom(Atom, L);
string1(Bin, L) when is_binary(Bin) ->
	case unicode:characters_to_list(Bin, utf8) of
		L1 when is_list(L1) ->
			[?Q, string_text(L1, []), ?Q | L];
		_ ->
			[$>, $> | string_array(binary_to_list(Bin), [$<, $< | L])]
	end;
string1([H | _] = List, L) when is_integer(H), H > 0, H < 16#80000000 ->
	case string_text(List, []) of
		false ->
			[$] | string_array(List, [$[ | L])];
		Str ->
			[?Q, Str, ?Q | L]
	end;
string1("", L) ->
	[?Q, ?Q | L];
string1(List, L) when is_list(List) ->
	[$] | string_array(List, [$[ | L])];
string1(Tuple, L) when is_tuple(Tuple) ->
	[$} | string_array(tuple_to_list(Tuple), [${ | L])];
string1(Any, L) ->
	[?Q, z_lib:any_to_iolist(Any), ?Q | L].

% 编码文字，返回false表示不是文字，否则返回编码后的字符串
string_text([?Q | T], L) ->
	string_text(T, [?Q, $\\ | L]);
string_text([$\\ | T], L) ->
	string_text(T, [$\\, $\\ | L]);
string_text([$\b | T], L) ->
	string_text(T, [$b, $\\ | L]);
string_text([$\t | T], L) ->
	string_text(T, [$t, $\\ | L]);
string_text([$\n | T], L) ->
	string_text(T, [$n, $\\ | L]);
string_text([$\v | T], L) ->
	string_text(T, [$v, $\\ | L]);
string_text([$\f | T], L) ->
	string_text(T, [$f, $\\ | L]);
string_text([$\r | T], L) ->
	string_text(T, [$r, $\\ | L]);
string_text([Ch | T], L) when is_integer(Ch), Ch < 32 ->
	string_text(T, L);
string_text([Ch | T], L) when is_integer(Ch), Ch < 127 ->
	string_text(T, [Ch | L]);
string_text([Ch | T], L) when is_integer(Ch), Ch < 16#80000000 ->
	string_text(T, z_lib:char_to_utf8(Ch, L));
string_text([_ | _], _L) ->
	false;
string_text([], L) ->
	lists:reverse(L).

% 编码数组
string_array([H], L) ->
	string1(H, L);
string_array([H | T], L) ->
	string_array(T, [$, | string1(H, L)]);
string_array([], L) ->
	L.

% 编码原子
string_atom(Atom, L) ->
	case erl_scan:reserved_word(Atom) of
		true ->
			[?SQ, atom_to_list(Atom), ?SQ | L];
		_ ->
			case atom_to_list(Atom) of
				[C | T] = S when C >= $a, C =< $z ->
					case atom_valid_char(T) of
						true ->
							[S | L];
						_ ->
							[?SQ, S, ?SQ | L]
					end;
				S ->
					[?SQ, S, ?SQ | L]
			end
	end.

% 小写原子允许的字符
atom_valid_char([C | T]) when C >= $a, C =< $z ->
	atom_valid_char(T);
atom_valid_char([C | T]) when C >= $A, C =< $Z ->
	atom_valid_char(T);
atom_valid_char([C | T]) when C >= $0, C =< $9 ->
	atom_valid_char(T);
atom_valid_char([$_ | T]) ->
	atom_valid_char(T);
atom_valid_char([$@ | T]) ->
	atom_valid_char(T);
atom_valid_char([_ | _]) ->
	false;
atom_valid_char([]) ->
	true.

%% -----------------------------------------------------------------
%%@doc 获得模块指定的属性
%% @spec get_mod_attr(Mod::atom(), Key::any()) -> return()
%% where
%% return() = {Key, [T]} | false
%%@end
%% -----------------------------------------------------------------
-spec get_mod_attr(atom(), any()) ->
	{any(), [any()]} | false.
%% -----------------------------------------------------------------
get_mod_attr(Module, Key) ->
	lists:keyfind(Key, 1, Module:module_info(attributes)).

%% -----------------------------------------------------------------
%%@doc 发送数据
%% @spec send(Dest::dest(),Msg::any()) -> any()
%% where
%% dest() = pid() | atom() | {atom(), node()}
%%@end
%% -----------------------------------------------------------------
-spec send(pid() | atom() | {atom(), node()}, any()) ->
	any().
%% -----------------------------------------------------------------
send(Dest, Msg) ->
	case catch erlang:send(Dest, Msg, [noconnect]) of
		noconnect ->
			spawn(erlang, send, [Dest, Msg]);
		Other ->
			Other
	end.

%% -----------------------------------------------------------------
%%@doc 回应消息
%% @spec reply({From::pid(), Ref::term()}, Msg::term()) -> any()
%%@end
%% -----------------------------------------------------------------
reply({From, Ref}, Msg) when is_pid(From) ->
	From ! {Ref, Msg};
reply(_, _Msg) ->
	error.

%% -----------------------------------------------------------------
%%@doc 获得当前的堆栈
%% @spec get_stacktrace() -> list()
%%@end
%% -----------------------------------------------------------------
-spec get_stacktrace() ->
	list().
%% -----------------------------------------------------------------
get_stacktrace() ->
	try
		throw("")
	catch
		_:_ ->
			erlang:get_stacktrace()
	end.

%% -----------------------------------------------------------------
%%@doc 字符串化堆栈信息
%% @spec string_stacktrace(Stacktrace::list()) -> list()
%%@end
%% -----------------------------------------------------------------
-spec string_stacktrace(list()) ->
	list().
%% -----------------------------------------------------------------
string_stacktrace(Stacktrace) ->
	lists:reverse([$] | string_stacktrace(Stacktrace, [$[])]).

string_stacktrace([H | T], L) when is_atom(H) ->
	string_stacktrace(T, [$, | lists:reverse(atom_to_list(H), L)]);
string_stacktrace([H | T], L) when is_integer(H) ->
	string_stacktrace(T, [$, | lists:reverse(integer_to_list(H), L)]);
string_stacktrace([H | T], L) when is_tuple(H) ->
	string_stacktrace(T, [$,, $} | string_stacktrace(tuple_to_list(H), [${ | L])]);
string_stacktrace([[I |_] = H | T], L) when I > 0, I < ?MAX_POSITIVE_INT32 ->
	string_stacktrace(T, [$,, ?Q | lists:reverse(H, [?Q | L])]);
string_stacktrace([H | T], L) when is_list(H) ->
	string_stacktrace(T, [$,, $] | string_stacktrace(H, [$[ | L])]);
string_stacktrace([], [_ | L]) ->
	L.

%% -----------------------------------------------------------------
%%@doc 获得指定进程的字典
%% @spec pid_dictionary(Pid::pid() | atom()) -> return()
%% where
%%  return() = pid() | atom()
%%@end
%% -----------------------------------------------------------------
-spec pid_dictionary(pid() | atom()) ->
	list() | undefined.
%% -----------------------------------------------------------------
pid_dictionary(Pid) when is_pid(Pid) ->
	case erlang:process_info(Pid, dictionary) of
		{_, L} -> L;
		E -> E
	end;
pid_dictionary(Pid) when is_atom(Pid) ->
	case whereis(Pid) of
		Pid when is_pid(Pid) ->
			case erlang:process_info(Pid, dictionary) of
				{_, L} -> L;
				E -> E
			end;
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 获得指定进程的字典中指定的键对应的值
%% @spec pid_dictionary(Pid::pid() | atom(), Key::any()) -> return()
%% where
%%  return() = {any(), any()} | false
%%@end
%% -----------------------------------------------------------------
-spec pid_dictionary(pid() | atom(), any()) ->
	{any(), any()} | false.
%% -----------------------------------------------------------------
pid_dictionary(Pid, Key) when is_pid(Pid) ->
	case erlang:process_info(Pid, dictionary) of
		{_, L} -> lists:keyfind(Key, 1, L);
		_ -> false
	end;
pid_dictionary(Name, Key) when is_atom(Name) ->
	case whereis(Name) of
		Pid when is_pid(Pid) ->
			case erlang:process_info(Pid, dictionary) of
				{_, L} -> lists:keyfind(Key, 1, L);
				_ -> false
			end;
		_ ->
			false
	end.

%% -----------------------------------------------------------------
%%@doc 执行(匿名或{M, A})函数
%% @spec fapply(F::function() | {atom(), atom()},A::any()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec fapply(function() | {atom(), atom()}, any()) ->
	any().
%% -----------------------------------------------------------------
fapply(F, A) when is_function(F) andalso is_list(A) ->
	apply(F, A);
fapply(F, A) when is_function(F) ->
	F(A);
fapply({M, F}, A) when is_list(A) ->
	apply(M, F, A);
fapply({M, F}, A) ->
	M:F(A).

%% -----------------------------------------------------------------
%%@doc 执行MFA，M是模块参数的元组，元组中第一个元素为模块名，后面的为参数
%% @spec mapply(MA::tuple(),F::atom(),A::any()) -> any()
%%@end
%% -----------------------------------------------------------------
-spec mapply(tuple(), atom(), list() | any()) ->
	any().
%% -----------------------------------------------------------------
mapply(MA, F, A) when is_list(A) ->
	apply(element(1, MA), F, [MA | A]);
mapply(MA, F, A) ->
	?MF(MA, F), A).

%% -----------------------------------------------------------------
%%@doc 用指定的字符或字符列表过滤字符串
%% @spec char_filter(Text::string(),Ignore::char()|string()) -> string()
%%@end
%% -----------------------------------------------------------------
-spec char_filter(string(), char() | string()) ->
	string().
%% -----------------------------------------------------------------
char_filter(Text, Ignore) when  is_list(Text), is_integer(Ignore) ->
	char_filter_1(Text, Ignore, [], 0, Text);
char_filter(Text, Ignore) when  is_list(Text), is_list(Ignore) ->
	char_filter_2(Text, Ignore, [], 0, Text).

char_filter_1([I | T], I, L, N, Text) ->
	char_filter_1(T, I, L, N + 1, Text);
char_filter_1([H | T], I, L, N, Text) ->
	char_filter_1(T, I, [H | L], N, Text);
char_filter_1([], _I, _L, 0, Text) ->
	Text;
char_filter_1([], _I, L, _N, _Text) ->
	lists:reverse(L).

char_filter_2([H | T], I, L, N, Text) ->
	case lists:member(H, I) of
		true ->
			char_filter_2(T, I, L, N + 1, Text);
		_ ->
			char_filter_2(T, I, [H | L], N, Text)
	end;
char_filter_2([], _I, _L, 0, Text) ->
	Text;
char_filter_2([], _I, L, _N, _Text) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc 用指定的字符替换字符串，如果替换字符为负数，表示过滤
%% @spec char_replace(Text::string(), C::char(), Replace::char()) -> string()
%%@end
%% -----------------------------------------------------------------
-spec char_replace(string(), char(), char()) ->
	string().
%% -----------------------------------------------------------------
char_replace(Text, C, Replace)
	when  is_list(Text), is_integer(C), is_integer(Replace), Replace < 0 ->
	char_filter_1(Text, C, [], 0, Text);
char_replace(Text, C, Replace)
	when  is_list(Text), is_integer(C), is_integer(Replace) ->
	char_replace_1(Text, C, Replace, [], 0, Text).

char_replace_1([C | T], C, R, L, N, Text) ->
	char_replace_1(T, C, R, [R | L], N + 1, Text);
char_replace_1([H | T], C, R, L, N, Text) ->
	char_replace_1(T, C, R, [H | L], N, Text);
char_replace_1([], _C, _R, _L, 0, Text) ->
	Text;
char_replace_1([], _C, _R, L, _N, _Text) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc 用指定的字符列表（2个元素为一组）替换字符串，如果替换字符为负数，表示过滤
%% @spec char_replace(Text::string(),Replace::list()) -> string()
%%@end
%% -----------------------------------------------------------------
-spec char_replace(string(), list()) ->
	string().
%% -----------------------------------------------------------------
char_replace(Text, Replace) when  is_list(Text), is_list(Replace) ->
	char_replace_2(Text, Replace, [], 0, Text).

char_replace_2([H | T], R, L, N, Text) ->
	case get_replace(H, R) of
		H ->
			char_replace_2(T, R, [H | L], N, Text);
		C when C < 0 ->
			char_replace_2(T, R, L, N + 1, Text);
		C ->
			char_replace_2(T, R, [C | L], N + 1, Text)
	end;
char_replace_2([], _R, _L, 0, Text) ->
	Text;
char_replace_2([], _R, L, _N, _Text) ->
	lists:reverse(L).

get_replace(C, [C, R | _T]) ->
	R;
get_replace(C, [_, _ | T]) ->
	get_replace(C, T);
get_replace(C, _) ->
	C.

%% -----------------------------------------------------------------
%%@doc 劈分字符串，找到第一个可以被劈分分隔符，返回左右两边的字符串
%% @spec split_first(Text::list(), Separator::separator()) -> return()
%% where
%% separator() = char()|string()
%% return() = {Left::list(), Right::list()} | false
%%@end
%% -----------------------------------------------------------------
-spec split_first(string(), char() | string()) ->
	{string(), string()} | false.
%% -----------------------------------------------------------------
split_first(Text, Separator) when is_list(Text),  is_integer(Separator) ->
	split_first_1(Text, Separator, []);
split_first(Text, Separator) when is_list(Text), is_list(Separator) ->
	split_first_2(Text, lists:reverse(Separator), []).

split_first_1([S | T], S, L) ->
	{lists:reverse(L), T};
split_first_1([H | T], S, L) ->
	split_first_1(T, S, [H | L]);
split_first_1([], _S, _L) ->
	false.

split_first_2([H | T], S, L) ->
	L1 = [H | L],
	case split_match(L1, S) of
		false ->
			split_first_2(T, S, L1);
		M ->
			{M, T}
	end;
split_first_2([], _S, _L) ->
	false.

%% -----------------------------------------------------------------
%%@doc 劈分字符串，可以使用字符、字符串或分界符来进行劈分
%% @spec split(Text::list(),Separator::separator()) -> return()
%% where
%% separator() = char() | string() | {char(), char()}
%% return() = [string()] | {Left, Middle, Right} | end_without_match | {right_not_match, Text}
%%@end
%% -----------------------------------------------------------------
-spec split(string(), char() | string() | {char(), char()}) ->
	[string()].
%% -----------------------------------------------------------------
split(Text, Separator) when is_list(Text),  is_integer(Separator) ->
	split_1(Text, Separator, [], []);
split(Text, Separator) when is_list(Text), is_list(Separator) ->
	split_2(Text, lists:reverse(Separator), [], []);
split(Text, {Boundary, Boundary}) when is_list(Text), is_integer(Boundary) ->
	split_3(Text, Boundary, 0, [], []);
split(Text, {Left, Right}) when is_list(Text), is_integer(Left), is_integer(Right) ->
	split_4(Text, Left, Right, 0, [], []).

split_1([S | T], S, L1, L2) ->
	split_1(T, S, [], [lists:reverse(L1) | L2]);
split_1([H | T], S, L1, L2) ->
	split_1(T, S, [H | L1], L2);
split_1([], _S, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]).

split_2([H | T], S, L1, L2) ->
	L = [H | L1],
	case split_match(L, S) of
		false ->
			split_2(T, S, L, L2);
		M ->
			split_2(T, S, [], [M | L2])
	end;
split_2([], _S, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]).

split_match([H | T1], [H | T2]) ->
	split_match(T1, T2);
split_match(L, []) ->
	lists:reverse(L);
split_match(_L1, _L2) ->
	false.

split_3([B | T], B, 0, L1, L2) ->
	split_3(T, B, 1, L1, L2);
split_3([B | T], B, 1, L1, L2) ->
	{lists:reverse(L1), lists:reverse(L2), T};
split_3([H | T], B, 0, L1, L2) ->
	split_3(T, B, 0, [H | L1], L2);
split_3([H | T], B, 1, L1, L2) ->
	split_3(T, B, 1, L1, [H | L2]);
split_3([], _B, _N, _L1, _L2) ->
	end_without_match.

split_4([Right | T] = Text, Left, Right, N, L1, L2) ->
	case N of
		0 ->
			{right_not_match, Text};
		1 ->
			{lists:reverse(L1), lists:reverse(L2), T};
		N ->
			split_4(T, Left, Right, N - 1, L1, [Right | L2])
	end;
split_4([Left | T], Left, Right, 0, L1, L2) ->
	split_4(T, Left, Right, 1, L1, L2);
split_4([Left | T], Left, Right, N, L1, L2) ->
	split_4(T, Left, Right, N + 1, L1, [Left | L2]);
split_4([H | T], Left, Right, 0, L1, L2) ->
	split_4(T, Left, Right, 0, [H | L1], L2);
split_4([H | T], Left, Right, N, L1, L2) ->
	split_4(T, Left, Right, N, L1, [H | L2]);
split_4([], _Left, _Right, _N, _L1, _L2) ->
	end_without_match.

%% -----------------------------------------------------------------
%%@doc 劈分字符串，Boundary为分界符，分界符内的字符不做劈分
%% @spec split(Text::string(), Separator::separator(), {Left::integer(), Right::integer()}) -> return()
%% where
%% separator() = char()|list()
%% return() = [string()] | end_without_match | {right_not_match, Text}
%%@end
%% -----------------------------------------------------------------
-spec split(string(), char() | list(), {integer(), integer()}) ->
	[string()].
%% -----------------------------------------------------------------
split(Text, Separator, {Boundary, Boundary})
	when is_list(Text), is_integer(Separator), is_integer(Boundary) ->
	split_1(Text, Separator, Boundary, 0, [], []);
split(Text, Separator, {Boundary, Boundary})
	when is_list(Text), is_list(Separator), is_integer(Boundary) ->
	split_2(Text, lists:reverse(Separator), Boundary, 0, [], []);
split(Text, Separator, {Left, Right})
	when is_list(Text), is_integer(Separator), is_integer(Left), is_integer(Right) ->
	split_3(Text, Separator, Left, Right, 0, [], []);
split(Text, Separator, {Left, Right})
	when is_list(Text), is_list(Separator), is_integer(Left), is_integer(Right) ->
	split_4(Text, lists:reverse(Separator), Left, Right, 0, [], []).

split_1([B | T], S, B, 0, L1, L2) ->
	split_1(T, S, B, 1, [B | L1], L2);
split_1([B | T], S, B, 1, L1, L2) ->
	split_1(T, S, B, 0, [B | L1], L2);
split_1([S | T], S, B, 0, L1, L2) ->
	split_1(T, S, B, 0, [], [lists:reverse(L1) | L2]);
split_1([H | T], S, B, N, L1, L2) ->
	split_1(T, S, B, N, [H | L1], L2);
split_1([], _S, _B, 0, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]);
split_1([], _S, _B, 1, _L1, _L2) ->
	end_without_match.

split_2([B | T], S, B, 0, L1, L2) ->
	split_2(T, S, B, 1, [B | L1], L2);
split_2([B | T], S, B, 1, L1, L2) ->
	split_2(T, S, B, 0, [B | L1], L2);
split_2([H | T], S, B, 0, L1, L2) ->
	L = [H | L1],
	case split_match(L, S) of
		false ->
			split_2(T, S, B, 0, L, L2);
		M ->
			split_2(T, S, B, 0, [], [M | L2])
	end;
split_2([H | T], S, B, 1, L1, L2) ->
	split_2(T, S, B, 1, [H | L1], L2);
split_2([], _S, _B, 0, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]);
split_2([], _S, _B, 1, _L1, _L2) ->
	end_without_match.

split_3([Left | T], S, Left, Right, N, L1, L2) ->
	split_3(T, S, Left, Right, N + 1, [Left | L1], L2);
split_3([Right | _T] = Text, _S, _Left, Right, 0, _L1, _L2) ->
	{right_not_match, Text};
split_3([Right | T], S, Left, Right, N, L1, L2) ->
	split_3(T, S, Left, Right, N - 1, [Right | L1], L2);
split_3([S | T], S, Left, Right, 0, L1, L2) ->
	split_3(T, S, Left, Right, 0, [], [lists:reverse(L1) | L2]);
split_3([H | T], S, Left, Right, N, L1, L2) ->
	split_3(T, S, Left, Right, N, [H | L1], L2);
split_3([], _S, _Left, _Right, 0, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]);
split_3([], _S, _Left, _Right, _N, _L1, _L2) ->
	end_without_match.

split_4([Left | T], S, Left, Right, N, L1, L2) ->
	split_4(T, S, Left, Right, N + 1, [Left | L1], L2);
split_4([Right | _T] = Text, _S, _Left, Right, 0, _L1, _L2) ->
	{right_not_match, Text};
split_4([Right | T], S, Left, Right, N, L1, L2) ->
	split_4(T, S, Left, Right, N - 1, [Right | L1], L2);
split_4([H | T], S, Left, Right, 0, L1, L2) ->
	L = [H | L1],
	case split_match(L, S) of
		false ->
			split_4(T, S, Left, Right, 0, L, L2);
		M ->
			split_4(T, S, Left, Right, 0, [], [M | L2])
	end;
split_4([H | T], S, Left, Right, N, L1, L2) ->
	split_4(T, S, Left, Right, N, [H | L1], L2);
split_4([], _S, _Left, _Right, 0, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]);
split_4([], _S, _Left, _Right, _N, _L1, _L2) ->
	end_without_match.

%% -----------------------------------------------------------------
%%@doc 劈分并过滤字符串，Separator为劈分字符，Ignore为忽略的字符
%% @spec split_filter(Text::string(), Separator::separator(), Ignore::ignore()) -> [string()]
%% where
%% separator() = list() | char()
%% ignore() = list() | char()
%%@end
%% -----------------------------------------------------------------
-spec split_filter(string(), list() | char(), list() | char()) ->
	[string()].
%% -----------------------------------------------------------------
split_filter(Text, Separator, Ignore)
	when is_list(Text), is_integer(Separator), is_integer(Ignore) ->
	split_filter_1(Text, Separator, Ignore, [], []);
split_filter(Text, Separator, Ignore)
	when is_list(Text), is_list(Separator), is_integer(Ignore) ->
	split_filter_2(Text, lists:reverse(Separator), Ignore, [], []);
split_filter(Text, Separator, Ignore)
	when is_list(Text), is_integer(Separator), is_list(Ignore) ->
	split_filter_3(Text, Separator, Ignore, [], []);
split_filter(Text, Separator, Ignore)
	when is_list(Text), is_list(Separator), is_list(Ignore) ->
	split_filter_4(Text, lists:reverse(Separator), Ignore, [], []).

split_filter_1([S | T], S, I, L1, L2) ->
	split_filter_1(T, S, I, [], [lists:reverse(L1) | L2]);
split_filter_1([I | T], S, I, L1, L2) ->
	split_filter_1(T, S, I, L1, L2);
split_filter_1([H | T], S, I, L1, L2) ->
	split_filter_1(T, S, I, [H | L1], L2);
split_filter_1([], _S, _I, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]).

split_filter_2([I | T], S, I, L1, L2) ->
	split_filter_2(T, S, I, L1, L2);
split_filter_2([H | T], S, I, L1, L2) ->
	L = [H | L1],
	case split_match(L, S) of
		false ->
			split_filter_2(T, S, I, L, L2);
		M ->
			split_filter_2(T, S, I, [], [M | L2])
	end;
split_filter_2([], _S, _I, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]).

split_filter_3([S | T], S, I, L1, L2) ->
	split_filter_3(T, S, I, [], [lists:reverse(L1) | L2]);
split_filter_3([H | T], S, I, L1, L2) ->
	case lists:member(H, I) of
		true ->
			split_filter_3(T, S, I, L1, L2);
		false ->
			split_filter_3(T, S, I, [H | L1], L2)
	end;
split_filter_3([], _S, _I, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]).

split_filter_4([S | T], S, I, L1, L2) ->
	split_filter_4(T, S, I, [], [lists:reverse(L1) | L2]);
split_filter_4([I | T], S, I, L1, L2) ->
	split_filter_4(T, S, I, L1, L2);
split_filter_4([H | T], S, I, L1, L2) ->
	case lists:member(H, I) of
		true ->
			L = [H | L1],
			case split_match(L, S) of
				false ->
					split_filter_4(T, S, I, L, L2);
				M ->
					split_filter_4(T, S, I, [], [M | L2])
			end;
		false ->
			split_filter_4(T, S, I, [H | L1], L2)
	end;
split_filter_4([], _S, _I, L1, L2) ->
	lists:reverse([lists:reverse(L1) | L2]).

%% -----------------------------------------------------------------
%%@doc 判定是否为iodata
%% @spec is_iodata(Data::term()) -> boolean()
%%@end
%% -----------------------------------------------------------------
-spec is_iodata(list()) ->
	true | false.
%% -----------------------------------------------------------------
is_iodata(Data) when is_binary(Data) ->
	true;
is_iodata(Data) when is_list(Data) ->
	try
		iolist_size(Data),
		true
	catch
		_:_ ->
			false
	end;
is_iodata(_) ->
	false.

%% -----------------------------------------------------------------
%%@doc 获得列表的最大最小值
%% @spec get_list_range(List::[term()]) -> return()
%% where
%% return() = {Len::integer(), Min::term(), Max::term()} | false
%%@end
%% -----------------------------------------------------------------
-spec get_list_range(list()) ->
	{integer(), any(), any()} | false.
%% -----------------------------------------------------------------
get_list_range([H | T]) ->
	get_list_range(T, H, H, 1);
get_list_range([]) ->
	{0, 0, 0};
get_list_range(_) ->
	false.

get_list_range([H | T], Min, Max, N) ->
	if
		H > Min ->
			if
				H < Max ->
					get_list_range(T, Min, Max, N + 1);
				true ->
					get_list_range(T, Min, H, N + 1)
			end;
		true ->
			get_list_range(T, H, Max, N + 1)
	end;
get_list_range([], Min, Max, N) ->
	{N, Min, Max}.

%% -----------------------------------------------------------------
%%@doc 检查列表的每个元素是否符合传入的最大最小值（都是闭区间）
%%```
%% 如果列表中元素都满足在区间内，则返回列表长度，否则返回false
%%'''
%% @spec check_list_range(L::[term()], Min::term(), Max::term()) -> return()
%% where
%% return() = N::integer() | false
%%@end
%% -----------------------------------------------------------------
-spec check_list_range(list(), any(), any()) ->
	integer() | false.
%% -----------------------------------------------------------------
check_list_range(L, Min, Max) when is_list(L) ->
	check_list_range(L, Min, Max, 0);
check_list_range(_, _Min, _Max) ->
	false.

check_list_range([H | T], Min, Max, N) when H >= Min, H =< Max ->
	check_list_range(T, Min, Max, N + 1);
check_list_range(L, _Min, _Max, N) ->
	case L of
		[] -> N;
		_ -> false
	end.

%% -----------------------------------------------------------------
%%@doc 将字符转成utf8的数字或列表
%% @spec char_to_utf8(C::char()) -> return()
%% where
%% return() = false | integer() | list()
%%@end
%% -----------------------------------------------------------------
-spec char_to_utf8(integer()) ->
	false | integer() | list().
%% -----------------------------------------------------------------
char_to_utf8(C) when C < 0 ->
	false;
char_to_utf8(C) when C < 128 ->
	%% 0xxxxxxx
	C;
char_to_utf8(C) when C < 16#800 ->
	%% 110xxxxx 10xxxxxx
	[16#C0 + (C bsr 6), 128 + (C band 16#3F)];
char_to_utf8(C) when C < 16#10000 ->
	%% 1110xxxx 10xxxxxx 10xxxxxx
	[16#E0 + (C bsr 12), 128 + ((C bsr 6) band 16#3F), 128 + (C band 16#3F)];
char_to_utf8(C) when C < 16#200000 ->
	%% 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
	[16#F0 + (C bsr 18), 128 + ((C bsr 12) band 16#3F),
	128 + ((C bsr 6) band 16#3F), 128 + (C band 16#3F)];
char_to_utf8(C) when C < 16#4000000 ->
	%% 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	[16#F8 + (C bsr 24), 128 + ((C bsr 18) band 16#3F),
	128 + ((C bsr 12) band 16#3F), 128 + ((C bsr 6) band 16#3F),
	128 + (C band 16#3F)];
char_to_utf8(C) when C < 16#80000000 ->
	%% 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	[16#FC + (C bsr 30), 128 + ((C bsr 24) band 16#3F),
	128 + ((C bsr 18) band 16#3F), 128 + ((C bsr 12) band 16#3F),
	128 + ((C bsr 6) band 16#3F), 128 + (C band 16#3F)];
char_to_utf8(_C) ->
	false.

%% -----------------------------------------------------------------
%%@doc 将字符以utf8编码方式放入到列表
%% @spec char_to_utf8(C::char(),L::[term()]) -> list()
%%@end
%% -----------------------------------------------------------------
-spec char_to_utf8(integer(), list()) ->
	list().
%% -----------------------------------------------------------------
char_to_utf8(C, L) when C < 0 ->
	L;
char_to_utf8(C, L) when C < 128 ->
	%% 0xxxxxxx
	[C | L];
char_to_utf8(C, L) when C < 16#800 ->
	%% 110xxxxx 10xxxxxx
	[128 + (C band 16#3F), 16#C0 + (C bsr 6) | L];
char_to_utf8(C, L) when C < 16#10000 ->
	%% 1110xxxx 10xxxxxx 10xxxxxx
	[128 + (C band 16#3F), 128 + ((C bsr 6) band 16#3F), 16#E0 + (C bsr 12) | L];
char_to_utf8(C, L) when C < 16#200000 ->
	%% 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
	[128 + (C band 16#3F), 128 + ((C bsr 6) band 16#3F),
	128 + ((C bsr 12) band 16#3F), 16#F0 + (C bsr 18) | L];
char_to_utf8(C, L) when C < 16#4000000 ->
	%% 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	[128 + (C band 16#3F), 128 + ((C bsr 6) band 16#3F),
	128 + ((C bsr 12) band 16#3F), 128 + ((C bsr 18) band 16#3F),
	16#F8 + (C bsr 24) | L];
char_to_utf8(C, L) when C < 16#80000000 ->
	%% 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	[128 + (C band 16#3F), 128 + ((C bsr 6) band 16#3F),
	128 + ((C bsr 12) band 16#3F), 128 + ((C bsr 18) band 16#3F),
	128 + ((C bsr 24) band 16#3F), 16#FC + (C bsr 30) | L];
char_to_utf8(_C, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 将字符以utf8编码方式放入到列表
%% @spec characters_to_utf8(C::char(),L::[term()]) -> list()
%%@end
%% -----------------------------------------------------------------
-spec characters_to_utf8(list()) ->
	list().
%% -----------------------------------------------------------------
characters_to_utf8([C | L]) when C < 0 ->
	characters_to_utf8(L);
characters_to_utf8([C | L]) when C < 128 ->
	%% 0xxxxxxx
	[C | characters_to_utf8(L)];
characters_to_utf8([C | L]) when C < 16#800 ->
	%% 110xxxxx 10xxxxxx
	[16#C0 + (C bsr 6), 128 + (C band 16#3F) | characters_to_utf8(L)];
characters_to_utf8([C | L]) when C < 16#10000 ->
	%% 1110xxxx 10xxxxxx 10xxxxxx
	[16#E0 + (C bsr 12), 128 + ((C bsr 6) band 16#3F), 128 + (C band 16#3F) | characters_to_utf8(L)];
characters_to_utf8([C | L]) when C < 16#200000 ->
	%% 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
	[16#F0 + (C bsr 18), 128 + ((C bsr 12) band 16#3F), 128 + ((C bsr 6) band 16#3F),
	128 + (C band 16#3F) | characters_to_utf8(L)];
characters_to_utf8([C | L]) when C < 16#4000000 ->
	%% 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	[16#F8 + (C bsr 24), 128 + ((C bsr 18) band 16#3F),
	128 + ((C bsr 12) band 16#3F), 128 + ((C bsr 6) band 16#3F),
	128 + (C band 16#3F) | characters_to_utf8(L)];
characters_to_utf8([C | L]) when C < 16#80000000 ->
	%% 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	[16#FC + (C bsr 30), 128 + ((C bsr 24) band 16#3F),
	128 + ((C bsr 18) band 16#3F), 128 + ((C bsr 12) band 16#3F),
	128 + ((C bsr 6) band 16#3F), 128 + (C band 16#3F) | characters_to_utf8(L)];
characters_to_utf8([_ | L]) ->
	characters_to_utf8(L);
characters_to_utf8([]) ->
	[].

%% -----------------------------------------------------------------
%%@doc utf8的数字列表转成字符列表
%% @spec utf8_to_characters(L::[term()]) -> return()
%% where
%% return() = false | integer() | list()
%%@end
%% -----------------------------------------------------------------
-spec utf8_to_characters(list()) ->
	list().
%% -----------------------------------------------------------------
utf8_to_characters(L) ->
	utf8_to_characters(L, []).

utf8_to_characters([C | T], L) when C < 128 ->
	%% 0xxxxxxx
	utf8_to_characters(T, [C | L]);
utf8_to_characters([C1, C2 | T], L)
	when C1 < 16#E0, C2 < 16#C0 ->
	%% 110xxxxx 10xxxxxx
	utf8_to_characters(T, [
		((C1 band 16#1F) bsl 6) + (C2 band 16#3F) | L]);
utf8_to_characters([C1, C2, C3 | T], L)
	when C1 < 16#F0, C2 < 16#C0, C3 < 16#C0 ->
	%% 1110xxxx 10xxxxxx 10xxxxxx
	utf8_to_characters(T, [((C1 band 16#F) bsl 12) +
		((C2 band 16#3F) bsl 6) + (C3 band 16#3F) | L]);
utf8_to_characters([C1, C2, C3, C4 | T], L)
	when C1 < 16#F8, C2 < 16#C0, C3 < 16#C0, C4 < 16#C0 ->
	%% 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
	utf8_to_characters(T, [
		((C1 band 16#7) bsl 18) + ((C2 band 16#3F) bsl 12) +
		((C3 band 16#3F) bsl 6) + (C4 band 16#3F) | L]);
utf8_to_characters([C1, C2, C3, C4, C5 | T], L)
	when C1 < 16#FC, C2 < 16#C0, C3 < 16#C0, C4 < 16#C0, C5 < 16#C0 ->
	%% 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	utf8_to_characters(T, [((C1 band 16#3) bsl 24) +
		((C2 band 16#3F) bsl 18) + ((C3 band 16#3F) bsl 12) +
		((C4 band 16#3F) bsl 6) + (C5 band 16#3F) | L]);
utf8_to_characters([C1, C2, C3, C4, C5, C6 | T], L)
	when C1 < 16#FE, C2 < 16#C0, C3 < 16#C0, C4 < 16#C0, C5 < 16#C0, C6 < 16#C0 ->
	%% 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	utf8_to_characters(T, [
		((C1 band 16#1) bsl 32) + ((C2 band 16#3F) bsl 24) +
		((C3 band 16#3F) bsl 18) + ((C4 band 16#3F) bsl 12) +
		((C5 band 16#3F) bsl 6) + (C6 band 16#3F) | L]);
utf8_to_characters([C | T], L) ->
	utf8_to_characters(T, [C | L]);
utf8_to_characters([], L) ->
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc 将翻转的utf8的数字列表转成字符列表
%% @spec rutf8_to_characters(L::[term()]) -> return()
%% where
%% return() = false | integer() | list()
%%@end
%% -----------------------------------------------------------------
-spec rutf8_to_characters(list()) ->
	list().
%% -----------------------------------------------------------------
rutf8_to_characters(L) ->
	rutf8_to_characters(L, []).

rutf8_to_characters([C | T], L) when C < 128 ->
	%% 0xxxxxxx
	rutf8_to_characters(T, [C | L]);
rutf8_to_characters([C2,  C1 | T], L)
	when C1 < 16#E0, C1 > 16#C0, C2 < 16#C0 ->
	%% 110xxxxx 10xxxxxx
	rutf8_to_characters(T, [
		((C1 band 16#1F) bsl 6) + (C2 band 16#3F) | L]);
rutf8_to_characters([C3, C2, C1 | T], L)
	when C1 < 16#F0, C1 > 16#E0, C2 < 16#C0, C3 < 16#C0 ->
	%% 1110xxxx 10xxxxxx 10xxxxxx
	rutf8_to_characters(T, [((C1 band 16#F) bsl 12) +
		((C2 band 16#3F) bsl 6) + (C3 band 16#3F) | L]);
rutf8_to_characters([C4, C3, C2, C1 | T], L)
	when C1 < 16#F8, C1 > 16#F0, C2 < 16#C0, C3 < 16#C0, C4 < 16#C0 ->
	%% 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
	rutf8_to_characters(T, [
		((C1 band 16#7) bsl 18) + ((C2 band 16#3F) bsl 12) +
		((C3 band 16#3F) bsl 6) + (C4 band 16#3F) | L]);
rutf8_to_characters([C5, C4, C3, C2, C1 | T], L)
	when C1 < 16#FC, C1 > 16#F8, C2 < 16#C0,
		C3 < 16#C0, C4 < 16#C0, C5 < 16#C0 ->
	%% 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	rutf8_to_characters(T, [((C1 band 16#3) bsl 24) +
		((C2 band 16#3F) bsl 18) + ((C3 band 16#3F) bsl 12) +
		((C4 band 16#3F) bsl 6) + (C5 band 16#3F) | L]);
rutf8_to_characters([C6, C5, C4, C3, C2, C1 | T], L)
	when C1 < 16#FE, C1 > 16#FC, C2 < 16#C0,
		C3 < 16#C0, C4 < 16#C0, C5 < 16#C0, C6 < 16#C0 ->
	%% 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
	rutf8_to_characters(T, [
		((C1 band 16#1) bsl 32) + ((C2 band 16#3F) bsl 24) +
		((C3 band 16#3F) bsl 18) + ((C4 band 16#3F) bsl 12) +
		((C5 band 16#3F) bsl 6) + (C6 band 16#3F) | L]);
rutf8_to_characters([C | T], L) ->
	rutf8_to_characters(T, [C | L]);
rutf8_to_characters([], L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 将数字变成十六进制字符
%% @spec digit_hex(C::integer()) -> Hex
%%@end
%% -----------------------------------------------------------------
digit_hex(C) when C < 10 -> $0 + C;
digit_hex(C) when C < 16 -> $A + (C - 10).

%% -----------------------------------------------------------------
%%@doc 将数字变成十六进制字符
%% @spec digit_hex(C::integer()) -> Hex
%%@end
%% -----------------------------------------------------------------
digit_hex_lower(C) when C < 10 -> $0 + C;
digit_hex_lower(C) when C < 16 -> $a + (C - 10).

%% -----------------------------------------------------------------
%%@doc 将十六进制字符变成数字
%% @spec hex_digit(C) -> integer()
%%@end
%% -----------------------------------------------------------------
hex_digit(C) when C >= $0, C =< $9 -> C - $0;
hex_digit(C) when C >= $a, C =< $f -> C - $a + 10;
hex_digit(C) when C >= $A, C =< $F -> C - $A + 10.

%% -----------------------------------------------------------------
%%@doc 将二进制数据变成hex字符串
%%```
%%'''
%% @spec bin_to_hex_str(B::binary(), Upper::boolean()) -> binary()
%%@end
%% -----------------------------------------------------------------
bin_to_hex_str(B, true) when is_binary(B) ->
	bin_to_hex_str1(B, {$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $A, $B, $C, $D, $E, $F});
bin_to_hex_str(B, _) when is_binary(B) ->
	bin_to_hex_str1(B, {$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $a, $b, $c, $d, $e, $f}).

bin_to_hex_str1(<<A:4, B:4, C:4, D:4, E:4, F:4, G:4, H:4, I:4, J:4, K:4, L:4, M:4, N:4, O:4, P:4, Rest/binary>>, T) ->
	[element(A+1, T), element(B+1, T), element(C+1, T), element(D+1, T), element(E+1, T), element(F+1, T), element(G+1, T), element(H+1, T), element(I+1, T), element(J+1, T), element(K+1, T), element(L+1, T), element(M+1, T), element(N+1, T), element(O+1, T), element(P+1, T) | bin_to_hex_str1(Rest, T)];
bin_to_hex_str1(<<A:4, B:4, C:4, D:4, E:4, F:4, G:4, H:4, Rest/binary>>, T) ->
	[element(A+1, T), element(B+1, T), element(C+1, T), element(D+1, T), element(E+1, T), element(F+1, T), element(G+1, T), element(H+1, T) | bin_to_hex_str1(Rest, T)];
bin_to_hex_str1(<<A:4, B:4, C:4, D:4, Rest/binary>>, T) ->
	[element(A+1, T), element(B+1, T), element(C+1, T), element(D+1, T) | bin_to_hex_str1(Rest, T)];
bin_to_hex_str1(<<A:4, B:4, Rest/binary>>, T) ->
	[element(A+1, T), element(B+1, T) | bin_to_hex_str1(Rest, T)];
bin_to_hex_str1(<<>>, _T) -> [].

%%
%%hex字符串转为二进制
%%
hex_str_to_bin(HexStr) ->
	hex_str_to_bin(lists:reverse(HexStr), <<>>).

hex_str_to_bin([P, O, N, M, L, K, J, I, H, G, F, E, D, C, B, A|T], Bin) ->
	hex_str_to_bin(T, <<(list_to_integer([A], 16)):4, (list_to_integer([B], 16)):4, (list_to_integer([C], 16)):4, (list_to_integer([D], 16)):4, (list_to_integer([E], 16)):4, (list_to_integer([F], 16)):4, (list_to_integer([G], 16)):4, (list_to_integer([H], 16)):4, 
					   (list_to_integer([I], 16)):4, (list_to_integer([J], 16)):4, (list_to_integer([K], 16)):4, (list_to_integer([L], 16)):4, (list_to_integer([M], 16)):4, (list_to_integer([N], 16)):4, (list_to_integer([O], 16)):4, (list_to_integer([P], 16)):4, Bin/binary>>);
hex_str_to_bin([H, G, F, E, D, C, B, A|T], Bin) ->
	hex_str_to_bin(T, <<(list_to_integer([A], 16)):4, (list_to_integer([B], 16)):4, (list_to_integer([C], 16)):4, (list_to_integer([D], 16)):4, (list_to_integer([E], 16)):4, (list_to_integer([F], 16)):4, (list_to_integer([G], 16)):4, (list_to_integer([H], 16)):4, Bin/binary>>);
hex_str_to_bin([D, C, B, A|T], Bin) ->
	hex_str_to_bin(T, <<(list_to_integer([A], 16)):4, (list_to_integer([B], 16)):4, (list_to_integer([C], 16)):4, (list_to_integer([D], 16)):4, Bin/binary>>);
hex_str_to_bin([B, A|T], Bin) ->
	hex_str_to_bin(T, <<(list_to_integer([A], 16)):4, (list_to_integer([B], 16)):4, Bin/binary>>);
hex_str_to_bin([], Bin) ->
	Bin.

%% -----------------------------------------------------------------
%%@doc 将二进制数据变成hex字符串二进制数据
%%```
%%'''
%% @spec bin_to_hex(B::binary()) -> binary()
%%@end
%% -----------------------------------------------------------------
bin_to_hex(B) when is_binary(B) ->
	bin_to_hex(B, <<>>).

bin_to_hex(<<A:8, B:8, C:8, D:8, E:8, F:8, G:8, H:8, Rest/binary>>, Acc) ->
	bin_to_hex(Rest, <<Acc/binary, (hex2(A)):16, (hex2(B)):16, (hex2(C)):16, (hex2(D)):16, (hex2(E)):16, (hex2(F)):16, (hex2(G)):16, (hex2(H)):16>>);
bin_to_hex(<<A:8, B:8, C:8, D:8, Rest/binary>>, Acc) ->
	bin_to_hex(Rest, <<Acc/binary, (hex2(A)):16, (hex2(B)):16, (hex2(C)):16, (hex2(D)):16>>);
bin_to_hex(<<A:8, B:8, Rest/binary>>, Acc) ->
	bin_to_hex(Rest, <<Acc/binary, (hex2(A)):16, (hex2(B)):16>>);
bin_to_hex(<<A:8, Rest/binary>>, Acc) ->
	bin_to_hex(Rest, <<Acc/binary, (hex2(A)):16>>);
bin_to_hex(<<>>, Acc) -> Acc.

hex2(X) ->
	element(X + 1, {
		16#3030, 16#3031, 16#3032, 16#3033, 16#3034, 16#3035, 16#3036,
		16#3037, 16#3038, 16#3039, 16#3041, 16#3042, 16#3043, 16#3044,
		16#3045, 16#3046, 16#3130, 16#3131, 16#3132, 16#3133, 16#3134,
		16#3135, 16#3136, 16#3137, 16#3138, 16#3139, 16#3141, 16#3142,
		16#3143, 16#3144, 16#3145, 16#3146, 16#3230, 16#3231, 16#3232,
		16#3233, 16#3234, 16#3235, 16#3236, 16#3237, 16#3238, 16#3239,
		16#3241, 16#3242, 16#3243, 16#3244, 16#3245, 16#3246, 16#3330,
		16#3331, 16#3332, 16#3333, 16#3334, 16#3335, 16#3336, 16#3337,
		16#3338, 16#3339, 16#3341, 16#3342, 16#3343, 16#3344, 16#3345,
		16#3346, 16#3430, 16#3431, 16#3432, 16#3433, 16#3434, 16#3435,
		16#3436, 16#3437, 16#3438, 16#3439, 16#3441, 16#3442, 16#3443,
		16#3444, 16#3445, 16#3446, 16#3530, 16#3531, 16#3532, 16#3533,
		16#3534, 16#3535, 16#3536, 16#3537, 16#3538, 16#3539, 16#3541,
		16#3542, 16#3543, 16#3544, 16#3545, 16#3546, 16#3630, 16#3631,
		16#3632, 16#3633, 16#3634, 16#3635, 16#3636, 16#3637, 16#3638,
		16#3639, 16#3641, 16#3642, 16#3643, 16#3644, 16#3645, 16#3646,
		16#3730, 16#3731, 16#3732, 16#3733, 16#3734, 16#3735, 16#3736,
		16#3737, 16#3738, 16#3739, 16#3741, 16#3742, 16#3743, 16#3744,
		16#3745, 16#3746, 16#3830, 16#3831, 16#3832, 16#3833, 16#3834,
		16#3835, 16#3836, 16#3837, 16#3838, 16#3839, 16#3841, 16#3842,
		16#3843, 16#3844, 16#3845, 16#3846, 16#3930, 16#3931, 16#3932,
		16#3933, 16#3934, 16#3935, 16#3936, 16#3937, 16#3938, 16#3939,
		16#3941, 16#3942, 16#3943, 16#3944, 16#3945, 16#3946, 16#4130,
		16#4131, 16#4132, 16#4133, 16#4134, 16#4135, 16#4136, 16#4137,
		16#4138, 16#4139, 16#4141, 16#4142, 16#4143, 16#4144, 16#4145,
		16#4146, 16#4230, 16#4231, 16#4232, 16#4233, 16#4234, 16#4235,
		16#4236, 16#4237, 16#4238, 16#4239, 16#4241, 16#4242, 16#4243,
		16#4244, 16#4245, 16#4246, 16#4330, 16#4331, 16#4332, 16#4333,
		16#4334, 16#4335, 16#4336, 16#4337, 16#4338, 16#4339, 16#4341,
		16#4342, 16#4343, 16#4344, 16#4345, 16#4346, 16#4430, 16#4431,
		16#4432, 16#4433, 16#4434, 16#4435, 16#4436, 16#4437, 16#4438,
		16#4439, 16#4441, 16#4442, 16#4443, 16#4444, 16#4445, 16#4446,
		16#4530, 16#4531, 16#4532, 16#4533, 16#4534, 16#4535, 16#4536,
		16#4537, 16#4538, 16#4539, 16#4541, 16#4542, 16#4543, 16#4544,
		16#4545, 16#4546, 16#4630, 16#4631, 16#4632, 16#4633, 16#4634,
		16#4635, 16#4636, 16#4637, 16#4638, 16#4639, 16#4641, 16#4642,
		16#4643, 16#4644, 16#4645, 16#4646}).

%% -----------------------------------------------------------------
%%@doc 获得整数的进制位数
%%```
%% I为10进制传入的数，Hex为将转换的进制数，函数返回为转换后的数的位数
%%'''
%% @spec get_integer_digit(I::integer(),Hex::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
get_integer_digit(I, Hex) when is_integer(I), is_integer(Hex), Hex > 1 ->
	case I < 0 of
		true ->
			get_integer_digit(-I, Hex, 1);
		false ->
			get_integer_digit(I, Hex, 1)
	end.

get_integer_digit(I, Hex, N) when I >= Hex ->
	get_integer_digit(I div Hex, Hex, N + 1);
get_integer_digit(_I, _Hex, N) ->
	N.

%% -----------------------------------------------------------------
%%@doc 将整数转成字符串，如果字符串长度不够，前面补零
%% @spec integer_to_list(Integer::integer(),CharCount::integer()) -> list()
%%@end
%% -----------------------------------------------------------------
integer_to_list(0, CharCount) ->
	padding([], CharCount + 1);
integer_to_list(Integer, CharCount) when Integer < 0 ->
	[45 | padding(integer_to_list(-Integer), CharCount - trunc(math:log10(-Integer)))];
integer_to_list(Integer, CharCount) ->
	padding(integer_to_list(Integer), CharCount - trunc(math:log10(Integer))).

padding(L, C) when C =< 1 ->
	L;
padding(L, C) ->
	padding([$0 | L], C - 1).

%% -----------------------------------------------------------------
%%@doc 连接原子、字符串、整数，返回原子
%% @spec to_atom(A::list()) -> atom()
%%@end
%% -----------------------------------------------------------------
-spec to_atom(list()) ->
	atom().
%% -----------------------------------------------------------------
to_atom(L) ->
	list_to_atom(lists:concat(L)).

%% -----------------------------------------------------------------
%%@doc 连接原子、字符串、整数，返回原子
%% @spec to_atom(A1::any(),A2::any()) -> atom()
%%@end
%% -----------------------------------------------------------------
-spec to_atom(any(), any()) ->
	atom().
%% -----------------------------------------------------------------
to_atom(A1, A2) when is_atom(A1), is_atom(A2) ->
	list_to_atom(atom_to_list(A1) ++ atom_to_list(A2));
to_atom(A1, A2) when is_atom(A1), is_list(A2) ->
	list_to_atom(atom_to_list(A1) ++ A2);
to_atom(A1, A2) when is_atom(A1), is_integer(A2) ->
	list_to_atom(atom_to_list(A1) ++ integer_to_list(A2));
to_atom(A1, A2) when is_list(A1), is_atom(A2) ->
	list_to_atom(A1 ++ atom_to_list(A2));
to_atom(A1, A2) when is_integer(A1), is_atom(A2) ->
	list_to_atom(integer_to_list(A1) ++ atom_to_list(A2));
to_atom(A1, A2) when is_list(A1), is_list(A2) ->
	list_to_atom(A1 ++ A2);
to_atom(A1, A2) when is_list(A1), is_integer(A2) ->
	list_to_atom(A1 ++ integer_to_list(A2));
to_atom(A1, A2) when is_integer(A1), is_list(A2) ->
	list_to_atom(integer_to_list(A1) ++ A2);
to_atom(A1, A2) when is_integer(A1), is_integer(A2) ->
	list_to_atom(integer_to_list(A1) ++ integer_to_list(A2)).

%% -----------------------------------------------------------------
%%@doc 将参数变成IO列表或二进制数据
%% @spec any_to_iolist(A::any()) -> return()
%% where
%% return() = iolist() | binary()
%%@end
%% -----------------------------------------------------------------
any_to_iolist(A) when is_binary(A) ->
	A;
any_to_iolist(A) when is_atom(A) ->
	atom_to_list(A);
any_to_iolist(A) when is_integer(A) ->
	integer_to_list(A);
any_to_iolist(A) when is_float(A) ->
	float_to_list(A);
any_to_iolist(A) when is_list(A) ->
	case is_iodata(A) of
		true ->
			A;
		false ->
			try unicode:characters_to_binary(A, utf8) of
				Bin when is_binary(Bin) ->
					Bin;
				_ ->
					io_lib:write(A)
			catch
				_:_ ->
					io_lib:write(A)
			end
	end;
any_to_iolist({}) ->
	"{}";
any_to_iolist(A) when is_pid(A) ->
	pid_to_list(A);
any_to_iolist(A) when is_port(A) ->
	erlang:port_to_list(A);
any_to_iolist(A) when is_reference(A) ->
	erlang:ref_to_list(A);
any_to_iolist(A) when is_function(A) ->
	erlang:fun_to_list(A);
any_to_iolist(A) ->
	io_lib:write(A).

%% -----------------------------------------------------------------
%%@doc 读取动态长度
%% @spec read_dynamic_length(Bin::binary(), Offset::integer()) -> return()
%% where
%% return() = {1, integer()} |
%%		{2, integer()} |
%%		{4, integer()} |
%%		false |
%%		{0, 4} |
%%		{0, 2} |
%%		{0, 0}
%%@end
%% -----------------------------------------------------------------
-spec read_dynamic_length(binary(), integer()) ->
	{1, integer()} |
	{2, integer()} |
	{4, integer()} |
	false |
	{0, 4} |
	{0, 2} |
	{0, 0}.
%% -----------------------------------------------------------------
read_dynamic_length(Bin, Offset) ->
	case Bin of
		<<_:Offset/binary, 2#1:1, Len:7, _/binary>> -> {1, Len};
		<<_:Offset/binary, 2#01:2, Len:14, _/binary>> -> {2, Len};
		<<_:Offset/binary, 2#001:3, Len:29, _/binary>> -> {4, Len};
		<<_:Offset/binary, 2#000:3, _:5, _/binary>> -> false;
		<<_:Offset/binary, 2#001:3, _:5, _/binary>> -> {0, 4};
		<<_:Offset/binary, 2#01:2, _:6, _/binary>> -> {0, 2};
		<<_:Offset/binary, _/binary>> -> {0, 0}
	end.

%% -----------------------------------------------------------------
%%@doc 读取动态长度的数据
%% @spec read_dynamic_data(Bin, Offset) -> return()
%% where
%% return() = {1, integer(), binary()} |
%%		{2, integer(), binary()} |
%%		{4, integer(), binary()} |
%%		false |
%%		{0, 4}
%%		| {0, 2}
%%		| {0, 0}
%%@end
%% -----------------------------------------------------------------
-spec read_dynamic_data(binary(), integer()) ->
	{1, integer(), binary()} |
	{2, integer(), binary()} |
	{4, integer(), binary()} |
	false |
	{0, 4}
	| {0, 2}
	| {0, 0}.
%% -----------------------------------------------------------------
read_dynamic_data(Bin, Offset) ->
	case Bin of
		<<_:Offset/binary, 2#1:1, Len:7, Data:Len/binary, _/binary>> ->
			{1, Len, Data};
		<<_:Offset/binary, 2#01:2, Len:14, Data:Len/binary, _/binary>> ->
			{2, Len, Data};
		<<_:Offset/binary, 2#001:3, Len:29, Data:Len/binary, _/binary>> ->
			{4, Len, Data};
		<<_:Offset/binary, 2#000:3, _:5, _/binary>> -> false;
		<<_:Offset/binary, 2#001:3, _:5, _/binary>> -> {0, 4};
		<<_:Offset/binary, 2#01:2, _:6, _/binary>> -> {0, 2};
		<<_:Offset/binary, _/binary>> -> {0, 0}
	end.

%% -----------------------------------------------------------------
%%@doc 获得二进制数据块中指定前后缀之间的值，前后缀必须为二进制数据或二进制匹配模式
%% @spec binary_get_part(Bin::binary(), Prefix::binary(), Suffix::binary(), Default) -> binary()
%%@end
%% -----------------------------------------------------------------
binary_get_part(Bin, Prefix, Suffix, Default) when is_binary(Bin) ->
	case binary:match(Bin, Prefix, []) of
		{Offset, Length} ->
			Loc = Offset + Length,
			case binary:match(Bin, Suffix, [{scope, {Loc, byte_size(Bin) - Loc}}]) of
				{VOffset, _} ->
					VSize = VOffset - Loc, 
					<<_:Loc/binary, V:VSize/binary, _/binary>> = Bin,
					V;
				nomatch ->
					Default
			end;
		nomatch ->
			Default
	end.

%% -----------------------------------------------------------------
%%@doc 二进制数据对指定模式的匹配数量
%% @spec binary_match_count(Bin::binary(), Pattern::pattern()) -> integer()
%% where
%% pattern() = tuple() | list() | binary()
%%@end
%% -----------------------------------------------------------------
binary_match_count(Bin, Pattern) when is_binary(Bin), is_tuple(Pattern) ->
	Size = byte_size(Bin),
	binary_match_count(Bin, 0, Size, Size, Pattern, 0);
binary_match_count(Bin, Pattern)
	when is_binary(Bin), (is_binary(Pattern) orelse is_list(Pattern)) ->
	Size = byte_size(Bin),
	CP = binary:compile_pattern(Pattern),
	binary_match_count(Bin, 0, Size, Size, CP, 0).

%% -----------------------------------------------------------------
%%@doc 二进制数据对指定模式的匹配数量
%% @spec binary_match_count(Bin::binary(), Start::integer(), Length::integer(), Pattern::pattern()) -> integer()
%% where
%% pattern() = tuple() | list() | binary()
%%@end
%% -----------------------------------------------------------------
binary_match_count(Bin, Start, Length, Pattern) when is_binary(Bin), is_tuple(Pattern) ->
	binary_match_count(Bin, Start, Length, Start + Length, Pattern, 0);
binary_match_count(Bin, Start, Length, Pattern)
	when is_binary(Bin), (is_binary(Pattern) orelse is_list(Pattern)) ->
	CP = binary:compile_pattern(Pattern),
	binary_match_count(Bin, Start, Length, Start + Length, CP, 0).

binary_match_count(Bin, Start, Length, SL, CP, N) ->
	case binary:match(Bin, CP, [{scope, {Start, Length}}]) of
		{Start1, Len1} ->
			binary_match_count(Bin, Start1 + Len1, SL - Start1 - Len1, SL, CP, N + 1);
		_ ->
			N
	end.

%% -----------------------------------------------------------------
%%@doc 二进制数据或iolist的异或算法
%% @spec (Data::list(),PK::list()) -> list()
%%	|(Bin::binary(),PK::list()) -> list()
%%@end
%% -----------------------------------------------------------------
binary_xor(Data, PK) when is_list(Data), is_list(PK) ->
	{_K, L} = xor_iolist(Data, PK, PK, []),
	lists:reverse(L);

binary_xor(Bin, PK) when is_binary(Bin), is_list(PK) ->
	{_K, L} = xor_binary(Bin, 0, PK, PK, []),
	lists:reverse(L).

%% -----------------------------------------------------------------
%%@doc 二进制数据的异或算法
%% @spec binary_xor(Bin::binary(), Start::integer(), End::integer(), PK::list()) -> list()
%%@end
%% -----------------------------------------------------------------
binary_xor(Bin, Start, End, PK) when is_binary(Bin), is_list(PK) ->
	{_K, L} = xor_binary(Bin, Start, End, PK, PK, []),
	lists:reverse(L).

%按字节异或的函数
xor_binary(_Bin, Start, End, K, _PK, L) when Start >= End ->
	{K, L};
xor_binary(Bin, Start, End, [], [P | K] = PK, L) ->
	<<_:Start/binary, H:8, _/binary>> = Bin,
	xor_binary(Bin, Start + 1, End, K, PK, [(H bxor P) | L]);
xor_binary(Bin, Start, End, [P | K], PK, L) ->
	<<_:Start/binary, H:8, _/binary>> = Bin,
	xor_binary(Bin, Start + 1, End, K, PK, [(H bxor P) | L]).

%按字节异或的函数
xor_binary(Bin, Start, [], [P | K] = PK, L) ->
	case Bin of
		<<_:Start/binary, H:8, _/binary>> ->
			xor_binary(Bin, Start + 1, K, PK, [(H bxor P) | L]);
		_ ->
			{[], L}
	end;
xor_binary(Bin, Start, [P | K] = Old, PK, L) ->
	case Bin of
		<<_:Start/binary, H:8, _/binary>> ->
			xor_binary(Bin, Start + 1, K, PK, [(H bxor P) | L]);
		_ ->
			{Old, L}
	end.

%iolist中的数据异或的函数
xor_iolist([H | T], K, PK, L) when is_binary(H) ->
	{K1, L1} = xor_binary(H, 0, K, PK, L),
	xor_iolist(T, K1, PK, L1);
xor_iolist([H | T], K, PK, L) when is_list(H) ->
	{K1, L1} = xor_iolist(H, K, PK, L),
	xor_iolist(T, K1, PK, L1);
xor_iolist([H | T], [], [P | K] = PK, L) ->
	xor_iolist(T, K, PK, [(H bxor P) | L]);
xor_iolist([H | T], [P | K], PK, L) ->
	xor_iolist(T, K, PK, [(H bxor P) | L]);
xor_iolist([], K, _PK, L) ->
	{K, L}.

%% -----------------------------------------------------------------
%%@doc 比较函数
%% @spec compare(I1::term(),I2::term()) -> return()
%% where
%% return() = 0 | 1 | -1
%%@end
%% -----------------------------------------------------------------
compare(I1, I2) when I1 > I2 -> 1;
compare(I1, I2) when I1 < I2 -> -1;
compare(_, _) -> 0.

%% -----------------------------------------------------------------
%%@doc 二分法查找元组
%%```
%% 默认元组中的元素也为元组，
%%		用元组中的指定位置的元素进行查找，Type为1表示升序，-1表示降序
%% Tuple是元组的元组   {{term()},{term()},...}
%%'''
%% @spec half_keyfind(Tuple::tuple(), Type::integer(), Key, N::integer()) -> return()
%% return() = {0, integer()} |
%%		{1, integer()} |
%%		{-1, integer()}
%%@end
%% -----------------------------------------------------------------
half_keyfind(Tuple, Type, Key, N) when Type =:= 1 orelse Type =:= -1 ->
	half_keyfind1(Tuple, Type, 1, tuple_size(Tuple), Key, N).

%二分法查找
half_keyfind1(Tuple, Type, Start, End, Key, N) when Start < End ->
	I = (Start + End) div 2,
	E = element(I, Tuple),
	X = element(N, E),
	case compare(Key, X) of
		0 ->
			{0, I};
		Type ->
			half_keyfind1(Tuple, Type, I + 1, End, Key, N);
		_ ->
			half_keyfind1(Tuple, Type, Start, I - 1, Key, N)
	end;
half_keyfind1(Tuple, _Type, Start, Start, Key, N) ->
	{compare(Key, element(N, element(Start, Tuple))), Start};
half_keyfind1(_Tuple, _Type, Start, _End, _Key, _N) ->
	{-1, Start}.

%% -----------------------------------------------------------------
%%@doc 二分法查找元组，直接比较元组中的元素
%% @spec half_find(Tuple::{term()}, Type::integer(), Key::term()) -> return()
%% where
%% return() = true | false
%%@end
%% -----------------------------------------------------------------
half_find({}, _Key) ->
	false;
half_find(Tuple, Key) ->
	half_find1(Tuple, 1, tuple_size(Tuple), Key).

%二分法查找
half_find1(Tuple, Start, End, Key) when Start < End ->
	I = (Start + End) div 2,
	case element(I, Tuple) of
		K when K > Key ->
			half_find1(Tuple, I + 1, End, Key);
		K when K < Key ->
			half_find1(Tuple, Start, I - 1, Key);
		Key ->
			true
	end;
half_find1(Tuple, Start, Start, Key) ->
	Key =:= element(Start, Tuple);
half_find1(_, _, _, _) ->
	false.

%% -----------------------------------------------------------------
%%@doc 二分法查找元组，直接比较元组中的元素，Type为1表示升序，-1表示降序
%% @spec half_find(Tuple::{term()}, Type::integer(), Key::term()) -> return()
%% where
%% return() = {0, integer()} |
%%		{1, integer()} |
%%		{-1, integer()}
%%@end
%% -----------------------------------------------------------------
half_find({}, _Type, _Key) ->
	{-1, 1};
half_find(Tuple, Type, Key) when Type =:= 1 orelse Type =:= -1 ->
	half_find1(Tuple, Type, 1, tuple_size(Tuple), Key).

%二分法查找
half_find1(Tuple, Type, Start, End, Key) when Start < End ->
	I = (Start + End) div 2,
	case compare(Key, element(I, Tuple)) of
		0 ->
			{0, I};
		Type ->
			half_find1(Tuple, Type, I + 1, End, Key);
		_ ->
			half_find1(Tuple, Type, Start, I - 1, Key)
	end;
half_find1(Tuple, _Type, Start, Start, Key) ->
	{compare(Key, element(Start, Tuple)), Start};
half_find1(_, _Type, Start, _, _) ->
	{-1, Start}.

%% -----------------------------------------------------------------
%%@doc 二分法查找升序或降序元组，Type为1表示升序，-1表示降序
%% @spec half_find(Tuple::{term()}, Type::integer(), Key::any(), Fun::function()) -> return()
%% where
%% return() = {0, integer()} |
%%		{1, integer()} |
%%		{-1, integer()}
%%@end
%% -----------------------------------------------------------------
-spec half_find(tuple(), integer(), any(), fun((any(), any()) -> 1 | 0 | -1 )) ->
	{0, integer()} |
	{1, integer()} |
	{-1, integer()}.
%% -----------------------------------------------------------------
half_find({}, _Type, _Key, _Fun) ->
	{-1, 1};
half_find(Tuple, Type, Key, Fun) when Type =:= 1 orelse Type =:= -1 ->
	half_find1(Tuple, Type, 1, tuple_size(Tuple), Key, Fun).

%二分法查找
half_find1(Tuple, Type, Start, End, Key, Fun) when Start < End ->
	I = (Start + End) div 2,
	case Fun(Key, element(I, Tuple)) of
		0 ->
			{0, I};
		Type ->
			half_find1(Tuple, Type, I + 1, End, Key, Fun);
		_ ->
			half_find1(Tuple, Type, Start, I - 1, Key, Fun)
	end;
half_find1(Tuple, _Type, Start, Start, Key, Fun) ->
	{Fun(Key, element(Start, Tuple)), Start};
half_find1(_, _Type, Start, _, _, _) ->
	{-1, Start}.

%% -----------------------------------------------------------------
%%@doc 遍历元组，从元组最后一个向前遍历
%%```
%%	Fun Arguments:(Argus::term(),Index::integer(),Element::term())
%%      Argus为传入的A，Index为此时遍历的元素的位置，Element为此时遍历的元素
%%	Fun Returns:
%%		{break, Args::term()} | %循环结束
%%		{ok, Args::term()} | %表示继续循环
%%		Args::term() %简化版，表示继续循环
%%'''
%% @spec tuple_foreach(Tuple::tuple(),F::function(),A::any()) -> tuple()
%%@end
%% -----------------------------------------------------------------
-spec tuple_foreach(tuple(), function(), any()) ->
	tuple().
%% -----------------------------------------------------------------
tuple_foreach(Tuple, F, A) ->
	tuple_foreach(Tuple, tuple_size(Tuple), F, A).

% 遍历元组
tuple_foreach(Tuple, N, F, A) when N > 0 ->
	case F(A, N, element(N, Tuple)) of
		{ok, A1} ->
			tuple_foreach(Tuple, N - 1, F, A1);
		{break, A1} ->
			A1;
		A1 ->
			tuple_foreach(Tuple, N - 1, F, A1)
	end;
tuple_foreach(_Tuple, _N, _F, A) ->
	A.

%% -----------------------------------------------------------------
%%@doc 将指定的元素插入到元组中指定的位置，元组中当前及后面的元素依次向后挪动
%% @spec tuple_insert(N::integer(),T::tuple(),Item::any()) -> tuple()
%%@end
%% -----------------------------------------------------------------
-spec tuple_insert(integer(), tuple(), any()) ->
	tuple().
%% -----------------------------------------------------------------
tuple_insert(N, T, Item) when N > 0 ->
	case tuple_size(T) of
		Size when N > Size ->
			erlang:append_element(T, Item);
		_ ->
			list_to_tuple(list_insert_index(N, tuple_to_list(T), Item))
	end.

%% -----------------------------------------------------------------
%%@doc 移除元组中指定位置的元素
%% @spec tuple_delete(N::integer(),T::tuple()) -> tuple()
%%@end
%% -----------------------------------------------------------------
-spec tuple_delete(integer(), tuple()) ->
	tuple().
%% -----------------------------------------------------------------
tuple_delete(N, T) when N > 0 ->
	case tuple_size(T) of
		Size when N > Size ->
			T;
		_ ->
			{_, L} = list_delete_index(N, tuple_to_list(T)),
			list_to_tuple(L)
	end.

%% -----------------------------------------------------------------
%%@doc 随机化指定的列表
%% @spec shuffle(L::list()) -> list()
%%@end
%% -----------------------------------------------------------------
-spec shuffle([_]) ->
	[_].
%% -----------------------------------------------------------------
shuffle([T]) ->
	[T];
shuffle([_ | _] = L) ->
	{M, S, MS} = os:timestamp(),
	[E || {_, E} <- lists:sort(shuffle1(L, M + S + MS + erlang:phash2(self()), []))];
shuffle([]) ->
	[].

%% -----------------------------------------------------------------
%%@doc 用指定的随机数，随机化指定的列表
%% @spec shuffle(L::list(),S::{integer(), integer(), integer()}) -> list()
%%@end
%% -----------------------------------------------------------------
-spec shuffle([_], {integer(), integer(), integer()}) ->
	[_].
%% -----------------------------------------------------------------
shuffle([T], _S) ->
	[T];
shuffle([_ | _] = L, S) ->
	[E || {_, E} <- lists:sort(shuffle1(L, S, []))];
shuffle([], _S) ->
	[].

shuffle1([H | T], S, L) ->
	R = random(S),
	shuffle1(T, R, [{R, H} | L]);
shuffle1([], _S, L) ->
	L.

%% -----------------------------------------------------------------
%% Function: member_order/2
%% Description: 判断指定的元素是否在排序列表中
%% Returns: true | false
%% -----------------------------------------------------------------
-spec member_order(term(), list()) ->
	true | false.
%% -----------------------------------------------------------------
member_order(Elem, [H | T]) when Elem > H ->
	member_order(Elem, T);
member_order(Elem, [H | _T]) ->
	Elem =:= H;
member_order(_, _) ->
	false.

%% -----------------------------------------------------------------
%% Function: delete_order/2
%% Description: 将指定的元素从排序列表中删除
%% Returns: true | false
%% -----------------------------------------------------------------
-spec delete_order(term(), list()) ->
	list() | false.
%% -----------------------------------------------------------------
delete_order(Elem, L) ->
	delete_order(Elem, L, []).

delete_order(Elem, [H | T], L) when Elem > H ->
	delete_order(Elem, T, [H | L]);
delete_order(Elem, [Elem | T], L) ->
	lists:reverse(L, T);
delete_order(_, _, _) ->
	false.

%% -----------------------------------------------------------------
%%@doc  合并2个排序列表（升序，由小到大），去掉重复的，返回的列表是倒排序的
%% @spec  merge_order(Left, Right) -> return()
%% where
%%  return() = {Same, Left, Right}
%%@end
%% -----------------------------------------------------------------
-spec merge_order(list(), list()) ->
	list().
%% -----------------------------------------------------------------
merge_order(Left, Right) ->
	merge_order(Left, Right, []).

merge_order([H1 | _] = Left, [H2 | T2], L) when H1 > H2 ->
	merge_order(Left, T2, [H2 | L]);
merge_order([H | T1], [H | T2], L) ->
	merge_order(T1, T2, [H | L]);
merge_order([H | T1], Right, L) ->
	merge_order(T1, Right, [H | L]);
merge_order([], Right, L) ->
	lists:reverse(Right, L);
merge_order(Left, [], L) ->
	lists:reverse(Left, L).

%% -----------------------------------------------------------------
%%@doc  合并2个排序列表（升序，由大到小），去掉重复的，返回的列表是倒排序的
%% @spec  merge_order_desc(Left, Right) -> return()
%% where
%%  return() = {Same, Left, Right}
%%@end
%% -----------------------------------------------------------------
-spec merge_order_desc(list(), list()) ->
	list().
%% -----------------------------------------------------------------
merge_order_desc(Left, Right) ->
	merge_order_desc(Left, Right, []).

merge_order_desc([H1 | _] = Left, [H2 | T2], L) when H1 < H2 ->
	merge_order_desc(Left, T2, [H2 | L]);
merge_order_desc([H | T1], [H | T2], L) ->
	merge_order_desc(T1, T2, [H | L]);
merge_order_desc([H | T1], Right, L) ->
	merge_order_desc(T1, Right, [H | L]);
merge_order_desc([], Right, L) ->
	lists:reverse(Right, L);
merge_order_desc(Left, [], L) ->
	lists:reverse(Left, L).

%% -----------------------------------------------------------------
%%@doc  比较2个排序列表（升序，由小到大），返回相同和不同的，返回的列表是倒排序的
%% @spec  compare_order(Left, Right) -> return()
%% where
%%  return() = {Same, Left, Right}
%%@end
%% -----------------------------------------------------------------
-spec compare_order(list(), list()) ->
	{list(), list(), list()}.
%% -----------------------------------------------------------------
compare_order(Left, Right) ->
	compare_order(Left, Right, [], [], []).

compare_order([H | T1], [H | T2], L, L1, L2) ->
	compare_order(T1, T2, [H | L], L1, L2);
compare_order([H1 | _] = Left, [H2 | T2], L, L1, L2) when H1 > H2 ->
	compare_order(Left, T2, L, L1, [H2 | L2]);
compare_order([H | T1], Right, L, L1, L2) ->
	compare_order(T1, Right, L, [H | L1], L2);
compare_order([], Right, L, L1, L2) ->
	{L, L1, lists:reverse(Right, L2)};
compare_order(Left, [], L, L1, L2) ->
	{L, lists:reverse(Left, L1), L2}.

%% -----------------------------------------------------------------
%%@doc 比较2个排序列表（升序，由小到大），返回相同和不同的，返回的列表是倒排序的
%%```
%% F为自定义的一个比较大小函数   参数为(N1::term(),N2::term())  返回0表示相等,1表示N1>N2,其余(例如-1)表示N1<N2
%%'''
%% @spec compare_order(Left::list(),Right::list(),F::function()) -> {Same::list(), Left::list(), Right::list()}
%%@end
%% -----------------------------------------------------------------
-spec compare_order(list(), list(), integer() | function()) ->
	{list(), list(), list()}.
%% -----------------------------------------------------------------
compare_order(Left, Right, I) when is_integer(I) ->
	compare_order1(Left, Right, I, [], [], []);
compare_order(Left, Right, F) when is_function(F) ->
	compare_order2(Left, Right, F, [], [], []).

compare_order1([H1 | T1] = Left, [H2 | T2] = Right, I, L, L1, L2) ->
	I1 = element(I, H1),
	I2 = element(I, H2),
	if
		I1 > I2 ->
			compare_order1(Left, T2, I, L, L1, [H2 | L2]);
		I1 < I2 ->
			compare_order1(T1, Right, I, L, [H1 | L1], L2);
		true ->
			compare_order1(T1, T2, I, [H1 | L], L1, L2)
	end;
compare_order1([], Right, _I, L, L1, L2) ->
	{L, L1, lists:reverse(Right, L2)};
compare_order1(Left, [], _I, L, L1, L2) ->
	{L, lists:reverse(Left, L1), L2}.

compare_order2([H1 | T1] = Left, [H2 | T2] = Right, F, L, L1, L2) ->
	case F(H1, H2) of
		R when R > 0 ->
			compare_order2(Left, T2, F, L, L1, [H2 | L2]);
		R when R < 0 ->
			compare_order2(T1, Right, F, L, [H1 | L1], L2);
		_ ->
			compare_order2(T1, T2, F, [H1 | L], L1, L2)
	end;
compare_order2([], Right, _F, L, L1, L2) ->
	{L, L1, lists:reverse(Right, L2)};
compare_order2(Left, [], _F, L, L1, L2) ->
	{L, lists:reverse(Left, L1), L2}.

%% -----------------------------------------------------------------
%%@doc 获得指定的元素在列表中的位置
%% @spec list_index(Item::any(),L::list()) -> 0 | integer()
%%@end
%% -----------------------------------------------------------------
-spec list_index(any(), list()) ->
	0 | integer().
%% -----------------------------------------------------------------
list_index(Item, L) ->
	list_index(Item, L, 1).

list_index(Item, [Item | _T], N) ->
	N;
list_index(Item, [_H | T], N) ->
	list_index(Item, T, N + 1);
list_index(_, _, _N) ->
	0.

%% -----------------------------------------------------------------
%%@doc 将指定的元素插入到列表中指定的位置，列表中后面的元素依次向后挪动
%% @spec list_insert_index(N::integer(),L::list(),Item::any()) -> list()
%%@end
%% -----------------------------------------------------------------
-spec list_insert_index(integer(), list(), any()) ->
	list().
%% -----------------------------------------------------------------
list_insert_index(N, L, Item) when N > 0 ->
	list_insert_index(N, L, Item, []);
list_insert_index(0, L, Item) ->
	[Item | L].

list_insert_index(N, [H | T], Item, L) when N > 0 ->
	list_insert_index(N - 1, T, Item, [H | L]);
list_insert_index(_N, List, Item, L) ->
	lists:reverse(L, [Item | List]).

%% -----------------------------------------------------------------
%%@doc 移除列表中指定位置的元素
%% @spec list_delete_index(N::integer(),L::list()) -> return()
%% where
%% return() = false | {any(), list()}
%%@end
%% -----------------------------------------------------------------
-spec list_delete_index(integer(), list()) ->
	{any(), list()}.
%% -----------------------------------------------------------------
list_delete_index(N, L) when N > 0 ->
	list_delete_index(N - 1, L, []).

list_delete_index(N, [H | T], L) when N > 0 ->
	list_delete_index(N - 1, T, [H | L]);
list_delete_index(_N, [H | T], L) ->
	{H, lists:reverse(L, T)};
list_delete_index(_N, _, _L) ->
	false.

%% -----------------------------------------------------------------
%%@doc 替换列表中第一个符合指定元素
%% @spec list_replace(Old,L::list(),New) -> list()
%%@end
%% -----------------------------------------------------------------
list_replace(Old, L, New) when is_list(L) ->
	list_replace(Old, L, New, [], L).

list_replace(Old, [Old | T], New, L, _LL) ->
	lists:reverse(L, [New | T]);
list_replace(Old, [H | T], New, L, LL) ->
	list_replace(Old, T, New, [H | L], LL);
list_replace(_Old, [], _New, _L, LL) ->
	LL.

%% -----------------------------------------------------------------
%%@doc 移除列表中的元素
%% @spec list_delete(E::any(),L::list()) -> list()
%%@end
%% -----------------------------------------------------------------
-spec list_delete(any(), list()) ->
	list().
%% -----------------------------------------------------------------
list_delete(E, L) ->
	list_delete(E, L, [], L).

list_delete(E, [E | T], L, _LL) ->
	lists:reverse(L, T);
list_delete(E, [H | T], L, LL) ->
	list_delete(E, T, [H | L], LL);
list_delete(_E, [], _L, LL) ->
	LL.

%% -----------------------------------------------------------------
%%@doc 删除列表中全部符合指定键的元素
%%```
%% 函数返回N表示满足删除的元素个数,List表示删除后的列表
%%'''
%% @spec list_deletes(E::any(),L::[term()]) -> return()
%% where
%% return() = {N::integer(), List::list()}
%%@end
%% -----------------------------------------------------------------
-spec list_deletes(any(), [_]) ->
	{0, [_]} |
	{integer(), [_]}.
%% -----------------------------------------------------------------
list_deletes(E, L) when is_list(L) ->
	list_deletes(E, L, [], 0, L).

list_deletes(E, [E | T], L, C, LL) ->
	list_deletes(E, T, L, C + 1, LL);
list_deletes(E, [H | T], L, C, LL) ->
	list_deletes(E, T, [H | L], C, LL);
list_deletes(_E, [], _L, 0, LL) ->
	{0, LL};
list_deletes(_E, [], L, C, _LL) ->
	{C, lists:reverse(L)}.

%% -----------------------------------------------------------------
%%@doc 删除列表中全部符合指定键的元素
%%```
%% L列表里面为元组，N标识了元组中Key的位置
%% 函数返回的integer()为删除的元素个数
%%'''
%% @spec list_keydeletes(Key::any(),N::integer(),L::[tuple()]) -> return()
%% where
%% return() = {integer(), list()}
%%@end
%% -----------------------------------------------------------------
-spec list_keydeletes(any(), integer(), [_]) ->
	{0, [_]} |
	{integer(), [_]}.
%% -----------------------------------------------------------------
list_keydeletes(Key, N, L) when is_integer(N), N > 0, is_list(L) ->
	list_keydeletes(Key, N, L, [], 0, L).

list_keydeletes(Key, N, [H | T], L, C, Src) when is_tuple(H), tuple_size(H) >= N, element(N, H) =:= Key ->
	list_keydeletes(Key, N, T, L, C + 1, Src);
list_keydeletes(Key, N, [H | T], L, C, Src) ->
	list_keydeletes(Key, N, T, [H | L], C, Src);
list_keydeletes(_Key, _N, [], _L, 0, Src) ->
	{0, Src};
list_keydeletes(_Key, _N, [], L, C, _Src) ->
	{C, lists:reverse(L)}.

%% -----------------------------------------------------------------
%%@doc 删除列表中第一个符合指定键的元素
%% @spec list_keydelete(Key::term(),N::integer(),L::[tuple()]) -> return()
%% where
%% return() = none | {ok, Old::tuple(), list()}
%%@end
%% -----------------------------------------------------------------
list_keydelete(Key, N, L) when is_integer(N), N > 0 ->
	list_keydelete(Key, N, L, []).

list_keydelete(Key, N,  [H | T], L)
	when is_tuple(H), tuple_size(H) >= N, element(N, H) =:= Key ->
	{ok, H, lists:reverse(L, T)};
list_keydelete(Key, N, [H | T], L) ->
	list_keydelete(Key, N, T, [H | L]);
list_keydelete(_Key, _N, [], _L) ->
	none.

%% -----------------------------------------------------------------
%%@doc 替换列表中第一个符合指定键的元素
%% @spec list_keyreplace(Key::term(),N::integer(),L::[tuple()],Tuple::tuple()) -> return()
%% where
%% return() = none | {ok, Old::tuple(), list()}
%%@end
%% -----------------------------------------------------------------
list_keyreplace(Key, N, L, Tuple) when is_integer(N), N > 0, is_tuple(Tuple) ->
	list_keyreplace(Key, N, L, [], Tuple).

list_keyreplace(Key, N, [H | T], L, Tuple)
	when is_tuple(H), tuple_size(H) >= N, element(N, H) =:= Key ->
	{ok, H, lists:reverse(L, [Tuple | T])};
list_keyreplace(Key, N, [H | T], L, Tuple) ->
	list_keyreplace(Key, N, T, [H | L], Tuple);
list_keyreplace(_Key, _N, [], _L, _Tuple) ->
	none.

%% -----------------------------------------------------------------
%%@doc 将2个列表的元素按顺序合并
%%```
%%	合并成{E_List1, E_List2}的倒序列表，超过另一个列表长度的元素将忽略
%%'''
%% @spec list_merge(L1::[term()],L2::[term()]) -> [{term(),term()}]
%%@end
%% -----------------------------------------------------------------
list_merge(L1, L2) when is_list(L1), is_list(L2) ->
	list_merge(L1, L2, []).

list_merge([H1 | T1], [H2 | T2], L) ->
	list_merge(T1, T2, [{H1, H2} | L]);
list_merge([], _, L) ->
	L;
list_merge(_, [], L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 从ets表的选出数据
%%```
%%	Fun Returns:
%%		{ok, Args} |
%%		{break, Args} |
%%		{update, Args, Item} |
%%		{update_break, Args, Item} |
%%		{delete, Args} |
%%		{delete_break, Args} |
%%		Args
%%'''
%% @spec ets_select(Ets::ets(),Fun::function(),Argus::any()) -> any()
%% where
%% ets() = integer() | atom()
%%@end
%% -----------------------------------------------------------------
-spec ets_select(integer() | atom(), function(), any()) ->
	any().
%% -----------------------------------------------------------------
ets_select(Ets, Fun, Args) ->
	ets_select1(Ets, ets:first(Ets), Fun, Args).

ets_select1(_Ets, '$end_of_table', _Fun, Args) ->
	Args;
ets_select1(Ets, Key, F, Args) ->
	case ets:lookup(Ets, Key) of
		[I] ->
			case F(Args, I) of
				{ok, A} ->
					ets_select1(Ets, ets:next(Ets, Key), F, A);
				{break, A} ->
					A;
				{update, A, Item} ->
					ets:insert(Ets, Item),
					ets_select1(Ets, ets:next(Ets, Key), F, A);
				{update_break, A, Item} ->
					ets:insert(Ets, Item),
					A;
				{delete, A} ->
					Next = ets:next(Ets, Key),
					ets:delete(Ets, Key),
					ets_select1(Ets, Next, F, A);
				{delete_break, A} ->
					ets:delete(Ets, Key),
					A;
				A ->
					ets_select1(Ets, ets:next(Ets, Key), F, A)
			end;
		_ ->
			ets_select1(Ets, ets:next(Ets, Key), F, Args)
	end.

%% -----------------------------------------------------------------
%%@doc 从ets表中选出数据
%%```
%%	Fun Returns:
%%		{ok, Args} |
%%		{break, Args} |
%%		{update, Args, Item} |
%%		{update_break, Args, Item} |
%%		{delete, Args} |
%%		{delete_break, Args} |
%%		Args
%% ets_select/3通过值来寻找满足条件的数据
%% ets_key_select/3通过键来寻找满足条件的数据
%%'''
%% @spec ets_key_select(Ets::ets(),Fun::function(),Argus::any()) -> any()
%% where
%% ets() = integer()|atom()
%%@end
%% -----------------------------------------------------------------
-spec ets_key_select(integer() | atom(), function(), any()) ->
	any().
%% -----------------------------------------------------------------
ets_key_select(Ets, Fun, Args) ->
	ets_key_select1(Ets, ets:first(Ets), Fun, Args).

ets_key_select1(_Ets, '$end_of_table', _Fun, Args) ->
	Args;
ets_key_select1(Ets, Key, F, Args) ->
	case F(Args, Key) of
		{ok, A} ->
			ets_key_select1(Ets, ets:next(Ets, Key), F, A);
		{break, A} ->
			A;
		{update, A, Item} ->
			ets:insert(Ets, Item),
			ets_key_select1(Ets, ets:next(Ets, Key), F, A);
		{update_break, A, Item} ->
			ets:insert(Ets, Item),
			A;
		{delete, A} ->
			Next = ets:next(Ets, Key),
			ets:delete(Ets, Key),
			ets_key_select1(Ets, Next, F, A);
		{delete_break, A} ->
			ets:delete(Ets, Key),
			A;
		A ->
			ets_key_select1(Ets, ets:next(Ets, Key), F, A)
	end.

%% -----------------------------------------------------------------
%%@doc 从ets表的选出数据
%%```
%%	Fun Returns:
%%		{ok, Args} |
%%		{break, Args} |
%%		{update, Item, Args} |
%%		{update_break, Item, Args} |
%%		{delete, Args} |
%%		{delete_break, Args} |
%%		Args
%% Fun函数的参数为(Args, Key, V),Args为传入的Args,Key为依次遍历ets表的数据的key,
%% V为对应key的元素的第I个数据（包括key在内）即{key,_,V}则I为3才能找出V
%%'''
%% @spec ets_value_select(Ets::ets(),I::integer(),Fun::function(),Argus::any()) -> any()
%% where
%% ets() = integer()|atom()
%%@end
%% -----------------------------------------------------------------
-spec ets_value_select(integer() | atom(), integer(), function(), any()) ->
	any().
%% -----------------------------------------------------------------
ets_value_select(Ets, I, Fun, Args) ->
	ets_value_select1(Ets, I, ets:first(Ets), Fun, Args).

ets_value_select1(_Ets, _I, '$end_of_table', _Fun, Args) ->
	Args;
ets_value_select1(Ets, I, Key, F, Args) ->
	try ets:lookup_element(Ets, Key, I) of
		V ->
			case F(Args, Key, V) of
				{ok, A} ->
					ets_value_select1(Ets, I, ets:next(Ets, Key), F, A);
				{break, A} ->
					A;
				{update, Item, A} ->
					ets:update_element(Ets, Key, {I, Item}),
					ets_value_select1(Ets, I, ets:next(Ets, Key), F, A);
				{update_break, Item, A} ->
					ets:update_element(Ets, Key, {I, Item}),
					A;
				{delete, A} ->
					Next = ets:next(Ets, Key),
					ets:delete(Ets, Key),
					ets_value_select1(Ets, I, Next, F, A);
				{delete_break, A} ->
					ets:delete(Ets, Key),
					A;
				A ->
					ets_value_select1(Ets, I, ets:next(Ets, Key), F, A)
			end
	catch
		_:_ ->
			ets_value_select1(Ets, I, ets:next(Ets, Key), F, Args)
	end.

%% -----------------------------------------------------------------
%%@doc 合并2棵SBT树，如果键相同，保留第一棵树的值
%% @spec merge_tree(Headers1::sb_trees(),Headers2::sb_trees()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
merge_tree(Headers1, Headers2) ->
	merge_tree1(sb_trees:to_list(Headers1), sb_trees:to_list(Headers2), []).

merge_tree1([{K1, _} | _] = L1, [{K2, _} = KV | T2], L) when K1 > K2 ->
	merge_tree1(L1, T2, [KV | L]);
merge_tree1([{K1, _} = KV | T1], [{K2, _} | _] = L2, L) when K1 < K2 ->
	merge_tree1(T1, L2, [KV | L]);
merge_tree1([KV | T1], [_ | T2], L) ->
	merge_tree1(T1, T2, [KV | L]);
merge_tree1(List1, [], L) ->
	sb_trees:from_orddict(lists:reverse(lists:reverse(List1, L)));
merge_tree1([], List2, L) ->
	sb_trees:from_orddict(lists:reverse(lists:reverse(List2, L))).

%% -----------------------------------------------------------------
%%@doc 合并2棵SBT树，如果键相同，保留第一棵树的值
%%```
%% 当两个树中有键相同时,函数F决定是删除还是保留某个值或是产生一个新值
%%'''
%% @spec merge_tree(Headers1::sb_trees(), Headers2::sb_trees(), F::function(), A::term()) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
merge_tree(Headers1, Headers2, F, A) when is_function(F) ->
	merge_tree1(sb_trees:to_list(Headers1), sb_trees:to_list(Headers2), F, A, []).

merge_tree1([{K1, _} | _] = L1, [{K2, _} = KV | T2], F, A, L) when K1 > K2 ->
	merge_tree1(L1, T2, F, A, [KV | L]);
merge_tree1([{K1, _} = KV | T1], [{K2, _} | _] = L2, F, A, L) when K1 < K2 ->
	merge_tree1(T1, L2, F, A, [KV | L]);
merge_tree1([{K, V1} = KV1 | T1], [{K, V2} = KV2 | T2], F, A, L) ->
	case F(A, K, V1, V2) of
		delete ->
			merge_tree1(T1, T2, F, A, L);
		{ok, V1} ->
			merge_tree1(T1, T2, F, A, [KV1 | L]);
		{ok, V2} ->
			merge_tree1(T1, T2, F, A, [KV2 | L]);
		{ok, V} ->
			merge_tree1(T1, T2, F, A, [{K, V} | L]);
		V ->
			merge_tree1(T1, T2, F, A, [{K, V} | L])
	end;
merge_tree1(List1, [], _F, _A, L) ->
	sb_trees:from_orddict(lists:reverse(lists:reverse(List1, L)));
merge_tree1([], List2, _F, _A, L) ->
	sb_trees:from_orddict(lists:reverse(lists:reverse(List2, L))).

%% -----------------------------------------------------------------
%%@doc 安全的路径，防止路径中“..”来访问上级目录，使用“/”开头也被过滤掉
%% @spec safe_path(Path::list()) -> return()
%% where
%% return() = string() | false
%%@end
%% -----------------------------------------------------------------
safe_path(Path) ->
	safe_path(Path, []).

safe_path(Path, L) ->
	case split_first(Path, $/) of
		{"..", Right} ->
			if
				L =:= [] -> false;
				true ->
					[_H | T] = L,
					safe_path(Right, T)
			end;
		{"", Right} ->
			safe_path(Right, L);
		{Left, Right} ->
			safe_path(Right, [Left | L]);
		_ ->
			string:join(lists:reverse([Path | L]), "/")
	end.

%% -----------------------------------------------------------------
%%@doc 获取路径
%% @spec dir_path(Path::list()) -> list()
%%@end
%% -----------------------------------------------------------------
dir_path("") ->
	"";
dir_path(".") ->
	"";
dir_path("/") ->
	"";
dir_path(Path) ->
	[_ | T] = lists:reverse(Path),
	dir_path1(T).

dir_path1([$/ | _] = L) ->
	lists:reverse(L);
dir_path1([_ | T]) ->
	dir_path1(T);
dir_path1([]) ->
	".".

%% -----------------------------------------------------------------
%%@doc 创建指定的目录，自动创建不存在的父目录
%% @spec make_dir(Dir::list()) -> return()
%% where
%% return() = ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
make_dir(Dir) ->
	case filelib:is_dir(Dir) of
		false ->
			case make_dir(filename:dirname(Dir)) of
				ok ->
					file:make_dir(Dir);
				E ->
					E
			end;
		true ->
			ok
	end.

%% -----------------------------------------------------------------
%%@doc 获取文件信息（{文件名, 大小, 类型, 修改时间, 创建时间}）
%%```
%% Returns: {文件名, 大小, 类型, 修改时间, 创建时间} | {error, Reason}
%%'''
%% @spec get_file_info(File::list()) -> return()
%% where
%% return() = {list(), integer(), regular | directory | atom(), {{integer(), integer(), integer()}, {integer(), integer(), integer()}},	{{integer(), integer(), integer()}, {integer(), integer(), integer()}}} | {error, atom()}
%%@end
%% -----------------------------------------------------------------
-spec get_file_info(list()) ->
	{list(), integer(), regular | directory | atom(),
		{{integer(), integer(), integer()}, {integer(), integer(), integer()}},
		{{integer(), integer(), integer()}, {integer(), integer(), integer()}}} | {error, atom()}.
%% -----------------------------------------------------------------
get_file_info(File) ->
	case file:read_file_info(File) of
		{ok, #file_info{size = Size, type = regular, mtime = MTime, ctime = CTime}} ->
			{File, Size, regular, MTime, CTime};
		{ok, #file_info{type = Type, mtime = MTime, ctime = CTime}} ->
			{File, 0, Type, MTime, CTime};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 参数Depth表示遍历深度
%%```
%%	返回排倒序后的{文件名, 大小, 类型, 修改时间, 创建时间}列表
%%'''
%% @spec list_file(Dir::list(),Depth::integer()) -> return()
%% where
%% return() = {ok, list()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
-spec list_file(list(), integer()) ->
	{ok, list()} | {error, atom()}.
%% -----------------------------------------------------------------
list_file(Dir, Depth) when is_integer(Depth) ->
	case file:list_dir(Dir) of
		{ok, L} ->
			{ok, traversal_file(none, Dir, "", lists:sort(L), [], Depth)};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 参数Traversal表示遍历深度
%%```
%%	返回排倒序后的{文件名, 大小, 类型, 修改时间, 创建时间}列表
%%'''
%% @spec list_file(Dir::list(),Depth::integer(),Skip::skip()) -> return()
%% where
%% skip() = function() | {atom(), atom(), list() | none}
%% return() = {ok, list()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
-spec list_file(list(), integer(), function() | {atom(), atom(), list() | none}) ->
	{ok, list()} | {error, atom()}.
%% -----------------------------------------------------------------
list_file(Dir, Depth, Skip) when is_integer(Depth) ->
	case file:list_dir(Dir) of
		{ok, L} ->
			{ok, traversal_file(Skip, Dir, "", lists:sort(L), [], Depth)};
		E ->
			E
	end.

%% 遍历指定的目录，列出其中的所有文件
traversal_file(Skip, Path, Dir, [H | T], L, Depth) ->
	F = Dir ++ H,
	FN = Path ++ [$/ | F],
	case file:read_file_info(FN) of
		{ok, #file_info{size = Size, type = regular, mtime = MTime, ctime = CTime}} ->
			Info = {F, Size, regular, MTime, CTime},
			case file_skip(Skip, Dir, H, Info) of
				true ->
					traversal_file(Skip, Path, Dir, T, [Info | L], Depth);
				_ ->
					traversal_file(Skip, Path, Dir, T, L, Depth)
			end;
		{ok, #file_info{type = directory, mtime = MTime, ctime = CTime}} ->
			Info = {F, 0, directory, MTime, CTime},
			case file_skip(Skip, Dir, H, Info) of
				true when Depth > 1 ->
					case file:list_dir(FN) of
						{ok, LL} ->
							traversal_file(Skip, Path, Dir, T, [Info | traversal_file(
								Skip, Path, F ++ "/", lists:sort(LL), L, Depth - 1)], Depth);
						_ ->
							traversal_file(Skip, Path, Dir, T, [Info | L], Depth)
					end;
				true ->
					traversal_file(Skip, Path, Dir, T, [Info | L], Depth);
				_ ->
					traversal_file(Skip, Path, Dir, T, L, Depth)
			end;
		_ ->
			traversal_file(Skip, Path, Dir, T, L, Depth)
	end;
traversal_file(_Skip, _Path, _Dir, [], L, _Depth) ->
	L.

file_skip(none, _Dir, _F, _Info) ->
	true;
file_skip({M, F, A}, Dir, F, Info) ->
	M:F(A, Dir, F, Info);
file_skip(Skip, Dir, F, Info) ->
	Skip(Dir, F, Info).

%% -----------------------------------------------------------------
%%@doc Compress a binary (with zlib headers and checksum)
%% @spec compress(Data) -> iolist()
%%@end
%% -----------------------------------------------------------------
compress(Data) ->
	Z = zlib:open(),
	zlib:deflateInit(Z, default),
	Bs = zlib:deflate(Z, Data, finish),
	zlib:deflateEnd(Z),
	zlib:close(Z),
	Bs.

%% -----------------------------------------------------------------
%%@doc Uncompress a binary (with zlib headers and checksum)
%% @spec uncompress(Data) -> iolist()
%%@end
%% -----------------------------------------------------------------
uncompress(Data) ->
	Z = zlib:open(),
	zlib:inflateInit(Z),
	Bs = zlib:inflate(Z, Data),
	zlib:inflateEnd(Z),
	zlib:close(Z),
	Bs.

%% -----------------------------------------------------------------
%%@doc Compress a binary (with gz headers and checksum)
%% @spec gzip(Data) -> iolist()
%%@end
%% -----------------------------------------------------------------
gzip(Data) ->
	Z = zlib:open(),
	zlib:deflateInit(Z, default, deflated, 16 + ?MAX_WBITS, 8, default),
	Bs = zlib:deflate(Z, Data, finish),
	zlib:deflateEnd(Z),
	zlib:close(Z),
	Bs.

%% -----------------------------------------------------------------
%%@doc Uncompress a binary (with gz headers and checksum)
%% @spec gunzip(Data) -> iolist()
%%@end
%% -----------------------------------------------------------------
gunzip(Data) ->
	Z = zlib:open(),
	zlib:inflateInit(Z, 16 + ?MAX_WBITS),
	Bs = zlib:inflate(Z, Data),
	zlib:inflateEnd(Z),
	zlib:close(Z),
	Bs.

%% -----------------------------------------------------------------
%%@doc Compress a binary (without zlib headers and checksum)
%% @spec zip(Data) -> iolist()
%%@end
%% -----------------------------------------------------------------
zip(Data) ->
	Z = zlib:open(),
	zlib:deflateInit(Z, default, deflated, -?MAX_WBITS, 8, default),
	Bs = zlib:deflate(Z, Data, finish),
	zlib:deflateEnd(Z),
	zlib:close(Z),
	Bs.

%% -----------------------------------------------------------------
%%@doc Uncompress a binary (without zlib headers and checksum)
%% @spec unzip(Data) -> iolist()
%%@end
%% -----------------------------------------------------------------
unzip(Data) ->
	Z = zlib:open(),
	zlib:inflateInit(Z, -?MAX_WBITS),
	Bs = zlib:inflate(Z, Data),
	zlib:inflateEnd(Z),
	zlib:close(Z),
	Bs.

%% -----------------------------------------------------------------
%%@doc compress to zip
%% @spec gunzip(Data::data(), Limit::integer()) -> binary()
%% where
%% data() = binary()|list()
%%@end
%% -----------------------------------------------------------------
gunzip(Data, Limit) when is_binary(Data); is_list(Data) ->
	unzlib(Data, 16 + ?MAX_WBITS, Limit).

%% -----------------------------------------------------------------
%%@doc Uncompress a binary (with zlib headers and checksum)
%% @spec uncompress(Data::data(),Limit) -> binary()
%% where
%% data() = binary()|list()
%%@end
%% -----------------------------------------------------------------
uncompress(Data, Limit) when is_binary(Data); is_list(Data) ->
	unzlib(Data, ?MAX_WBITS, Limit).

%% -----------------------------------------------------------------
%%@doc Compress a binary (without zlib headers and checksum)
%% @spec unzip(Data::data(),Limit) -> binary()
%% where
%% data() = binary()|list()
%%@end
%% -----------------------------------------------------------------
unzip(Data, Limit) when is_binary(Data); is_list(Data) ->
	unzlib(Data, -?MAX_WBITS, Limit).

% 解压缩指定的二进制数据，解压出来的长度必须小于Limit
unzlib(Data, WindowBits, Limit) ->
	Z = zlib:open(),
	zlib:inflateInit(Z, WindowBits),
	L = inflate(Z, Data, Limit),
	zlib:inflateEnd(Z),
	zlib:close(Z),
	L.

inflate(Z, Data, Limit) ->
	try
		true = port_command(Z, Data),
		zcall(Z, ?INFLATE, <<?Z_NO_FLUSH:32>>),
		collect(Z, Limit)
	after
		flush(Z)
	end.

zcall(Z, Cmd, Arg) ->
	case port_control(Z, Cmd, Arg) of
		[0 | Res] ->
			list_to_atom(Res);
		[1 | Res] ->
			erlang:error(list_to_atom(Res));
		[2, A, B, C, D] ->
			(A bsl 24) + (B bsl 16) + (C bsl 8) + D;
		[3, A, B, C, D] ->
			erlang:error({need_dictionary, (A bsl 24) + (B bsl 16) + (C bsl 8)+D})
	end.

collect(Z, Limit) ->
	collect(Z, [], 0, Limit).

collect(_Z, _L, N, Limit) when N > Limit ->
	erlang:error({overflow_limit, N});
collect(Z, L, N, Limit) ->
	receive
		{Z, {data, Bin}} ->
			collect(Z, [Bin | L], N + byte_size(Bin), Limit)
		after 0 ->
			lists:reverse(L)
	end.

flush(Z) ->
	receive
		{Z, {data, _}} ->
			flush(Z)
		after 0 ->
			ok
	end.
