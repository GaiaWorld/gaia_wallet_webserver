%% 
%% @doc 数据库对比同步函数库
%%


-module(compare_lib).

%% ====================================================================
%% API functions
%% ====================================================================
-export([checksum/1, hash/2, db_digest/1, compare_digest/2, get/2, get_records/2, split_set/2, get_split_num/1, compare_by_range/2, keys/1, keys_part/3]).

%%获取数据的校验和
checksum(Data) ->
	erlang:crc32(term_to_binary(Data)).

%%
%%计算指定数据指定类型的哈希值
%%
hash(Type, Data) when is_list(Data) ->
	crypto:bytes_to_integer(compute_hash(Type, Data));
hash(Type, Data) ->
	crypto:bytes_to_integer(crypto:hash(Type, erlang:term_to_binary(Data))).

%%
%%获取指定时间 指定数量的数据摘要
%%
db_digest({M, F, A}) ->
	[{StartKey, _}|_]=DataL=apply(M, F, A),
	{TailKey, _}=lists:last(DataL),
	[StartKey, TailKey, length(DataL), crypto:bytes_to_integer(compute_hash(md5, DataL))].

%%
%%比较双方的数据摘要
%%
compare_digest([StartKey, TailKey, Size, Hash], [StartKey, TailKey, Size, Hash]) ->
	same;
compare_digest(Left, Right) ->
	compare_digest(Left, Right, []).

%%
%%获取交集和补集
%%
get(C0, C1) ->
	get(C0, C1, [], [], []).

%%
%%获取交集和补集
%%
get_records(C0, C1) ->
	get_record(C0, C1, [], [], []).

%%
%%分隔的数量
%%
split_set(Len, Split) when is_integer(Len), is_integer(Split), Len >= 0, Len >= Split, Split >= 0 ->
	Len div Split;
split_set(Len, Split) when is_integer(Len), is_integer(Split), Len >= 0, Split >= 0 ->
	Len.

%%
%%分隔的数量
%%
get_split_num(0) ->
	0;
get_split_num(Len) when is_integer(Len), Len > 0 ->
	get_split_num(Len, 16).
		
%%
%%求重叠，交叉和无关
%%
compare_by_range({Start0, Tail0}, {Start1, Tail1}) ->
	compare_by_range(Start0, Tail0, Start1, Tail1).

%%
%%获取指定表的所有键
%%
keys(Table) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, Status, Pid}] when Status =:= running; Status =:= only_read; Status =:= sync ->
			{ok, Iterator}=zm_db_table:iterate(Pid, descending),
			keys(Iterator, []);
		_ ->
			false
	end.

%%
%%获取指定表中指定键开始指定数量的键
%%
keys_part(Table, Key, Len) ->
	case ets:lookup(zm_db_server, Table) of
		[{Table, _, _, Status, Pid}] when Status =:= running; Status =:= only_read; Status =:= sync ->
			{ok, Iterator}=zm_db_table:iterate(Pid, descending),
			keys_part1(Iterator, Key, Len, []);
		_ ->
			false
	end.
	
%% ====================================================================
%% Internal functions
%% ====================================================================

compute_hash(Type, DataL) ->
	compute_hash(DataL, crypto:hash_init(Type)).

compute_hash1([{_, _} = KV|T], Context) ->
	compute_hash1(T, crypto:hash_update(Context, term_to_binary(KV)));
compute_hash1([], Context) ->
	crypto:hash_final(Context).

compare_digest([H|T0], [H|T1], L) ->
	compare_digest(T0, T1, [H|L]);
compare_digest([H0|T0], [H1|T1], L) ->
	compare_digest(T0, T1, [{diff, H0, H1}|L]);
compare_digest([], [], L) ->
	list_to_tuple(lists:reverse(L)).

get([H|T0], [H|T1], I, C0, C1) ->
	get(T0, T1, [H|I], C0, C1);
get([H0|T0], [H1|_] = L, I, C0, C1) when H0 < H1 ->
	get(T0, L, I, [H0|C0], C1);
get([H0|_] = L, [H1|T1], I, C0, C1) when H0 > H1 ->
	get(L, T1, I, C0, [H1|C1]);
get([], [_|_] = L, I, C0, C1) ->
	{lists:reverse(I), lists:reverse(C0), lists:reverse(get1(L, C1))};
get([_|_] = L, [], I, C0, C1) ->
	{lists:reverse(I), lists:reverse(get1(L, C0)), lists:reverse(C1)};
get([], [], I, C0, C1) ->
	{lists:reverse(I), lists:reverse(C0), lists:reverse(C1)}.

get1([H|T], L) ->
	get1(T, [H|L]);
get1([], L) ->
	L.

get_record([{Key, _, _, _} = R|T0], [{Key, _, _, _}|T1], I, C0, C1) ->
	get_record(T0, T1, [R|I], C0, C1);
get_record([{Key0, _, _, _} = R0|T0], [{Key1, _, _, _}|_] = L, I, C0, C1) when Key0 < Key1 ->
	get_record(T0, L, I, [R0|C0], C1);
get_record([{Key0, _, _, _}|_] = L, [{Key1, _, _, _} = R1|T1], I, C0, C1) when Key0 > Key1 ->
	get_record(L, T1, I, C0, [R1|C1]);
get_record([], [_|_] = L, I, C0, C1) ->
	{lists:reverse(I), lists:reverse(C0), lists:reverse(get_record1(L, C1))};
get_record([_|_] = L, [], I, C0, C1) ->
	{lists:reverse(I), lists:reverse(get_record1(L, C0)), lists:reverse(C1)};
get_record([], [], I, C0, C1) ->
	{lists:reverse(I), lists:reverse(C0), lists:reverse(C1)}.

get_record1([H|T], L) ->
	get_record1(T, [H|L]);
get_record1([], L) ->
	L.

get_split_num(Len, 16) ->
	F=trunc(math:log(Len) / math:log(16) * 10) / 10,
	if
		F > 1.6 ->
			get_split_num(Len, 16 bsl 1);
		true ->
			16
	end;
get_split_num(Len, R) ->
	F=trunc(math:log(Len) / math:log(R) * 10) / 10,
	if
		F >= 1.0, F =< 1.6 ->
			R;
		F > 1.6 ->
			get_split_num(Len, R bsl 1);
		true ->
			get_split_num(Len, R bsr 1)
	end.

compare_by_range(Start, Tail, Start, Tail) ->
	{{}, {{closed, Start}, {closed, Tail}}, {}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Tail0 < Start1; Tail1 < Start0 ->
	{{{closed, Start0}, {closed, Tail0}}, {}, {{closed, Start1}, {closed, Tail1}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Tail0 =:= Start1 ->
	{{{closed, Start0}, {open, Tail0}}, {Start1}, {{open, Start1}, {closed, Tail1}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Tail1 =:= Start0 ->
	{{{closed, Start1}, {open, Tail1}}, {Start0}, {{open, Start0}, {closed, Tail0}}};

compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 < Start1, Tail0 < Tail1  ->
	{{{closed, Start0}, {open, Start1}}, {{closed, Start1}, {closed, Tail0}}, {{open, Tail0}, {closed, Tail1}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 =:= Start1, Tail0 < Tail1  ->
	{{}, {{closed, Start0}, {closed, Tail0}}, {{open, Tail0}, {closed, Tail1}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 > Start1, Tail0 < Tail1  ->
	{{{closed, Start1}, {open, Start0}}, {{closed, Start0}, {closed, Tail0}}, {{open, Tail0}, {closed, Tail1}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 > Start1, Tail0 =:= Tail1  ->
	{{{closed, Start1}, {open, Start0}}, {{closed, Start0}, {closed, Tail0}}, {}};

compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 > Start1, Tail0 > Tail1 ->
	{{closed, Start1}, {open, Start0}, {{closed, Start0}, {closed, Tail1}}, {{open, Tail1}, {closed, Tail0}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 =:= Start1, Tail0 > Tail1  ->
	{{}, {{closed, Start1}, {closed, Tail1}}, {{open, Tail1}, {closed, Tail0}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 < Start1, Tail0 > Tail1  ->
	{{{closed, Start0}, {open, Start1}}, {{closed, Start1}, {closed, Tail1}}, {{open, Tail1}, {closed, Tail0}}};
compare_by_range(Start0, Tail0, Start1, Tail1) when Start0 < Start1, Tail0 =:= Tail1  ->
	{{{closed, Start0}, {open, Start1}}, {{closed, Start1}, {closed, Tail1}}, {}}.

keys(Iterator, L) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, _, _}, NewIterator} ->
			keys(NewIterator, [Key|L]);
		over ->
			L
	end.

keys_part1(_, _, 0, L) ->
	L;
keys_part1(Iterator, Key1, Len, L) ->
	case zm_db_table:iterate_next(Iterator) of
		{ok, {Key, _, _}, NewIterator} when Key1 =< Key ->
			keys_part1(NewIterator, Key1, Len - 1, [Key|L]);
		{ok, _, _} ->
			L;
		over ->
			L
	end.
		

	

