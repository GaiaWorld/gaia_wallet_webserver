%%% 排行榜函数库


-module(zm_rank_list).

-description("zm_rank_list").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).
-vsn(1).

%%%=======================EXPORT=======================
-export([thumbnail/7, thumbnail/9, index_iterate/6, select_index_first/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 缩略图，扫描指定的表（表结构一般为{{_, ID} | ID, {Prefix, Score}}），取前N名（要注意去重），其后的名次生成一个{固定数量,分数范围}的列表
%% @spec thumbnail(Tab, Prefix, BeforeCount, Range, RangeIncreasing) -> return()
%% where
%%  return() = {ok, {Amount, BeforeNList, RangeList}} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
thumbnail(Src, Tab, Prefix, BeforeCount, Range, RangeIncreasing, KeyDropPrefix) ->
	case select_before(Tab, fun select_prefix/2, Prefix, <<>>, <<>>, BeforeCount, []) of
		[_ | _] = L ->
			% 数据库在修改数据时，索引有微小可能会出现键重复，因此要做去重操作
			L1 = lists:reverse(lists:sort(lists:ukeysort(2, [{V, K} || {{_, V}, K} <- L]))),
			{[Last | _] = Before, BC, R} = list_split(L1, BeforeCount, []),
			case select_range(Tab, fun select_prefix/2, Prefix, Range, RangeIncreasing, list_range(R, 1, [{0, Last}])) of
				[_ | _] = RL ->
					{N, RL1} = list_sum(RL, 0, []),
					case KeyDropPrefix of
						true ->
							{ok, {BeforeCount - BC + N, [{V, K} || {V, {_, K}} <- Before], RL1}};
						_ ->
							{ok, {BeforeCount - BC + N, Before, RL1}}
					end;
				E ->
					zm_log:warn(?MODULE, thumbnail, Src, select_range, [{tab, Tab}, {prefix, Prefix}, {range, Range}, {error, E}]),
					E
			end;
		[] ->
			{ok, {0, [], []}};
		E ->
			zm_log:warn(?MODULE, thumbnail, Src, select_before, [{tab, Tab}, {prefix, Prefix}, {before, BeforeCount}, {range, Range}, {error, E}]),
			E
	end.

%% -----------------------------------------------------------------
%%@doc 计算每个排行榜缩略图，并存放在指定的数据表里面
%% @spec rank(KeyList, RankTab, LimitCount, BeforeCount, Range, RangeIncreasing, KeyDropPrefix, SaveTab, L) -> return()
%% where
%%  return() = [{Key, R}, ...]
%%@end
%% -----------------------------------------------------------------
thumbnail(Src, [Prefix | T], RankTab, BeforeCount, Range, RangeIncreasing, KeyDropPrefix, SaveTab, L) ->
	R = thumbnail(Src, RankTab, Prefix, BeforeCount, Range, RangeIncreasing, KeyDropPrefix),
	thumbnail(Src, T, RankTab, BeforeCount, Range, RangeIncreasing, KeyDropPrefix, SaveTab, [{Prefix, R} | L]);
thumbnail(Src, _, _, _, _, _, _, SaveTab, L) ->
	case zm_db_client:writes([{SaveTab, Prefix, R, 0} || {Prefix, R} <- L], ?MODULE, 0) of
		ok -> ok;
		E ->
			zm_log:warn(?MODULE, thumbnail, Src, db_writes, [{tab, SaveTab}, {list, L}, {error, E}])
	end,
	L.

%% -----------------------------------------------------------------
%%@doc 索引迭代，可以用于计算排名
%% @spec index_iterate(Tab, Prefix, Range, KeyDropPrefix, Fun, A) -> return()
%% where
%%  return() = {ok, A1} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
index_iterate(Tab, Prefix, Range, KeyDropPrefix, Fun, A) ->
	index_iterate(Tab, fun select_prefix/2, Prefix, <<>>, <<>>, Range, KeyDropPrefix, Fun, A).

%% -----------------------------------------------------------------
%%@doc 获得大于或小于指定值的第一个元素
%% @spec select_index_first(Tab, Prefix, Value, Ascending) -> return()
%% where
%%  return() = {ok, list()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
select_index_first(Src, Tab, Prefix, Value, true) ->
	select_index_first(Src, Tab, Prefix, {ascending, open, {{Prefix, Value}, <<>>}});
select_index_first(Src, Tab, Prefix, Value, _) ->
	select_index_first(Src, Tab, Prefix, {descending, open, {{Prefix, Value}, -1}}).

% 获得大于或小于指定值的第一个元素
select_index_first(Src, Tab, Prefix, KeyRange) ->
	case zm_db_client:select_index(Tab, KeyRange, {fun select_prefix/2, {Prefix, 1}}, all, key, true) of
		{ok, []}  = R -> R;
		{T, R} when T =:= ok orelse T=:= limit ->
			{ok, [{V, K} || {{_, V}, K} <- R]};
		E ->
			zm_log:warn(?MODULE, select_index_first, Src, select_index_first, [{tab, Tab}, {prefix, Prefix}, {range, KeyRange}, {error, E}]),
			E
	end.

%%%===================LOCAL FUNCTIONS==================
% 选择函数
select_prefix({Prefix, C}, {{Prefix, _Value}, _Key, _Vsn, _Time}) when C > 1 ->
		{true, {Prefix, C - 1}};
select_prefix({Prefix, _}, {{Prefix, _}, _, _, _}) ->
		break_true;
select_prefix(_, _) ->
		break_false.


% 选前N名
select_before(Tab, F, Prefix, Value, Key, BeforeCount, L) ->
	case zm_db_client:select_index(Tab, {descending, open, {{Prefix, Value}, Key}}, {F, {Prefix, BeforeCount}}, all, key, true) of
		{ok, R} ->
			R ++ L;
		{limit, R} ->
			case length(R) of
				Len when Len < BeforeCount ->
					{{_, V}, Key1} = lists:last(R),
					select_before(Tab, F, Prefix, V, Key1, BeforeCount - Len, L ++ R);
				_ ->
					R ++ L
			end;
		E ->
			E
	end.

% 将列表分开
list_split([H | T], C, L) when C > 0 ->
	list_split(T, C - 1, [H | L]);
list_split(R, C, L) ->
	{L, C, R}.

% 计算列表的数量和范围值
list_range([_H | [_ | _] = T], C, L) ->
	list_range(T, C + 1, L);
list_range([H], C, L) ->
	[{C, H} | L];
list_range(_, _C, L) ->
	L.

% 选范围
select_range(Tab, F, Prefix, Range, RangeIncreasing, [{_, {Value, Key}} | _] = L) ->
	case zm_db_client:select_index(Tab, {descending, open, {{Prefix, Value}, Key}}, {F, {Prefix, Range}}, all, key, true) of
		{ok, []} ->
			L;
		{T, R} when T =:= ok orelse T=:= limit ->
			select_range(Tab, F, Prefix, Range + RangeIncreasing, RangeIncreasing, list_range([{V, K} || {{_, V}, K} <- R], 1, L));
		E ->
			E
	end.

% 计算列表的数量
list_sum([{C, {V, _K}} | T], N, L) ->
	list_sum(T, N + C, [{V, C} | L]);
list_sum(_, N, L) ->
	{N, L}.

% 索引迭代
index_iterate(Tab, F, Prefix, Value, Key, Range, KeyDropPrefix, Fun, A) ->
	case zm_db_client:select_index(Tab, {descending, open, {{Prefix, Value}, Key}}, {F, {Prefix, Range}}, all, key, true) of
		{ok, []} ->
			{ok, A};
		{T, R} when T =:= ok orelse T=:= limit ->
			A1 = case KeyDropPrefix of
				true ->
					handle1(R, Fun, A);
				_ ->
					handle2(R, Fun, A)
			end,
			{{_, V}, SuffixKey1} = lists:last(R),
			index_iterate(Tab, F, Prefix, V, SuffixKey1, Range, KeyDropPrefix, Fun, A1);
		E ->
			E
	end.

% 索引迭代，函数执行
handle1([{{_, V}, {_, K}} | T], Fun, A) ->
	A1 = Fun(A, K, V),
	handle1(T, Fun, A1);
handle1(_, _Fun, A) ->
	A.

% 索引迭代，函数执行
handle2([{{_, V}, K} | T], Fun, A) ->
	A1 = Fun(A, K, V),
	handle2(T, Fun, A1);
handle2(_, _Fun, A) ->
	A.
