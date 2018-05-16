%%@doc 数据存储的工具模块
%%@end


-module(zm_storage_util).

-description("storage util").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([read/4, dyn_read/6, dyn_write/5, select/10, select/8, select_index/10]).

%%%=======================DEFINE=======================
-define(KEY_LIMIT, 50000).
-define(ATTR_LIMIT, 40000).
-define(VALUE_LIMIT, 10000).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  读函数，Time为0表示没有该项
%% @spec  read(Value, Vsn, Time, From) -> return()
%% where
%%  return() =  any() | {error, {Error, Reason, ST}}
%%@end
%% -----------------------------------------------------------------
read(Value, Vsn, Time, From) ->
	try
		z_lib:reply(From, {ok, Value, Vsn, Time})
	catch
		_:Reason ->
			z_lib:reply(From, {error, {Reason, erlang:get_stacktrace()}})
	end.

%% -----------------------------------------------------------------
%%@doc  动态读函数，Time为0表示没有该项
%% @spec  dyn_read(Key, Value, Vsn, Time, Fun, From) -> return()
%% where
%%  return() =  any() | {error, {Error, Reason, ST}}
%%@end
%% -----------------------------------------------------------------
dyn_read(Key, Value, Vsn, 0, Fun, From) ->
	dyn_read_(Key, Value, Vsn, 0, Fun, From);
dyn_read(Key, Value, Vsn, Time, Fun, From) ->
	try
		dyn_read_(Key, Value, Vsn, Time, Fun, From)
	catch
		_:Reason ->
			z_lib:reply(From, {error, {Reason, erlang:get_stacktrace()}})
	end.

dyn_read_(Key, Value, Vsn, Time, Fun, From) ->
	case dyn_read_(Key, Value, Vsn, Time, Fun) of
		ok ->
			ok;
		{reply, Value1} ->
			z_lib:reply(From, {ok, Value1, Vsn, Time});
		E ->
			z_lib:reply(From, {error, E})
	end.

dyn_read_(Key, Value, Vsn, Time, {M, F, A}) ->
	apply(M, F, [A, Key, Value, Vsn, Time]);
dyn_read_(Key, Value, Vsn, Time, {Fun, A}) ->
	Fun(A, Key, Value, Vsn, Time);
dyn_read_(Key, Value, Vsn, Time, Fun) ->
	Fun(Key, Value, Vsn, Time).

%% -----------------------------------------------------------------
%%@doc  动态写函数，Time为0表示没有该项
%% @spec  dyn_write(Key, Value, Vsn, Time, Fun) -> return()
%% where
%%  return() =  any() | {error, {Error, Reason, ST}}
%%@end
%% -----------------------------------------------------------------
%TODO
dyn_write(Key, Value, Vsn, Time, {{M, F, A}, _}) ->
	try
		apply(M, F, [A, Key, Value, Vsn, Time])
	catch
		Error:Reason ->
			{error, {Error, Reason, erlang:get_stacktrace()}}
	end;
dyn_write(Key, Value, Vsn, Time, {Fun, _}) ->
	try
		Fun(Key, Value, Vsn, Time)
	catch
		Error:Reason ->
			{error, {Error, Reason, erlang:get_stacktrace()}}
	end;
dyn_write(_Key, _Value, _Vsn, _Time, _L) ->
	ok.

%% -----------------------------------------------------------------
%%@doc  选择方法
%% @spec  select(Ets, Table, Range, Filter, FilterType, ResultType, NL, Limit, Node, Get) -> return()
%% where
%%  return() =  {ok, R} | {break, R} | {limit, R} | {error, R}
%%@end
%% -----------------------------------------------------------------
select(Ets, Table, Range, Filter, FilterType, ResultType, NL, Limit, Node, Get) ->
	{F, A} = get_select_fun(get_filter_fun(Filter), FilterType, ResultType, NL, Limit, Get),
	{_, Duplication, _} = zm_db:get(Table),
	{_, _, NT, _, _} = zm_db:nodes(),
	case Range of
		{open, Start, open, End} when Start < End ->
			select_(Ets, next, ets:next(Ets, Start), fun select_range1/2,
				{Duplication, NT, NL, Node, '<', End}, F, A);
		{open, Start, _Closed, End} when Start < End ->
			select_(Ets, next, ets:next(Ets, Start), fun select_range2/2,
				{Duplication, NT, NL, Node, '=<', End}, F, A);
		{_Closed, Start, open, End} when Start < End ->
			select_(Ets, next, Start, fun select_range1/2,
				{Duplication, NT, NL, Node, '<', End}, F, A);
		{_Closed, Start, _Closed, End} when Start < End ->
			select_(Ets, next, Start, fun select_range2/2,
				{Duplication, NT, NL, Node, '=<', End}, F, A);
		{open, Start, open, End} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_range3/2,
				{Duplication, NT, NL, Node, '>', End}, F, A);
		{open, Start, _Closed, End} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_range4/2,
				{Duplication, NT, NL, Node, '>=', End}, F, A);
		{_Closed, Start, open, End} ->
			select_(Ets, prev, Start, fun select_range3/2,
				{Duplication, NT, NL, Node, '>', End}, F, A);
		{_Closed, Start, _Closed, End} ->
			select_(Ets, prev, Start, fun select_range4/2,
				{Duplication, NT, NL, Node, '>=', End}, F, A);
		{ascending, open, Start} ->
			select_(Ets, next, ets:next(Ets, Start), fun select_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		{ascending, _Closed, Start} ->
			select_(Ets, next, Start, fun select_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		{descending, open, Start} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		{descending, _Closed, Start} ->
			select_(Ets, prev, Start, fun select_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		ascending ->
			select_(Ets, next, ets:first(Ets), fun select_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		descending ->
			select_(Ets, prev, ets:last(Ets), fun select_direction/2,
				{Duplication, NT, NL, Node}, F, A)
	end.

%% -----------------------------------------------------------------
%%@doc  本地选择方法
%% @spec select(Ets, _Table, Range, Filter, FilterType, ResultType, Limit, Get) -> return()
%% where
%%  return() =  {ok, R} | {break, R} | {limit, R} | {error, R}
%%@end
%% -----------------------------------------------------------------
select(Ets, _Table, Range, Filter, FilterType, ResultType, Limit, Get) ->
	{F, A} = get_select_fun(get_filter_fun(Filter), FilterType, ResultType, Limit, Get),
	case Range of
		{open, Start, open, End} when Start < End ->
			select_(Ets, next, ets:next(Ets, Start), fun select_local_range1/2,
				{'<', End}, F, A);
		{open, Start, _Closed, End} when Start < End ->
			select_(Ets, next, ets:next(Ets, Start), fun select_local_range2/2,
				{'=<', End}, F, A);
		{_Closed, Start, open, End} when Start < End ->
			select_(Ets, next, Start, fun select_local_range1/2,
				{'<', End}, F, A);
		{_Closed, Start, _Closed, End} when Start < End ->
			select_(Ets, next, Start, fun select_local_range2/2,
				{'=<', End}, F, A);
		{open, Start, open, End} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_local_range3/2,
				{'>', End}, F, A);
		{open, Start, _Closed, End} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_local_range4/2,
				{'>=', End}, F, A);
		{_Closed, Start, open, End} ->
			select_(Ets, prev, Start, fun select_local_range3/2,
				{'>', End}, F, A);
		{_Closed, Start, _Closed, End} ->
			select_(Ets, prev, Start, fun select_local_range4/2,
				{'>=', End}, F, A);
		{ascending, open, Start} ->
			select_(Ets, next, ets:next(Ets, Start), fun select_local_direction/2,
				none, F, A);
		{ascending, _Closed, Start} ->
			select_(Ets, next, Start, fun select_local_direction/2,
				none, F, A);
		{descending, open, Start} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_local_direction/2,
				none, F, A);
		{descending, _Closed, Start} ->
			select_(Ets, prev, Start, fun select_local_direction/2,
				none, F, A);
		ascending ->
			select_(Ets, next, ets:first(Ets), fun select_local_direction/2,
				none, F, A);
		descending ->
			select_(Ets, prev, ets:last(Ets), fun select_local_direction/2,
				none, F, A)
	end.

%% -----------------------------------------------------------------
%%@doc  选择方法
%% @spec  select_index(Ets, Table, Range, Filter, FilterType, ResultType, NL, Limit, Node, Get) -> return()
%% where
%%  return() =  {ok, R} | {break, R} | {limit, R} | {error, R}
%%@end
%% -----------------------------------------------------------------
select_index(Ets, Table, Range, Filter, FilterType, ResultType, NL, Limit, Node, Get) ->
	{F, A} = get_select_fun(get_filter_fun(Filter), FilterType, ResultType, NL, Limit, Get),
	{_, Duplication, _} = zm_db:get(Table),
	{_, _, NT, _, _} = zm_db:nodes(),
	case Range of
		{open, Start, open, End} when Start < End ->
			select_(Ets, next, ets:next(Ets, Start), fun select_index_range1/2,
				{Duplication, NT, NL, Node, '<', End}, F, A);
		{open, Start, _Closed, End} when Start < End ->
			select_(Ets, next, ets:next(Ets, Start), fun select_index_range2/2,
				{Duplication, NT, NL, Node, '=<', End}, F, A);
		{_Closed, Start, open, End} when Start < End ->
			select_(Ets, next, Start, fun select_index_range1/2,
				{Duplication, NT, NL, Node, '<', End}, F, A);
		{_Closed, Start, _Closed, End} when Start < End ->
			select_(Ets, next, Start, fun select_index_range2/2,
				{Duplication, NT, NL, Node, '=<', End}, F, A);
		{open, Start, open, End} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_index_range3/2,
				{Duplication, NT, NL, Node, '>', End}, F, A);
		{open, Start, _Closed, End} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_index_range4/2,
				{Duplication, NT, NL, Node, '>=', End}, F, A);
		{_Closed, Start, open, End} ->
			select_(Ets, prev, Start, fun select_index_range3/2,
				{Duplication, NT, NL, Node, '>', End}, F, A);
		{_Closed, Start, _Closed, End} ->
			select_(Ets, prev, Start, fun select_index_range4/2,
				{Duplication, NT, NL, Node, '>=', End}, F, A);
		{ascending, open, Start} ->
			select_(Ets, next, ets:next(Ets, Start), fun select_index_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		{ascending, _Closed, Start} ->
			select_(Ets, next, Start, fun select_index_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		{descending, open, Start} ->
			select_(Ets, prev, ets:prev(Ets, Start), fun select_index_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		{descending, _Closed, Start} ->
			select_(Ets, prev, Start, fun select_index_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		ascending ->
			select_(Ets, next, ets:first(Ets), fun select_index_direction/2,
				{Duplication, NT, NL, Node}, F, A);
		descending ->
			select_(Ets, prev, ets:last(Ets), fun select_index_direction/2,
				{Duplication, NT, NL, Node}, F, A)
	end.

%%%===================LOCAL FUNCTIONS==================
%获得过滤函数
get_filter_fun(none) ->
	{fun(_, _Args) -> true end, none};
get_filter_fun({M, F, A}) ->
	{fun M:F/2, A};
get_filter_fun({F, _A} = FA) when is_function(F) ->
	FA.

%获得选择函数
get_select_fun({Filter, A}, FilterType, count, _NL, _Limit, Get) ->
	{fun select_count/2, {0, Filter, A, Get, FilterType}};
get_select_fun({Filter, A}, FilterType, key, NL, 0, Get) ->
	{fun select_key/2, {[], Filter, A, Get, FilterType, ?KEY_LIMIT div length(NL)}};
get_select_fun({Filter, A}, FilterType, key, _NL, Limit, Get) ->
	{fun select_key/2, {[], Filter, A, Get, FilterType, Limit}};
get_select_fun({Filter, A}, all, all, NL, 0, Get) ->
	{fun select_all_all/2, {[], Filter, A, Get, ?VALUE_LIMIT div length(NL)}};
get_select_fun({Filter, A}, all, all, _NL, Limit, Get) ->
	{fun select_all_all/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, value, all, NL, 0, Get) ->
	{fun select_all_value/2, {[], Filter, A, Get, ?VALUE_LIMIT div length(NL)}};
get_select_fun({Filter, A}, value, all, _NL, Limit, Get) ->
	{fun select_all_value/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, _Attr, all, NL, 0, Get) ->
	{fun select_all_attr/2, {[], Filter, A, Get, ?VALUE_LIMIT div length(NL)}};
get_select_fun({Filter, A}, _Attr, all, _NL, Limit, Get) ->
	{fun select_all_attr/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, all, value, NL, 0, Get) ->
	{fun select_value_all/2, {[], Filter, A, Get, ?VALUE_LIMIT div length(NL)}};
get_select_fun({Filter, A}, all, value, _NL, Limit, Get) ->
	{fun select_value_all/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, value, value, NL, 0, Get) ->
	{fun select_value_value/2, {[], Filter, A, Get, ?VALUE_LIMIT div length(NL)}};
get_select_fun({Filter, A}, value, value, _NL, Limit, Get) ->
	{fun select_value_value/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, _Attr, value, NL, 0, Get) ->
	{fun select_value_attr/2, {[], Filter, A, Get, ?VALUE_LIMIT div length(NL)}};
get_select_fun({Filter, A}, _Attr, value, _NL, Limit, Get) ->
	{fun select_value_attr/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, all, _Attr, NL, 0, Get) ->
	{fun select_attr_all/2, {[], Filter, A, Get, ?ATTR_LIMIT div length(NL)}};
get_select_fun({Filter, A}, all, _Attr, _NL, Limit, Get) ->
	{fun select_attr_all/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, value, _Attr, NL, 0, Get) ->
	{fun select_attr_value/2, {[], Filter, A, Get, ?ATTR_LIMIT div length(NL)}};
get_select_fun({Filter, A}, value, _Attr, _NL, Limit, Get) ->
	{fun select_attr_value/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, _Attr, _Attr, NL, 0, Get) ->
	{fun select_attr_attr/2, {[], Filter, A, Get, ?ATTR_LIMIT div length(NL)}};
get_select_fun({Filter, A}, _Attr, _Attr, _NL, Limit, Get) ->
	{fun select_attr_attr/2, {[], Filter, A, Get, Limit}}.

%获得本地选择函数
get_select_fun({Filter, A}, FilterType, count, _Limit, Get) ->
	{fun select_count/2, {0, Filter, A, Get, FilterType}};
get_select_fun({Filter, A}, FilterType, key, 0, Get) ->
	{fun select_key/2, {[], Filter, A, Get, FilterType, ?KEY_LIMIT}};
get_select_fun({Filter, A}, FilterType, key, Limit, Get) ->
	{fun select_key/2, {[], Filter, A, Get, FilterType, Limit}};
get_select_fun({Filter, A}, all, all, 0, Get) ->
	{fun select_all_all/2, {[], Filter, A, Get, ?VALUE_LIMIT}};
get_select_fun({Filter, A}, all, all, Limit, Get) ->
	{fun select_all_all/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, value, all, 0, Get) ->
	{fun select_all_value/2, {[], Filter, A, Get, ?VALUE_LIMIT}};
get_select_fun({Filter, A}, value, all, Limit, Get) ->
	{fun select_all_value/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, _Attr, all, 0, Get) ->
	{fun select_all_attr/2, {[], Filter, A, Get, ?VALUE_LIMIT}};
get_select_fun({Filter, A}, _Attr, all, Limit, Get) ->
	{fun select_all_attr/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, all, value, 0, Get) ->
	{fun select_value_all/2, {[], Filter, A, Get, ?VALUE_LIMIT}};
get_select_fun({Filter, A}, all, value, Limit, Get) ->
	{fun select_value_all/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, value, value, 0, Get) ->
	{fun select_value_value/2, {[], Filter, A, Get, ?VALUE_LIMIT}};
get_select_fun({Filter, A}, value, value, Limit, Get) ->
	{fun select_value_value/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, _Attr, value, 0, Get) ->
	{fun select_value_attr/2, {[], Filter, A, Get, ?VALUE_LIMIT}};
get_select_fun({Filter, A}, _Attr, value, Limit, Get) ->
	{fun select_value_attr/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, all, _Attr, 0, Get) ->
	{fun select_attr_all/2, {[], Filter, A, Get, ?ATTR_LIMIT}};
get_select_fun({Filter, A}, all, _Attr, Limit, Get) ->
	{fun select_attr_all/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, value, _Attr, 0, Get) ->
	{fun select_attr_value/2, {[], Filter, A, Get, ?ATTR_LIMIT}};
get_select_fun({Filter, A}, value, _Attr, Limit, Get) ->
	{fun select_attr_value/2, {[], Filter, A, Get, Limit}};
get_select_fun({Filter, A}, _Attr, _Attr, 0, Get) ->
	{fun select_attr_attr/2, {[], Filter, A, Get, ?ATTR_LIMIT}};
get_select_fun({Filter, A}, _Attr, _Attr, Limit, Get) ->
	{fun select_attr_attr/2, {[], Filter, A, Get, Limit}}.

%选择方向下的键，判断是否为本节点所有
select_direction({Duplication, NT, NL, Node}, Key) ->
	Node =:= zm_db:first_active_dht_node(Key, Duplication, NT, NL).

%选择范围下的键，判断是否为本节点所有
select_range1({Duplication, NT, NL, Node, '<', End}, Key) when Key < End ->
	Node =:= zm_db:first_active_dht_node(Key, Duplication, NT, NL);
select_range1({_Duplication, _NT, _NL, _Node, '<', _End}, _Key) ->
	break.
select_range2({Duplication, NT, NL, Node, '=<', End}, Key) when Key =< End ->
	Node =:= zm_db:first_active_dht_node(Key, Duplication, NT, NL);
select_range2({_Duplication, _NT, _NL, _Node, '=<', _End}, _Key) ->
	break.
select_range3({Duplication, NT, NL, Node, '>', End}, Key) when Key > End ->
	Node =:= zm_db:first_active_dht_node(Key, Duplication, NT, NL);
select_range3({_Duplication, _NT, _NL, _Node, '>', _End}, _Key) ->
	break.
select_range4({Duplication, NT, NL, Node, '>=', End}, Key) when Key >= End ->
	Node =:= zm_db:first_active_dht_node(Key, Duplication, NT, NL);
select_range4({_Duplication, _NT, _NL, _Node, '>=', _End}, _Key) ->
	break.

%本地选择方向下的键
select_local_direction(_, _) ->
	true.

%本地选择范围下的键
select_local_range1({'<', End}, Key) when Key < End ->
	true;
select_local_range1({'<', _End}, _Key) ->
	break.
select_local_range2({'=<', End}, Key) when Key =< End ->
	true;
select_local_range2({'=<', _End}, _Key) ->
	break.
select_local_range3({'>', End}, Key) when Key > End ->
	true;
select_local_range3({'>', _End}, _Key) ->
	break.
select_local_range4({'>=', End}, Key) when Key >= End ->
	true;
select_local_range4({'>=', _End}, _Key) ->
	break.

%选择递归
select_(_Ets, _Action, '$end_of_table', _SF, _SA, _Fun, Args) ->
	{ok, element(1, Args)};
select_(Ets, Action, Key, SF, SA, Fun, Args) ->
	case SF(SA, Key) of
		true ->
			case Fun(Args, Key) of
				{next, A} ->
					select_(Ets, Action, ets:Action(Ets, Key), SF, SA, Fun, A);
				R ->
					R
			end;
		false ->
			select_(Ets, Action, ets:Action(Ets, Key), SF, SA, Fun, Args);
		break ->
			{ok, element(1, Args)}
	end.

%选择获得数量
select_count({C, Filter, A, Get, Type} = Args, Key) ->
	case Get(Key, Type) of
		none ->
			{next, Args};
		R ->
			try Filter(A, R) of
				true -> {next, {C + 1, Filter, A, Get, Type}};
				false -> {next, Args};
				{true, A1} -> {next, {C + 1, Filter, A1, Get, Type}};
				{false, A1} -> {next, {C, Filter, A1, Get, Type}};
				break_true -> {break, C + 1};
				break_false -> {break, C}
			catch
				Error:Reason ->
					{error, {{Error, Reason}, {args, R}, erlang:get_stacktrace()}}
			end
	end.

%选择获得键
select_key({L, Filter, A, Get, Type, Limit} = Args, Key) ->
	case Get(Key, Type) of
		none ->
			{next, Args};
		R ->
			try Filter(A, R) of
				true when Limit > 0 ->
					{next, {[Key | L], Filter, A, Get, Type, Limit - 1}};
				true ->
					{limit, [Key | L]};
				false ->
					{next, Args};
				{true, A1} when Limit > 0 ->
					{next, {[Key | L], Filter, A1, Get, Type, Limit - 1}};
				{true, _A1} ->
					{limit, [Key | L]};
				{false, A1} ->
					{next, {L, Filter, A1, Get, Type, Limit}};
				break_true ->
					{break, [Key | L]};
				break_false ->
					{break, L}
			catch
				Error:Reason ->
					{error, {{Error, Reason}, {args, R}, erlang:get_stacktrace()}}
			end
	end.

%选择获得属性，参数为属性
select_attr_attr({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, attr) of
		none ->
			{next, Args};
		R ->
			select_fun(Args, R, R)
	end.

%选择获得属性，参数为值
select_attr_value({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		{_, Value, Vsn, Time} ->
			select_fun(Args, {Key, Value}, {Key, Vsn, Time})
	end.

%选择获得属性，参数为全部
select_attr_all({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		{_, _Value, Vsn, Time} = R ->
			select_fun(Args, R, {Key, Vsn, Time})
	end.

%选择获得值，参数为属性
select_value_attr({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, attr) of
		none ->
			{next, Args};
		R ->
			select_fun_next(Args, R, fun select_value_attr_next/3)
	end.
%选择获得值，参数为属性，再次判断
select_value_attr_next({L, Filter, A, Get, Limit} = Args, {Key, Vsn, Time}, Type) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		{_, Value, Vsn, Time} ->
			{Type, {[{Key, Value} | L], Filter, A, Get, Limit - 1}};
		{_, Value, Vsn1, Time1} ->
			select_fun(Args, {Key, Vsn1, Time1}, {Key, Value})
	end.

%选择获得值，参数为值
select_value_value({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, value) of
		none ->
			{next, Args};
		R ->
			select_fun(Args, R, R)
	end.

%选择获得值，参数为全部
select_value_all({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		{_, Value, _Vsn, _Time} = R ->
			select_fun(Args, R, {Key, Value})
	end.

%选择获得全部，参数为属性
select_all_attr({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, attr) of
		none ->
			{next, Args};
		R ->
			select_fun_next(Args, R, fun select_all_attr_next/3)
	end.
%选择获得全部，参数为属性，再次判断
select_all_attr_next({L, Filter, A, Get, Limit} = Args, {Key, Vsn, Time}, Type) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		{_, _Value, Vsn, Time} = R ->
			{Type, {[R | L], Filter, A, Get, Limit - 1}};
		{_, _Value, Vsn1, Time1} = R ->
			select_fun(Args, {Key, Vsn1, Time1}, R)
	end.

%选择获得全部，参数为值
select_all_value({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		{_, Value, _Vsn, _Time} = R ->
			select_fun(Args, {Key, Value}, R)
	end.

%选择获得全部，参数为全部
select_all_all({_L, _Filter, _A, Get, _Limit} = Args, Key) ->
	case Get(Key, all) of
		none ->
			{next, Args};
		R ->
			select_fun(Args, R, R)
	end.

%选择函数，返回列表参数
select_fun({L, Filter, A, Get, Limit} = Args, Data, R) ->
	try Filter(A, Data) of
		true when Limit > 0 ->
			{next, {[R | L], Filter, A, Get, Limit - 1}};
		true ->
			{limit, [R | L]};
		false ->
			{next, Args};
		{true, A1} when Limit > 0 ->
			{next, {[R | L], Filter, A1, Get, Limit - 1}};
		{true, _A1} ->
			{limit, [R | L]};
		{false, A1} ->
			{next, {L, Filter, A1, Get, Limit}};
		break_true ->
			{break, [R | L]};
		break_false ->
			{break, L}
	catch
		Error:Reason ->
			{error, {{Error, Reason}, {args, Data}, erlang:get_stacktrace()}}
	end.

%选择函数，返回列表参数或调用继续函数
select_fun_next({L, Filter, A, Get, Limit} = Args, Data, Fun) ->
	try Filter(A, Data) of
		true -> Fun(Args, Data, next);
		false -> {next, Args};
		{true, A1} -> Fun({L, Filter, A1, Get, Limit}, Data, next);
		{false, A1} -> {next, {L, Filter, A1, Get, Limit}};
		%break_true -> Fun(Args, Data, break); bug代码
		break_true -> 
			case Fun(Args, Data, break) of
				{Type, {L1, _, _, _, _}} ->
					{Type, L1};
				L1 ->
					L1
			end;
		break_false -> {break, L}
	catch
		Error:Reason ->
			{error, {{Error, Reason}, {args, Data}, erlang:get_stacktrace()}}
	end.


%选择方向下的键，判断是否为本节点所有
select_index_direction({Duplication, NT, NL, Node}, {_, K}) ->
	Node =:= zm_db:first_active_dht_node(K, Duplication, NT, NL).

%选择范围下的键，判断是否为本节点所有
select_index_range1({Duplication, NT, NL, Node, '<', End}, {_, K} = Key) when Key < End ->
	Node =:= zm_db:first_active_dht_node(K, Duplication, NT, NL);
select_index_range1({_Duplication, _NT, _NL, _Node, '<', _End}, _Key) ->
	break.
select_index_range2({Duplication, NT, NL, Node, '=<', End}, {_, K} = Key) when Key =< End ->
	Node =:= zm_db:first_active_dht_node(K, Duplication, NT, NL);
select_index_range2({_Duplication, _NT, _NL, _Node, '=<', _End}, _Key) ->
	break.
select_index_range3({Duplication, NT, NL, Node, '>', End}, {_, K} = Key) when Key > End ->
	Node =:= zm_db:first_active_dht_node(K, Duplication, NT, NL);
select_index_range3({_Duplication, _NT, _NL, _Node, '>', _End}, _Key) ->
	break.
select_index_range4({Duplication, NT, NL, Node, '>=', End}, {_, K} = Key) when Key >= End ->
	Node =:= zm_db:first_active_dht_node(K, Duplication, NT, NL);
select_index_range4({_Duplication, _NT, _NL, _Node, '>=', _End}, _Key) ->
	break.

