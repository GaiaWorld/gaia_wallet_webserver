%%@doc 数据的缓冲模块
%%```
%%% 如果Sizeout（K为单位）小于或等于0，表示不缓冲数据。
%%% 如果Sizeout为infinity，表示缓冲的数据没有大小的限制。
%%%如果内存超过Sizeout，则按时间段将老数据清除，每次将大小降为当前内存一半左右。
%%% Timeout（毫秒为单位）必须为正整数，表示数据缓存的超时期限，如果大于等于4294967295（49.8天），表示永不超期。
%%% 每次整理先将超时数据清除，然后清除老数据。
%%% 在创建时可以设置读取操作是否读取时间更新，默认为更新。
%%'''
%%@end


-module(zm_cache).

-description("key value cache").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([new/2, new/3, reset/3, read_time_update/1, memory_size/1, is_overflow/1]).
-export([get/1, get/2, get/3, lookup/3, set/3, set/4, sets/2, sets/3, delete/2, update/2, update/3]).
-export([collate/1, collate/2, clear/1, destroy/1]).

%%%=======================DEFINE=======================
-define(INFINITY, 4294967295).
-define(COUNTS, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  new cache
%% @spec  new(Sizeout::integer(), Timeout::integer()) -> return()
%% where
%%  return() =  Cache::cache()
%%@end
%% -----------------------------------------------------------------
new(Sizeout, _Timeout) when Sizeout =< 0 ->
	{0, 0, 0, true};
new(Sizeout, Timeout) when is_integer(Sizeout), Sizeout > 0 ->
	{ets:new(?MODULE, [public]), Sizeout * 1024 div erlang:system_info(wordsize), Timeout, true};
new(Sizeout, Timeout) ->
	{ets:new(?MODULE, [public]), Sizeout, Timeout, true}.

%% -----------------------------------------------------------------
%%@doc  new cache
%% @spec  new(Sizeout::integer(), Timeout::integer(), ReadTimeUpdate::boolean()) -> return()
%% where
%%  return() =  Cache::cache()
%%@end
%% -----------------------------------------------------------------
new(Sizeout, _Timeout, ReadTimeUpdate) when Sizeout =< 0 ->
	{0, 0, 0, ReadTimeUpdate};
new(Sizeout, Timeout, true) when is_integer(Sizeout), Sizeout > 0 ->
	{ets:new(?MODULE, [public]), Sizeout * 1024 div erlang:system_info(wordsize), Timeout, true};
new(Sizeout, Timeout, ReadTimeUpdate) ->
	{ets:new(?MODULE, []), Sizeout, Timeout, ReadTimeUpdate}.

%% -----------------------------------------------------------------
%%@doc  reset cache
%% @spec  reset(Cache::cache(), Sizeout::integer(), Timeout::integer()) -> return()
%% where
%%  return() =  Cache::cache()
%%@end
%% -----------------------------------------------------------------
reset({0, _, _, ReadTimeUpdate}, Sizeout, _Timeout) when Sizeout =< 0 ->
	{0, 0, 0, ReadTimeUpdate};
reset({Ets, _, _, ReadTimeUpdate}, Sizeout, _Timeout) when Sizeout =< 0 ->
	ets:delete(Ets),
	{0, 0, 0, ReadTimeUpdate};
reset({0, _, _, true}, Sizeout, Timeout) when is_integer(Sizeout), Sizeout > 0 ->
	{ets:new(?MODULE, [public]), Sizeout * 1024 div erlang:system_info(wordsize), Timeout, true};
reset({0, _, _, ReadTimeUpdate}, Sizeout, Timeout) ->
	{ets:new(?MODULE, []), Sizeout, Timeout, ReadTimeUpdate};
reset({Ets, _, _, ReadTimeUpdate}, Sizeout, Timeout) when is_integer(Sizeout), Sizeout > 0 ->
	{Ets, Sizeout * 1024 div erlang:system_info(wordsize), Timeout, ReadTimeUpdate};
reset({Ets, _, _, ReadTimeUpdate}, Sizeout, Timeout) ->
	{Ets, Sizeout, Timeout, ReadTimeUpdate}.

%% -----------------------------------------------------------------
%%@doc  cache is read time update
%% @spec  read_time_update(Cache::cache()) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
read_time_update({_, _, _, ReadTimeUpdate}) ->
	ReadTimeUpdate.

%% -----------------------------------------------------------------
%%@doc  get cache memory size (word size)
%% @spec  memory_size(Cache::cache()) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
memory_size({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}) when Ets =/= 0 ->
	ets:info(Ets, memory);
memory_size(_) ->
	0.

%% -----------------------------------------------------------------
%%@doc  is overflow cache
%% @spec  is_overflow(Cache::cache()) -> return()
%% where
%%  return() =  true | false
%%@end
%% -----------------------------------------------------------------
is_overflow({0, _Sizeout, _Timeout, _ReadTimeUpdate}) ->
	true;
is_overflow({Ets, Sizeout, _Timeout, _ReadTimeUpdate}) when is_integer(Sizeout) ->
	Sizeout < ets:info(Ets, memory);
is_overflow(_) ->
	false.

%% -----------------------------------------------------------------
%%@doc  get all from cache
%% @spec  get(Cache::cache()) -> return()
%% where
%%  return() =  none | list()
%%@end
%% -----------------------------------------------------------------
get({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}) when Ets =/= 0 ->
	ets:tab2list(Ets);
get(_) ->
	none.

%% -----------------------------------------------------------------
%%@doc  get from cache
%% @spec  get(Cache::cache(), Key) -> return()
%% where
%%  return() =  none | {ok, Value}
%%@end
%% -----------------------------------------------------------------
get({Ets, _Sizeout, _Timeout, true}, Key) when Ets =/= 0 ->
	case ets:lookup(Ets, Key) of
		[{_, Value, _}] ->
			ets:update_element(Ets, Key, {3, zm_time:now_millisecond()}),
			{ok, Value};
		[] ->
			none
	end;
get({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key) when Ets =/= 0 ->
	case ets:lookup(Ets, Key) of
		[{_, Value, _}] ->
			{ok, Value};
		[] ->
			none
	end;
get(_, _Key) ->
	none.

%% -----------------------------------------------------------------
%%@doc  get from cache
%% @spec  get(Cache::cache(), Key, Time::integer()) -> return()
%% where
%%  return() =  none | {ok, Value}
%%@end
%% -----------------------------------------------------------------
get({Ets, _Sizeout, _Timeout, true}, Key, Time) when Ets =/= 0 ->
	case ets:lookup(Ets, Key) of
		[{_, Value, _}] ->
			ets:update_element(Ets, Key, {3, Time}),
			{ok, Value};
		[] ->
			none
	end;
get({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key, _Time) when Ets =/= 0 ->
	case ets:lookup(Ets, Key) of
		[{_, Value, _}] ->
			{ok, Value};
		[] ->
			none
	end;
get(_, _Key, _Time) ->
	none.

%% -----------------------------------------------------------------
%%@doc  lookup from cache
%% @spec  lookup({Ets, Sizeout, Timeout, ReadTimeUpdate}, Key, OnlyTime::boolean()) -> return()
%% where
%%  return() =  none | Time | {Key, Value, Time}
%%@end
%% -----------------------------------------------------------------
lookup({0, _Sizeout, _Timeout, _ReadTimeUpdate}, _Key, _OnlyTime) ->
	none;
lookup({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key, true) ->
	try
		ets:lookup_element(Ets, Key, 3)
	catch
		_:_ ->
			none
	end;
lookup({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key, _) ->
	try ets:lookup(Ets, Key) of
		[R] ->
			R;
		[] ->
			none
	catch
		_:_ ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc  set to cache
%% @spec  set({Ets, Sizeout, Timeout, ReadTimeUpdate}, Key, Value) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
set({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key, Value) when Ets =/= 0 ->
	ets:insert(Ets, {Key, Value, zm_time:now_millisecond()});
set(_, _Key, _Value) ->
	true.

%% -----------------------------------------------------------------
%%@doc  set to cache
%% @spec  set({Ets, Sizeout, Timeout, ReadTimeUpdate}, Key, Value, Time) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
set({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key, Value, Time) when Ets =/= 0 ->
	ets:insert(Ets, {Key, Value, Time});
set(_, _Key, _Value, _Time) ->
	true.

%% -----------------------------------------------------------------
%%@doc  set list to cache
%% @spec  sets({Ets, Sizeout, Timeout, ReadTimeUpdate}, L) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
sets({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, L) when Ets =/= 0 ->
	Time = zm_time:now_millisecond(),
	ets:insert(Ets, [{K, V, Time} || {K, V} <- L]);
sets(_, _L) ->
	true.

%% -----------------------------------------------------------------
%%@doc  set list to cache
%% @spec  sets({Ets, Sizeout, Timeout, ReadTimeUpdate}, L, Time) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
sets({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, L, Time) when Ets =/= 0 ->
	ets:insert(Ets, [{K, V, Time} || {K, V} <- L]);
sets(_, _L, _Time) ->
	true.

%% -----------------------------------------------------------------
%%@doc  delete to cache
%% @spec  delete({Ets, Sizeout, Timeout, ReadTimeUpdate}, Key) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
delete({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key) when Ets =/= 0 ->
	ets:delete(Ets, Key);
delete(_, _Key) ->
	true.

%% -----------------------------------------------------------------
%%@doc  update key time
%% @spec  update({Ets, Sizeout, Timeout, ReadTimeUpdate}, Key) -> return()
%% where
%%  return() =  none | true | false
%%@end
%% -----------------------------------------------------------------
update({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key) when Ets =/= 0 ->
	try
		ets:update_element(Ets, Key, {3, zm_time:now_millisecond()})
	catch
		_:_ ->
			false
	end;
update(_, _Key) ->
	none.

%% -----------------------------------------------------------------
%%@doc  update key time
%% @spec  update({Ets, Sizeout, Timeout, ReadTimeUpdate}, Key, Time) -> return()
%% where
%%  return() =  none | true | false
%%@end
%% -----------------------------------------------------------------
update({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}, Key, Time) when Ets =/= 0 ->
	try
		ets:update_element(Ets, Key, {3, Time})
	catch
		_:_ ->
			false
	end;
update(_, _Key, _Time) ->
	none.

%% -----------------------------------------------------------------
%%@doc  collate cache
%% @spec  collate({Ets, Sizeout, Timeout, ReadTimeUpdate}) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
collate({Ets, _Sizeout, _Timeout, _ReadTimeUpdate} = Cache) when Ets =/= 0 ->
	collate(Cache, zm_time:now_millisecond());
collate(_) ->
	true.

%% -----------------------------------------------------------------
%%@doc  collate cache，超时的要删除，大于等于Time的要保留，
%% @spec  collate({Ets, Sizeout, Timeout, ReadTimeUpdate}, Time) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
collate({0, _Sizeout, _Timeout, _ReadTimeUpdate}, _Time) ->
	true;
collate({Ets, Sizeout, Timeout, _ReadTimeUpdate}, Time) when is_integer(Sizeout), Timeout < ?INFINITY ->
	collate_time(Ets, Time - Timeout),
	collate_size(Ets, Sizeout, Time);
collate({Ets, Sizeout, _Timeout, _ReadTimeUpdate}, Time) when is_integer(Sizeout) ->
	collate_size(Ets, Sizeout, Time);
collate({Ets, _Sizeout, Timeout, _ReadTimeUpdate}, Time) when Timeout < ?INFINITY ->
	collate_time(Ets, Time - Timeout);
collate(_, _Time) ->
	true.

%% -----------------------------------------------------------------
%%@doc  clear cache
%% @spec  clear({Ets, Sizeout, Timeout, ReadTimeUpdate}) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
clear({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}) when Ets =/= 0 ->
	ets:delete_all_objects(Ets);
clear(_) ->
	true.

%% -----------------------------------------------------------------
%%@doc  destroy cache
%% @spec  destroy({Ets, Sizeout, Timeout, ReadTimeUpdate}) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
destroy({Ets, _Sizeout, _Timeout, _ReadTimeUpdate}) when Ets =/= 0 ->
	ets:delete(Ets);
destroy(_) ->
	true.

%%%===================LOCAL FUNCTIONS==================
% 根据时间进行整理，删除超时的数据
collate_time(Ets, Time) when Time > 0 ->
	ets:select_delete(Ets, [{{'_','_','$1'},[{'<','$1',Time}],[true]}]),
	true.

% 根据大小进行整理，删除最老的数据
collate_size(Ets, Sizeout, Time) ->
	case z_lib:get_values(ets:info(Ets), [{memory, 0}, {size, 0}]) of
		[Memory, Size] when Sizeout < Memory, Size > 0 ->
			TSize = tuple_size(?COUNTS),
			% 遍历表，并统计时间
			F = fun
				(A, _K, T) when T < Time ->
					I = case z_math:log2(Time - T) of
						V when V >= TSize -> TSize;
						V -> V + 1
					end,
					{ok, setelement(I, A, element(I, A) + 1)};
				(A, _K, _) ->
					{ok, A}
			end,
			TimeRange = z_lib:ets_value_select(Ets, 3, F, ?COUNTS),
			collate_time(Ets, Time - time_out(TimeRange, Size div 2, TSize, 0));
		_ ->
			true
	end.

% 计算去除一半条目所对应的超时时间
time_out(TimeRange, Size, I, C) when I > 0, Size > C ->
	time_out(TimeRange, Size, I - 1, C + element(I, TimeRange));
time_out(TimeRange, Size, I, C) when I > 0 ->
	T1 = 1 bsl (I - 1),
	T1 + (T1 * (C - Size) div (element(I, TimeRange) + 1));
time_out(_TimeRange, _Size, _I, _C) ->
	0.
