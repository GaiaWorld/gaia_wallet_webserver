%%
%%@doc	用于bts的缓存模块
%%

-module(bts_cache).

%% ====================================================================
%% Include files
%% ====================================================================
-define(MIN_CAPACITY, 16#1).
-define(MAX_CAPACITY, 16#fff).
-define(EMPTY_TYPE, undefined).
-define(ORDER_TYPE, order).
-define(MAP_TYPE, map).
-define(CACHE_EOF, '$end_of_table').
-define(INIT_REF, 0).
-define(UN_WRITEBACK_TYPE, 1).
-define(WRITEBACKED_TYPE, 2).
-define(COPYED_TYPE, 8).
-define(OUT_OF_CACHE, cache_busy).
-define(FIX_CACHE_TIME, 16#ffffffff).

%% ====================================================================
%% API functions
%% ====================================================================
-export([new/4, new/3, is_active/1, size/1, size/2, memory_size/1]).
-export([get/2, get/3, copy/2, uncopy/2, insert/3, inserts/2, load/3, load/2, modify/3, modify/2, remove/2, delete/2, clear/1]).
-export([keys/1, keys/2, keys/6, keys/7, iterator/1, iterate_next/1, to_list/1, collate/2, collate/1, destroy/1]).

%%
%% 构造一个指定的缓存
%% 
new(Type, Capacity, Sizeout, Timeout) when is_integer(Capacity), is_integer(Sizeout), is_integer(Timeout), 
											 Capacity >= ?MIN_CAPACITY, Capacity =< ?MAX_CAPACITY, Sizeout > 0, Sizeout =< Capacity, Timeout > 0 ->
	case Type of
		?ORDER_TYPE ->
			new_handle(ets:new(?MODULE, [ordered_set, public]), Capacity, Sizeout, Timeout);
		?MAP_TYPE ->
			new_handle(ets:new(?MODULE, [public, {read_concurrency, true}]), Capacity, Sizeout, Timeout);
		?EMPTY_TYPE ->
			?EMPTY_TYPE;
		_ ->
			{error, invalid_access}
	end;
new(_, _, _, _) ->
	{error, invalid_args}.

%%
%% 构造一个指定的有序缓存
%% 
new(Capacity, Sizeout, Timeout) ->
	new(?ORDER_TYPE, Capacity, Sizeout, Timeout).

%%
%% 缓存是否活动
%%
is_active({Ets, Capacity, _, _, _}) ->
	case ets:info(Ets, memory) of
		undefined ->
			false;
		Size when Capacity > Size ->
			true;
	 	_ ->
			false
	end.

%%
%% 获取当前缓存的记录数量
%%
size({Ets, _, _, _, _}) ->
	ets:info(Ets, size).

%%
%% 获取当前缓存中指定类型的记录数量
%%
size({Ets, _, _, _, _}, Type) ->
	ets:select_count(Ets, [{{'$1', '_', '$3', '_'}, [{'=:=', '$3', Type}], [true]}]).

%%
%% 获取当前缓存的内存占用
%%
memory_size({Ets, _, _, _, _}) ->
	ets:info(Ets, memory).

%%
%% 获取指定主键的缓存数据
%%
get({Ets, _, _, _, _}, Key) ->
	case ets:lookup(Ets, Key) of
		[{_, Value, Type, _}] ->
			ets:update_element(Ets, Key, {4, z_lib:now_millisecond()}),
			{ok, Type, Value};
		[] ->
			false
	end.

%%
%% 获取指定主键的部分缓存数据
%%
get({Ets, _, _, _, _}, Key, Index) when is_integer(Index) ->
	case ets:lookup(Ets, Key) of
		[{_, Value, Type, _}] ->
			ets:update_element(Ets, Key, {4, z_lib:now_millisecond()}),
			{ok, Type, element(Index, Value)};
		[] ->
			false
	end.

%%
%% 复制指定主键的缓存数据
%%
copy({Ets, _, _, _, _}, Key) ->
	case ets:lookup(Ets, Key) of
		[{_, Value, Type, _}] ->
			ets:update_element(Ets, Key, {3, Type bxor ?COPYED_TYPE}),
			{ok, Type, Value};
		[] ->
			false
	end.

%%
%% 解除指定主键的复制标记
%%
uncopy({Ets, _, _, _, _}, Key) ->
	ets:update_element(Ets, Key, {3, ets:lookup_element(Ets, Key, 3) bxor ?COPYED_TYPE}).

%%
%% 插入指定主键的未回写数据
%%
insert({Ets, Capacity, _, _, _}, Key, Value) ->
	case ets:info(Ets, memory) >= Capacity of
		false ->
			ets:insert_new(Ets, {Key, Value, ?UN_WRITEBACK_TYPE, z_lib:now_millisecond()});
		true ->
			{error, ?OUT_OF_CACHE}
	end.

%%
%% 插入多条未回写数据
%%
inserts({Ets, Capacity, _, _, _}, KVS) ->
	case ets:info(Ets, memory) >= Capacity of
		false ->
			Time = z_lib:now_millisecond(),
			ets:insert_new(Ets, [{Key, Value, ?UN_WRITEBACK_TYPE, Time} || {Key, Value} <- KVS]);
		true ->
			{error, ?OUT_OF_CACHE}
	end.

%%
%% 加载指定主键的已回写数据
%%
load({Ets, Capacity, _, _, _}, Key, Value) ->
	case ets:info(Ets, memory) >= Capacity of
		false ->
			ets:insert_new(Ets, {Key, Value, ?WRITEBACKED_TYPE, z_lib:now_millisecond()});
		true ->
			{error, ?OUT_OF_CACHE}
	end.

%%
%% 加载多条已回写数据
%%
load({Ets, Capacity, _, _, _}, KVS) ->
	case ets:info(Ets, memory) >= Capacity of
		false ->
			Time = z_lib:now_millisecond(),
			ets:insert_new(Ets, [{Key, Value, ?WRITEBACKED_TYPE, Time} || {Key, Value} <- KVS]);
		true ->
			{error, ?OUT_OF_CACHE}
	end.

%%
%% 修改指定主键的缓存数据
%%
modify({Ets, Capacity, _, _, _}, Key, Value) ->
	case ets:info(Ets, memory) >= Capacity of
		false ->
			ets:update_element(Ets, Key, [{2, Value}, {4, z_lib:now_millisecond()}]);
		true ->
			{error, ?OUT_OF_CACHE}
	end.

%%
%% 修改指定主键的缓存数据
%%
modify({Ets, Capacity, _, _, _}, Key) ->
	case ets:info(Ets, memory) >= Capacity of
		false ->
			ets:update_element(Ets, Key, [{3, ?WRITEBACKED_TYPE}]);
		true ->
			{error, ?OUT_OF_CACHE}
	end.

%%
%% 移除指定主键的缓存数据
%%
remove({Ets, _, _, _, _}, Key) ->
	case ets:lookup(Ets, Key) of
		[{_, Value, _, _}] ->
			ets:delete(Ets, Key),
			Value;
		[] ->
			undefined
	end.

%%
%% 删除指定主键的缓存数据
%%
delete({Ets, _, _, _, _}, Key) ->
	ets:delete(Ets, Key).

%%
%% 清空缓存
%%
clear({Ets, _, _, _, _}) ->
	ets:delete_all_objects(Ets),
	ets:delete_all_objects(Ets).

%%
%% 获取缓存的键列表
%%
keys({Ets, _, _, _, _}) ->
	ets:select(Ets, [{{'$1', '_', '_', '_'}, [], ['$1']}]).

%%
%% 获取缓存中指定类型的键列表
%%
keys({Ets, _, _, _, _}, Type) ->
	ets:select(Ets, [{{'$1', '_', '$3', '_'}, [{'=:=', '$3', Type}], ['$1']}]).

%%
%% 获取缓存中指定范围的键列表
%%
keys({Ets, _, _, _, _}, Index, StartType, StartKey, TailType, TailKey) when is_integer(Index), Index >= 0 ->
	SI=case StartType of
		   open ->
			   '>';
		   close ->
			   '>='
	end,
	TI=case TailType of
		   open ->
			   '<';
		   close ->
			   '=<'
	end,
	case Index of
		0 ->
			ets:select(Ets, [{{'$1', '_', '_', '_'}, [{SI, '$1', StartKey}, {TI, '$1', TailKey}], ['$1']}]);
		_ ->
			ets:select(Ets, [{{'$1', '_', '_', '_'}, [{SI, {element, Index, '$1'}, StartKey}, {TI, {element, Index, '$1'}, TailKey}], ['$1']}])
	end.

%%
%% 获取缓存中满足条件的键列表
%%
keys({Ets, _, _, _, _}, Type, Index, StartType, StartVal, TailType, TailVal) when is_integer(Index), Index >= 0 ->
	SI=case StartType of
		   open ->
			   '>';
		   close ->
			   '>='
	end,
	TI=case TailType of
		   open ->
			   '<';
		   close ->
			   '=<'
	end,
	case Index of
		0 ->
			ets:select(Ets, [{{'$1', '$2', '$3', '_'}, [{'=:=', '$3', Type}, {SI, '$2', StartVal}, {TI, '$2', TailVal}], ['$1']}]);
		_ ->
			ets:select(Ets, [{{'$1', '$2', '$3', '_'}, [{'=:=', '$3', Type}, {SI, {element, Index, '$2'}, StartVal}, {TI, {element, Index, '$2'}, TailVal}], ['$1']}])
	end.

%%
%% 获取缓存的迭代器
%%
iterator({Ets, _, _, _, _} = Cache) ->
	case ets:select(Ets, [{{'$1', '_', '$3', '$4'}, [], [{{'$1', '$3', '$4'}}]}], round(math:sqrt(?MODULE:size(Cache)))) of
		{L, C} ->
			{lists:reverse(L), C};
		?CACHE_EOF ->
			{[], ?CACHE_EOF}
	end.

%%
%% 迭代缓存
%% 
iterate_next({[H|T], C}) ->
	{H, {T, C}};
iterate_next({[], ?CACHE_EOF}) ->
	over;
iterate_next({[], C}) ->
	case ets:select_reverse(C) of
		{L, NC} ->
			[H|T]=lists:reverse(L),
			{H, {T, NC}};
		?CACHE_EOF ->
			over
	end.

%%
%% 将缓存转换为列表
%%
to_list({Ets, _, _, _, _}) ->
	ets:tab2list(Ets).

%%
%% 整理缓存
%%
collate({Ets, _, Sizeout, Timeout, _}, Time) when Timeout < ?FIX_CACHE_TIME ->
	collate_time(Ets, Time - Timeout),
	collate_size(Ets, Sizeout);
collate({Ets, _, Sizeout, _, _}, _) ->
	collate_size(Ets, Sizeout).

%%
%% 整理缓存
%%
collate(Cache) ->
	collate(Cache, z_lib:now_millisecond()).

%%
%% 销毁缓存
%%
destroy({Ets, _, _, _, _}) ->
	ets:delete(Ets).

%% ====================================================================
%% Internal functions
%% ====================================================================

new_handle(EtsPid, Capacity, Sizeout, Timeout) ->
	WordSize=erlang:system_info(wordsize),
	{EtsPid, (Capacity bsl 20) div WordSize, (Sizeout bsl 10) div WordSize, Timeout, self()}.

collate_time(Ets, Time) when Time > 0 ->
	ets:select_delete(Ets, [{{'_', '_', '$3', '$4'}, [{'=:=', '$3', ?WRITEBACKED_TYPE}, {'<', '$4', Time}], [true]}]),
	ok.

collate_size(Ets, Sizeout) ->
	case ets:info(Ets, memory) of
		Size when Size >= Sizeout ->
			ets:select_delete(Ets, [{{'_', '_', '$3', '_'}, [{'=:=', '$3', ?WRITEBACKED_TYPE}], [true]}]);
		_ ->
			ok
	end,
	ok.
			
