%% @author Administrator
%% @doc 基于伙伴系统的有缓存的变长数据结构，对数据提供统一的查询，插入和删除接口


-module(bs_variable).

%% ====================================================================
%% Include files
%% ====================================================================
-include("bs_variable.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([new/6, new_data_reader/3, continue/4, get_cache/1, get_alloter/1, set_commited/2, collate/1]).
-export([lookup/2, save/3, unsave/2, insert/4, update/4, delete/2]).

%%
%%构建一个指定缓存大小的变长数据结构
%%
new({Mod, Fun, Args}, {M, Type}, Capacity, Sizeout, FreeBlocks, Timeout) ->
	Alloter=bs_alloter:new(?BS_MAX_STORAGE_SIZE, 0, FreeBlocks),
	Reader=new_reader(Mod, Fun, Args),
	case M:new(Type, Capacity, Sizeout, Timeout) of
		{error, Reason} ->
			erlang:error(Reason);
		Cache ->
			{Cache, M, Alloter, Reader}
	end.

%%
%%构建一个数据读取器
%%
new_data_reader({_, _, _, Reader} = Handle, DatPoint, {F, A}) ->
	Reader(Handle, DatPoint, F, A).

%%
%%继续中断的操作
%%
continue(From, Idx, Handle, Point) ->
	QueueKey={?DATA_ASYN_WAIT, Idx, Point},
	{NextFrom, NextType}=case queue:out(get(QueueKey)) of
		{empty, _} ->
			erase(QueueKey),
			{?EMPTY, ?EMPTY};
		{{value, Item}, NewWaitLoadQueue} ->
			put(QueueKey, NewWaitLoadQueue),
			Item
	end,
	case erase({?DATA_INTERRUPT_STATE, From}) of
		{{lookup, _} = Req, lookup, [Key]} ->
			{lookup(Handle, Key), Req, NextFrom, NextType};
		{{update, _, _} = Req, update, [Key, Value, IsCheckSum]} ->
			{update(Handle, Key, Value, IsCheckSum), Req, NextFrom, NextType};
		{{delete, _, _} = Req, delete, [Key]} ->
			{delete(Handle, Key), Req, NextFrom, NextType};
		undefined ->
			erlang:error({continue_error, {invalid_interrupt_state, Handle, From}})
	end.

%%
%%检索指定关键字的数据
%%
lookup({_, _, _, _} = Handle, Key) ->
	lookup_data(Handle, Key, lookup, [Key]);
lookup(_, _) ->
	{lookup_error, invalid_handle}.

%%
%%保存变长数据
%%
save({?EMPTY, _, _, _}, _, _) ->
	true;
save({Cache, M, _, _}, Key, Value) ->
	M:insert(Cache, Key, Value).

%%
%%移除变长数据
%%
unsave({?EMPTY, _, _, _}, _) ->
	true;
unsave({Cache, M, _, _}, Key) ->
	M:delete(Cache, Key).

%%
%%插入一个指定大小的数据
%%
insert({_, _, Alloter, _}, Key, Value, IsCheckSum) ->
	Data={Key, Value},
	{ok, {data_alloc(Alloter, Data, IsCheckSum), Data}};
insert(_, _, _, _) ->
	{insert_error, invalid_handle}.

%%
%%更新一个指定数据
%%
update({_, _, Alloter, _} = Handle, Key, Value, IsCheckSum) ->
	Data={Key, Value},
	case lookup_data1(Handle, Key, update, [Key, Value, IsCheckSum]) of
		{ok, {OldDatPoint, _, _, _}} ->
			{ok, {OldDatPoint, data_alloc(Alloter, Data, IsCheckSum), Data}};
		AsynReader ->
			AsynReader
	end;
update(_, _, _, _) ->
	{update_error, invalid_handle}.

%%
%%删除一个指定位置的数据
%%
delete({_, _, _, _} = Handle, Key) ->
	case lookup_data1(Handle, Key, delete, [Key]) of
		{ok, {OldDatPoint, _, _, _}} ->
			{ok, {OldDatPoint, ?EMPTY, Key}};
		AsynReader ->
			AsynReader
	end;
delete(_, _) ->
	{delete_error, invalid_handle}.
	
%%
%%获取句柄中的缓存模块
%%
get_cache({?EMPTY, _, _, _}) ->
	?EMPTY;
get_cache({Cache, M, _, _}) ->
	{M, Cache};
get_cache(_) ->
	?EMPTY.

%%
%%获取句柄中的空间分配器
%%
get_alloter({_, _, Alloter, _}) ->
	Alloter;
get_alloter(_) ->
	?EMPTY.

%%
%%将指定数据设置为已提交状态
%%
set_commited({?EMPTY, _, _, _}, _) ->
	ok;
set_commited({Cache, M, _, _}, Keys) ->
	[?EMPTY || Key <- lists:flatten(Keys), M:modify(Cache, Key)],
	ok.

%%
%%整理缓存
%%
collate({?EMPTY, _, _, _}) ->
	ok;
collate({Cache, M, _, _}) ->
	M:collate(Cache, z_lib:now_millisecond()).

%% ====================================================================
%% Internal functions
%% ====================================================================

new_reader(M, F, A) ->
	fun(_Handle, Point, ContinueFun, ContinueArg) ->
		fun(BTSMV, Src, From, Type, Idx, Key, Vsn, Time, Req) ->
				case get({?DATA_ASYN_WAIT, Idx, Point}) of
					undefined ->
						put({?DATA_ASYN_WAIT, Idx, Point}, queue:new()),
						put({?DATA_INTERRUPT_STATE, From}, {Req, ContinueFun, ContinueArg}),
						M:F(BTSMV, A, Src, From, Type, Idx, Point, Key, Vsn, Time);
					WaitLoadQueue ->

						put({?DATA_ASYN_WAIT, Idx, Point}, queue:in({From, Type}, WaitLoadQueue)),
						put({?DATA_INTERRUPT_STATE, From}, {Req, ContinueFun, ContinueArg})
				end
		end
	end.

data_size(Data, IsCheckSum) ->
	case IsCheckSum of
		true ->
			erlang:external_size(Data) + 8;
		false ->
			erlang:external_size(Data) + 4
	end.

data_alloc({_, _, 0} = Alloter, Data, IsCheckSum) ->
	case bs_alloter:alloc(Alloter, data_size(Data, IsCheckSum)) of
		{{_, Offset} = Point, _} ->
			{bs_alloter:size(Point), Offset};
		?EMPTY ->
			erlang:error(not_enough)
	end.

lookup_data({?EMPTY, _, _, _}, _, F, A) ->
	{?DATA_ASYN_WAIT, {F, A}};
lookup_data({Cache, M, _, _}, Key, F, A) ->
	case M:get(Cache, Key) of
		{ok, _, {_, Value, Vsn, Time}} ->
			{ok, {Value, Vsn, Time}};
		false ->
			{?DATA_ASYN_WAIT, {F, A}}
	end.

lookup_data1({?EMPTY, _, _, _}, _, F, A) ->
	{?DATA_ASYN_WAIT, {F, A}};
lookup_data1({Cache, M, _, _}, Key, F, A) ->
	case M:get(Cache, Key) of
		{ok, _, {DatPoint, Value, Vsn, Time}} ->
			{ok, {DatPoint, Value, Vsn, Time}};
		false ->
			{?DATA_ASYN_WAIT, {F, A}}
	end.

