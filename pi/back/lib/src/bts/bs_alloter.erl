%% Author: Administrator
%% Created: 2012-11-29
%% Description: 基于伙伴系统的存储空间分配器，管理存储空间的分配与释放
-module(bs_alloter).

%%
%% Include files
%%
-include("bsys.hrl").

%%
%% Exported Functions
%%
-export([new/3, new/2, alloc/2, free/2, query_free/2, to_free/1, from_free/2, pos/1, size/1]).
-compile({inline, [sz2pos/1, buddy_off/1, prev_area/1, next_area/1,
				   first_block/2, insert_block/2, alloc_block/4, delete_block/2, split_block/5, split_block1/4,
				   free_block/4, merge_block/4, merge_block1/5
				  ]}).
-compile({inline_size, 32}).

%%
%% API Functions
%%

%%
%% Function: new/2
%% Description: 根据指定初始化数据，构造分配器
%% Arguments: 
%%	MaxSize
%%	AllocSize
%%	Inits
%% Returns: Handle
%% 
new(MaxSize, 0, Inits) 
  when is_integer(MaxSize), MaxSize > 0, is_list(Inits) ->
	MaxOrder=max_order(MaxSize),
	FreeTab=ets:new(?FREE_AREA_TABLE(erlang:pid_to_list(self())), [ordered_set]),
	case Inits of
		[] ->
			load([{{max_order(MaxSize), ?PINIT}, ?FREE_FLAG}], FreeTab, MaxOrder);
		_ ->
			load(Inits, FreeTab, MaxOrder)
	end,
	{FreeTab, MaxOrder, 0};
new(MaxSize, AllocSize, Inits) 
  when is_integer(MaxSize), MaxSize > 0, is_integer(AllocSize), is_list(Inits) ->
	Pos=sz2pos(AllocSize),
	MaxOrder=max_order(MaxSize),
	FreeTab=ets:new(?FREE_AREA_TABLE(erlang:pid_to_list(self())), [ordered_set]),
	case Inits of
		[] ->
			load([{{max_order(MaxSize), ?PINIT}, ?FREE_FLAG}], FreeTab, MaxOrder);
		_ ->
			load(Inits, FreeTab, MaxOrder)
	end,
	{FreeTab, MaxOrder, Pos}.

%%
%% Function: new/1
%% Description: 构造分配器, 加载初始block
%% Arguments: 
%%	MaxSize
%%	AllocSize
%% Returns: Handle
%% 
new(MaxSize, Size) when MaxSize > 0 ->
	new(MaxSize, Size, []).

%%
%% Function: alloc/2
%% Description: 分配一个连续存储空间
%% Arguments: 
%%	Handle
%%	Size
%% Returns: {Point, L} | undefined
%% 
alloc({FreeTab, MaxOrder, 0}, Size) 
  when is_integer(FreeTab), is_integer(MaxOrder), is_integer(Size), MaxOrder > 0, Size > 0 ->
	alloc_block(FreeTab, MaxOrder, sz2pos(Size), []);
alloc({FreeTab, MaxOrder, Pos}, _) 
  when is_integer(FreeTab), is_integer(MaxOrder), is_integer(Pos), MaxOrder > 0, Pos >= 0 ->
	alloc_block(FreeTab, MaxOrder, Pos, []).
	
%%
%% Function: free/2
%% Description: 释放一个已分配的连续存储空间
%% Arguments: 
%%	Handle
%%	Point
%% Returns: {ok, L} | undefined
%% 
free({FreeTab, MaxOrder, _}, {Pos, Offset}) 
  when is_integer(FreeTab), is_integer(MaxOrder), is_integer(Pos), 
	   is_integer(Offset), MaxOrder > 0, Pos >= 0, Offset >= 0 ->
	case ets:lookup(FreeTab, {MaxOrder, 0}) of
		[] ->
			free_block(FreeTab, MaxOrder, Pos, Offset);
		_ ->
			?EMPTY_BLOCK
	end;
free(_, ?EMPTY_BLOCK) ->
	{ok, []}.

%%
%% Function: query_free/2
%% Description: 查询指定区域的空闲块
%% Arguments: 
%%	Handle
%%	L
%% Returns: FreeList
%% 
query_free(_, []) ->
	[];
query_free({FreeTab, MaxOrder, _}, L) 
  when is_integer(FreeTab), is_integer(MaxOrder), is_list(L), MaxOrder > 0 ->
	A=hd(L),
	B=hd(lists:reverse(L)),
	if
		A < B ->
			select_free(FreeTab, A, B);
		true ->
			select_free(FreeTab, B, A)
	end.

%%
%% Function: to_free/2
%% Description: 获取当前的空闲块列表
%% Arguments: 
%%	Handle
%%	L
%% Returns: FreeList
%% 
to_free({FreeTab, MaxOrder, _}) 
  when is_integer(FreeTab), is_integer(MaxOrder), MaxOrder > 0 ->
	ets:tab2list(FreeTab).

%%
%% Function: from_free/2
%% Description: 从空闲块列表恢复分配器
%% Arguments: 
%%	Handle
%%	FreeBlocks
%% Returns: ok | error
%% 
from_free({_, _, _}, []) ->
	ok;
from_free({FreeTab, MaxOrder, _}, [{_, _}|_] = FreeBlocks) 
  when is_integer(FreeTab), is_integer(MaxOrder), MaxOrder > 0 ->
	ets:delete_all_objects(FreeTab),
	case ets:insert_new(FreeTab, FreeBlocks) of
		true ->
			ok;
		false ->
			error
	end.

%%
%% Function: pos/1
%% Description: 获取指定大小的块所属的区域
%% Arguments: 
%%	Point
%% Returns: Pos
%% 
pos({Size, Offset}) when is_integer(Size), is_integer(Offset), Size > 0, Offset >= 0 ->
	sz2pos(Size).

%%
%% Function: size/1
%% Description: 块指针指向的存储空间大小
%% Arguments: 
%%	Point
%% Returns: Size
%% 
size({Pos, Offset}) when is_integer(Pos), is_integer(Offset), Pos >= 0, Offset >= 0 ->
	?POW(Pos).
	
%%
%% Local Functions
%%

max_order(MaxSize) when is_integer(MaxSize), MaxSize > 0, MaxSize =< ?MAX_STORAGE_SIZE ->
	if
		MaxSize rem 2 =:= 0 ->
			1 + trunc(math:log(MaxSize - 1) / math:log(2));
		true ->
			throw({error, invalid_size})
	end.

sz2pos(Size) when is_integer(Size), Size > ?MIN_PSIZE ->
	1 + trunc(math:log(Size - 1) / math:log(2));
sz2pos(?MIN_PSIZE) ->
	0.

buddy_off({Pos, Offset}) ->
	Offset bxor (1 bsl Pos).
	
prev_area(Pos) ->
	Pos - 1.

next_area(Pos) ->
	Pos + 1.

load([{{Pos, Offset} = Point, Value}|T], FreeTab, MaxOrder) 
  when is_integer(Pos), is_integer(Offset), Pos >= ?PINIT, Pos =< MaxOrder, Offset >= ?PINIT ->
	case ets:insert_new(FreeTab, {Point, Value}) of
		true ->
			load(T, FreeTab, MaxOrder);
		false ->
			erlang:error({load_failed, FreeTab, MaxOrder, Point})
	end;
load([], _FreeTab, _MaxOrder) ->
	ok.

first_block(FreeTab, Pos) ->
	case ets:next(FreeTab, {Pos, ?PBOF}) of
		{Pos, _} = Head ->
			Head;
		{_, _} ->
			?EMPTY_BLOCK;
		'$end_of_table' ->
			?EMPTY_BLOCK
	end.

insert_block(FreeTab, {_, _} = Point) ->
	case ets:insert_new(FreeTab, {Point, ?FREE_FLAG}) of
		true ->
			ok;
		false ->
			erlang:error({insert_block_failed, FreeTab, Point})
	end.
	
delete_block(FreeTab, {_, _} = Point) ->
	case ets:lookup(FreeTab, Point) of
		[{_, _}] ->
			ets:delete(FreeTab, Point);
		[] ->
			erlang:error({delete_block_failed, FreeTab, Point})
	end.

alloc_block(FreeTab, MaxOrder, Pos, L) ->
	case first_block(FreeTab, Pos) of
		?EMPTY_BLOCK ->
			split_block(FreeTab, MaxOrder, Pos, next_area(Pos), L);
		{_, _} = Point ->
			delete_block(FreeTab, Point), 
			{Point, [Pos|L]}
	end.

split_block(FreeTab, MaxOrder, Pos, CurPos, L) when MaxOrder >= CurPos ->
	case first_block(FreeTab, CurPos) of
		?EMPTY_BLOCK ->
			split_block(FreeTab, MaxOrder, Pos, next_area(CurPos), L);
		{_, _} = Point ->
			split_block1(FreeTab, Pos, Point, L)
	end;
split_block(_FreeTab, _MaxOrder, _Pos, _CurPos, _L) ->
	?EMPTY_BLOCK.

split_block1(FreeTab, Pos, {_, CurPos, Offset}, L) when Pos < CurPos ->
	insert_block(FreeTab, {CurPos, buddy_off({CurPos, Offset})}),
	split_block1(FreeTab, Pos, {CurPos, prev_area(CurPos), Offset}, [CurPos|L]);
split_block1(FreeTab, Pos, {CurPos, Offset} = Point, L) when Pos < CurPos ->
	delete_block(FreeTab, Point),
	split_block1(FreeTab, Pos, {CurPos, prev_area(CurPos), Offset}, [CurPos|L]);
split_block1(FreeTab, Pos, {_, Pos, Offset}, L) ->
	Point={Pos, Offset},
	insert_block(FreeTab, {Pos, buddy_off(Point)}),
	{Point, [Pos|L]};
split_block1(FreeTab, Pos, {Pos, _} = Point, L) ->
	erlang:error({invalid_split, FreeTab, Point, L}).

free_block(FreeTab, MaxOrder, Pos, Offset) when MaxOrder >= Pos ->
	merge_block(FreeTab, Pos, Offset, []);
free_block(_FreeTab, _MaxOrder, _Pos, _Offset) ->
	?EMPTY_BLOCK.

merge_block(FreeTab, Pos, Offset, L) ->
	Point={Pos, Offset},
	Buddy=buddy_off(Point),
	case first_block(FreeTab, Pos) of
		?EMPTY_BLOCK ->
			{insert_block(FreeTab, Point), [Pos|L]};
		{_, _} ->
			case ets:lookup(FreeTab, {Pos, Buddy}) of
				[{{_, _}, ?FREE_FLAG}] ->
					merge_block1(FreeTab, Pos, Offset, Buddy, L);
				[] ->
					{insert_block(FreeTab, Point), [Pos|L]}
			end
	end.

merge_block1(FreeTab, Pos, Offset, Buddy, L) ->
	delete_block(FreeTab, {Pos, Buddy}),
	Off=if
		Offset < Buddy ->
			Offset;
		Offset > Buddy ->
			Buddy;
		true ->
			erlang:error({invalid_buddy, FreeTab, Pos, Offset, Buddy})
	end,
	merge_block(FreeTab, next_area(Pos), Off, [Pos|L]).

select_free(FreeTab, Start, Tail) ->
	ets:select(FreeTab, [{{{'$1', '$2'}, '_'}, [{'>=', '$1', Start}], [{{'$1', '$2'}}]}, 
						 {{{'$3', '$4'}, '_'}, [{'=<', '$3', Tail}], [{{'$3', '$4'}}]}]).