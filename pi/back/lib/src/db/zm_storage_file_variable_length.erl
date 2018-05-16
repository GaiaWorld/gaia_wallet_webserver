%%@doc 变长数据的文件存储模块
%%```
%%% 文件格式为：[4字节块长+2字节键长（0表示空块）+键数据+4字节版本（0表示已删除）+6字节时间+4字节数据长度+数据]
%%% 负责维护删除记录和空块位置
%%'''
%%@end


-module(zm_storage_file_variable_length).

-description("data base storage file by variable length").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([init/2, clear/1, load/5, read/3, write/7]).
-export([send_read/7, send_write/9, send_read_write/5, send_delete/7]).
-export([send_dyn_read/8, send_dyn_write/8]).

%%%=======================DEFINE=======================
-define(READ_BUFFER, 6*1024*1024).

-define(LIMIT_SIZE, 8*1024*1024).

-define(BLOCK_KEY_VSN_TIME_SIZE, 20).
-define(BLOCK_KEY_LEN_SIZE, 6).
-define(BLOCK_LEN_SIZE, 4).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  初始化模块
%% @spec  init(Ets, Length) -> return()
%% where
%%  return() =  {integer(), integer(), integer()}
%%@end
%% -----------------------------------------------------------------
init(Ets, _Length) ->
	{Ets, ets:new(?MODULE, [private]), ets:new(?MODULE, [private, ordered_set])}.

%% -----------------------------------------------------------------
%%@doc   清理数据，返回ok
%% @spec  clear({Ets, Deleted, Empty}) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
clear({_Ets, Deleted, Empty}) ->
	ets:delete_all_objects(Deleted),
	ets:delete_all_objects(Empty).

%% -----------------------------------------------------------------
%%@doc  从指定的文件进程中加载读写数据到ets表中，IgnoreError表示是否忽略错误，返回文件长度
%% @spec  load(Args, Cache, Pid, File, IgnoreError) -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
load(Args, Cache, Pid, File, IgnoreError) ->
	load(Args, Cache, Pid, File, 0, ?READ_BUFFER, <<>>, IgnoreError).

%% -----------------------------------------------------------------
%%@doc  从存储中读入数据
%% @spec  read(Args, Pid, Info::{File, {BlockSize, Loc}, KSize, VSize}) -> return()
%% where
%%  return() =  Data
%%@end
%% -----------------------------------------------------------------
read(_Args, Pid, {_File, {_BlockSize, Loc}, KSize, VSize} = Info) ->
	case zm_file:read(Pid, Loc + KSize + ?BLOCK_KEY_VSN_TIME_SIZE, VSize) of
		{ok, Data} ->
			Data;
		eof ->
			erlang:error({eof, Info});
		{error, Reason} ->
			erlang:error({Reason, Info})
	end.

%% -----------------------------------------------------------------
%%@doc   写入数据到文件，返回数据长度
%% @spec  write(Args, Key, Vsn, Time, V, Pid, Loc) -> return()
%% where
%%  return() =  integer() | {error, Reason}
%%@end
%% -----------------------------------------------------------------
write(_Args, Key, Vsn, Time, V, Pid, Loc) ->
	K = term_to_binary(Key, [{minor_version, 1}]),
	VSize = byte_size(V),
	case byte_size(K) of
		KSize when KSize >= ?LIMIT_SIZE ->
			{error, key_overflow};
		KSize when KSize + VSize >= ?LIMIT_SIZE ->
			{error, overflow};
		KSize ->
			BlockSize = KSize + ?BLOCK_KEY_VSN_TIME_SIZE + VSize,
			case zm_file:write(Pid, Loc,
				[<<(BlockSize - ?BLOCK_LEN_SIZE):32, KSize:16>>,
				K, <<Vsn:32, Time:48, VSize:32>>, V]) of
				ok ->
					BlockSize;
				E ->
					E
			end
	end.

%% -----------------------------------------------------------------
%%@doc   从存储中读入数据，向存储进程发消息
%% @spec  send_read(Args, Reads, Key, {File, {BlockSize, Loc}, KSize, VSize}, Vsn, Time, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
send_read(_Args, Reads, Key, {File, {_BlockSize, Loc}, KSize, VSize}, Vsn, Time, From) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?BLOCK_KEY_VSN_TIME_SIZE,
		VSize, self(), {read, Key, Vsn, Time, From}).

%% -----------------------------------------------------------------
%%@doc   写入数据，返回数据文件长度和{写入的位置、键长度、值长度}
%% @spec  send_write({Ets, Deleted, Empty}, Key, Value, Vsn, Time, none, From, {File, Pid}, WSize) -> return()
%% where
%%  return() =  {integer(), {integer(), integer(), integer(), integer()}} | Error
%%@end
%% -----------------------------------------------------------------
send_write({_Ets, Deleted, Empty}, Key, Value, Vsn, Time, none, From, {File, Pid}, WSize) ->
	%无数据，检查删除表
	case ets:lookup(Deleted, Key) of
		[] ->
			case kv_bin(Key, Value, ?LIMIT_SIZE) of
				{K, KSize, V, VSize} ->
					% 检查空块是否有合适的
					BlockSize = KSize + ?BLOCK_KEY_VSN_TIME_SIZE + VSize,
					case get_block(Empty, BlockSize) of
						none ->
							write_pid(Pid, WSize, Key, Vsn, Time, K, KSize, V, VSize, From),
							{WSize + BlockSize, {File, {BlockSize, WSize}, KSize, VSize}};
						Info ->
							write_pid(Pid, Info, Key, Vsn, Time, K, KSize, V, VSize, From),
							{WSize, {File, Info, KSize, VSize}}
					end;
				E ->
					E
			end;
		[{_, {Size, Loc} = Info, KSize}] ->
			case value_bin(KSize, Value, ?LIMIT_SIZE) of
				{V, VSize} ->
					ets:delete(Deleted, Key),
					% 检查原块是否可以容纳
					BlockSize = KSize + VSize + ?BLOCK_KEY_VSN_TIME_SIZE,
					if
						Size < BlockSize ->
							erase_pid(Pid, Loc),
							ets:insert(Empty, Info),
							write_pid(Pid, WSize, Key, Vsn, Time,
								term_to_binary(Key, [{minor_version, 1}]),
								KSize, V, VSize, From),
							{WSize + BlockSize, {File, {BlockSize, WSize}, KSize, VSize}};
						true ->
							write_pid(Pid, Loc, Key, Vsn, Time, KSize, V, VSize, From),
							{WSize, {File, Info, KSize, VSize}}
					end;
				E ->
					E
			end
	end;
send_write({_Ets, _Deleted, Empty}, Key, Value, Vsn, Time,
	{File, {Size, Loc} = Info, KSize, _VSize}, From, {File, Pid}, WSize) ->
	%数据在读写文件中
	case value_bin(KSize, Value, ?LIMIT_SIZE) of
		{V, VSize} ->
			% 检查原块是否可以容纳
			BlockSize = KSize + VSize + ?BLOCK_KEY_VSN_TIME_SIZE,
			if
				Size < BlockSize ->
					erase_pid(Pid, Loc),
					ets:insert(Empty, Info),
					write_pid(Pid, WSize, Key, Vsn, Time,
						term_to_binary(Key, [{minor_version, 1}]),
						KSize, V, VSize, From),
					{WSize + BlockSize, {File, {BlockSize, WSize}, KSize, VSize}};
				true ->
					write_pid(Pid, Loc, Key, Vsn, Time, KSize, V, VSize, From),
					{WSize, {File, Info, KSize, VSize}}
			end;
		E ->
			E
	end;
send_write({_Ets, _Deleted, Empty}, Key, Value, Vsn, Time, _Info, From, {File, Pid}, WSize) ->
	%数据在读文件中
	case kv_bin(Key, Value, ?LIMIT_SIZE) of
		{K, KSize, V, VSize} ->
			% 检查空块是否有合适的
			BlockSize = KSize + ?BLOCK_KEY_VSN_TIME_SIZE + VSize,
			case get_block(Empty, BlockSize) of
				none ->
					write_pid(Pid, WSize, Key, Vsn, Time, K, KSize, V, VSize, From),
					{WSize + BlockSize, {File, {BlockSize, WSize}, KSize, VSize}};
				Info ->
					write_pid(Pid, Info, Key, Vsn, Time, K, KSize, V, VSize, From),
					{WSize, {File, Info, KSize, VSize}}
			end;
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc   从存储中读入数据用于写，向存储进程发消息
%% @spec  send_read_write(Args, Reads, Key, {File, {BlockSize, Loc}, KSize, VSize}, Ref) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
send_read_write(_Args, Reads, Key, {File, {_BlockSize, Loc}, KSize, VSize}, Ref) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?BLOCK_KEY_VSN_TIME_SIZE,
		VSize, self(), {read_write, Key, Ref}).

%% -----------------------------------------------------------------
%%@doc   删除数据，返回数据文件长度
%% @spec  send_delete({Ets, Deleted, Empty}, Key, Time, {File, Info, KSize, VSize}, From, {File, Pid}, WSize) -> return()
%% where
%%  return() =  integer() | Error
%%@end
%% -----------------------------------------------------------------
send_delete({_Ets, Deleted, _Empty}, Key, Time,
	{File, {_, Loc} = Info, KSize, _VSize}, From, {File, Pid}, WSize) ->
	%数据在读写文件中
	delete_pid(Pid, Loc, Key, Time, KSize, From),
	ets:insert(Deleted, {Key, Info, KSize}),
	WSize;
send_delete({_Ets, Deleted, Empty}, Key, Time,
	{_File, _Info, KSize, _VSize}, From, {_, Pid}, WSize) ->
	%数据在读文件中
	% 检查空块是否有合适的
	BlockSize = KSize + ?BLOCK_KEY_VSN_TIME_SIZE,
	case get_block(Empty, BlockSize) of
		none ->
			delete_pid(Pid, WSize, Key, Time,
				term_to_binary(Key, [{minor_version, 1}]), KSize, From),
			ets:insert(Deleted, {Key, {BlockSize, WSize}, KSize}),
			WSize + BlockSize;
		Info ->
			delete_pid(Pid, Info, Key, Time,
				term_to_binary(Key, [{minor_version, 1}]), KSize, From),
			ets:insert(Deleted, {Key, Info, KSize}),
			WSize
	end.

%% -----------------------------------------------------------------
%%@doc   从存储中读入数据，向存储进程发消息
%% @spec  send_dyn_read(Args, Reads, Key, {File, {BlockSize, Loc}, KSize, VSize}, Vsn, Time, Fun, From) -> return()
%% where
%%  return() =  {Loc, Size, From, Ref}
%%@end
%% -----------------------------------------------------------------
send_dyn_read(_Args, Reads, Key, {File, {_BlockSize, Loc}, KSize, VSize},
	Vsn, Time, Fun, From) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?BLOCK_KEY_VSN_TIME_SIZE,
		VSize, self(), {dyn_read, Key, Vsn, Time, Fun, From}).

%% -----------------------------------------------------------------
%%@doc   往存储中写入数据，向存储进程发消息
%% @spec  send_dyn_write(Args, Reads, Key, {File, {BlockSize, Loc}, KSize, VSize}, Vsn, Time, Fun, From) -> return()
%% where
%%  return() =  {Loc, Size, From, Ref}
%%@end
%% -----------------------------------------------------------------
send_dyn_write(_Args, Reads, Key, {File, {_BlockSize, Loc}, KSize, VSize}, Vsn, Time, Fun, From) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?BLOCK_KEY_VSN_TIME_SIZE,
		VSize, self(), {dyn_write, Key, Vsn, Time, Fun, From}).

%%%===================LOCAL FUNCTIONS==================
% 循环读取文件中的数据条目，返回文件长度
load(Args, Cache, Pid, File, Offset, Number, Suffix, IgnoreError) ->
	case zm_file:read(Pid, Offset, Number) of
		{ok, Bin} ->
			R = read_bins(Args, Cache, File, Offset, Bin, Suffix, IgnoreError),
			load(Args, Cache, Pid, File, Offset + byte_size(Bin),
				Number, R, IgnoreError);
		eof ->
			Offset;
		{error, Reason} ->
			erlang:error({load_error, Reason, File, Offset})
	end.

% 读取2个二进制数据中的记录
read_bins(Args, Cache, File, Start, Bin, Suffix, IgnoreError) ->
	case Suffix of
		<<>> ->
			read_bin(Args, Cache, File, Start, Bin, 0, IgnoreError);
		<<Size:32, _/binary>> ->
			Len1 = byte_size(Suffix),
			Len2 = Size + ?BLOCK_LEN_SIZE - Len1,
			Prefix = case Bin of
				<<Sub:Len2/binary, _/binary>> ->
					Sub;
				_ ->
					erlang:error({load_error,
						"read prefix binary error", File, {Start, Len2}})
			end,
			R = read_bin(Args, Cache, File, Start - Len1,
				list_to_binary([Suffix, Prefix]), 0, IgnoreError),
			[erlang:error({load_error,
				"read overage error", File, {Start, Len2}}) || R =/= <<>>],
			read_bin(Args, Cache, File, Start, Bin, Len2, IgnoreError);
		_ ->
			read_bin(Args, Cache, File, Start - byte_size(Suffix),
				list_to_binary([Suffix, Bin]), 0, IgnoreError)
	end.

% 读取二进制数据中的记录
read_bin(Args, Cache, File, Start, Bin, Offset, IgnoreError) ->
	case Bin of
		<<_:Offset/binary, Size:32, Block:Size/binary, _/binary>> ->
			BlockSize = ?BLOCK_LEN_SIZE + Size,
			read_block(Args, Cache, File, Start + Offset, IgnoreError, BlockSize, Block),
			read_bin(Args, Cache, File, Start, Bin, Offset + BlockSize, IgnoreError);
		<<_:Offset/binary, SubBin/binary>> ->
			SubBin
	end.

% 读取二进制数据块中的记录
read_block({_Ets, _Deleted, Empty} = Args, {IsWrite, _} = Cache, File, Offset, IgnoreError, BlockSize, Block) ->
	case Block of
		<<0:16, _/binary>> when IsWrite ->
			ets:insert(Empty, {BlockSize, Offset});
		<<0:16, _/binary>> ->
			empty_entry;
		<<KSize:16, K:KSize/binary, Vsn:32, Time:48,
			VSize:32, V:VSize/binary, _/binary>> ->
			try read_entry(Args, Cache, File,
					{BlockSize, Offset}, KSize, K, Vsn, Time, VSize, V) of
				R ->
					[erlang:error({read_entry_error, R, File, Offset})
						|| R =/= true, IgnoreError =/= true],
					case get(lasttime) of
						T when Time > T ->
							put(lasttime, Time);
						_ ->
							Time
					end
			catch
				_:E ->
					[erlang:error({read_entry_error, E, File, Offset})
						|| IgnoreError =/= true]
			end;
		_ when IgnoreError =/= true ->
			erlang:error({read_entry_error, invalid_entry, File, Offset});
		_ ->
			invalid_entry
	end.

% 只读数据，如果版本为0，表示删除
read_entry({Ets, _Deleted, _Empty}, {false, Cache}, _File, _Offset, _KSize, K, 0, _Time, _VSize, _V) ->
	Key = binary_to_term(K),
	ets:delete(Ets, Key),
	zm_cache:delete(Cache, Key);
% 读写数据，如果版本为0，表示删除
read_entry({Ets, Deleted, _Empty}, {true, Cache}, _File, Offset, KSize, K, 0, _Time, _VSize, _V) ->
	Key = binary_to_term(K),
	ets:delete(Ets, Key),
	zm_cache:delete(Cache, Key),
	ets:insert(Deleted, {Key, Offset, KSize});
% 只读数据，读取键值和版本时间
read_entry({Ets, _Deleted, _Empty}, {false, Cache}, File, Offset, KSize, K, Vsn, Time, VSize, V) ->
	Key = binary_to_term(K),
	Value = binary_to_term(V),
	ets:insert(Ets, {Key, {File, Offset, KSize, VSize}, Vsn, Time}),
	case zm_cache:is_overflow(Cache) of
		false ->
			zm_cache:set(Cache, Key, Value);
		_ ->
			true
	end;
% 读写数据，读取键值和版本时间
read_entry({Ets, _Deleted, _Empty}, {true, Cache}, File, Offset, KSize, K, Vsn, Time, VSize, V) ->
	Key = binary_to_term(K),
	Value = binary_to_term(V),
	ets:insert(Ets, {Key, {File, Offset, KSize, VSize}, Vsn, Time}),
	case zm_cache:is_overflow(Cache) of
		false ->
			zm_cache:set(Cache, Key, Value);
		_ ->
			true
	end.

% 将键和值变成二进制数据
kv_bin(Key, Value, Length) ->
	V = term_to_binary(Value, [{minor_version, 1}]),
	case byte_size(V) of
		VSize when VSize >= Length ->
			value_overflow;
		VSize ->
			K = term_to_binary(Key, [{minor_version, 1}]),
			case byte_size(K) of
				KSize when KSize >= Length ->
					key_overflow;
				KSize when KSize + VSize >= Length ->
					overflow;
				KSize ->
					{K, KSize, V, VSize}
			end
	end.

% 将键变成二进制数据
value_bin(KSize, Value, Length) ->
	V = term_to_binary(Value, [{minor_version, 1}]),
	case byte_size(V) of
		VSize when VSize >= Length ->
			value_overflow;
		VSize when VSize + KSize >= Length ->
			overflow;
		VSize ->
			{V, VSize}
	end.

% 抹去块中的数据，向文件进程发送消息
erase_pid(Pid, Loc) ->
	zm_file:send_write(Pid, Loc + ?BLOCK_LEN_SIZE, <<0:16>>, none, none).

% 写入已有数据，向文件进程发送消息
write_pid(Pid, Loc, Key, Vsn, Time, KSize, V, VSize, From) ->
	zm_file:send_write(Pid, Loc + ?BLOCK_KEY_LEN_SIZE + KSize,
		[<<Vsn:32, Time:48, VSize:32>>, V], self(), {write, Key, Vsn, Time, From}).

% 已有块写入数据，向文件进程发送消息
write_pid(Pid, {_BlockSize, Loc}, Key, Vsn, Time, K, KSize, V, VSize, From) ->
	zm_file:send_write(Pid, Loc + ?BLOCK_LEN_SIZE,
		[<<KSize:16>>, K, <<Vsn:32, Time:48, VSize:32>>, V],
		self(), {write, Key, Vsn, Time, From});
% 新增写入数据，向文件进程发送消息
write_pid(Pid, Loc, Key, Vsn, Time, K, KSize, V, VSize, From) ->
	BlockSize = KSize + ?BLOCK_KEY_VSN_TIME_SIZE + VSize,
	zm_file:send_write(Pid, Loc, [<<(BlockSize - ?BLOCK_LEN_SIZE):32,
		KSize:16>>, K, <<Vsn:32, Time:48, VSize:32>>, V],
		self(), {write, Key, Vsn, Time, From}).

%删除已有数据，向文件进程发送消息
delete_pid(Pid, Loc, Key, Time, KSize, From) ->
	zm_file:send_write(Pid, Loc + ?BLOCK_KEY_LEN_SIZE + KSize,
		<<0:32, Time:48>>, self(), {delete, Key, Time, From}).

% 新增已有块删除数据，向文件进程发送消息
delete_pid(Pid, {_BlockSize, Loc}, Key, Time, K, KSize, From) ->
	zm_file:send_write(Pid, Loc + ?BLOCK_LEN_SIZE,
		[<<KSize:16>>, K, <<0:32, Time:48, 0:32>>], self(), {delete, Key, Time, From});
% 新增删除数据，向文件进程发送消息
delete_pid(Pid, Loc, Key, Time, K, KSize, From) ->
	BlockSize = KSize + ?BLOCK_KEY_VSN_TIME_SIZE,
	zm_file:send_write(Pid, Loc, [<<(BlockSize - ?BLOCK_LEN_SIZE):32, KSize:16>>,
		K, <<0:32, Time:48, 0:32>>], self(), {delete, Key, Time, From}).

% 获得可用的块大小和位置
get_block(Empty, Size) ->
	case ets:lookup(Empty, Size) of
		[R] ->
			ets:delete(Empty, Size),
			R;
		[] ->
			case ets:next(Empty, Size) of
				'$end_of_table' ->
					none;
				Key ->
					[R] = ets:lookup(Empty, Key),
					ets:delete(Empty, Key),
					R
			end
	end.
