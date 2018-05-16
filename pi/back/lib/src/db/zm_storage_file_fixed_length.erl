%%@doc 定长数据的文件存储模块
%%```
%%% 文件格式为：[2字节键长+键数据+4字节版本（0表示已删除）+6字节时间+4字节数据长度+数据]
%%% 负责维护删除记录
%%'''
%%@end


-module(zm_storage_file_fixed_length).

-description("data base storage file by fixed length").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([init/2, clear/1, load/5, read/3, write/7]).
-export([send_read/7, send_write/9, send_read_write/5, send_delete/7]).
-export([send_dyn_read/8, send_dyn_write/8]).

%%%=======================DEFINE=======================
-define(READ_BUFFER, 6*1024*1024).

-define(KEY_VSN_TIME_SIZE, 16).
-define(KEY_LEN_SIZE, 2).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  初始化模块
%% @spec  init(Ets, Length) -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
init(Ets, Length) ->
	{Ets, ets:new(?MODULE, [private]), Length + ?KEY_VSN_TIME_SIZE}.

%% -----------------------------------------------------------------
%%@doc   清理数据，返回ok
%% @spec  clear({Ets, Deleted, Length}) -> return()
%% where
%%  return() =  true
%%@end
%% -----------------------------------------------------------------
clear({_Ets, Deleted, _Length}) ->
	ets:delete_all_objects(Deleted).

%% -----------------------------------------------------------------
%%@doc  从指定的文件进程中加载读写数据到ets表中，IgnoreError表示是否忽略错误，返回文件长度
%% @spec  load(Args, Cache, Pid, File, IgnoreError) -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
load({_Ets, _Deleted, Length} = Args, Cache, Pid, File, IgnoreError) ->
	% 根据固定长度，算出合适的块长度和加载长度
	Number = if
		Length >= ?READ_BUFFER ->
			Length;
		true ->
			(?READ_BUFFER div Length) * Length
	end,
	load(Args, Cache, Pid, File, 0, Number, <<>>, IgnoreError).

%% -----------------------------------------------------------------
%%@doc  从存储中读入数据
%% @spec  read(Args, Pid, Info::{File, Loc, KSize, VSize}) -> return()
%% where
%%  return() =  Data
%%@end
%% -----------------------------------------------------------------
read(_Args, Pid, {_File, Loc, KSize, VSize} = Info) ->
	case zm_file:read(Pid, Loc + KSize + ?KEY_VSN_TIME_SIZE, VSize) of
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
write({_Ets, _Deleted, Length}, Key, Vsn, Time, V, Pid, Loc) ->
	Len = Length - ?KEY_VSN_TIME_SIZE,
	K = term_to_binary(Key, [{minor_version, 1}]),
	VSize = byte_size(V),
	case byte_size(K) of
		KSize when KSize >= Len ->
			{error, key_overflow};
		KSize when KSize + VSize >= Len ->
			{error, overflow};
		KSize ->
			case zm_file:write(Pid, Loc,
				[<<KSize:16>>, K, <<Vsn:32, Time:48, VSize:32>>, V,
				<<0:((Length - KSize - ?KEY_VSN_TIME_SIZE - VSize) * 8)>>]) of
				ok ->
					Length;
				E ->
					E
			end
	end.

%% -----------------------------------------------------------------
%%@doc   从存储中读入数据，向存储进程发消息
%% @spec  send_read(Args, Reads, Key, {File, Loc, KSize, VSize}, Vsn, Time, From) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
send_read(_Args, Reads, Key, {File, Loc, KSize, VSize}, Vsn, Time, From) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?KEY_VSN_TIME_SIZE,
		VSize, self(), {read, Key, Vsn, Time, From}).

%% -----------------------------------------------------------------
%%@doc   写入数据，返回数据文件长度和{写入的位置、键长度、值长度}
%% @spec  send_write(Args, Key, Value, Vsn, Time, none, From, {File, Pid}, WSize) -> return()
%% where
%%  return() =  {integer(), {integer(), integer(), integer(), integer()}} | Error
%%@end
%% -----------------------------------------------------------------
send_write({_Ets, Deleted, Length}, Key, Value, Vsn, Time, none, From, {File, Pid}, WSize) ->
	%无数据，检查删除表
	case ets:lookup(Deleted, Key) of
		[] ->
			case kv_bin(Key, Value, Length - ?KEY_VSN_TIME_SIZE) of
				{K, KSize, V, VSize} ->
					write_pid(Pid, WSize, Key, Vsn, Time, K, KSize, V, VSize, Length, From),
					{WSize + Length, {File, WSize, KSize, VSize}};
				E ->
					E
			end;
		[{_, Loc, KSize}] ->
			case value_bin(KSize, Value, Length - ?KEY_VSN_TIME_SIZE) of
				{V, VSize} ->
					ets:delete(Deleted, Key),
					write_pid(Pid, Loc, Key, Vsn, Time, KSize, V, VSize, From),
					{WSize, {File, Loc, KSize, VSize}};
				E ->
					E
			end
	end;
send_write({_Ets, _Deleted, Length}, Key, Value, Vsn, Time,
	{File, Loc, KSize, _VSize}, From, {File, Pid}, WSize) ->
	%数据在读写文件中
	case value_bin(KSize, Value, Length - ?KEY_VSN_TIME_SIZE) of
		{V, VSize} ->
			write_pid(Pid, Loc, Key, Vsn, Time, KSize, V, VSize, From),
			{WSize, {File, Loc, KSize, VSize}};
		E ->
			E
	end;
send_write({_Ets, _Deleted, Length}, Key, Value, Vsn, Time, _Info, From, {File, Pid}, WSize) ->
	%数据在读文件中
	case kv_bin(Key, Value, Length - ?KEY_VSN_TIME_SIZE) of
		{K, KSize, V, VSize} ->
			write_pid(Pid, WSize, Key, Vsn, Time, K, KSize, V, VSize, Length, From),
			{WSize + Length, {File, WSize, KSize, VSize}};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc   从存储中读入数据用于写，向存储进程发消息
%% @spec  send_read_write(Args, Reads, Key, {File, Loc, KSize, VSize}, Ref) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
send_read_write(_Args, Reads, Key, {File, Loc, KSize, VSize}, Ref) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?KEY_VSN_TIME_SIZE,
		VSize, self(), {read_write, Key, Ref}).

%% -----------------------------------------------------------------
%%@doc   删除数据，返回数据文件长度
%% @spec  send_delete({Ets, Deleted, Length}, Key, Time, {File, Loc, KSize, VSize}, From, {File, Pid}, WSize) -> return()
%% where
%%  return() =  integer() | Error
%%@end
%% -----------------------------------------------------------------
send_delete({_Ets, Deleted, _Length}, Key, Time,
	{File, Loc, KSize, _VSize}, From, {File, Pid}, WSize) ->
	%数据在读写文件中
	delete_pid(Pid, Loc, Key, Time, KSize, From),
	ets:insert(Deleted, {Key, Loc, KSize}),
	WSize;
send_delete({_Ets, Deleted, Length}, Key, Time,
	{_File, _Loc, KSize, _VSize}, From, {_, Pid}, WSize) ->
	%数据在读文件中
	delete_pid(Pid, WSize, Key, Time, term_to_binary(Key, [{minor_version, 1}]), KSize, Length, From),
	ets:insert(Deleted, {Key, WSize, KSize}),
	WSize + Length.

%% -----------------------------------------------------------------
%%@doc   从存储中读入数据，向存储进程发消息
%% @spec  send_dyn_read(Args, Reads, Key, {File, Loc, KSize, VSize}, Vsn, Time, Fun, From) -> return()
%% where
%%  return() =  {Loc, Size, From, Ref}
%%@end
%% -----------------------------------------------------------------
send_dyn_read(_Args, Reads, Key, {File, Loc, KSize, VSize}, Vsn, Time, Fun, From) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?KEY_VSN_TIME_SIZE,
		VSize, self(), {dyn_read, Key, Vsn, Time, Fun, From}).

%% -----------------------------------------------------------------
%%@doc   往存储写入数据，向存储进程发消息
%% @spec  send_dyn_write(Args, Reads, Key, {File, Loc, KSize, VSize}, Vsn, Time, Fun, From) -> return()
%% where
%%  return() =  {Loc, Size, From, Ref}
%%@end
%% -----------------------------------------------------------------
send_dyn_write(_Args, Reads, Key, {File, Loc, KSize, VSize}, Vsn, Time, Fun, From) ->
	zm_file:send_read(sb_trees:get(File, Reads),
		Loc + KSize + ?KEY_VSN_TIME_SIZE,
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
read_bins({_Ets, _Deleted, Length} = Args, Cache, File, Start, Bin, Suffix, IgnoreError) ->
	case Suffix of
		<<>> ->
			read_bin(Args, Cache, File, Start, Bin, 0, IgnoreError);
		_ ->
			Len1 = byte_size(Suffix),
			Len2 = Length - Len1,
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
			read_bin(Args, Cache, File, Start + Len2, Bin, Len2, IgnoreError)
	end.

% 读取二进制数据中的记录
read_bin({_Ets, _Deleted, Length} = Args, Cache, File, Start, Bin, Offset, IgnoreError) ->
	case Bin of
		<<_:Offset/binary, Block:Length/binary, _/binary>> ->
			read_block(Args, Cache, File, Start + Offset, IgnoreError, Block),
			read_bin(Args, Cache, File, Start, Bin, Offset + Length, IgnoreError);
		<<_:Offset/binary, SubBin/binary>> ->
			SubBin
	end.

% 读取二进制数据块中的记录
read_block(Args, Cache, File, Offset, IgnoreError, Block) ->
	case Block of
		<<KSize:16, K:KSize/binary, Vsn:32, Time:48,
			VSize:32, V:VSize/binary, _/binary>> ->
			try read_entry(Args, Cache, File,
					Offset, KSize, K, Vsn, Time, VSize, V) of
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

% 读取键长度错误
read_entry(_Args, _Cache, _File, _Offset, 0, _K, _Vsn, _Time, _VSize, _V) ->
	empty_entry_key;
% 只读数据，如果版本为0，表示删除
read_entry({Ets, _Deleted, _Length}, none, _File, _Offset, _KSize, K, 0, _Time, _VSize, _V) ->
	Key = binary_to_term(K),
	ets:delete(Ets, Key);
% 读写数据，如果版本为0，表示删除
read_entry({Ets, Deleted, _Length}, Cache, _File, Offset, KSize, K, 0, _Time, _VSize, _V) ->
	Key = binary_to_term(K),
	ets:delete(Ets, Key),
	zm_cache:delete(Cache, Key),
	ets:insert(Deleted, {Key, Offset, KSize});
% 只读数据，读取键值和版本时间
read_entry({Ets, _Deleted, _Length}, none, File, Offset, KSize, K, Vsn, Time, VSize, _V) ->
	Key = binary_to_term(K),
	ets:insert(Ets, {Key, {File, Offset, KSize, VSize}, Vsn, Time});
% 读写数据，读取键值和版本时间
read_entry({Ets, _Deleted, _Length}, Cache, File, Offset, KSize, K, Vsn, Time, VSize, V) ->
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

% 写入已有数据，向文件进程发送消息
write_pid(Pid, Loc, Key, Vsn, Time, KSize, V, VSize, From) ->
	zm_file:send_write(Pid, Loc + ?KEY_LEN_SIZE + KSize,
		[<<Vsn:32, Time:48, VSize:32>>, V], self(), {write, Key, Vsn, Time, From}).

% 新增写入数据，向文件进程发送消息
write_pid(Pid, Loc, Key, Vsn, Time, K, KSize, V, VSize, BlockSize, From) ->
	zm_file:send_write(Pid, Loc, [<<KSize:16>>, K, <<Vsn:32, Time:48, VSize:32>>, V,
		<<0:((BlockSize - KSize - ?KEY_VSN_TIME_SIZE - VSize) * 8)>>],
		self(), {write, Key, Vsn, Time, From}).

%删除已有数据，向文件进程发送消息
delete_pid(Pid, Loc, Key, Time, KSize, From) ->
	zm_file:send_write(Pid, Loc + ?KEY_LEN_SIZE + KSize,
		<<0:32, Time:48>>, self(), {delete, Key, Time, From}).

% 新增删除数据，向文件进程发送消息
delete_pid(Pid, Loc, Key, Time, K, KSize, BlockSize, From) ->
	zm_file:send_write(Pid, Loc, [<<KSize:16>>, K,
		<<0:32, Time:48, 0:((BlockSize - KSize - ?KEY_VSN_TIME_SIZE) * 8)>>],
		self(), {delete, Key, Time, From}).
