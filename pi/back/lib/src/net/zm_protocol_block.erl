%%%@doc 基于块的通讯协议处理模块
%%```
%%% 消息格式为：[2字节消息的长度+1消息的版本信息]
%%% 版本信息为：[4个位的version+1个位的encryption+1个位的crc+1个位的compress+1个位的kv类型]
%%% kv类型为0表示消息为二进制数据，为1表示消息为KeyValue类型。
%%% key为字符串，Value为标准格式的数据或：1字节整数，4字节整数，字符串，二进制数据。
%%% compress为0表示无压缩，为1表示进行zlib压缩。有压缩字节的下限，默认为64字节。
%%% crc为0表示无效验，为1表示进行adler32效验。
%%% encryption为0表示不加密。为1表示使用异或算法进行加密。
%%% 通讯如果采用加密，则连接建立时，由服务器端发送2个挑战码(每个4字节），分别对应发送和接收。
%%% 解码过程时，如果为压缩数据，为了防止溢出攻击，解压缩时会判断解压数据是否超过4194304字节。
%%% 编码过程是先处理compress，然后是crc（注意！如果有压缩就无效验），最后是encryption。解码过程反之。
%%'''
%%@end


-module(zm_protocol_block).

-description("data <-> msg").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([init/2, encode/2, decode/2, get_rpk/1, get_spk/1]).

%%%=======================DEFINE=======================
-define(KV, 2#1).
-define(COMPRESS, 2#10).
-define(CHECKSUM, 2#100).
-define(XOR_ENCRYPTION, 2#1000).
-define(XXTEA_ENCRYPTION, 2#10000).

-define(COMPRESS_SIZE, 16#1f).

-define(UNCOMPRESS_LIMIT, 16#400000).

-define(BYTE, 97).
-define(INT32, 98).
-define(STRING, 107).
-define(BINARY, 109).
-define(List, 108).
-define(TermHead, 131).
-define(NEW_FLOAT_TAG,70).

-define(DICT_NAME, lists:concat([?MODULE, '_list_to_str'])).  %%是否把列表尽量转成字符串

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 初始化参数
%% @spec init(Argus::tuple(),Socket) -> return()
%% where
%% return() =  {RVersion, SVersion, RKey, SKey}
%%@end
%% -----------------------------------------------------------------
init({RVersion, SVersion}, {M, F, A}) ->
	put(?DICT_NAME, true),
	RVersion1 = zm_con:format_version(RVersion),
	SVersion1 = zm_con:format_version(SVersion),
	RPK = get_rpk(RVersion1),
	SPK = get_spk(SVersion1),
	M:F(A, RPK, SPK),
	{RVersion1, SVersion1, zm_con:get_encryption(RPK), zm_con:get_encryption(SPK)};
init({RVersion, SVersion}, Socket) ->
	put(?DICT_NAME, true),
	RVersion1 = zm_con:format_version(RVersion),
	SVersion1 = zm_con:format_version(SVersion),
	RPK = get_rpk(RVersion1),
	SPK = get_spk(SVersion1),
	zm_con:send_pk(Socket, RPK, SPK),
	{RVersion1, SVersion1, zm_con:get_encryption(RPK), zm_con:get_encryption(SPK)};
init({RVersion, SVersion, ListToStr}, {M, F, A}) ->
	case ListToStr of
		{_, false} ->
			put(?DICT_NAME, false);
		_ ->
			put(?DICT_NAME, true)
	end,
	RVersion1 = zm_con:format_version(RVersion),
	SVersion1 = zm_con:format_version(SVersion),
	RPK = get_rpk(RVersion1),
	SPK = get_spk(SVersion1),
	M:F(A, RPK, SPK),
	{RVersion1, SVersion1, zm_con:get_encryption(RPK), zm_con:get_encryption(SPK)};
init({RVersion, SVersion, ListToStr}, Socket) ->
	case ListToStr of
		{_, false} ->
			put(?DICT_NAME, false);
		_ ->
			put(?DICT_NAME, true)
	end,
	RVersion1 = zm_con:format_version(RVersion),
	SVersion1 = zm_con:format_version(SVersion),
	RPK = get_rpk(RVersion1),
	SPK = get_spk(SVersion1),
	zm_con:send_pk(Socket, RPK, SPK),
	{RVersion1, SVersion1, zm_con:get_encryption(RPK), zm_con:get_encryption(SPK)}.

%% -----------------------------------------------------------------
%%@doc 将消息转换成二进制数据
%% @spec encode(Argus::tuple(),Term) -> return()
%% where
%% return() =  {ok, tuple(), iolist()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
encode({RV, {KV, Compress, Checksum, Encryption} = SV, RK, SK} = Args, Term) ->
	case write_msg(KV, Compress, Checksum, Encryption, SK, Term) of
		{ok, SK, Data} ->
			{ok, Args, Data};
		{ok, SK1, Data} ->
			{ok, {RV, SV, RK, SK1}, Data};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 将二进制数据转换成消息
%% @spec decode(Argus::tuple(),Bin) -> return()
%% where
%% return() = {ok, tuple()} | {ok, tuple(), list()} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
decode({{KV, Compress, Checksum, Encryption} = RV, SV, RK, SK} = Args, Bin) ->
	case read_header(Encryption, Checksum, Compress, KV, RK, Bin) of
		{ok, RK, Data} ->
			{ok, Args, Data};
		{ok, RK1, Data} ->
			{ok, {RV, SV, RK1, SK}, Data};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 获得接收版本对应的挑战码
%% @spec get_rpk(Argus::tuple()) -> integer()
%%@end
%% -----------------------------------------------------------------
get_rpk({_KV, _Compress, _Checksum, false}) ->
	0;
get_rpk(_) ->
	{_M, S, MS} = os:timestamp(),
	z_lib:random(S * 1000 + MS div 1000).

%% -----------------------------------------------------------------
%%@doc 获得发送版本对应的挑战码
%% @spec get_spk(Argus::tuple()) -> integer()
%%@end
%% -----------------------------------------------------------------
get_spk({_KV, _Compress, _Checksum, false}) ->
	0;
get_spk(_) ->
	z_lib:random(erlang:phash2(self())).

%%%===================LOCAL FUNCTIONS==================
% 写入消息
write_msg(true, Compress, Checksum, Encryption, SK, {Cmd, KVList}) ->
	case write_map(KVList, []) of
		L when is_list(L) ->
			write_compress(Compress, Checksum, Encryption, SK,
				write_cmd(Cmd, lists:reverse(L)), ?KV);
		E ->
			E
	end;
write_msg(_, Compress, Checksum, Encryption, SK, {Cmd, Data}) ->
	write_compress(Compress, Checksum, Encryption, SK, write_cmd(Cmd, Data), 0).

% 写入压缩数据
write_compress(true, Checksum, Encryption, SK, Data, KV) ->
	case iolist_size(Data) of
		Size when Size > ?COMPRESS_SIZE ->
			write_checksum(false, Encryption, SK, z_lib:compress(Data), KV, ?COMPRESS);
		_ ->
			write_checksum(Checksum, Encryption, SK, Data, KV, 0)
	end;
write_compress(_, Checksum, Encryption, SK, Data, KV) ->
	write_checksum(Checksum, Encryption, SK, Data, KV, 0).

% 写入效验数据
write_checksum(true, Encryption, SK, Data, KV, Compress) ->
	Crc = erlang:adler32(Data),
	D = Crc band 16#ff,
	C = (Crc bsr 8) band 16#ff,
	B = (Crc bsr 16) band 16#ff,
	A = (Crc bsr 24) band 16#ff,
	write_encryption(Encryption, SK, [A, B, C, D | Data], KV, Compress, ?CHECKSUM);
write_checksum(_, Encryption, SK, Data, KV, Compress) ->
	write_encryption(Encryption, SK, Data, KV, Compress, 0).

% 写入加密数据
write_encryption({_, _} = Encryption, SK, Data, KV, Compress, CheckSum) ->
	SK1 = zm_con:next_encryption(SK),
	write_header(SK1, zm_con:encode(Encryption, Data, SK1), KV, Compress, CheckSum, ?XXTEA_ENCRYPTION);
write_encryption('xor', SK, Data, KV, Compress, CheckSum) ->
	SK1 = zm_con:next_encryption(SK),
	write_header(SK1, zm_con:encode('xor', Data, SK1), KV, Compress, CheckSum, ?XOR_ENCRYPTION);
write_encryption(_, SK, Data, KV, Compress, CheckSum) ->
	write_header(SK, Data, KV, Compress, CheckSum, 0).

% 写入头信息
write_header(SK, Data, KV, Compress, CheckSum, Encryption) ->
	{ok, SK, [(KV + Compress + CheckSum + Encryption) | Data]}.

% 写入消息的命令字
write_cmd(Cmd, Data) when is_atom(Cmd) ->
	Bin = atom_to_binary(Cmd, utf8),
	write_cmd(Bin, byte_size(Bin), Data);
write_cmd(Cmd, Data) when is_binary(Cmd) ->
	write_cmd(Cmd, byte_size(Cmd), Data);
write_cmd(Cmd, Data) when is_list(Cmd) ->
	write_cmd(Cmd, iolist_size(Cmd), Data).

% 写入消息的命令字长度
write_cmd(Cmd, Size, _Data) when Size > 16#ff ->
	erlang:error({overflow, Cmd, Size, 16#ff});
write_cmd(Cmd, Size, Data) when is_binary(Data) ->
	[Size, Cmd, Data];
write_cmd(Cmd, Size, Data) when is_list(Data) ->
	try
		iolist_size(Data),
		[Size, Cmd | Data]
	catch
		_:_ ->
			[Size, Cmd, term_to_binary(Data, [{minor_version, 1}])]
	end;
write_cmd(Cmd, Size, Data) ->
	[Size, Cmd, term_to_binary(Data, [{minor_version, 1}])].

% 写入键值表
write_map([{K, V} | T], L) ->
	write_map(T, write16(V, write8(K, L)));
write_map([_ | T], L) ->
	write_map(T, L);
write_map([], L) ->
	L.

% 写入8位长度的数据
write8(V, L) when is_atom(V) ->
	Bin = atom_to_binary(V, utf8),
	write8(Bin, byte_size(Bin), L);
write8(V, L) when is_list(V) ->
	write8(V, iolist_size(V), L);
write8(V, L) when is_integer(V) ->
	V1 = integer_to_list(V),
	write8(V1, length(V1), L);
write8(V, L) when is_binary(V) ->
	write8(V, byte_size(V), L);
write8(V, L) ->
	Bin = term_to_binary(V, [{minor_version, 1}]),
	write8(Bin, byte_size(Bin), L).

write8(V, Size, L) when Size =< 16#ff ->
	[V, Size | L];
write8(V, Size, _L) ->
	erlang:error({overflow, V, Size, 16#ff}).

% 写入16位长度的数据
write16(V, L) when is_integer(V), V >= 0, V =< 16#ff ->
	[V, ?BYTE, 2, 0 | L];
write16(V, L) when is_integer(V), V >= -16#80000000, V =< 16#7fffffff ->
	[<<V:32>>, ?INT32, 5, 0 | L];
write16(V, L) when is_binary(V) ->
	write16(V, ?BINARY, byte_size(V) + 1, L);
write16([H | _] = V, L) when H > 0, H < 16#80000000 ->
	case get(?DICT_NAME) of
		true ->
			try unicode:characters_to_binary(V, utf8) of
				Bin when is_binary(Bin) ->
					write16(Bin, ?STRING, byte_size(Bin) + 1, L);
				_ ->
					Bin = term_to_binary(V, [{minor_version, 1}]),
					write16(Bin, byte_size(Bin), L)
			catch
				_:_ ->
					Bin = term_to_binary(V, [{minor_version, 1}]),
					write16(Bin, byte_size(Bin), L)
			end;
		_ ->
			Bin = term_to_binary(V, [{minor_version, 1}]),
			write16(Bin, byte_size(Bin), L)
	end;
write16("", L) ->
	case get(?DICT_NAME) of
		true ->
			[?STRING, 1, 0 | L];
		_ ->
			Bin = term_to_binary("", [{minor_version, 1}]),
			write16(Bin, byte_size(Bin), L)
	end;
write16(V, L) ->
	Bin = term_to_binary(V, [{minor_version, 1}]),
	write16(Bin, byte_size(Bin), L).

write16(V, Size, L) when Size =< 16#ffff ->
	[V, Size band 16#ff, Size bsr 8 | L];
write16(V, Size, _L) ->
	erlang:error({overflow, V, Size, 16#ffff}).

write16(V, Type, Size, L) when Size =< 16#ffff ->
	[V, Type, Size band 16#ff, Size bsr 8 | L];
write16(V, _Type, Size, _L) ->
	erlang:error({overflow, V, Size, 16#ffff}).

% 读取头部
read_header(Encryption, Checksum, Compress, KV, RK, Bin) ->
	case Bin of
		<<Version:8, Sub/binary>> ->
			read_encryption(Encryption, Checksum, Compress, KV, RK, Sub, Version);
		_ ->
			erlang:error(none_header)
	end.

% 读取解码数据
read_encryption(false, Checksum, Compress, KV, RK, Bin, Version) ->
	read_checksum(Checksum, Compress, KV, RK, Bin, Version);
read_encryption(Encryption, Checksum, Compress, KV, RK, Bin, Version) ->
	E = case Encryption of
				{_, _} ->
					?XXTEA_ENCRYPTION;
				'xor' ->
					?XOR_ENCRYPTION
			end,
	case Version band E of
		0 ->
			erlang:error(need_encryption);
		_ ->
			RK1 = zm_con:next_encryption(RK),
			read_checksum(Checksum, Compress, KV, RK1,
				zm_con:decode(Encryption, Bin, RK1), Version)
	end.

% 读取效验数据
read_checksum(true, Compress, KV, RK, Bin, Version) when is_binary(Bin) ->
	case Version band ?CHECKSUM of
		0 ->
			read_compress(Compress, KV, RK, Bin, Version);
		_ ->
			case Bin of
				<<Crc:32, Sub/binary>> ->
					case erlang:adler32(Sub) of
						Crc ->
							read_compress(Compress, KV, RK, Sub, Version);
						Crc1 ->
							erlang:error({invalid_crc, Crc, Crc1})
					end;
				_ ->
					erlang:error(none_crc)
			end
	end;
read_checksum(true, Compress, KV, RK, Data, Version) ->
	case Version band ?CHECKSUM of
		0 ->
			read_compress(Compress, KV, RK, Data, Version);
		_ ->
			case Data of
				[A, B, C, D | T] ->
					Crc = (A bsl 24) + (B bsl 16) + (C bsl 8) + D,
					case erlang:adler32(T) of
						Crc ->
							read_compress(Compress, KV, RK, T, Version);
						Crc1 ->
							erlang:error({invalid_crc, Crc, Crc1})
					end;
				_ ->
					erlang:error(none_crc)
			end
	end;
read_checksum(_, Compress, KV, RK, Bin, Version) ->
	case Version band ?CHECKSUM of
		0 ->
			read_compress(Compress, KV, RK, Bin, Version);
		_ ->
			erlang:error(does_not_support_checksum)
	end.

% 读取压缩数据
read_compress(true, KV, RK, Bin, Version) ->
	case Version band ?COMPRESS of
		0 ->
			read_msg(KV, RK, Bin, Version);
		_ ->
			read_msg(KV, RK, z_lib:uncompress(Bin, ?UNCOMPRESS_LIMIT), Version)
	end;
read_compress(_, KV, RK, Bin, Version) ->
	case Version band ?COMPRESS of
		0 ->
			read_msg(KV, RK, Bin, Version);
		_ ->
			erlang:error(does_not_support_compression)
	end.

% 数据转换成二进制
data_to_binary(Bin) when is_binary(Bin) ->
	Bin;
data_to_binary([Bin]) when is_binary(Bin) ->
	Bin;
data_to_binary(Data) ->
	list_to_binary(Data).

% 读取消息数据
read_msg(true, RK, Data, Version) ->
	case Version band ?KV of
		0 ->
			read_cmd(RK, data_to_binary(Data));
		_ ->
			read_cmd_kv(RK, data_to_binary(Data))
	end;
read_msg(_, RK, Data, Version) ->
	case Version band ?KV of
		0 ->
			read_cmd(RK, data_to_binary(Data));
		_ ->
			erlang:error(does_not_support_kv)
	end.

% 读取二进制消息数据
read_cmd(RK, Bin) ->
	case Bin of
		<<0:8, _/binary>> ->
			{ok, RK, {"", {Bin, 1}}};
		<<Len:8, _Data:Len/binary, _/binary>> ->
			{ok, RK, {binary_to_list(Bin, 2, Len + 1), {Bin, Len + 1}}};
		<<Len:8, _/binary>> ->
			erlang:error({none_cmd, Len});
		_ ->
			erlang:error(none_cmd)
	end.

% 读取键值消息数据
read_cmd_kv(RK, Bin) ->
	case Bin of
		<<0:8, _/binary>> ->
			case read8(Bin, 1, []) of
				L when is_list(L) ->
					{ok, RK, {"", L}};
				E ->
					E
			end;
		<<Len:8, _Data:Len/binary, _/binary>> ->
			case read8(Bin, Len + 1, []) of
				L when is_list(L) ->
					{ok, RK, {binary_to_list(Bin, 2, Len + 1), L}};
				E ->
					E
			end;
		<<Len:8, _/binary>> ->
			erlang:error({none_cmd, Len});
		_ ->
			erlang:error(none_cmd)
	end.

% 读取8位数据的键
read8(Bin, Offset, L) ->
	case Bin of
		<<_:Offset/binary, 0:8, _/binary>> ->
			read16(Bin, Offset + 1, "", L);
		<<_:Offset/binary, Len:8, _Data:Len/binary, _/binary>> ->
			read16(Bin, Offset + Len + 1,
				binary_to_list(Bin, Offset + 2, Offset + Len + 1), L);
		<<_:Offset/binary, Len:8, _/binary>> ->
			erlang:error({none_key, byte_size(Bin), Offset, Len});
		_ ->
			L
	end.

% 读取16位数据的值
read16(Bin, Offset, Key, L) ->
	case Bin of
		<<_:Offset/binary, Len:16, _:Len/binary, _/binary>> ->
			read8(Bin, Offset + Len + 2,
				[{Key, read_data(Bin, Offset + 2, Len)} | L]);
		<<_:Offset/binary, Len:16, _/binary>> ->
			erlang:error({none_value, byte_size(Bin), Offset, Len});
		_ ->
			erlang:error({none_value, byte_size(Bin), Offset})
	end.

% 读取16位数据的值
read_data(Bin, Offset, Len) ->
	Len1 = Len - 1,
	case Bin of
		<<_:Offset/binary, ?BYTE:8, V:8, _/binary>> ->
			V;
		<<_:Offset/binary, ?INT32:8, V:32, _/binary>> ->
			if
				V >= 16#80000000 ->
					V - 16#ffffffff - 1;
				true ->
					V
			end;
		<<_:Offset/binary, ?STRING:8, Data:Len1/binary, _/binary>> ->
			unicode:characters_to_list(Data, utf8);
		<<_:Offset/binary, ?BINARY:8, Data:Len1/binary, _/binary>> ->
			Data;
		<<_:Offset/binary, ?NEW_FLOAT_TAG:8, Data:Len1/binary, _/binary>> ->
			binary_to_term(list_to_binary([?TermHead, ?NEW_FLOAT_TAG|binary_to_list(Data)]));
		<<_:Offset/binary, ?List:8, Data:Len1/binary, _/binary>> ->
			%% todo 更改实现方式，提高效率，不要二次转换
			binary_to_term(list_to_binary([?TermHead, ?List|binary_to_list(Data)]));
		<<_:Offset/binary, Data:Len/binary, _/binary>> ->
			binary_to_term(Data, [safe])
	end.
