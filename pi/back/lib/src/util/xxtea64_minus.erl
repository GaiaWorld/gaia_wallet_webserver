%% @author Administrator
%% @doc xxtea64的简化版本，由外部设置加密轮数.

-module(xxtea64_minus).

%% ====================================================================
%% Include files
%% ====================================================================
-define(DELTA, 16#9e3779b97f4a7800).
-define(MAX_LENGTH, 16#ffffffffffffffff).
-define(BLOCK_LENGTH, 8).
-define(KEY_LENGTH, 32).
%%最小加密轮数
-define(MIN_ROUND, 1).
%%最大加密轮数
-define(MAX_ROUND, 32).

%% ====================================================================
%% Exported Functions
%% ====================================================================
-export([encrypt/3, decrypt/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% -----------------------------------------------------------------
%% Func: encrypt/3
%% Description: 加密, 加密后的数据：1字节填充数据长度 + 加密的数据
%% Returns: Data
%% -----------------------------------------------------------------
encrypt(Rounds, Key, Bin) when is_integer(Rounds), is_binary(Bin), Rounds >= ?MIN_ROUND, Rounds =< ?MAX_ROUND ->
	Size=byte_size(Bin),
	if
		(Size >= 1) and (Size =< ?MAX_LENGTH) ->
			{Fill, Blocks}=align_data(Size, Bin),
			ByteSize=byte_size(Blocks),
			PartSize=ByteSize - ?BLOCK_LENGTH,
			<<_:PartSize/binary, Z:64>> = Blocks,
			<<_:64, Part/binary>> = Blocks,
			Data=encrypt(Rounds, Part, Blocks, parse_key(Key), Z, 0),
			<<Fill:8, Data/binary>>;
		true ->
			erlang:error(invalid_data_length)
	end;
encrypt(Rounds, Key, Data) ->
	iolist_size(Data),
	encrypt(Rounds, Key, iolist_to_binary(Data)).

%% -----------------------------------------------------------------
%% Func: decrypt/3
%% Description: 解密
%% Returns: Data
%% -----------------------------------------------------------------
decrypt(Rounds, Key, Bin) when is_integer(Rounds), is_binary(Bin), Rounds >= ?MIN_ROUND, Rounds =< ?MAX_ROUND ->
	<<Fill:8, Blocks/binary>> = Bin,
	Size=byte_size(Blocks),
	if
		(Size >= 1) and (Size =< ?MAX_LENGTH) ->
			ByteSize=byte_size(Blocks),
			BlockSize=(ByteSize div ?BLOCK_LENGTH),
			PartSize=ByteSize - ?BLOCK_LENGTH,
			<<Y:64, _/binary>> = Blocks,
			<<Part:PartSize/binary, _:64>> = Blocks,
			RealSize=Size - Fill,
			<<Data:RealSize/binary, _/binary>> = decrypt((Rounds * ?DELTA) band ?MAX_LENGTH, BlockSize, reverse(Part), reverse(Blocks), parse_key(Key), Y),
			Data;
		true ->
			erlang:error(invalid_data_length)
	end;
decrypt(Rounds, Key, Data) ->
	iolist_size(Data),
	decrypt(Rounds, Key, iolist_to_binary(Data)).

%% ====================================================================
%% Internal functions
%% ====================================================================

encrypt(0, _, Blocks, _, _, _) ->
	Blocks;
encrypt(Cycles, Part, Blocks, Key, Z, Sum) ->
	NewSum=next_sum(Sum),
	{NewZ, NewBlocks}=encrypt(0, Part, Blocks, Key, NewSum, (NewSum bsr 16) band 3, Z, <<>>),
	<<_:64, NewPart/binary>> = NewBlocks,
	encrypt(Cycles - 1, NewPart, NewBlocks, Key, NewZ, NewSum).

encrypt(P, <<Y:64, Next/binary>>, <<X:64, Bin/binary>>, Key, Sum, E, Z, Result) ->
	NewZ=(X + mx(P, Key, Sum, E, Y, Z)) band ?MAX_LENGTH,
	encrypt(P + 1, Next, Bin, Key, Sum, E, NewZ, <<Result/binary, NewZ:64>>);
encrypt(P, <<>>, <<X:64>>, Key, Sum, E, Z, <<Y:64, _/binary>> = Result) ->
	NewZ=(X + mx(P, Key, Sum, E, Y, Z)) band ?MAX_LENGTH,
	{NewZ, <<Result/binary, NewZ:64>>}.

align_data(Size, Bin) ->
	if
		Size >= ?BLOCK_LENGTH ->
			align_data1(?BLOCK_LENGTH - Size rem ?BLOCK_LENGTH, Bin);
		true ->
			align_data1(?BLOCK_LENGTH - Size, Bin)
	end.

align_data1(1, Bin) ->
	{1, <<Bin/binary, 16#ff:8>>};
align_data1(2, Bin) ->
	{2, <<Bin/binary, 16#ffff:16>>};
align_data1(3, Bin) ->
	{3, <<Bin/binary, 16#ffffff:24>>};
align_data1(4, Bin) ->
	{4, <<Bin/binary, 16#ffffffff:32>>};
align_data1(5, Bin) ->
	{5, <<Bin/binary, 16#ffffffffff:40>>};
align_data1(6, Bin) ->
	{6, <<Bin/binary, 16#ffffffffffff:48>>};
align_data1(7, Bin) ->
	{7, <<Bin/binary, 16#ffffffffffffff:56>>};
align_data1(8, Bin) ->
	{0, Bin}.

parse_key(Bin) when is_binary(Bin) ->
	if
		byte_size(Bin) < ?KEY_LENGTH ->
			erlang:error(invalid_key_length);
		byte_size(Bin) > ?KEY_LENGTH ->
			<<K0:64, K1:64, K2:64, K3:64, _/binary>> = Bin,
			{K0, K1, K2, K3};
		true ->
			<<K0:64, K1:64, K2:64, K3:64>> = Bin,
			{K0, K1, K2, K3}
	end;
parse_key([N0, N1, N2, N3, N4, N5, N6, N7]) when is_integer(N0) ->
	<<K0:64, K1:64, K2:64, K3:64>> = <<N0:32, N1:32, N2:32, N3:32, N4:32, N5:32, N6:32, N7:32>>,
	{K0, K1, K2, K3};
parse_key(Key) when is_list(Key) ->
	parse_key(unicode:characters_to_binary(Key)).

next_sum(Sum) ->
	(Sum + ?DELTA) band ?MAX_LENGTH.

prev_sum(Sum) ->
	(Sum - ?DELTA) band ?MAX_LENGTH.

mx(P, K, Sum, E, Y, Z) ->
	Left=(((Z bsr 6) bxor (Y bsl 3)) + ((Y bsr 4) bxor (Z bsl 5))),
	Right=((Sum bxor Y) + (element(((P band 3) bxor E) + 1, K) bxor Z)),
	(((Z bsr 6) bxor (Y bsl 3)) + ((Y bsr 4) bxor (Z bsl 5))) bxor ((Sum bxor Y) + (element(((P band 3) bxor E) + 1, K) bxor Z)).

decrypt(Sum, BlockSize, Part, Blocks, Key, Y) ->
	{NewY, NewBlocks}=decrypt(BlockSize - 1, Part, Blocks, Key, Sum, (Sum bsr 16) band 3, Y, <<>>),
	case prev_sum(Sum) of
		NewSum when NewSum =/= 0 ->
			<<_:64, NewPart/binary>> = NewBlocks,
			decrypt(NewSum, BlockSize, NewPart, NewBlocks, Key, NewY);
		0 ->
			reverse(NewBlocks)
	end.

decrypt(P, <<Z:64, Next/binary>>, <<X:64, Bin/binary>>, Key, Sum, E, Y, Result) ->
	NewY=(X - mx(P, Key, Sum, E, Y, Z)) band ?MAX_LENGTH,
	decrypt(P - 1, Next, Bin, Key, Sum, E, NewY, <<Result/binary, NewY:64>>);
decrypt(P, <<>>, <<X:64>>, Key, Sum, E, Y, Result) ->
	<<Z:64, _/binary>> = Result,
	NewY=(X - mx(P, Key, Sum, E, Y, Z)) band ?MAX_LENGTH,
	{NewY, <<Result/binary, NewY:64>>}.

reverse(Bin) -> 
	list_to_binary(reverse(binary_to_list(Bin), ?BLOCK_LENGTH, [])).

reverse([], _, L) ->
	lists:concat(L);
reverse(Data, N, L) ->
	{H, T} = lists:split(N, Data),
	reverse(T, N, [H|L]).