%%%@doc 连接模块
%%@end


-module(zm_con).

-description("connection").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([format_version/1]).
-export([send_pk/3, get_encryption/1, next_encryption/1, encode/3, decode/3]).
-export([con_count/2, max_count/3, ip_filter/3, statistics/3, statistics/2]).

%%%=======================DEFINE=======================
-define(KV, kv).
-define(COMPRESS, compress).
-define(CHECKSUM, checksum).
-define(XOR_ENCRYPTION, encryption).
-define(XXTEA_ENCRYPTION, xxtea).

-define(RAND_MASK, 123459876).
-define(RAND_A, 16807).
-define(RAND_Q, 127773).
-define(MAX_POSITIVE_INT32, 16#7fffffff).

-define(DEFAULT_VERSION, [encryption, checksum, compress, kv]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 获得格式化后的通讯版本
%% @spec format_version(Args) -> Version::version()
%% where
%% version() = tuple()
%%@end
%% -----------------------------------------------------------------
format_version(Args) ->
	L = if
		is_list(Args) -> Args;
		Args =:= default -> ?DEFAULT_VERSION
	end,
	Encryption=case lists:member(?XOR_ENCRYPTION, L) of
		true ->
			'xor';
		false ->
			case lists:keyfind(?XXTEA_ENCRYPTION, 1, L) of
				R when is_tuple(R) ->
					R;
				false ->
					false
			end
	end,
	{lists:member(?KV, L),
	lists:member(?COMPRESS, L),
	lists:member(?CHECKSUM, L),
	Encryption}.

%% -----------------------------------------------------------------
%%@doc 发送密钥数据
%% @spec send_pk(Socket,RH::integer(),SH::integer()) -> return()
%% where
%% return() =  ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
send_pk(Socket, RH, SH) when RH > 0; SH > 0 ->
	zm_socket:send(Socket, <<RH:32, SH:32>>);
send_pk(_Socket, _, _) ->
	ok.

%% -----------------------------------------------------------------
%%@doc 根据挑战码获得密钥数据
%% @spec get_encryption(H) -> list()
%%@end
%% -----------------------------------------------------------------
get_encryption(H) when H > 0 ->
	H1 = random(H + 11),
	H2 = random(H1 + 13),
	H3 = random(H2 + 17),
	H4 = random(H3 + 19),
	H5 = random(H4 + 23),
	H6 = random(H5 + 29),
	H7 = random(H6 + 31),
	H8 = random(H7 + 37),
	[H1, H2, H3, H4, H5, H6, H7, H8];
get_encryption(_) ->
	[].

%% -----------------------------------------------------------------
%%@doc  根据本次密钥数据获得下次密钥数据
%% @spec next_encryption(L::list()) -> list()
%%@end
%% -----------------------------------------------------------------
next_encryption([_ | _] = L) ->
	[random(H) || H <- L];
next_encryption([]) ->
	[].

%% -----------------------------------------------------------------
%%@doc 加密数据
%% @spec encode('xxtea'|'xor',Data,Key) -> list()
%%@end
%% -----------------------------------------------------------------
encode({?XXTEA_ENCRYPTION, Rounds}, Data, Key) ->
	xxtea64_minus:encrypt(Rounds, Key, Data);
encode('xor', Data, Key) ->
	z_lib:binary_xor(Data, get_xor(Key, [])).

%% -----------------------------------------------------------------
%%@doc 解密数据
%% @spec decode('xor'|{},Data,Key) -> list()
%%@end
%% -----------------------------------------------------------------
decode({?XXTEA_ENCRYPTION, Rounds}, Data, Key) ->
	xxtea64_minus:decrypt(Rounds, Key, Data);
decode('xor', Data, Key) ->
	z_lib:binary_xor(Data, get_xor(Key, [])).

%% -------------------------------------------------------------------
%%@doc 获得当前连接数量
%% @spec con_count(L::list(),N::integer()) -> integer()
%%@end
%% -------------------------------------------------------------------
con_count([H | T], N) when H > 0 ->
	case ets:info(H, size) of
		undefined ->
			con_count(T, N);
		C ->
			con_count(T, C + N)
	end;
con_count([_ | T], N) ->
	con_count(T, N);
con_count([], N) ->
	N.

%% -------------------------------------------------------------------
%%@doc 检查是否超过最大数量
%% @spec max_count(Socket,Contables,ConAmount) -> boolean()
%%@end
%% -------------------------------------------------------------------
max_count(Socket, ConTables, ConAmount) ->
	case con_count(ConTables, 0) of
		C when C > ConAmount ->
			inet:close(Socket),
			false;
		_ ->
			true
	end.

%% -------------------------------------------------------------------
%%@doc ip检查是否能够通过
%% @spec ip_filter(Socket,IP,IpFilter::list()) -> boolean()
%%@end
%% -------------------------------------------------------------------
ip_filter(Socket, IP, IpFilter) when is_list(IpFilter) ->
	ip_filter1(Socket, IP, tuple_size(IP), IpFilter);
ip_filter(_Socket, _IP, _IpFilter) ->
	true.

%% -----------------------------------------------------------------
%%@doc 统计请求、回应及发送的消息的数量和大小
%% @spec statistics(Tree, CmdType, Size) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
statistics(Tree, CmdType, Size) ->
	sb_trees:action(CmdType, fun add/2, Size, Tree).

add(Size, {_, {Amount, DataSize}}) ->
	{Amount + 1, DataSize + Size};
add(Size, none) ->
	{1, Size}.

%% -----------------------------------------------------------------
%%@doc 统计合并，请求、回应及发送的消息的数量和大小
%% @spec statistics(Tree,L) -> sb_trees()
%%@end
%% -----------------------------------------------------------------
statistics(Tree, L) ->
	sb_trees:from_dict(L, fun merge/2, Tree).

merge({Count, Size}, {_, {Amount, DataSize}}) ->
	{Amount + Count, DataSize + Size};
merge(CountSize, none) ->
	CountSize.

%%%===================LOCAL FUNCTIONS==================

% 获得整数的字节表达列表
get_xor([H | T], L) ->
	D = H band 16#ff,
	C = (H bsr 8) band 16#ff,
	B = (H bsr 16) band 16#ff,
	A = (H bsr 24) band 16#ff,
	get_xor(T, [D, C, B, A | L]);
get_xor([], L) ->
	lists:reverse(L).

% ip过滤
ip_filter1(Socket, IP, Size, [H | T]) when tuple_size(H) =:= Size ->
	case ip_filter2(IP, H, Size) of
		true ->
			true;
		_ ->
			ip_filter1(Socket, IP, Size, T)
	end;
ip_filter1(Socket, _, _, _) ->
	inet:close(Socket),
	false.

ip_filter2(IP, Filter, N) when N > 0 ->
	IP1 = element(N, IP),
	case element(N, Filter) of
		IP1 ->
			ip_filter2(IP, Filter, N - 1);
		Filter1 when is_atom(Filter1) ->
			ip_filter2(IP, Filter, N - 1);
		{Min, Max} when IP1 >= Min, IP1 =< Max ->
			ip_filter2(IP, Filter, N - 1);
		Filter1 when is_list(Filter1) ->
			case lists:member(IP1, Filter1) of
				true ->
					ip_filter2(IP, Filter, N - 1);
				_ ->
					false
			end;
		_ ->
			false
	end;
ip_filter2(_, _, _) ->
	true.


%% -----------------------------------------------------------------
%% Function: random/1
%% Description: 随机数生成器，采用倍增同余算法，返回正整数范围的随机数
%% Returns: integer()
%% 由于z_lib中的随机数生成器修改，导致前后台算法不同，挑战码计算错误，所以通讯还是用旧的实现
%% -----------------------------------------------------------------
random(Seed) ->
	% 防止种子为0
	R = Seed bxor ?RAND_MASK,
	% C语言的写法，可防止溢出
	% -define(RAND_R, 2836).
	% K = R div ?RAND_Q,
	% S = ?RAND_A * (R - K * ?RAND_Q) - ?RAND_R * K,
	S = ?RAND_A * R - (R div ?RAND_Q) * ?MAX_POSITIVE_INT32,
	if
		S < 0 ->
			S + ?MAX_POSITIVE_INT32;
		true ->
			S
	end.