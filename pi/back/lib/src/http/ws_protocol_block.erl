%% @author Administrator
%% @doc @todo Add description to ws_protocol_block.

-module(ws_protocol_block).

%% ====================================================================
%% Include files
%% ====================================================================

%%当前支持的版本
-define(SUPPORT_VERSION, [13]).
-define(FRAMING_FIN, 16#0).
-define(TAIL_FRAME_FIN, 16#1).
-define(FOLLOW_UP_OPCODE, 16#0).
-define(TEXT_FRAME_OPCODE, 16#1).
-define(BINARY_FRAME_OPCODE, 16#2).
-define(CLOSE_CONNECT_OPCODE, 16#8).
-define(PING_FRAME_OPCODE, 16#9).
-define(PONG_FRAME_OPCODE, 16#a).
-define(DEFAULT_MASK, 16#1).
-define(UN_MASK, 16#0).
-define(TWO_BYTE_LEN_FLAG, 16#7e).
-define(EIGHT_BYTE_LEN_FLAG, 16#7f).
-define(BYTE_MAX_LEN, 16#7d).
-define(TWO_BYTE_MAX_LEN, 16#ffff).
-define(EIGHT_BYTE_MAX_LEN, 16#ffffffffffffffff).
%%默认的解析字符编码
-define(DEFAULT_CHARSET, utf8).
-define(CMD_BASE64_DOWN, "base64_down").
-define(CMD_BASE64_PARARM, "data").

%% ====================================================================
%% Exported Functions
%% ====================================================================
-export([init/2, supported/1, encode/3, decode/4, encode_base64/3, decode_base64/2, mask/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%% -----------------------------------------------------------------
%% Func: init/2
%% Description: 初始化参数
%% Returns: {RVersion, SVersion, RKey, SKey}
%% -----------------------------------------------------------------
init({M, A}, MFA) ->
	{M, M:init(A, MFA)}.

%% -----------------------------------------------------------------
%% Func: supported/3
%% Description: 判断版本是否支持
%% Returns: true | false
%% -----------------------------------------------------------------
supported(Version) when is_integer(Version) ->
	lists:member(Version, ?SUPPORT_VERSION);
supported(Version) when is_list(Version) ->
	supported(list_to_integer(Version));
supported(_) ->
	false.

%% -----------------------------------------------------------------
%% Func: encode/3
%% Description: 将消息转换成二进制数据
%% Returns: {ok, tuple(), iolist()} | {error, Reason}
%% -----------------------------------------------------------------
encode(Args, IsMask, Term) ->
	case writeN(Args, IsMask, Term) of
		{NewArgs, Data} ->
			{ok, NewArgs, Data};
		Data ->
			{ok, Args, Data}
	end.

%% -----------------------------------------------------------------
%% Func: decode/4
%% Description: 将二进制数据转换成消息
%% Returns: {ok, tuple()} | {ok, tuple(), list()} | {error, Reason}
%% -----------------------------------------------------------------
decode(Args, Cache, Bin, Queue) ->
	read8(Args, Cache, Bin, Queue).

%% -----------------------------------------------------------------
%% Func: encode_base64/3
%% Description: 将消息转换成二进制数据并进行base64编码
%% Returns: {ok, tuple(), base64()} | {error, Reason}
%% -----------------------------------------------------------------
encode_base64({M, A}, IsMask, {_, [_|_] = KVList} = Msg) ->
	case proplists:get_value("", KVList) of
		undefined ->
			{error, invalid_seq};
		Seq ->
			{ok, NewA, Data} = M:encode(A, Msg),
			Bin=list_to_binary(Data),
			Size=size(Bin),
			{ok, {M, NewA}, write_text([{?CMD_BASE64_PARARM, base64:encode_to_string(<<Size:16, Bin/binary>>)}], IsMask, ?CMD_BASE64_DOWN, Seq, [])}
	end.

%% -----------------------------------------------------------------
%% Func: decode_base64/2
%% Description: 将字符串base64解码为二进制数据并转换成消息
%% Returns: {ok, tuple(), list()} | {error, Reason}
%% -----------------------------------------------------------------
decode_base64({M, A}, Data) ->
	case M:decode(A, base64:decode(Data)) of
		{ok, NewA, Msg} ->
			{ok, {M, NewA}, Msg};
		E ->
			E
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================
%%写N个节字
writeN(_, IsMask, {Cmd, [{"", Seq} | KVList]}) when is_list(Seq) ->
	write_text(KVList, IsMask, Cmd, Seq, []);
writeN({M, A}, IsMask, {_, KVList} = Msg) when is_list(KVList) ->
	%%返回二进制数据
	{ok, NewA, Data} = M:encode(A, Msg),
	Bin=list_to_binary(Data),
	Size=size(Bin),
	{{M, NewA}, write32(IsMask, ?BINARY_FRAME_OPCODE, <<Size:16, Bin/binary>>)};
writeN(_, IsMask, {_, <<_:64>> = HandData}) ->
	write32(IsMask, ?BINARY_FRAME_OPCODE, HandData).

%%写文本
write_text([{K, V} | T], IsMask, Cmd, Seq, IOList) ->
	Key=to_string(K),
	Value=to_string(V),
	write_text(T, IsMask, Cmd, Seq, [Value, "=", Key, "&"|IOList]);
write_text([], IsMask, Cmd, Seq, IOList) when is_integer(Seq) ->
	write32(IsMask, ?TEXT_FRAME_OPCODE, unicode:characters_to_binary([Cmd, "?=", integer_to_list(Seq)|lists:reverse(IOList)], ?DEFAULT_CHARSET));
write_text([], IsMask, Cmd, Seq, IOList) when is_list(Seq) ->
	write32(IsMask, ?TEXT_FRAME_OPCODE, unicode:characters_to_binary([Cmd, "?=", Seq|lists:reverse(IOList)], ?DEFAULT_CHARSET)).

to_string(Term) when is_list(Term) ->
	Term;
to_string(Term) when is_atom(Term) ->
	atom_to_list(Term);
to_string(Term) when is_integer(Term) ->
	integer_to_list(Term);
to_string(Term) when is_float(Term) ->
	float_to_list(Term);
to_string(_) ->
	erlang:error(invalid_term).

%%写32位
write32(true, Opcode, Data) ->
	MaskKey=random_mask_key(4, <<>>),
	PayLoad=mask(Data, MaskKey),
	write7(Opcode, ?DEFAULT_MASK, size(PayLoad), <<MaskKey/binary, PayLoad/binary>>);
write32(false, Opcode, PayLoad) ->
	if
		is_binary(PayLoad) ->
			write7(Opcode, ?UN_MASK, size(PayLoad), PayLoad);
		true ->
			write7(Opcode, ?UN_MASK, length(PayLoad), list_to_binary(PayLoad))
	end.

random_mask_key(0, MaskKey) ->
	MaskKey;
random_mask_key(N, MaskKey) ->
	Byte=z_lib:random(1, 255),
	random_mask_key(N - 1, <<MaskKey/binary, Byte:8>>).

%%写7位
write7(Opcode, Mask, Size, Bin) when Size =< ?BYTE_MAX_LEN ->
	write8(Opcode, <<Mask:1, Size:7, Bin/binary>>);
write7(Opcode, Mask, Size, Bin) when Size =< ?TWO_BYTE_MAX_LEN ->
	write8(Opcode, <<Mask:1, ?TWO_BYTE_LEN_FLAG:7, Size:16, Bin/binary>>);
write7(Opcode, Mask, Size, Bin) when Size =< ?EIGHT_BYTE_MAX_LEN ->
	write8(Opcode, <<Mask:1, ?EIGHT_BYTE_LEN_FLAG:7, Size:64, Bin/binary>>);
write7(_, _, Size, _) ->
	erlang:error({invalid_data_len, Size}).

%%写8位
write8(Opcode, Bin) ->
	<<?TAIL_FRAME_FIN:1, 0:1, 0:1, 0:1, Opcode:4, Bin/binary>>.

%%读8位
read8(Args, Cache, <<Fin:1, 0:1, 0:1, 0:1, Opcode:4, Bin/binary>>, Queue) ->
	case read7(Args, Cache, Fin, Opcode, Bin) of
		{continue, NewCache, NewArgs, <<>>} ->
			{NewArgs, queue:in_r({continue, queue:len(Queue), NewCache}, Queue)};
		{continue, NewCache, NewArgs, Next} ->
			read8(NewArgs, NewCache, Next, Queue);
  		{NewCache, NewArgs, Data, <<>>} ->
			{NewArgs, queue:in({NewCache, Data}, Queue)};
		{NewCache, NewArgs, Data, Next} ->
			read8(NewArgs, NewCache, Next, queue:in({Cache, Data}, Queue));
		{_, NewArgs, wait} ->
			{NewArgs, queue:in_r({wait, queue:len(Queue)}, Queue)};
		{_, NewArgs, closed} ->
			%前台主动关闭连接
			{NewArgs, queue:in_r(closed, Queue)}
	end;
read8(_, _, _, _) ->
	erlang:error(invalid_frame).

%%读7位
read7(Args, {?BINARY_FRAME_OPCODE, Cache}, ?FRAMING_FIN, ?FOLLOW_UP_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readNext(Cache, Args, fun(Data, Next) -> 
							{continue, {?BINARY_FRAME_OPCODE, [Data|Cache]}, Args, Next}
		  end, Len, Mask, Bin);
read7({M, A} = Args, {?BINARY_FRAME_OPCODE, Cache}, ?TAIL_FRAME_FIN, ?FOLLOW_UP_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readNext(Cache, Args, fun(Data, Next) -> 
							case M:decode(A, list_to_binary(lists:reverse([Data|Cache]))) of
								{ok, NewA, Msg} ->
									{{0, []}, {M, NewA}, Msg, Next};
								{error, Reason} ->
									erlang:error(Reason)
							end
		  end, Len, Mask, Bin);
read7(Args, {0, Cache}, ?FRAMING_FIN, ?BINARY_FRAME_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readN(Cache, Args, fun(Data, Next) ->
							{continue, {?BINARY_FRAME_OPCODE, [Data|Cache]}, Args, Next}
		  end, Len, Mask, Bin);
read7({M, A} = Args, Cache, ?TAIL_FRAME_FIN, ?BINARY_FRAME_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readN(Cache, Args, fun(Data, Next) ->
							case M:decode(A, Data) of
								{ok, NewA, Msg} ->
									{Cache, {M, NewA}, Msg, Next};
								{error, Reason} ->
									erlang:error(Reason)
							end
		  end, Len, Mask, Bin);
read7(Args, {?TEXT_FRAME_OPCODE, Cache}, ?FRAMING_FIN, ?FOLLOW_UP_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readNext(Cache, Args, fun(Data, Next) -> 
							{continue, {?TEXT_FRAME_OPCODE, [Data|Cache]}, Args, Next}
		  end, Len, Mask, Bin);
read7(Args, {?TEXT_FRAME_OPCODE, Cache}, ?TAIL_FRAME_FIN, ?FOLLOW_UP_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readNext(Cache, Args, fun(Data, Next) -> 
							   {{0, []}, Args, to_list(list_to_binary(lists:reverse([Data|Cache]))), Next}
		  end, Len, Mask, Bin);
read7(Args, {0, Cache}, ?FRAMING_FIN, ?TEXT_FRAME_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readNext(Cache, Args, fun(Data, Next) ->
							{continue, {?TEXT_FRAME_OPCODE, [Data|Cache]}, Args, Next}
		  end, Len, Mask, Bin);
read7(Args, Cache, ?TAIL_FRAME_FIN, ?TEXT_FRAME_OPCODE, <<Mask:1, Len:7, Bin/binary>>) ->
	readNext(Cache, Args, fun(Data, Next) -> 
							{Cache, Args, to_list(Data), Next}
			   end, Len, Mask, Bin);
read7(Args, Cache, ?TAIL_FRAME_FIN, Opcode, <<>>) when Opcode =:= ?BINARY_FRAME_OPCODE; Opcode =:= ?TEXT_FRAME_OPCODE; Opcode =:= ?FOLLOW_UP_OPCODE ->
	{Cache, Args, wait};
read7(Args, Cache, ?FRAMING_FIN, Opcode, <<>>) when Opcode =:= ?BINARY_FRAME_OPCODE; Opcode =:= ?TEXT_FRAME_OPCODE; Opcode =:= ?FOLLOW_UP_OPCODE ->
	{Cache, Args, wait};
read7(_, _, ?TAIL_FRAME_FIN, ?PING_FRAME_OPCODE, Bin) ->
	%TODO 当前浏览器并没实现pingpong...
	io:format("!!!!!!!!!!!!!websocket ping~n"),
	ok;
read7(Cache, Args, ?TAIL_FRAME_FIN, ?CLOSE_CONNECT_OPCODE, _) ->
	{Cache, Args, closed};
read7(_, _, _, _, _) ->
	%其它操作码直接忽略
	{error, ignore}.

%%读N个字节
readN(Cache, Args, F, Len, Mask, Bin) ->
	case readN(Len, Mask, Bin) of
		{_, _, wait} ->
			{Cache, Args, wait};
		{S, Size, {Bin_, Next}} ->
			<<_:S, Data:Size/binary>> = Bin_,
			F(Data, Next)
	end.

%%读N个字节
readNext(Cache, Args, F, Len, Mask, Bin) ->
	case readNext(Len, Mask, Bin) of
		{_, wait} ->
			{Cache, Args, wait};
		{Size, {Bin_, Next}} ->
			<<Data:Size/binary>> = Bin_,
			F(Data, Next)
	end.

readN(?TWO_BYTE_LEN_FLAG, Mask, <<RealLen:16, Bin/binary>>) ->
	{16, RealLen - 2, valid_integrity(Mask, RealLen, Bin)};
readN(?EIGHT_BYTE_LEN_FLAG, Mask, <<RealLen:64, Bin/binary>>) ->
	{64, RealLen - 8, valid_integrity(Mask, RealLen, Bin)};
readN(RealLen, Mask, Bin) ->
	{16, RealLen - 2, valid_integrity(Mask, RealLen, Bin)}.

readNext(?TWO_BYTE_LEN_FLAG, Mask, <<RealLen:16, Bin/binary>>) ->
	{RealLen, valid_integrity(Mask, RealLen, Bin)};
readNext(?EIGHT_BYTE_LEN_FLAG, Mask, <<RealLen:64, Bin/binary>>) ->
	{RealLen, valid_integrity(Mask, RealLen, Bin)};
readNext(RealLen, Mask, Bin) ->
	{RealLen, valid_integrity(Mask, RealLen, Bin)}.

valid_integrity(Mask, RealLen, Bin) when size(Bin) >= RealLen ->
	read32(Mask, RealLen, Bin);
valid_integrity(_, _, _) ->
	wait.

%%读32位
read32(?DEFAULT_MASK, RealLen, Bin) ->
	<<MaskKey:4/binary, PayLoad:RealLen/binary, Next/binary>> = Bin,
	{mask(PayLoad, MaskKey), Next};
read32(_, RealLen, Bin) ->
	<<PayLoad:RealLen/binary, Next/binary>> = Bin,
	{PayLoad, Next}.

mask(PayLoad, <<MA:8, MB:8, MC:8, MD:8>>) ->
	mask(PayLoad, MA, MB, MC, MD, size(PayLoad), <<>>).

mask(_, _, _, _, _, 0, Data) ->
	Data;
mask(PayLoad, MA, _, _, _, 1, Data) ->
	<<A:8>> = PayLoad,
	<<Data/binary, (A bxor MA)>>;
mask(PayLoad, MA, MB, _, _, 2, Data) ->
	<<A:8, B:8>> = PayLoad,
	<<Data/binary, (A bxor MA), (B bxor MB)>>;
mask(PayLoad, MA, MB, MC, _, 3, Data) ->
	<<A:8, B:8, C:8>> = PayLoad,
	<<Data/binary, (A bxor MA), (B bxor MB), (C bxor MC)>>;
mask(PayLoad, MA, MB, MC, MD, _, Data) ->
	<<A:8, B:8, C:8, D:8, Next/binary>> = PayLoad,
	mask(Next, MA, MB, MC, MD, size(Next), <<Data/binary, (A bxor MA), (B bxor MB), (C bxor MC), (D bxor MD)>>).

to_list(Data) ->
	case unicode:characters_to_list(Data, ?DEFAULT_CHARSET) of
		{incomplete, _, _} = R ->
			erlang:error(R);
		{error, _, _} = R ->
			erlang:error(R);
		L ->
			L
	end.


	


