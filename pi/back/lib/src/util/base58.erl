%%@doc base 58 编解码
%%```
%%% A simple Erlang Module to convert a binary or an integer into its base58 equivalent 
%%% @reference <a href="https://en.bitcoin.it/wiki/Base58Check_encoding">Base58Check_encoding</a>
%%%  
%%% The encoding and decoding functions can be used to decode Bitcoin based base58 string,
%%% for example "6UwLL9Risc3QfPqBUvKofHmBQ7wMtjvM" can be decoded to 16#00010966776006953D5567439E5E39F86A0D273BEED61967F
%%%
%%% This module is distributed under an Apache 2.0 Licence
%%'''
%%@end

-module(base58).
-description("base58").
-author({'David Ellefsen'}).

%%%=======================EXPORT=======================
-export([check/1, encode_int/1, decode_int/1, encode/1, decode/1]).

%%%=======================INLINE=======================
-compile({inline, [b58char/1, charb58/1, encode_int/2, decode_int/2]}).
-compile({inline_size, 32}).


%%%=================EXPORTED FUNCTIONS=================
%% @doc This is an internal function that encodes the equivalent base58 number
%% to the corresponding alphabet character.
%%
%% @spec b58char(integer()) -> byte()

-spec b58char(integer()) -> byte().
b58char(0) -> $1;
b58char(1) -> $2;
b58char(2) -> $3;
b58char(3) -> $4;
b58char(4) -> $5;
b58char(5) -> $6;
b58char(6) -> $7;
b58char(7) -> $8;
b58char(8) -> $9;
b58char(9) -> $A;
b58char(10) -> $B;
b58char(11) -> $C;
b58char(12) -> $D;
b58char(13) -> $E;
b58char(14) -> $F;
b58char(15) -> $G;
b58char(16) -> $H;
b58char(17) -> $J;
b58char(18) -> $K;
b58char(19) -> $L;
b58char(20) -> $M;
b58char(21) -> $N;
b58char(22) -> $P;
b58char(23) -> $Q;
b58char(24) -> $R;
b58char(25) -> $S;
b58char(26) -> $T;
b58char(27) -> $U;
b58char(28) -> $V;
b58char(29) -> $W;
b58char(30) -> $X;
b58char(31) -> $Y;
b58char(32) -> $Z;
b58char(33) -> $a;
b58char(34) -> $b;
b58char(35) -> $c;
b58char(36) -> $d;
b58char(37) -> $e;
b58char(38) -> $f;
b58char(39) -> $g;
b58char(40) -> $h;
b58char(41) -> $i;
b58char(42) -> $j;
b58char(43) -> $k;
b58char(44) -> $m;
b58char(45) -> $n;
b58char(46) -> $o;
b58char(47) -> $p;
b58char(48) -> $q;
b58char(49) -> $r;
b58char(50) -> $s;
b58char(51) -> $t;
b58char(52) -> $u;
b58char(53) -> $v;
b58char(54) -> $w;
b58char(55) -> $x;
b58char(56) -> $y;
b58char(57) -> $z.

%% @doc This is an internal function that decodes a base58 character into its equivalent value
%%
%% @spec charb58(byte()) -> integer()

-spec charb58(byte()) -> integer().
charb58($1) -> 0;
charb58($2) -> 1;
charb58($3) -> 2;
charb58($4) -> 3;
charb58($5) -> 4;
charb58($6) -> 5;
charb58($7) -> 6;
charb58($8) -> 7;
charb58($9) -> 8;
charb58($A) -> 9;
charb58($B) -> 10;
charb58($C) -> 11;
charb58($D) -> 12;
charb58($E) -> 13;
charb58($F) -> 14;
charb58($G) -> 15;
charb58($H) -> 16;
charb58($J) -> 17;
charb58($K) -> 18;
charb58($L) -> 19;
charb58($M) -> 20;
charb58($N) -> 21;
charb58($P) -> 22;
charb58($Q) -> 23;
charb58($R) -> 24;
charb58($S) -> 25;
charb58($T) -> 26;
charb58($U) -> 27;
charb58($V) -> 28;
charb58($W) -> 29;
charb58($X) -> 30;
charb58($Y) -> 31;
charb58($Z) -> 32;
charb58($a) -> 33;
charb58($b) -> 34;
charb58($c) -> 35;
charb58($d) -> 36;
charb58($e) -> 37;
charb58($f) -> 38;
charb58($g) -> 39;
charb58($h) -> 40;
charb58($i) -> 41;
charb58($j) -> 42;
charb58($k) -> 43;
charb58($m) -> 44;
charb58($n) -> 45;
charb58($o) -> 46;
charb58($p) -> 47;
charb58($q) -> 48;
charb58($r) -> 49;
charb58($s) -> 50;
charb58($t) -> 51;
charb58($u) -> 52;
charb58($v) -> 53;
charb58($w) -> 54;
charb58($x) -> 55;
charb58($y) -> 56;
charb58($z) -> 57.

%% @doc Check to see if a passed Base58 string contains the correct characters.
%% 'true' will be returned if the string contains the correct characters and
%% 'false' will be returned otherwise.
%%
%% @spec check(base58()) -> boolean()
%% @type base58() = string().

-spec check(base58()) -> boolean().
-type base58() :: string().
check(Base58) ->
	try
		[C || C <- Base58, charb58(C) =:= 58] =:= []
	catch
		_:_ ->
			false
	end.

%% @doc Convert an unsigned integer into its Base58 equivalent.
%%
%% @spec encode_int(integer(), list()) -> base58()

-spec encode_int(integer(), list()) -> base58().
encode_int(I, L) when I > 0 ->
	encode_int(I div 58, [b58char(I rem 58) | L]);
encode_int(0, L) -> L.

%% @doc Convert an unsigned integer into its Base58 equivalent.
%%
%% @spec encode_int(integer()) -> base58()

-spec encode_int(integer()) -> base58().
encode_int(I) when I > 0 ->
	encode_int(I, []);
encode_int(0) -> [$1].

%% @doc Convert a Base58 string into a unsigned integer value. This is an
%% internal function that is not exposed to the user.
%%
%% @spec decode_int(char(),base58()) -> integer()

-spec decode_int(char(),base58()) -> integer().
decode_int(C, [X | Xs]) ->
	decode_int(C * 58 + charb58(X), Xs);
decode_int(C, []) -> C.

%% @doc Convert a Base58 string into a unsigned integer value.
%% The Base58 string must be encoded in a big-endian representation.
%%
%% @spec decode_int(base58()) -> integer()

-spec decode_int(base58()) -> integer().
decode_int([Char | Str]) ->
	decode_int(charb58(Char), Str).

%% @doc Convert a binary into a Base58 encoded string. The resulting Base58
%% encoded string will be in a big-endian representation of the original binary.
%%
%% @spec encode(binary()) -> base58()

-spec encode(binary()) -> base58().
encode(Binary) ->
	encode_int(binary:decode_unsigned(Binary)).

%% @doc Convert a Base58 string into the binary equivalent of the unsigned
%% integer representation. The Base58 string must be encoded in a big-endian
%% representation.
%%
%% @spec decode(base58()) -> binary()

-spec decode(base58()) -> binary().
decode(Base58) ->
	binary:encode_unsigned(decode_int(Base58)).
