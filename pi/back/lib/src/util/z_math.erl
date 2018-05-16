%%@doc 基础数学库函数
%%@end


-module(z_math).

-description("z math").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([ceil/1, floor/1, is_prime/1, prime/2, prime_list/2, log2/1, fib/1, fib_i/1, fib_n/1]).
-export([vec3_dot/2, vec3_cross/2, vec3_length_sq/1, vec3_length/1, vec3_distance_sq/2, vec3_distance/2, vec3_angle_cos/2, vec3_angle/2, vec3_lerp/3, vec3_normalize/1]).
-export([pl_distance/3, line_rect/8]).
-export([matrix2_mul/2, matrix2_pow/2]).
-export([truncating/2]).
-export([random/1, random/2, random/3, dhash/3]).

%%%=======================INLINE=======================
-compile({inline, [is_prime/3, prime_1/1, prime_2/1, prime_list/4, fib/4, matrix2_mul/2, random/3]}).
-compile({inline_size, 32}).

%%%=======================DEFINE=======================
-define(MAX_POSITIVE_INT32, 16#7fffffff).

-define(RAND_A, 16807).
-define(RAND_Q, 127773).
-define(RAND_MASK, 123459876).

-define(RANDOM_SEED, '$RandomSeed').

-define(CEIL(N), case trunc(N) of X when N > X -> X + 1; X -> X end).
-define(FLOOR(N), case trunc(N) of X when N < X -> X - 1; X -> X end).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc 向上取整，返回的是大于N的最小整数。如果为了性能，可以用定义的宏。
%% @spec ceil(N::number()) -> integer()
%%@end
%% -----------------------------------------------------------------
ceil(N) ->
	case trunc(N) of
		X when N > X -> X + 1;
		X -> X
	end.

%% -----------------------------------------------------------------
%%@doc 向下取整，返回的是小于N的最小整数。如果为了性能，可以用定义的宏。
%% @spec floor(N::number()) -> integer()
%%@end
%% -----------------------------------------------------------------
floor(N) ->
	case trunc(N) of
		X when N < X -> X - 1;
		X -> X
	end.

%% -----------------------------------------------------------------
%%@doc 判断指定的数是否为素数，注意：1不是素数，2是素数
%% @spec is_prime(N::integer()) -> boolean()
%%@end
%% -----------------------------------------------------------------
is_prime(N) when N > 2 ->
	case N rem 2 of
		0 ->
			false;
		_ ->
			is_prime(N, 3, trunc(math:sqrt(N)) + 1)
	end;
is_prime(2) ->
	true;
is_prime(1) ->
	false.

is_prime(N, A, B) when A =< B ->
	case N rem A of
		0 ->
			false;
		_ ->
			is_prime(N, A + 2, B)
	end;
is_prime(_, _, _) ->
	true.

%% -----------------------------------------------------------------
%%@doc 素数算法
%%```
%% 根据参数Flag（less 或 great），返回比指定数小或者大的素数
%%'''
%% @spec prime(N::integer(),Flag::atom()) -> integer()
%%@end
%% -----------------------------------------------------------------
prime(N, less) when N > 1 ->
	prime_1(N);
prime(N, _) when N > 1 ->
	prime_2(N);
prime(_, _) ->
	2.

prime_1(N) ->
	case is_prime(N) of
		true ->
			N;
		false ->
			prime_1(N - 1)
	end.

prime_2(N) ->
	case is_prime(N) of
		true ->
			N;
		false ->
			prime_2(N + 1)
	end.

%% -----------------------------------------------------------------
%%@doc 素数算法，返回在指定范围（1~2^32）内的所有素数
%%```
%% 返回为{N,List} N为返回的素数列表的大小，List为返回的素数列表
%%'''
%% @spec prime_list(N1::integer(),N2::integer()) -> {N, [integer()]}
%%@end
%% -----------------------------------------------------------------
prime_list(N1, N2) when N1 > 0, N2 > N1 ->
	prime_list(N1 - 1, N2, 0, []).

prime_list(N1, N1, C, L)->
	{C, L};
prime_list(N1, N2, C, L) ->
	case is_prime(N2) of
		true ->
			prime_list(N1, N2 - 1, C + 1, [N2 | L]);
		false ->
			prime_list(N1, N2 - 1, C, L)
	end.

%% -----------------------------------------------------------------
%%@doc 以2为底的正整数的对数
%% @spec log2(N::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
log2(N) ->
	if N < 65536 ->
		if N < 256 ->
			if N < 16 ->
				if N < 4 ->
					if N < 1 -> -1;
					N < 2 -> 0;
					true -> 1
					end;
				true ->
					if N < 8 -> 2;
					true -> 3
					end
				end;
			true ->
				if N < 64 ->
					if N < 32 -> 4;
					true -> 5
					end;
				true ->
					if N < 128 -> 6;
					true -> 7
					end
				end
			end;
		true ->
			if N < 4096 ->
				if N < 1024 ->
					if N < 512 -> 8;
					true -> 9
					end;
				true ->
					if N < 2048 -> 10;
					true -> 11
					end
				end;
			true ->
				if N < 16384 ->
					if N < 8192 -> 12;
					true -> 13
					end;
				true ->
					if N < 32768 -> 14;
					true -> 15
					end
				end
			end
		end;
	true ->
		16 + log2(N bsr 16)
	end.

%% -----------------------------------------------------------------
%%@doc 计算Fibonacci数列
%% @spec fib(N::integer()) -> [integer()]
%%@end
%% -----------------------------------------------------------------
fib(N) when is_integer(N), N > 0 ->
	fib(0, 1, N, []).

fib(N1, N2, N, L) when N > 0 ->
	fib(N2, N1 + N2, N - 1, [N1 | L]);
fib(_N1, _N2, _N, L) ->
	L.

%% -----------------------------------------------------------------
%%@doc 计算指定的数在Fibonacci数列中的位置和前后的Fibonacci数
%% @spec fib_i(N::integer()) -> {I,N1, N2}
%%@end
%% -----------------------------------------------------------------
fib_i(N) when is_integer(N), N > 0 ->
	fib_i(N, 0, 1, 1).

fib_i(N, N1, N2, I) when N >= N2 ->
	fib_i(N, N2, N1 + N2, I + 1);
fib_i(_N, N1, N2, I) ->
	{I, N1, N2}.

%% -----------------------------------------------------------------
%%@doc 计算Fibonacci数列中第N个数
%%```
%% FN1为Fibonacci数列中第N个数，FN2为Fibonacci数列中第N+1个数
%%'''
%% @spec fib_n(N::integer()) -> {FN1, FN2}
%%@end
%% -----------------------------------------------------------------
fib_n(N) when is_integer(N), N > 2 ->
	{M00, M01, _M10, _M11} = matrix2_pow({1, 1, 1, 0}, N - 1),
	{M01, M00};
fib_n(2) when is_integer(2) ->
	{1, 1};
fib_n(1) ->
	{0, 1}.

%% -----------------------------------------------------------------
%% Function: vec3_dot/2
%% Description: 3维向量的点乘
%% Returns: number()
%% -----------------------------------------------------------------
vec3_dot({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	{X1 * X2 + Y1 * Y2 + Z1 * Z2}.

%% -----------------------------------------------------------------
%% Function: vec3_dot/2
%% Description: 3维向量的叉乘，返回与v1、v2垂直的向量，v1、v2、返回右手定则
%% Returns: {number(), number(), number()}
%% -----------------------------------------------------------------
vec3_cross({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	{Y1 * Z2 - Z1 * Y2, Z1 * X2 - X1 * Z2, X1 * Y2 - Y1 * X2}.

%% -----------------------------------------------------------------
%% Function: vec3_length_sq/1
%% Description: 3维向量的长度的平方
%% Returns: number()
%% -----------------------------------------------------------------
vec3_length_sq({X, Y, Z}) ->
	X * X + Y * Y + Z * Z.

%% -----------------------------------------------------------------
%% Function: vec3_length/1
%% Description: 3维向量的长度
%% Returns: number()
%% -----------------------------------------------------------------
vec3_length({X, Y, Z}) ->
	math:sqrt(X * X + Y * Y + Z * Z).

%% -----------------------------------------------------------------
%% Function: vec_distance_sq/1
%% Description: 2个3维向量的距离的平方
%% Returns: number()
%% -----------------------------------------------------------------
vec3_distance_sq({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	X = X1 - X2,
	Y = Y1 - Y2,
	Z = Z1 - Z2,
	X * X + Y * Y + Z * Z.

%% -----------------------------------------------------------------
%% Function: vec_distance_sq/1
%% Description: 2个3维向量的距离
%% Returns: number()
%% -----------------------------------------------------------------
vec3_distance({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	X = X1 - X2,
	Y = Y1 - Y2,
	Z = Z1 - Z2,
	math:sqrt(X * X + Y * Y + Z * Z).

%% -----------------------------------------------------------------
%% Function: vec3_angle_cos/2
%% Description: 2个3维向量的夹角cos值
%% Returns: number()
%% -----------------------------------------------------------------
vec3_angle_cos({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	if
		X1 == 0.0 andalso Y1 == 0.0 andalso Z1 == 0.0 ->
			0;
		X2 == 0.0 andalso Y2 == 0.0 andalso Z2 == 0.0 ->
			0;
		true ->
			D = math:sqrt((X1 * X1 + Y1 * Y1 + Z1 * Z1) * (X2 * X2 + Y2 * Y2 + Z2 * Z2)),
			(X1 * X2 + Y1 * Y2 + Z1 * Z2) / D
	end.

%% -----------------------------------------------------------------
%% Function: vec3_angle/2
%% Description: 2个3维向量的夹角，单位为弧度
%% Returns: number()
%% -----------------------------------------------------------------
vec3_angle({X1, Y1, Z1}, {X2, Y2, Z2}) ->
	if
		X1 == 0.0 andalso Y1 == 0.0 andalso Z1 == 0.0 ->
			0;
		X2 == 0.0 andalso Y2 == 0.0 andalso Z2 == 0.0 ->
			0;
		true ->
			D = math:sqrt((X1 * X1 + Y1 * Y1 + Z1 * Z1) * (X2 * X2 + Y2 * Y2 + Z2 * Z2)),
			math:acos((X1 * X2 + Y1 * Y2 + Z1 * Z2) / D)
	end.

%% -----------------------------------------------------------------
%% Function: vec3_lerp
%% Description: 线性计算插值，inter_linear
%% Returns: {number(), number(), number()}
%% -----------------------------------------------------------------
vec3_lerp({X1, Y1, Z1}, {X2, Y2, Z2}, Frac) ->
	{X1 + (X2 - X1) * Frac, Y1 + (Y2 - Y1) * Frac, Z1 + (Z2 - Z1) * Frac}.

%% -----------------------------------------------------------------
%% Function: vec3_normalize/1
%% Description: 单位化: 0向量返回0向量；否则返回同向的长度为1的向量
%% Returns: {number(), number(), number()}
%% -----------------------------------------------------------------
vec3_normalize({X, Y, Z} = V) ->
	if
		X == 0.0 andalso Y == 0.0 andalso Z == 0.0 ->
			V;
		true ->
			Len = math:sqrt(X * X + Y * Y + Z * Z),
			{X / Len, Y / Len, Z / Len}
	end.


%% -----------------------------------------------------------------
%% Function: pl_distance/3
%% Description: 计算点到两点对应直线的距离平方，叉乘法
%% Returns: number()
%% -----------------------------------------------------------------
pl_distance({X, Y, Z}, {X1, Y1, Z1}, {X2, Y2, Z2}) ->
	PX1 = X1 - X,
	PY1 = Y1 - Y,
	PZ1 = Z1 - Z,
	PX2 = X2 - X,
	PY2 = Y2 - Y,
	PZ2 = Z2 - Z,
	% 叉乘后的向量，其长度代表2个向量所夹的平行四边形的面积
	XX1 = PY1 * PZ2 - PZ1 * PY2,
	YY1 = PX1 * PZ2 - PZ1 * PX2,
	ZZ1 = PX1 * PY2 - PY1 * PX2,
	% 边长
	XX2 = X1 - X2,
	YY2 = Y1 - Y2,
	ZZ2 = Z1 - Z2,
	(XX1 * XX1 + YY1 * YY1 + ZZ1 * ZZ1) / (XX2 * XX2 + YY2 * YY2 + ZZ2 * ZZ2).

%% -----------------------------------------------------------------
%% Function: line_rect/8
%% Description: 判断线段和矩形是否相交
%% Returns: true | false
%% -----------------------------------------------------------------
line_rect(RX1, RY1, RX2, RY2, LX1, LY1, LX2, LY2) ->
		if
			%首先判断线段的两个端点是否在矩形内，在就必然相交了，返回true
			LX1 >= RX1 andalso LX1 =< RX2 andalso LY1 >= RY1 andalso LY1 =< RY2 ->
				true;
			LX2 >= RX1 andalso LX2 =< RX2 andalso LY2 >= RY1 andalso LY2 =< RY2 ->
				true;
			% 判断线段的所在的包围矩形 和 矩形之间是否相交， 不相交就肯定返回false
			LX1 < RX1 andalso LX2 < RX1 ->
				false;
			LX1 > RX2 andalso LX2 > RX2 ->
				false;
			LY1 < RY1 andalso LY2 < RY1 ->
				false;
			LY1 > RY2 andalso LY2 > RY2 ->
				false;
			true ->
				% 判断矩形的四个顶点是否在线段的两侧，在两侧则相交否则就不相交，
				% 矢量叉积， 0表示共线，大于0表示直线上方，小于0表示直线下方
				P = LX2 * LY1 - LX1 * LY2,
				if
					(LY2 - LY1) * RX1 + (LX1 - LX2) * RY1 + P > 0 ->
						if
							(LY2 - LY1) * RX2 + (LX1 - LX2) * RY2 + P > 0 ->
								true;
							(LY2 - LY1) * RX1 + (LX1 - LX2) * RY2 + P > 0 ->
								true;
							(LY2 - LY1) * RX2 + (LX1 - LX2) * RY1 + P > 0 ->
								true;
							true ->
								false
						end;
					true ->
						if
							(LY2 - LY1) * RX2 + (LX1 - LX2) * RY2 + P < 0 ->
								true;
							(LY2 - LY1) * RX1 + (LX1 - LX2) * RY2 + P < 0 ->
								true;
							(LY2 - LY1) * RX2 + (LX1 - LX2) * RY1 + P < 0 ->
								true;
							true ->
								false
						end
				end
		end.

%% -----------------------------------------------------------------
%%@doc 2阶矩阵的乘法
%% @spec matrix2_mul({M100::number(), M101::number(), M110::number(), M111::number()}, {M200::number(), M201::number(), M210::number(), M211::number()}) -> {M00::number(), M01::number(), M10::number(), M11::number()}
%%@end
%% -----------------------------------------------------------------
matrix2_mul({M100, M101, M110, M111}, {M200, M201, M210, M211}) ->
	{
		M100 * M200 + M101 * M210,
		M100 * M201 + M101 * M211,
		M110 * M200 + M111 * M210,
		M110 * M201 + M111 * M211
	}.

%% -----------------------------------------------------------------
%%@doc 2阶矩阵的阶乘
%% @spec matrix2_pow(M::{M00::number(), M01::number(), M10::number(), M11::number()},K::integer()) -> {M00::number(), M01::number(), M10::number(), M11::number()}
%%@end
%% -----------------------------------------------------------------
matrix2_pow(M, K) when K > 3 ->
	if
		(K rem 2) =:= 0 ->
			M1 = matrix2_pow(M, K div 2),
			matrix2_mul(M1, M1);
		true ->
			M1 = matrix2_pow(M, K div 2),
			matrix2_mul(M, matrix2_mul(M1, M1))
	end;
matrix2_pow(M, 3) ->
	matrix2_mul(M, matrix2_mul(M, M));
matrix2_pow(M, 2) ->
	matrix2_mul(M, M);
matrix2_pow(M, 1) ->
	M.

%% -----------------------------------------------------------------
%%@doc 保留浮点数的小数点后几位有效数字，或整数后几位归零
%% @spec truncating(Number::number(),X::integer()) -> float()
%%@end
%% -----------------------------------------------------------------
truncating(Number, X) when is_float(Number), is_integer(X) ->
	N = math:pow(10, X),
	trunc(Number * N) / N;
truncating(Number, X) when is_integer(Number), is_integer(X) ->
	N = math:pow(10, X),
	trunc(Number / N) * N.

%% -----------------------------------------------------------------
%%@doc 随机数生成器，采用倍增同余算法，返回正整数范围的随机数
%% @spec random(Seed::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
random(Seed) ->
	% 防止种子为0
	R = Seed bxor ?RAND_MASK,
	% C语言的写法，可防止溢出
	%-define(RAND_R, 2836).
	%K = R div ?RAND_Q,
	%S = ?RAND_A * (R - K * ?RAND_Q) - ?RAND_R * K,
	S = ?RAND_A * R - (R div ?RAND_Q) * ?MAX_POSITIVE_INT32,
	if
		S < 0 ->
			S + ?MAX_POSITIVE_INT32;
		true ->
			S
	end.

%% -----------------------------------------------------------------
%%@doc 生成指定范围的随机数（随机值可以取到N1和N2）
%% @spec random(N1::integer(),N2::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
random(N1, N2) ->
	Seed = case get(?RANDOM_SEED) of
		S when is_integer(S) ->
			S;
		_ ->
			{_M, S, MicroSecs} = os:timestamp(),
			S + MicroSecs + erlang:phash2(self())
	end,
	put(?RANDOM_SEED, random(Seed)),
	random(Seed, N1, N2).

%% -----------------------------------------------------------------
%%@doc 用指定的种子，生成指定范围的随机数（随机值可以取到N1和N2）
%% @spec random(Seed::integer(),N1::integer(),N2::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
random(Seed, N1, N2) when N2 > N1 ->
	(Seed rem (N2 - N1 + 1)) + N1;
random(Seed, N1, N2) when N2 < N1 ->
	(Seed rem (N1 - N2 + 1)) + N2;
random(_Seed, N, N) ->
	N.

%% -----------------------------------------------------------------
%%@doc 双重散列算法，K是hash值，M是区间值（必须为质数），I是计算次数。
%% @spec dhash(K::integer(),M::integer(),I::integer()) -> integer()
%%@end
%% -----------------------------------------------------------------
dhash(K, M, I) ->
	((K rem M) + I * (1 + (K rem (M - 1)))) rem M.
