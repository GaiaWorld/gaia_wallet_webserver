%%@doc 时间函数
%%@end


-module(zm_time).

-description("time").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([now_second/0, now_millisecond/0, fix_millisecond/0]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc  当前的秒
%% @spec  now_second() -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
-spec now_second() ->
	integer().
%% -----------------------------------------------------------------
now_second() ->
	{M, S, _} = now(),
	M * 1000000 + S.

%% -----------------------------------------------------------------
%%@doc  当前的毫秒
%% @spec  now_millisecond() -> return()
%% where
%%  return() =  integer()
%%@end
%% -----------------------------------------------------------------
-spec now_millisecond() ->
	integer().
%% -----------------------------------------------------------------
now_millisecond() ->
	{M, S, MS} = now(),
	M * 1000000000 + S * 1000 + MS div 1000.

%% -----------------------------------------------------------------
%%@doc  获得全局时间和本地时间
%% @spec  fix_millisecond() -> return()
%% where
%%  return() =  {integer(), integer()}
%%@end
%% -----------------------------------------------------------------
-spec fix_millisecond() ->
	{integer(), integer()}.
%% -----------------------------------------------------------------
fix_millisecond() ->
	{M, S, MS} = now(),
	T = M * 1000000000 + S * 1000 + MS div 1000,
	{T, T}.
