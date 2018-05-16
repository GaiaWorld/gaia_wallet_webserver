%%@doc 会话错误模块
%%@end

-module(zm_session_error).

-description("session error").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([handle/6]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc handle
%% @spec handle(Args::any(), Con::any(), Attr::any(), Info::any(), Msg::any(), Error::any()) -> return()
%% where
%%  return() =  ok
%%@end
%% -----------------------------------------------------------------
handle(_Args, _Con, _Attr, _Info, _Msg, {throw, {_Type, _Why, _Vars}, _Stacktrace}) ->
	ok;
handle(_Args, Con, Attr, Info, {Src, Cmd, Msg}, {Error, Reason, Stacktrace}) ->
	% 记录所有无类型的错误
	zm_log:warn(?MODULE, handle, Src, Cmd,
		[{con, Con}, {attr, Attr}, {info, Info}, {msg, Msg},
		{error, Error}, {reason, Reason}, {stacktrace, lists:flatten(z_lib:string(Stacktrace))}]).
