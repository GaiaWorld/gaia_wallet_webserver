%%@doc http错误模块
%%@end


-module(zm_http_error).

-description("http error").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([handle/6]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc http错误处理
%% @spec (TFile::list(), Con, Info, Headers, any(), Error) -> ok
%%       |(any(), Con, Info, Headers, any(), Error) -> ok
%%@end
%% -----------------------------------------------------------------
handle(TFile, Con, Info, Headers, _Body, Error) when is_list(TFile) ->
	{ok, {_Info, Template}} = zm_res_loader:read(TFile),
	RespHeaders = zm_http:resp_headers(Info, Headers, ".htm"),
	send_error(Con, Info, RespHeaders, Template, Error);
handle(_Args, Con, Info, Headers, _Body, Error) ->
	RespHeaders = zm_http:resp_headers(Info, Headers, ".txt"),
	send_error(Con, Info, RespHeaders, Error).

%%%===================LOCAL FUNCTIONS==================
% 将错误页面返回到浏览器
send_error(Con, Info, Headers, Template, {Error, {Code, Reason}, StackTrace})
	when is_integer(Code) ->
	Data = [{"error", z_lib:any_to_iolist(Error)}, {"reason", z_lib:any_to_iolist(Reason)},
		{"stacktrace", z_lib:string_stacktrace(StackTrace)} | zm_http_show:get_global(Info)],
	zm_http:send(Con, [{code, Code} | Info], Headers, zm_template:decode(Template, Data));
send_error(Con, Info, Headers, Template, {Error, Reason, StackTrace}) ->
	Data = [{"error", z_lib:any_to_iolist(Error)}, {"reason", z_lib:any_to_iolist(Reason)},
		{"stacktrace", z_lib:string_stacktrace(StackTrace)} | zm_http_show:get_global(Info)],
	zm_http:send(Con, [{code, 500} | Info], Headers, zm_template:decode(Template, Data)).

% 将错误码、错误信息和堆栈返回到浏览器
send_error(Con, Info, RespHeaders, {_Error, {Code, Reason}, StackTrace})
	when Code =:= 301 orelse Code =:=302 ->
	zm_http:send(Con, [{code, Code} | Info],
		sb_trees:insert("Location", Reason, RespHeaders),
		[z_lib:any_to_iolist(Reason), $\n, z_lib:string_stacktrace(StackTrace)]);
send_error(Con, Info, RespHeaders, {_Error, {Code, Reason}, StackTrace})
	when is_integer(Code) ->
	zm_http:send(Con, [{code, Code} | Info], RespHeaders,
		[z_lib:any_to_iolist(Reason), $\n, z_lib:string_stacktrace(StackTrace)]);
send_error(Con, Info, RespHeaders, Error) ->
	zm_http:send(Con, [{code, 500} | Info], RespHeaders, io_lib:write(Error)).
