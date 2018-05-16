%% @author luobin
%% @doc @todo 通用prot错误处理模块
%%默认值[{log, true}, {type, "type"}, {error_num, -1}]
%%配置[{log, true | false}, {type, error_handle :: 错误编号的key}, {error_num, -1 :: 异常错误编号}]
%%websock接口配置{session_error_handler, handle, []}
%%http接口配置{session_error_handler, http_handle, none}


-module(session_error_handler).

%% ====================================================================
%% API functions
%% ====================================================================
-export([handle/6, http_handle/6]).

-define(LOG, log).
-define(ERROR_NUM, error_num).
-define(TYPE, type).
-define(MSG_TYPE, msg_type).

%% -----------------------------------------------------------------
%%@doc session错误处理
%% @spec (TFile::list(), Con, Info, Headers, any(), Error) -> ok
%%       |(any(), Con, Info, Headers, any(), Error) -> ok
%%@end
%% -----------------------------------------------------------------
handle(Arg, Con, _Attr, Info, {Src, Cmd, Msg}, {Error, Reason, Stacktrace}) ->
	Logo = zm_http:new_scid(z_lib:get_value(Info, ip, {0, 0, 0, 0}), z_lib:get_value(Info, port, 0), self()),
	write_log(Arg, {Src, Cmd, Msg, Logo}, {Error, Reason, Stacktrace}),
	send_msg(Arg, Con, {Cmd, Msg, Logo}, Reason).

%% -----------------------------------------------------------------
%%@doc http错误处理
%% @spec (TFile::list(), Con, Info, Headers, any(), Error) -> ok
%%       |(any(), Con, Info, Headers, any(), Error) -> ok
%%@end
%% -----------------------------------------------------------------
http_handle(TFile, Con, Info, Headers, _Body, Error) when is_list(TFile) ->
	{ok, {_Info, Template}} = zm_res_loader:read(TFile),
	RespHeaders = zm_http:resp_headers(Info, Headers, ".htm"),
	send_error(Con, Info, RespHeaders, Template, Error);
http_handle(_Args, Con, Info, Headers, _Body, Error) ->
	RespHeaders = zm_http:resp_headers(Info, Headers, ".txt"),
	send_error(Con, Info, RespHeaders, Error).

%% ====================================================================
%% Internal functions
%% ====================================================================
%%记录日志
write_log(Arg, {Src, Cmd, Msg, Logo}, {Error, Reason, Stacktrace}) ->	
	case z_lib:get_value(Arg, ?LOG, true) of
		true ->
			zm_log:warn(?MODULE, session_error_handler, handle, "session exception handling", [
																	{src, Src},
																	{cmd, Cmd},
																	{logo, Logo},
																	{msg, Msg},
																	{error, {Error, Reason, Stacktrace}}]);
		_ ->
			ok
	end.

%%发送消息
send_msg(Arg, Con, {Cmd, Msg, Logo}, {Type, Why}) ->
	MsgType = z_lib:get_value(Arg, ?MSG_TYPE, "r_err"),
	S = case Why of
				[H | _] when H > 0, H < 16#10ffff ->
					Why;
				_ ->
					io_lib:write(Why)
			end,
	zm_session:r_send(Con, {Cmd, {MsgType, [
				{"", z_lib:get_value(Msg, "", 0)},
				{logo, Logo},
				{"type", Type},
				{"why", S}]}});
send_msg(Arg, Con, {Cmd, Msg, Logo}, Reason) ->
	MsgType = z_lib:get_value(Arg, ?MSG_TYPE, "r_err"),
	S = case Reason of
				[H | _] when H > 0, H < 16#10ffff ->
					Reason;
				_ ->
					io_lib:write(Reason)
			end,
	zm_session:r_send(Con, {Cmd, {MsgType, [
				{"", z_lib:get_value(Msg, "", 0)},
				{logo, Logo},
				{z_lib:get_value(Arg, ?TYPE, "type"), z_lib:get_value(Arg, ?ERROR_NUM, -1)},
				{"why", S}]}}).


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

% 将错误码、错误信息返回到浏览器
send_error(Con, Info, RespHeaders, {Error, {Code, Reason}, StackTrace})
	when Code =:= 301 orelse Code =:=302 ->
	zm_log:info(?MODULE, session_error_handler, http_handle, "301 or 302 Jump!!!!", [
																	{con, Con},
																	{info, Info},
																	{code, Code},
																	{resp_headers, RespHeaders},
																	{error, {Error, Reason, StackTrace}}]),
	zm_http:send(Con, [{code, Code} | Info],
		sb_trees:insert("Location", Reason, RespHeaders),[]);
send_error(Con, Info, RespHeaders, {Error, {Code, Reason}, StackTrace})
	when is_integer(Code) ->
	zm_log:warn(?MODULE, session_error_handler, http_handle, "code send_error!!!!", [
																	{con, Con},
																	{info, Info},
																	{code, Code},
																	{resp_headers, RespHeaders},
																	{error, {Error, Reason, StackTrace}}]),
	R = case Code of
			404 ->
				"404 Object Not Found";
			500 ->
				"500 Server Error ";
			_ ->
				[]
		end,
	zm_http:send(Con, [{code, Code} | Info], RespHeaders,R);
send_error(Con, Info, RespHeaders, Error) ->
	zm_log:warn(?MODULE, session_error_handler, http_handle, "other send_error!!!!", [
																	{con, Con},
																	{info, Info},
																	{resp_headers, RespHeaders},
																	{error, Error}]),
	zm_http:send(Con, [{code, 500} | Info], RespHeaders, []).

	