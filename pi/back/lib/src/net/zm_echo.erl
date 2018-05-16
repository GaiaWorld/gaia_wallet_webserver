%%%@doc 通讯回应模块
%%@end


-module(zm_echo).

-description("echo port").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([echo/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 处理客户端发送的echo请求
%% @spec echo(Arg::list(),Con,Attr,Info,{Bin, Offset} | KVList) -> return()
%% where
%% return() =  {ok, [], Info, Data}
%%@end
%% -----------------------------------------------------------------
echo({"", TimeSeparator, Append}, _Con, _Attr, Info, {Bin, Offset}) ->
	<<_:Offset/binary, Data/binary>> = Bin,
	{ok, [], Info, append_bin(TimeSeparator, Append, Data)};
echo({"", TimeKey, Append}, _Con, _Attr, Info, KVList) ->
	{ok, [], Info, append_list(TimeKey, Append, KVList)};
echo({Cmd, TimeSeparator, Append}, _Con, _Attr, Info, {Bin, Offset}) ->
	<<_:Offset/binary, Data/binary>> = Bin,
	{ok, [], Info, {Cmd, append_bin(TimeSeparator, Append, Data)}};
echo({Cmd, TimeKey, Append}, _Con, _Attr, Info, KVList) ->
	{ok, [], Info, {Cmd, append_list(TimeKey, Append, KVList)}}.

%%%===================LOCAL FUNCTIONS==================
% 附加bin
append_bin(0, [ _ | _] = Append, Data) ->
	[Data, Append];
append_bin(0, _, Data) ->
	Data;
append_bin(TimeSeparator, [ _ | _] = Append, Data) ->
	Now = zm_time:now_millisecond(),
	[<<Now:64, TimeSeparator:8>>, Data, Append];
append_bin(TimeSeparator, _, Data) ->
	Now = zm_time:now_millisecond(),
	[<<Now:64, TimeSeparator:8>>, Data].

% 附加KVList
append_list(0, [ _ | _] = Append, KVList) ->
	KVList ++ Append;
append_list(0, _, KVList) ->
	KVList;
append_list(TimeKey, [ _ | _] = Append, KVList) ->
	[{TimeKey, zm_time:now_millisecond()} | KVList ++ Append];
append_list(TimeKey, _, KVList) ->
	[{TimeKey, zm_time:now_millisecond()} | KVList].
