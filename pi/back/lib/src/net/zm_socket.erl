%%@doc 端口模块
%%```
%%% 用来封装tcp socket和ssl socket
%%'''
%%@end


-module(zm_socket).

-description("socket").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([peername/1, setopts/2, send/2, controlling_process/2, close/1]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 获得远端的ip地址
%% @spec peername(Socket) -> return()
%% where
%% return() =  {ok, {IP, Port}}
%%@end
%% -----------------------------------------------------------------
peername(Socket) when is_port(Socket) ->
	inet:peername(Socket);
peername(Socket) ->
	ssl:peername(Socket).

%% -----------------------------------------------------------------
%%@doc 设置指定端口的选项
%% @spec setopts(Socket,Opts::list()) -> ok
%%@end
%% -----------------------------------------------------------------
setopts(Socket, Opts) when is_port(Socket) ->
	inet:setopts(Socket, Opts);
setopts(Socket, Opts) ->
	ssl:setopts(Socket, Opts).

%% -----------------------------------------------------------------
%%@doc 端口发送方法
%% @spec send(Socket,Data) -> ok
%%@end
%% -----------------------------------------------------------------
send(Socket, Data) when is_port(Socket) ->
	gen_tcp:send(Socket, Data);
send(Socket, Data) ->
	ssl:send(Socket, Data).

%% -----------------------------------------------------------------
%%@doc 端口控制权转交方法
%% @spec controlling_process(Socket,Pid::pid()) -> ok
%%@end
%% -----------------------------------------------------------------
controlling_process(Socket, Pid) when is_port(Socket) ->
	inet:tcp_controlling_process(Socket, Pid);
controlling_process(Socket, Pid) ->
	ssl:controlling_process(Socket, Pid).

%% -----------------------------------------------------------------
%%@doc 端口关闭方法
%% @spec close(Socket) -> ok
%%@end
%% -----------------------------------------------------------------
close(Socket) when is_port(Socket) ->
	inet:close(Socket);
close(Socket) ->
	ssl:close(Socket).
