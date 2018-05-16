%%%@doc flash 用于flash访问授权的通讯协议处理模块
%%@end


-module(flash_policy_service).

-description("valid flash socket").
-copyright({seasky,'www.seasky.cn'}).


%%%=======================EXPORT=======================
-export([init/2, decode/2]).

%%%=======================DEFINE=======================
-define(DEFAULT_REQUEST, <<"<policy-file-request/>\0">>).
-define(DEFAULT_RESPONSE, <<"<cross-domain-policy><site-control permitted-cross-domain-policies=\"all\"/><allow-access-from domain=\"*\" to-ports=\"*\"/></cross-domain-policy>\0">>).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 初始化授权处理参数
%% @spec init(Argus::argus(),Socket) -> return()
%% where
%% argus() = tuple()|default
%% return() =  {Socket, Request, Response}
%%@end
%% -----------------------------------------------------------------
init({Request, Response}, Socket) ->
	{Socket, Request, Response};
init(default, Socket) ->
	{Socket, ?DEFAULT_REQUEST, ?DEFAULT_RESPONSE}.

%% -----------------------------------------------------------------
%%@doc 解码客户端发送过来的flash访问授权请求
%% @spec decode(Argus::tuple(),Request) -> return()
%% where
%% return() =  {ok, Argus} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
decode({Socket, Request, Response} = Args, Request) ->
	zm_socket:send(Socket, Response),
	{ok, Args};
decode(_, _) ->
	{error, invalid_connect}.
