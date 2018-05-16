%%@doc 微信的用户消息处理，回复消息
%%```
%%% 
%%'''
%%@end


-module(wx_cmd).

%%%=======================EXPORT=======================
-export([token/5, send/5, send_all/5]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc 获得微信的token
%% @spec oauth(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
token({WXCertMap, WXTokenUrl, AheadOfTime}, _Con, Info, Headers, Body) ->
    Force = z_lib:get_value(Body, "force", ""),
    AccessToken = wx_sign:get_access_token(WXCertMap, WXTokenUrl, AheadOfTime, Force =/= ""),
    {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), AccessToken}.

%% -----------------------------------------------------------------
%%@doc 微信消息发送测试
%% @spec oauth(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
send({WXCertMap, WXTokenUrl, AheadOfTime, SendUrl}, _Con, Info, Headers, Body) ->
    R = case z_lib:get_value(Body, "oid", "") of
            "" -> "need oid";
            OID ->
                Text = z_lib:get_value(Body, "text", ""),
                wx_timer:send(WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, Text, [OID])
        end,
    {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), R}.

%% -----------------------------------------------------------------
%%@doc 微信群发消息发送测试
%% @spec oauth(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
send_all({WXActiveTab, WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, Timeout, Src}, _Con, Info, Headers, Body) ->
    Text = z_lib:get_value(Body, "text", ""),
    L = wx_timer:day_send({WXActiveTab, WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, 0, Timeout, Text, Src}, none),
    L1 = [Oid || Oid <- L, is_list(Oid)],
    {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), integer_to_list(length(L1))}.


