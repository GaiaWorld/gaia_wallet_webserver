%% -- coding: utf-8 
%%@doc
%%@end
-module(app_wxpay_port).

-description("app_wxpay_port").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).
-vsn(1).


-export([wxpay/8, app_pay/8, h5_pay/8, wx_orderquery/5, wxpayback/5]).
%%%=======================INCLUDE=======================
-include_lib("xmerl/include/xmerl.hrl").

%% -----------------------------------------------------------------
%%@doc wxpay的支付请求
%% -----------------------------------------------------------------
wxpay({WXUnionIDTab, Url, ApiKey, ReqParms, JSParms, GoodsTypeList}, Uid, EID, OID, Money, Type, Info, _Msg) ->
    io:format("wxpay!!!!in!!!!!~n"),
    OpenID = zm_app_db:read(WXUnionIDTab, Uid, 3000),
    io:format("wxpay!!!!OpenID:~p~n", [OpenID]),
    [erlang:throw({500, "not found openid", []}) || OpenID =:= ""],
    % 生成验证码
    IP = z_lib:get_value(Info, ip, {182, 188, 27, 115}),
    [_ | IPStr] = lists:flatten([[$. | integer_to_list(A)] || A <- tuple_to_list(IP)]),
    PL1 = lists:sort(zm_app_util:replace_text(ReqParms, {zm_app_util:random_str(z_lib:now_millisecond(), [], 0), z_lib:get_value(GoodsTypeList, Type, ""), OID, integer_to_list(Money), IPStr, OpenID})),
    Sign = string:to_upper(wx_sign(PL1, ApiKey)),
    io:format("wxpay!!!!Sign:~p~n", [Sign]),
    % 生成发送的xml数据
    PL2 = [{"sign", Sign} | PL1],
    Data = "<xml>" ++ lists:flatten([[$<, K, $>, V, $<, $/, K, $>] || {K, V} <- PL2]) ++ "</xml>",
    zm_log:info(?MODULE, pay, app, wxpay1, [{{Uid, EID}, Data}]),
    Msg = zm_app_util:https_post(Url, Data),
    zm_log:info(?MODULE, pay, app, wxpay2, [{{Uid, EID}, Msg}]),
    XL = zm_app_util:read_xml(Msg),
    io:format("wxpay!!!!XL:~p~n", [XL]),
    [RC1, RC2] = z_lib:get_values(XL, [{"return_code", ""}, {"result_code", ""}]),
    [erlang:throw({500, "wxpay return error", [Msg]}) || RC1 =/= "SUCCESS"],
    [erlang:throw({500, "wxpay result error", [Msg]}) || RC2 =/= "SUCCESS"],
    PrepayID = z_lib:get_value(XL, "prepay_id", ""),
    % 生成前端参数的签名
    MS = z_lib:now_millisecond(),
    JSL1 = lists:sort(zm_app_util:replace_text(JSParms, {zm_app_util:random_str(MS, [], 0), integer_to_list(MS div 1000), "prepay_id=" ++ PrepayID})),
    JSSign = wx_sign(JSL1, ApiKey),
    % 生成前端的json字符串
    JSL2 = [{"paySign", JSSign} | JSL1],
    [_ | JSData] = lists:flatten([[[$,, $", K, $", $:, $", V, $"] || {K, V} <- JSL2], $}]),
    {[${, JSData], PrepayID}.

%% -----------------------------------------------------------------
%%@doc wxapp支付
%% -----------------------------------------------------------------
app_pay({Url, ApiKey, ReqParms, JSParms, GoodsTypeList}, Uid, EID, OID, Money, Type, Info, _Msg) ->
    io:format("wx app pay!!!!in!!!!!~n"),
    IP = z_lib:get_value(Info, ip, {182, 188, 27, 115}),
    [_ | IPStr] = lists:flatten([[$. | integer_to_list(A)] || A <- tuple_to_list(IP)]),
    PL1 = lists:sort(zm_app_util:replace_text(ReqParms, {zm_app_util:random_str(z_lib:now_millisecond(), [], 0), z_lib:get_value(GoodsTypeList, Type, ""), OID, integer_to_list(Money), IPStr})),
    Sign = string:to_upper(wx_sign(PL1, ApiKey)),
    io:format("wxpay!!!!Sign:~p~n", [Sign]),
    % 生成发送的xml数据
    PL2 = [{"sign", Sign} | PL1],
    Data = "<xml>" ++ lists:flatten([[$<, K, $>, V, $<, $/, K, $>] || {K, V} <- PL2]) ++ "</xml>",
    zm_log:info(?MODULE, pay, app, wxpay1, [{{Uid, EID}, Data}]),
    Msg = zm_app_util:https_post(Url, Data),
    zm_log:info(?MODULE, pay, app, wxpay2, [{{Uid, EID}, Msg}]),
    XL = zm_app_util:read_xml(Msg),
    io:format("wxpay!!!!XL:~p~n", [XL]),
    [RC1, RC2] = z_lib:get_values(XL, [{"return_code", ""}, {"result_code", ""}]),
    [erlang:throw({500, "wxpay return error", [Msg]}) || RC1 =/= "SUCCESS"],
    [erlang:throw({500, "wxpay result error", [Msg]}) || RC2 =/= "SUCCESS"],
    PrepayID = z_lib:get_value(XL, "prepay_id", ""),
    % 生成前端参数的签名
    MS = z_lib:now_millisecond(),
    JSL1 = lists:sort(zm_app_util:replace_text(JSParms, {PrepayID, zm_app_util:random_str(MS, [], 0), integer_to_list(MS div 1000)})),
    JSSign = wx_sign(JSL1, ApiKey),
    % 生成前端的json字符串
    JSL2 = [{"sign", JSSign} | JSL1],
    [_ | JSData] = lists:flatten([[[$,, $", K, $", $:, $", V, $"] || {K, V} <- JSL2], $}]),
    {[${, JSData], PrepayID}.

%% -----------------------------------------------------------------
%%@doc wxH5支付
%% -----------------------------------------------------------------
h5_pay({Url, ApiKey, ReqParms, GoodsTypeList}, Uid, EID, OID, Money, Type, Info, _Msg) ->
    io:format("wx app pay!!!!in!!!!!~n"),
    IP = z_lib:get_value(Info, ip, {182, 188, 27, 115}),
    [_ | IPStr] = lists:flatten([[$. | integer_to_list(A)] || A <- tuple_to_list(IP)]),
    PL1 = lists:sort(zm_app_util:replace_text(ReqParms, {zm_app_util:random_str(z_lib:now_millisecond(), [], 0), z_lib:get_value(GoodsTypeList, Type, ""), OID, integer_to_list(Money), IPStr})),
    Sign = string:to_upper(wx_sign(PL1, ApiKey)),
    io:format("wxpay!!!!Sign:~p~n", [Sign]),
    % 生成发送的xml数据
    PL2 = [{"sign", Sign} | PL1],
    Data = "<xml>" ++ lists:flatten([[$<, K, $>, V, $<, $/, K, $>] || {K, V} <- PL2]) ++ "</xml>",
    zm_log:info(?MODULE, pay, h5, wxpay1, [{{Uid, EID}, Data}]),
    Msg = zm_app_util:https_post(Url, Data),
    zm_log:info(?MODULE, pay, h5, wxpay2, [{{Uid, EID}, Msg}]),
    XL = zm_app_util:read_xml(Msg),
    io:format("wxpay!!!!XL:~p~n", [XL]),
    [RC1, RC2] = z_lib:get_values(XL, [{"return_code", ""}, {"result_code", ""}]),
    [erlang:throw({500, "wxpay return error", [Msg]}) || RC1 =/= "SUCCESS"],
    [erlang:throw({500, "wxpay result error", [Msg]}) || RC2 =/= "SUCCESS"],
    PrepayID = z_lib:get_value(XL, "prepay_id", ""),
    MwebUrl = z_lib:get_value(XL, "mweb_url", ""),
    % 生成前端的json字符串
    JSL2 = [{"mweb_url", MwebUrl}],
    [_ | JSData] = lists:flatten([[[$,, $", K, $", $:, $", V, $"] || {K, V} <- JSL2], $}]),
    {[${, JSData], PrepayID}.

%%@doc wxpay的订单查询请求
%% -----------------------------------------------------------------
wx_orderquery({Url, ApiKey, Parms}, Uid, Rid, OID, _Record) ->
    PL1 = lists:sort(zm_app_util:replace_text(Parms, {zm_app_util:random_str(z_lib:now_millisecond(), [], 0), OID})),
    Sign = wx_sign(PL1, ApiKey),
    % 生成发送的xml数据
    PL2 = [{"sign", Sign} | PL1],
    Data = "<xml>" ++ lists:flatten([[$<, K, $>, V, $<, $/, K, $>] || {K, V} <- PL2]) ++ "</xml>",
    zm_log:info(?MODULE, pay, app, wx_orderquery1, [{{Uid, Rid}, Data}]),
    Msg = zm_app_util:https_post(Url, Data),
    zm_log:info(?MODULE, pay, app, wx_orderquery2, [{{Uid, Rid}, Msg}]),
    XL = zm_app_util:read_xml(Msg),
    [RC1, RC2] = z_lib:get_values(XL, [{"return_code", ""}, {"result_code", ""}]),
    [erlang:throw({500, "wxpay return error", [Msg]}) || RC1 =/= "SUCCESS"],
    [erlang:throw({500, "wxpay result error", [Msg]}) || RC2 =/= "SUCCESS"],
    case z_lib:get_value(XL, "trade_state", "") of
        "SUCCESS" ->
            Amt = z_lib:get_value(XL, "total_fee", ""),
            {list_to_integer(Amt) div 100, XL};
        "USERPAYING" ->
            wait;
        "NOTPAY" ->
            wait;
        _ ->
            {fail, XL}
    end.


% 微信签名
wx_sign(PL, ApiKey) ->
    [_ | ParmStr] = lists:flatten([[$&, K, $=, V] || {K, V} <- PL]),
    public_lib:md5_str(ParmStr ++ "&key=" ++ ApiKey).


%% -----------------------------------------------------------------
%%@doc wxpayback支付成功的通知回调
%% @spec wxpayback(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
wxpayback({PayTab, ApiKey, MsgType, Src}, _Con, Info, Headers, Body) ->
    Info1 = [{body, Body} | Info],
    zm_log:info(?MODULE, wxpayback, Src, info, Info1),
    XL = zm_app_util:read_xml(binary_to_list(Body)),
    [RC1, RC2] = z_lib:get_values(XL, [{"return_code", ""}, {"result_code", ""}]),
    io:format("wxpayback!!!!!!!!!XL:~p~n", [XL]),
    Result = if
                 RC1 =:= "SUCCESS" andalso RC2 =:= "SUCCESS" ->
                     [Amt, OID] = z_lib:get_values(XL, [{"total_fee", ""}, {"out_trade_no", ""}]),
                     Sign = string:to_upper(wx_sign(lists:keydelete("sign", 1, XL), ApiKey)),
                     io:format("wxpayback!!!!!!!!!Sign:~p~n", [Sign]),
                     case z_lib:get_value(XL, "sign", "") of
                         Sign when Amt > "" ->
                             case tax_pay:pay_back(PayTab, MsgType, Src, OID, list_to_integer(Amt), [{time, erlang:localtime()} | Info] ++ XL) of
                                 ok -> "SUCCESS";
                                 fail -> "FAIL"
                             end;
                         _ ->
                             zm_log:warn(?MODULE, wxpayback, Src, verify, Info1),
                             "FAIL"
                     end;
                 true ->
                     zm_log:warn(?MODULE, wxpayback, Src, result, Info1),
                     "FAIL"
             end,
    {ok, Info, zm_http:del_header(zm_http:resp_headers(Info, Headers, ".txt"), "Content-Encoding"),
            "<xml><return_code><![CDATA[" ++ Result ++ "]]></return_code><return_msg><![CDATA[OK]]></return_msg></xml>"}.


