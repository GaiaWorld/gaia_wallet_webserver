%%@doc 因为获取的access_token以及jsapi_ticket都有时间限制（7200秒后失效），且调用接口的次数有限制，所以我们得把得到的access_token和jsapi_ticket缓存到内存里，每次请求都检查是否已过期。
%%```
%%% 
%%'''
%%@end


-module(wx_sign).

%%%=======================EXPORT=======================
-export([get_access_token/3, get_access_token/4, check_open_id/2, get_jsapi_ticket/4, check/5, oauth/5, userinfo/5, sign/5]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc 获取微信通讯需要的access_token
%% @spec get_access_token(WXMap::atom(), Url::string(), AheadOfTime::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
get_access_token(WXMap, Url, AheadOfTime) ->
    get_access_token(WXMap, Url, AheadOfTime, false).

%% -----------------------------------------------------------------
%%@doc 获取微信通讯需要的access_token
%% @spec get_access_token(WXMap::atom(), Url::string(), AheadOfTime::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
get_access_token(WXMap, Url, AheadOfTime, Force) ->
    Now = z_lib:now_second(),
    case zm_config:get(WXMap, access_token, {0, 0, 0}) of
        {_, _, ExpireTime} when ExpireTime < Now orelse Force =:= true ->
            [AccessToken, Expires] = wx_http(Url, [{"access_token", ""}, {"expires_in", 7200}]),
            zm_log:info(?MODULE, get_access_token, 'htdocs', get_access_token, [{ok, [AccessToken, Expires]}]),
            zm_config:set(WXMap, {access_token, AccessToken, Now - AheadOfTime + Expires}),
            AccessToken;
        {_, R, _} ->
            R
    end.

%% -----------------------------------------------------------------
%%@doc 检查用户openid是否合法, 
%% @spec check_open_id(Args::list(), OpenID::string()) -> return()
%% where
%%  return() = ok|{error, integer()}
%%@end
%% -----------------------------------------------------------------
check_open_id(WXCertMap, OpenID) ->
    Now = z_lib:now_second(),
    case zm_config:get(WXCertMap, OpenID, {0, 0, 0}) of
        {_, AccessToken, ExpireTime} when ExpireTime >= Now ->
            R = zm_app_util:https_get("https://api.weixin.qq.com/sns/auth?access_token=" ++ AccessToken ++ "&openid=" ++ OpenID),
            Json = zm_json:decode(R),
            case z_lib:get_value(Json, "errcode", 0) of
                0 ->
                    ok;
                ErrCode ->
                    {error, ErrCode}
            end;
        {_, _, ExpireTime} ->
            {error, {access_token_timeout, ExpireTime}}
    end.

%% -----------------------------------------------------------------
%%@doc 获取微信通讯需要的access_token
%% @spec get_access_token(WXMap::atom(), Url::string(), AheadOfTime::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
get_jsapi_ticket(WXMap, AccessTokenUrl, Url, AheadOfTime) ->
    Now = z_lib:now_second(),
    case zm_config:get(WXMap, jsapi_ticket, {0, 0, 0}) of
        {_, _, ExpireTime} when ExpireTime < Now ->
            AccessToken = get_access_token(WXMap, AccessTokenUrl, AheadOfTime),
            [Ticket, Expires] = wx_http(Url ++ AccessToken, [{"ticket", ""}, {"expires_in", 7200}]),
            zm_config:set(WXMap, {jsapi_ticket, Ticket, Now - AheadOfTime + Expires}),
            Ticket;
        {_, R, _} ->
            R
    end.

%% -----------------------------------------------------------------
%%@doc 检查用户是否已经有微信的openid, 注意一定要在微信的接口权限中的网页授权获取用户基本信息这一项里填写合法的域名
%% @spec oauth(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
check({OpenIDTab, WXCertMap, Suffixs, WXStr, SidKey, DispatchUrl, UrlSuffix}, _Con, Info, Headers, Body) ->
    %%io:format("!!!!!!check start, Body:~p~n", [Body]),
    % 检查是否在微信浏览器内
    UA = string:to_lower(z_lib:get_value(Headers, "user-agent", "")),
    IP = z_lib:get_value(Info, ip, ""),
    %%io:format("!!!!!!UA:~p, WXStr:~p~n", [UA, WXStr]),
    case string:str(UA, WXStr) of
        0 ->
            %%io:format("!!!!!!openid already exist~n"),
            {ok, Info, Headers, Body};
        _ ->
            case ip_filter:filter_ipv4([{192, 168, '$any', '$any'}], IP) of
                {ok, _} ->
                    {ok, Info, Headers, Body};
                _ ->
                    % 检查是否为指定文件
                    Path = z_lib:get_value(Info, path, "/"),
                    case match(Path, Suffixs) of
                        true ->
                            %%io:format("!!!!!!Path:~p, Suffixs:~p~n", [Path, Suffixs]),
                            % 获取Sid，如果没有，则跳转到微信认证的地址
                            case zm_http:get_cookie(Headers, SidKey, 0) of
                                0 ->
                                    Sid = zm_http:new_scid(z_lib:get_value(Info, ip, {0, 0, 0, 0}), z_lib:get_value(Info, port, 0), self()),
                                    %%io:format("!!!!!!Sid:~p~n", [Sid]),
                                    throw({302, DispatchUrl ++ http_uri:encode(Sid ++ "_" ++ src_url(Info)) ++ UrlSuffix});
                                Sid ->
                                    % 如果该Sid没有对应的openid，则跳转到微信认证的地址
                                    OpenID = zm_app_db:read(OpenIDTab, Sid),
                                    %%io:format("!!!!!!Sid:~p, OpenID:~p~n", [Sid, OpenID]),
                                    %%判断是否是分享过来的连接
                                    case z_lib:get_value(Body, "eid", "") of
                                        "" ->
                                            ok;
                                        EID ->
                                            zm_db_client:write('tax/wx@share_integral', OpenID, list_to_integer(EID), 0, ?MODULE, 0)
                                    end,
                                    %% 							[throw({302, DispatchUrl ++ http_uri:encode(Sid++"_"++src_url(Info)) ++ UrlSuffix}) || OpenID =:= "" orelse check_open_id(WXCertMap, OpenID) =/= ok],
                                    [throw({302, DispatchUrl ++ http_uri:encode(Sid ++ "_" ++ src_url(Info)) ++ UrlSuffix}) || OpenID =:= ""],
                                    {ok, Info, Headers, Body}
                            end;
                        _ ->
                            %%io:format("!!!!!!Body:~p~n", [Body]),
                            {ok, Info, Headers, Body}
                    end
            end
    end.

% 路径匹配
match(Path, [H | T]) ->
    case string:str(Path, H) of
        0 -> match(Path, T);
        _ -> true
    end;
match(Path, _) ->
    Path =:= "/".

% 获得原始请求url
src_url(Info) ->
    [Portocol, Host, Uri] = z_lib:get_values(Info, [{portocol, http}, {host, ""}, {uri, ""}]),
    atom_to_list(Portocol) ++ "://" ++ Host ++ Uri.

%% -----------------------------------------------------------------
%%@doc 微信的oauth2的认证，以{type, sessionid}为键，记录openid
%% @spec oauth(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
oauth({OpenIDTab, InfoTab, {SidKey, InfoKey}, SignUrl, MoreGet, MoreUrl}, _Con, Info, Headers, Body) ->
    io:format("!!!!!!oauth2, Body:~p~n", [Body]),
    [Code, State] = z_lib:get_values(Body, [{"code", ""}, {"state", ""}]),
    [throw({400, "{errcode:400, errmsg:'invalid args'}"}) || Code =:= ""],
    % 微信有毛病， scope=snsapi_userinfo 时，会把state的" 也就是 %22 给过滤掉，只能改成自己劈分字符串
    % 分析state， Sid_Redirct
    {I, Redirct} = z_lib:split_first(State, $_),
    {RID, ID} = if
                    I =:= "" ->
                        case zm_http:get_cookie(Headers, SidKey, 0) of
                            0 ->
                                Sid = zm_http:new_scid(z_lib:get_value(Info, ip, {0, 0, 0, 0}), z_lib:get_value(Info, port, 0), self()),
                                {{SidKey, Sid}, Sid};
                            Sid ->
                                {none, Sid}
                        end;
                    true ->
                        {{SidKey, I}, I}
                end,
    [Token, OpenID, Expires] = wx_http(SignUrl ++ Code, [{"access_token", ""}, {"openid", ""}, {"expires_in", 7200}]),
    zm_db_client:write(OpenIDTab, ID, OpenID, 0, ?MODULE, 0),
    AccessToken = case MoreGet of
                      {WXCertMap, WXTokenUrl, AheadOfTime} ->
                          get_access_token(WXCertMap, WXTokenUrl, AheadOfTime);
                      WXCertMap ->
                          zm_config:set(WXCertMap, {OpenID, Token, z_lib:now_second() - 0 + Expires}),
                          Token
                  end,
    UInfo = try
                RData = zm_app_util:https_get(MoreUrl ++ AccessToken ++ "&openid=" ++ OpenID),
                Json1 = zm_json:decode(RData),
                case z_lib:get_value(Json1, "errcode", 0) of
                    EC when EC > 0 ->
                        %获取用户详细信息失败
                        zm_log:warn(?MODULE, oauth, 'htdocs', oauth, [{error, RData}]),
                        unicode:characters_to_binary("{\"sid\":\"" ++ ID ++ "\", \"uinfo\":" ++ "{\"openid\":\"" ++ OpenID ++ "\"}}", utf8);
                    _ ->
                        zm_db_client:write(InfoTab, OpenID, RData, 0, ?MODULE, 0),
                        unicode:characters_to_binary("{\"sid\":\"" ++ ID ++ "\", \"uinfo\":" ++ RData ++ "}", utf8)
                end
            catch
                Error:Reason ->
                    zm_log:warn(?MODULE, oauth, 'htdocs', oauth, [{error, {Error, Reason}},
                        {stacktrace, erlang:get_stacktrace()}]),
                    unicode:characters_to_binary("{\"sid\":\"" ++ ID ++ "\", \"uinfo\":" ++ "{\"openid\":\"" ++ OpenID ++ "\"}}", utf8)
            end,
    zm_log:info(?MODULE, oauth2, 'htdocs', oauth_ok, [{info, Info}, {uinfo, UInfo}]),
    {ok, Info, zm_http:resp_headers(Info, Headers, ".html", RID),
        filter(["<html><head><meta charset=\"UTF-8\"><script type='text/javascript'>localStorage.", InfoKey, "='", UInfo, "';location.href='", Redirct, "';</script></head></html>"])}.

%% -----------------------------------------------------------------
%%@doc 微信获取用户的信息，用户应该已经通过了微信认证
%% @spec userinfo(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
userinfo({OpenIDTab, InfoTab, SidKey}, _Con, Info, Headers, Body) ->
    R = case z_lib:get_value(Body, "oid", "") of
            "" ->
                case zm_http:get_cookie(Headers, SidKey, 0) of
                    0 -> "{}";
                    Sid ->
                        case zm_app_db:read(OpenIDTab, Sid) of
                            "" -> "{}";
                            OID ->
                                case zm_app_db:read(InfoTab, OID) of
                                    "" -> "{\"openid\":\"" ++ OID ++ "\"}";
                                    UInfo -> unicode:characters_to_binary(UInfo, utf8)
                                end
                        end
                end;
            OID ->
                case zm_app_db:read(InfoTab, OID) of
                    "" -> "{\"openid\":\"" ++ OID ++ "\"}";
                    UInfo -> unicode:characters_to_binary(UInfo, utf8)
                end
        end,
    {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), R}.

%% -----------------------------------------------------------------
%%@doc 给用户发送签名
%% @spec sign(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
sign({WXMap, AccessTokenUrl, JsapiTicketUrl, AheadOfTime, Appid}, _Con, Info, Headers, Body) ->
    % 获得签名所用的url
    MS = z_lib:now_millisecond(),
    Now = MS div 1000,
    [URL, NonceStr, TimeStamp, AppID] = z_lib:get_values(Body,
        [{"url", ""}, {"noncestr", zm_app_util:random_str(MS, [], 0)}, {"timestamp", integer_to_list(Now)}, {"appid", Appid}]),
    [throw({400, "{errcode:400, errmsg:'invalid args'}"}) || URL =:= ""],
    JsapiTicket = get_jsapi_ticket(WXMap, AccessTokenUrl, JsapiTicketUrl, AheadOfTime),
    PL = [{"jsapi_ticket", JsapiTicket}, {"noncestr", NonceStr}, {"timestamp", TimeStamp}, {"url", URL}],
    Sign = wx_sign(PL),
    % 生成前端的json字符串
    PL1 = [{"signature", Sign}, {"nonceStr", NonceStr}, {"timestamp", list_to_integer(TimeStamp)}, {"appId", AppID}],
    Json = sb_trees:from_dict(PL1),
    {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), zm_json:encode(Json)}.


% 和微信通讯，获得返回的json中的数据
wx_http(AccessTokenUrl, KeyDefaultList) ->
    %%io:format("!!!!!!wx_http, AccessTokenUrl:~p~n", [AccessTokenUrl]),
    RData = zm_app_util:https_get(AccessTokenUrl),
    Json = zm_json:decode(RData),
    EC = z_lib:get_value(Json, "errcode", 0),
    [throw({400, RData}) || EC > 0],
    if
        is_list(KeyDefaultList) -> z_lib:get_values(Json, KeyDefaultList);
        true -> Json
    end.

% 微信签名
wx_sign(PL) ->
    [_ | ParmStr] = lists:flatten([[$&, K, $=, V] || {K, V} <- PL]),
    z_lib:bin_to_hex_str(crypto:sha(ParmStr), false).


filter(String) ->
    FilterStr = string:tokens(String, "'"),
    string:join(FilterStr, "").
