%%@doc 微信的用户消息处理，回复消息
%%```
%%% 
%%'''
%%@end


-module(wx_msg).

%%%=======================EXPORT=======================
-export([recv/5, resp/3]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc 微信验证和用户消息处理
%% @spec oauth(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
recv({_WXMsgTab, _WXActiveTab, _HanderList, Src, KDL, Token, _EncodingAESKey, _WXCertMap, _WXTokenUrl, _AheadOfTime},
    _Con, Info, Headers, Body) when is_list(Body) ->
    io:format("!!!!!!Body0:~p~n", [Body]),
    %接入认证
    [Sign, Nonce, TimeStamp, Echo] = z_lib:get_values(Body, KDL),
    case wx_sign([{"nonce", Nonce}, {"timestamp", TimeStamp}, {"token", Token}]) of
        Sign ->
            zm_log:info(?MODULE, recv1, Src, info, [{body1, Body} | Info]),
            {ok, Info, zm_http:del_header(zm_http:resp_headers(Info, Headers, ".txt"), "Content-Encoding"), Echo};
        _ ->
            throw({400, "{errcode:400, errmsg:'invalid args'}"})
    end;
recv({WXMsgTab, WXActiveTab, HanderList, Src, _KDL, _Token, _EncodingAESKey, WXCertMap, WXTokenUrl, AheadOfTime},
    _Con, Info, Headers, Body) ->
    io:format("!!!!!!Body1:~p~n", [Body]),
    zm_log:info(?MODULE, recv2, Src, info, [{body1, Body} | Info]),
    XL = zm_app_util:read_xml(binary_to_list(Body)),
    Now = z_lib:now_second(),
    [CTIME, OID, MID, TO] = z_lib:get_values(XL, [{"CreateTime", ""}, {"FromUserName", ""}, {"MsgId", ""}, {"ToUserName", ""}]),
    zm_app_db:write(WXActiveTab, OID, Now, ?MODULE),
    XL1 = lists:keydelete("FromUserName", 1, lists:keydelete("CreateTime", 1, lists:keydelete("MsgId", 1, lists:keydelete("ToUserName", 1, XL)))),
    ID = {CTIME, OID, MID},
    R = case zm_db_client:read(WXMsgTab, ID, ?MODULE, 3000, 5000) of
            {ok, [], _, _} ->
                zm_app_db:write(WXMsgTab, ID, XL1, ?MODULE),
                try handle(HanderList, ID, z_lib:get_value(XL1, "MsgType", ""), XL1) of
                    none -> "success";
                    [H | _] = RS when is_integer(H), H > 0, H < 16#80000000 ->
                        ["<xml><ToUserName><![CDATA[", OID, "]]></ToUserName><FromUserName><![CDATA[", TO, "]]></FromUserName><CreateTime>", integer_to_list(Now), "</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[", unicode:characters_to_binary(RS, utf8), "]]></Content></xml>"];
                    RL ->
                        zm_log:info(?MODULE, recv1, Src, handle2, [{r, RL}]),
                        ["<xml><ToUserName><![CDATA[", OID, "]]></ToUserName><FromUserName><![CDATA[", TO, "]]></FromUserName><CreateTime>", integer_to_list(Now), "</CreateTime><MsgType><![CDATA[news]]></MsgType><ArticleCount>", integer_to_list(length(RL)), "</ArticleCount><Articles>", to_items(RL, {"Title", "Description", "PicUrl", "Url"}), "</Articles></xml>"]
                catch
                    Error:Reason ->
                        zm_log:warn(?MODULE, recv, Src, handle, [{error, {{Error, Reason}, erlang:get_stacktrace()}} | XL]),
                        "success"
                end;
            _ ->
                "success"
        end,
    zm_log:info(?MODULE, recv1, Src, handle, [{r, R}]),
    {ok, Info, zm_http:del_header(zm_http:resp_headers(Info, Headers, ".txt"), "Content-Encoding"), R}.

% 处理消息
handle([{MsgType, {M, F, A}} | _], ID, MsgType, Msg) ->
    M:F(A, ID, Msg);
handle([{"text", Content, {M, F, A}} | T], ID, MsgType, Msg) ->
    case z_lib:get_value(Msg, "Content", "") of
        Content ->
            M:F(A, ID, Msg);
        _ ->
            handle(T, ID, MsgType, Msg)
    end;
handle([{MsgType, Event, {M, F, A}} | T], ID, MsgType, Msg) ->
    case z_lib:get_value(Msg, "Event", "") of
        Event ->
            M:F(A, ID, Msg);
        _ ->
            handle(T, ID, MsgType, Msg)
    end;
handle([{MsgType, Event, EventKey, {M, F, A}} | T], ID, MsgType, Msg) ->
    case z_lib:get_value(Msg, "Event", "") of
        Event ->
            case z_lib:get_value(Msg, "EventKey", "") of
                EventKey ->
                    M:F(A, ID, Msg);
                _ ->
                    handle(T, ID, MsgType, Msg)
            end;
        _ ->
            handle(T, ID, MsgType, Msg)
    end;
handle([_ | T], ID, MsgType, Msg) ->
    handle(T, ID, MsgType, Msg);
handle(_, _ID, _MsgType, _Msg) ->
    none.

% 将结果集放入到items中
to_items([E | T], Keys) ->
    [to_item(E, tuple_size(E), Keys, ["</item>"]) | to_items(T, Keys)];
to_items([], _Keys) ->
    [].

to_item(T, N, Keys, L) when N > tuple_size(Keys) ->
    to_item(T, N - 1, Keys, L);
to_item(T, N, Keys, L) when N > 0 ->
    case element(N, T) of
        [H | _] = S when is_integer(H), H > 0, H < 16#80000000 ->
            Key = element(N, Keys),
            to_item(T, N - 1, Keys, [$<, Key, "><![CDATA[", unicode:characters_to_binary(S, utf8), "]]></", Key, $> | L]);
        _ ->
            to_item(T, N - 1, Keys, L)
    end;
to_item(_, _, _, [_]) ->
    [];
to_item(_, _, _, L) ->
    ["<item>" | L].

%% -----------------------------------------------------------------
%%@doc 用户消息回复
%% @spec resp(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
resp(A, _ID, _Msg) ->
    A.

% 微信签名
wx_sign(PL) ->
    z_lib:bin_to_hex_str(crypto:hash(sha, lists:flatten([V || {_, V} <- lists:sort(PL)])), false).