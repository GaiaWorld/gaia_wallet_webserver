%%@doc 记录用户的微信或qq号，值是sessionid。
%%```
%%% 
%%'''
%%@end


-module(yy_save).

%%%=======================EXPORT=======================
-export([save/5]).

%% -----------------------------------------------------------------
%%@doc 记录
%% @spec save(Args::list(), Con::tuple(), Info::[{Key::string(), Value::any()}], Headers::[{Key::string(), Value::string()}], Body::[{Key::string(), Value::string()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
save({SaveTab, SidKey}, _Con, Info, Headers, Body) ->
    [Type, Name, Award] = z_lib:get_values(Body, [{"type", ""}, {"name", ""}, {"award", ""}]),
    [throw({400, "{errcode:400, errmsg:'invalid args'}"}) || Type =:= "" orelse Name =:= ""],
    UA = z_lib:get_value(Headers, "user-agent", ""),
    % 获取Sid，如果没有，则跳转到微信认证的地址
    Sid = zm_http:get_cookie(Headers, SidKey, 0),
    zm_db_client:write(SaveTab, {Type, Name, Award}, {Sid, UA}, 0, ?MODULE, 0),
    {ok, Info, zm_http:del_header(zm_http:resp_headers(Info, Headers, ".jpg"), "Content-Encoding"), <<>>}.
