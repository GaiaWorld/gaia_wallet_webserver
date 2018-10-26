%%%-------------------------------------------------------------------
%%% @author luobin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%% 登录通用代理
%%% @end
%%% Created : 22. 八月 2017 10:32
%%%-------------------------------------------------------------------
-module(pt_porxy).
-author("luobin").

%% API
-export([porxy/5, test/5]).

test(_, _Con, Info, Headers, Body) ->
    io:format("!!!!!!!!!test!!!!!!!!!!!Info:~p, Headers:~p~n", [Info, Headers]),
    throw({302, "http://www.baidu.com"}),
    {ok, Info, Headers, Body}.

porxy({PtUrl}, _Con, Info, Headers, _Body) ->
    Head = [{K, V} || {K, V} <- sb_trees:to_list(Headers), is_list(V)],
    %%去掉pt
    [$/, $p, $t, $/ | Uri] = z_lib:get_value(Info, uri, ""),
    GetUrl = PtUrl ++ Uri,
    io:format("!!!!!GetUrl:~p, Head:~p~n", [GetUrl, Head]),
    {RC, RHead, RBody} = http_get(GetUrl, Head),
    io:format("!!!!!RC:~p, RHead:~p~n", [RC, RHead]),
    Headers2 = sb_trees:from_dict(RHead),
    {ok, [{code, RC} | Info], Headers2, RBody}.


% 发送请求
http_get(Url, Head) ->
    % 启动网络环境
    Result = inets:start(),
    if
        (Result =:= ok) orelse (Result =:= {error, {already_started, inets}}) ->
            try httpc:request(get, {Url, Head}, [{timeout, 5000}, {autoredirect, false}], [{body_format, binary}]) of
                {ok, {{_, RC, _}, RHead, Body}} ->
                    {RC, RHead, Body};
                E ->
                    io:format("!!!!!!!E:~p~n", [E])
            catch
                _: Reason ->
                    erlang:throw({500, [{errcode, 500},{errmsg, "send error"}, {result, lists:flatten(io_lib:write(Reason))}, {url, Url}]})
            end;
        true ->
            erlang:throw({500, [{errcode, 500},{errmsg, "inets start error"}, {result, lists:flatten(io_lib:write(Result))}, {url, Url}]})
    end.

