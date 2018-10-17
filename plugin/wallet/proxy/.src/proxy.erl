%%%-------------------------------------------------------------------
%%% @author user
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. 十月 2018 13:32
%%%-------------------------------------------------------------------
-module(proxy).
-author("user").

%% API
-export([get/5,http_get/1,https_get/1]).


%%从前台接受请求，与其中的值
get([{rite,A,B},{kline,C,D,E}], _Con, Info, Headers, Body) ->
%% 	{UID, _, _} = z_lib:get_value(Attr, ?UID, ""),
%% 	{EID, _, _} = z_lib:get_value(Attr, ?EID, ""),
%% 	{_AuthL, _, _} = z_lib:get_value(Attr, ?SYS_AUTH, ""),
  %%要请求的url地址
%  "CNY" ++ "appid=" ++ Appkey
  io:format("!!!!!!!!!!in get ~n"),
  io:format("!#!$!$!!!!!!!!!!!Body:~p~n",[Body]),
  Url = z_lib:get_value(Body, "url", []),
  io:format("!#!$!$!!!!!!!!!!!Url:~p~n",[Url]),
  %%获取当前类型
  Type = z_lib:get_value(Body, "type", []),
  io:format("!#!$!$!!!!!!!!!!!Type:~p~n",[Type]),
  Urlnow = case Type of
    %%取汇率
      "rite" ->
       %%整形转字符串
    %    Appkey = integer_to_list(A),
     %   Sign   = integer_to_list(B),
       io:format("!#!$!$!!!!!!!!!!!A:~p~n",[A]),
       io:format("!#!$!$!!!!!!!!!!!B:~p~n",[B]),
     Url ++ "&appkey=" ++ A ++ "&sign=" ++ B;
    %% 取K线图
    "kline" ->
      %Period = integer_to_list(C),
     % Size = integer_to_list(D),
      %AccessKeyId = integer_to_list(E),
      Url++"&period="++C++"&size="++D++"&AccessKeyId="++E;
    _ ->
      erlang:throw({500, [{errcode, 500},{errmsg, "Type error"}, {result, lists:flatten(io_lib:write("error"))}, {url, Url}]})
  end,
  io:format("!!!!!!!Urlnow:~p~n",[Urlnow]),
  %%调用自身方法，从服务器远端获取当前数据
  case  Urlnow  of
    [$h, $t, $t, $p, $s | _] ->
      {RC, Data} = https_get(Urlnow),
  io:format("!!!!!!!Data:~p~n",[Data]),
  NewHeaders=zm_http:set_header(zm_http:resp_headers(Info, Headers, ".txt", none), "Access-Control-Allow-Origin", "*"),
  %%代理向客户端发送请求，返回当前结果,读取到的结果放回到Content中
  {ok, [{code, RC} | Info], NewHeaders, Data};
[$h, $t, $t, $p, $: | _] ->
      {RC,Data}=http_get(Urlnow),
      io:format("!!!!!!!Data:~p~n",[Data]),
      NewHeaders=zm_http:set_header(zm_http:resp_headers(Info, Headers, ".txt", none), "Access-Control-Allow-Origin", "*"),
      %%代理向客户端发送请求，返回当前结果,读取到的结果放回到Content中
      {ok, [{code, RC} | Info], NewHeaders, Data};
  _ ->
erlang:throw({500, [{errcode, 500},{errmsg, "Request protocol error"}, {result, lists:flatten(io_lib:write("error"))}, {url, Url}]})
  end.

http_get(Url) ->
  % 启动网络环境
  Result = inets:start(),
  if
    (Result =:= ok) orelse (Result =:= {error, {already_started, inets}}) ->
      try httpc:request(get, {Url, [{"connection", "keep-alive"}]}, [{timeout, 5000}], [{body_format, binary}]) of
        {ok, {{_, RC, _}, _, Body}} when RC >= 200, RC < 400 ->
          {RC, Body};
        E ->
          erlang:throw({500, [{errcode, 500},{errmsg, "send result error"}, {result, lists:flatten(io_lib:write(E))}, {url, Url}]})
      catch
        _: Reason ->
          erlang:throw({500, [{errcode, 500},{errmsg, "send error"}, {result, lists:flatten(io_lib:write(Reason))}, {url, Url}]})
      end;
    true ->
      erlang:throw({500, [{errcode, 500},{errmsg, "inets start error"}, {result, lists:flatten(io_lib:write(Result))}, {url, Url}]})
  end.


https_get(Url) ->
  % 启动网络环境
  Result = inets:start(),
  if
    (Result =:= ok) orelse (Result =:= {error, {already_started, inets}}) ->
      SR = ssl:start(),
      if
        (SR =:= ok) orelse (SR =:= {error, {already_started, ssl}}) ->
          try httpc:request(get, {Url, [{"connection", "keep-alive"}]}, [{timeout, 5000}], [{body_format, binary}]) of
            {ok, {{_, RC, _}, _, Body}} when RC >= 200, RC < 400 ->
              {RC,Body};
            {ok, {{_, RC, Header}, _, Body}} ->
              zm_log:warn(?MODULE, https_get, https_get, "request error", [
                {errcode, RC},
                {header, Header},
                {body, Body}]),
              erlang:throw({RC, [{errcode, RC},{errmsg, "send result error"}]});
            E ->
              erlang:throw({500, [{errcode, 500},{errmsg, "send result error"}]})
          catch
            _: Reason ->
              erlang:throw({500, [{errcode, 500},{errmsg, "send error"}]})
          end;
        true ->
          erlang:throw({500, [{errcode, 500},{errmsg, "ssl start error"}]})
      end;
    true ->
      erlang:throw({500, [{errcode, 500},{errmsg, "inets start error"}]})
  end.



