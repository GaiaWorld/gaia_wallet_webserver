%%%-------------------------------------------------------------------
%%% @author luobin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. 八月 2017 17:46
%%%-------------------------------------------------------------------
-module(short_con_port).
-author("luobin").

-include("../../include/table.hrl").

%% API
-export([create/5, show/5]).

%%创建短连接
create([Table, TableMap, PtPluginIndexTab], _Con, Info, Headers, Body) ->
  io:format("!!!!!!!!!!!!access!!Info:~p~n", [Info]),
  io:format("!!!!!!!!!!!!access!!Headers:~p~n", [Headers]),
  io:format("!!!!!!!!!!!!access!!Body:~p~n", [Body]),
  [Url, _Qs] = z_lib:get_values(Info, [{host, ""}, {qs, ""}]),
  io:format("!!!!!!!!!!!!access!!Url:~p~n", [Url]),
  %%获取需要转换的url
  Url2 = z_lib:get_value(Body, "url", ""),
  %%生成短连接字符串
  case zm_app_db:read(TableMap, Url2) of
    ?GEN_DB_NIL ->
      {ok, ID} = zm_unique_int:get(PtPluginIndexTab, ?INDEX_SHORT_CON, 1),
      zm_app_db:write(Table, ID, Url2, ?MODULE),
      zm_app_db:write(TableMap, Url2, ID, ?MODULE);
    ID ->
      ID
  end,
  V = lists:concat(["http://", Url, "/s/",  ID]),
  io:format("!!!!!!!!!!!!access!!V:~p~n", [V]),
  {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), V}.

%%读取连接
show([Table, _TableMap], _Con, Info, Headers, Body) ->
    io:format("!!!!!!!!!!!!access!!Info:~p~n", [Info]),
    io:format("!!!!!!!!!!!!access!!Headers:~p~n", [Headers]),
    io:format("!!!!!!!!!!!!access!!Body:~p~n", [Body]),
    [Url, _Qs] = z_lib:get_values(Info, [{host, ""}, {qs, ""}]),
    io:format("!!!!!!!!!!!!access!!Url:~p~n", [Url]),
    Path = z_lib:get_value(Info, path, ""),
    [_, ShortStr | _] = string:tokens(Path, "/"),
    io:format("!!!!!!!!!!!!access!!ShortStr:~p~n", [ShortStr]),
    Url2 = zm_app_db:read(Table, list_to_integer(ShortStr)),
    io:format("!!!!!!!!!!!!access!!V:~p~n", [Url2]),
    [throw({302, list_to_utf8(Url2)})|| Url2 =/= ?GEN_DB_NIL],
    {ok, Info, zm_http:resp_headers(Info, Headers, ".txt", none), "none"}.


list_to_utf8(Str) ->
    binary:list_to_bin(xmerl_ucs:to_utf8(Str)).
