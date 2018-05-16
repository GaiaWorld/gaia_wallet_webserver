%%@doc 微信的用户消息处理，回复消息
%%```
%%% 
%%'''
%%@end


-module(wx_timer).

%%%=======================EXPORT=======================
-export([daytime/2, day_send/2, select_index/4, send/6]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc 用户距离上次活动时间的时间差超过规定的时间，提示用户上线
%% @spec daytime(Args::tuple(), Now::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
daytime({WXActiveTab, WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, DayTimeDelay, Timeout, Text, Src}, Now) ->
    Time = Now div 1000 - DayTimeDelay,
    case select_index(Src, WXActiveTab, Time - Timeout, Time) of
        {ok, [_ | _] = L} ->
            send(WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, Text, [Oid || {_, Oid} <- L]);
        E -> E
    end.

%% -----------------------------------------------------------------
%%@doc 用户距离上次活动时间的时间差超过规定的时间，提示用户上线
%% @spec daytime(Args::tuple(), Now::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
day_send({WXActiveTab, WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, DayTimeDelay, Timeout, Text, Src}, _Date) ->
    Now = z_lib:now_second(),
    case select_index(Src, WXActiveTab, Now - Timeout, Now - DayTimeDelay) of
        {ok, [_ | _] = L} ->
            send(WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, Text, [Oid || {_, Oid} <- L]);
        E -> E
    end.

%% -----------------------------------------------------------------
%%@doc 获得指定范围的元素
%% @spec send(WXCertMap::atom(), WXTokenUrl::string(), AheadOfTime::integer(), SendUrl::string(), Text::string()|tuple(), Now::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
select_index(Src, Tab, V1, V2) ->
    KeyRange = {open, {V1, -1}, closed, {V2, <<>>}},
    case zm_db_client:select_index(Tab, KeyRange, none, all, key, true) of
        {ok, []} = R ->
            R;
        {T, R} when T =:= ok orelse T =:= limit ->
            {ok, R};
        E ->
            zm_log:warn(?MODULE, select_index, Src, select_index, [{tab, Tab}, {range, KeyRange}, {error, E}]),
            E
    end.

%% -----------------------------------------------------------------
%%@doc 发送消息
%% @spec send(WXCertMap::atom(), WXTokenUrl::string(), AheadOfTime::integer(), SendUrl::string(), Text::string()|tuple(), Now::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
send(WXCertMap, WXTokenUrl, AheadOfTime, SendUrl, Text, L) ->
    AccessToken = wx_sign:get_access_token(WXCertMap, WXTokenUrl, AheadOfTime),
    % AccessToken = "oJLER3kBpc3o8rF0jXHKVmT9plX0IXKD9aPWpc11k6E0UBYmmBUNyHz76Uv_63m2NNQO4-kOfAOV--K-f78TBxYYDUfeKPwY7yBpKuDpv9ENMBhAIAUTI",
    SendUrl1 = SendUrl ++ AccessToken,
    {Prefix, Suffix} = get_text(Text),
    [send(SendUrl1, Prefix, Oid, Suffix) || Oid <- L].


%%%===================LOCAL FUNCTIONS==================
% 发送消息
send(Url, Prefix, Oid, Suffix) ->
    try
        S = unicode:characters_to_binary([Prefix, Oid, Suffix], utf8),
        R = zm_app_util:https_post(Url, S),
        zm_log:info(?MODULE, send, 'timer', send_ok, [{url, Url}, {s, S}, {r, R}]),
        Oid
    catch
        Error : Reason ->
            zm_log:warn(?MODULE, send, 'timer', send_fail, [{url, Url}, {prefix, Prefix}, {oid, Oid}, {suffix, Suffix}, {error, {Error, Reason}}, {stacktrace, erlang:get_stacktrace()}]),
            {Error, Reason}
    end.

% 获得文字或图文消息的前后段
get_text([H | _] = Text) when is_integer(H), H > 0, H < 16#80000000 ->
    {"{\"touser\":\"", ["\", \"msgtype\":\"text\", \"text\":{\"content\":\"", Text, "\"}}"]};
get_text(Texts) ->
    {"{\"touser\":\"", ["\", \"msgtype\":\"news\", \"news\":{\"articles\":[", to_items(Texts, {"title", "description", "picurl", "url"}), "]}}"]}.

% 将结果集放入到items中
to_items([], _Keys) ->
    [];
to_items([E | T], Keys) ->
    [$,, ${, to_item(E, tuple_size(E), Keys, []), $} | to_items(T, Keys)].

to_item(T, N, Keys, L) when N > tuple_size(Keys) ->
    to_item(T, N - 1, Keys, L);
to_item(T, N, Keys, L) when N > 0 ->
    case element(N, T) of
        [H | _] = S when is_integer(H), H > 0, H < 16#80000000 ->
            Key = element(N, Keys),
            to_item(T, N - 1, Keys, [$,, $", Key, $", $=, $", S, $" | L]);
        _ ->
            to_item(T, N - 1, Keys, L)
    end;
to_item(_, _, _, [$, | L]) ->
    L;
to_item(_, _, _, L) ->
    L.
