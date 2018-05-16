%%% 应用的一些工具方法


-module(zm_app_util).

-description("zm_app_util").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).
-vsn(1).

%%%=======================EXPORT=======================
-export([http_get/1, https_get/1, http_post/2, https_post/2, http_post/3, https_post/3]).
-export([replace_text/2, read_xml/1, random_str/3]).

%%%=================EXPORTED FUNCTIONS=================
% 发送请求
http_get(Url) ->
	% 启动网络环境
	Result = inets:start(),
	if
		(Result =:= ok) orelse (Result =:= {error, {already_started, inets}}) ->
			try httpc:request(get, {Url, [{"connection", "keep-alive"}]}, [{timeout, 5000}], [{body_format, binary}]) of
				{ok, {{_, RC, _}, _, Body}} when RC >= 200, RC < 400 ->
					unicode:characters_to_list(Body, utf8);
				E ->
					erlang:throw({500, [{errcode, 500},{errmsg, "send result error"}, {result, lists:flatten(io_lib:write(E))}, {url, Url}]})
			catch
				_: Reason ->
					erlang:throw({500, [{errcode, 500},{errmsg, "send error"}, {result, lists:flatten(io_lib:write(Reason))}, {url, Url}]})
			end;
		true ->
			erlang:throw({500, [{errcode, 500},{errmsg, "inets start error"}, {result, lists:flatten(io_lib:write(Result))}, {url, Url}]})
	end.

% 发送请求
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
							unicode:characters_to_list(Body, utf8);
						E ->
							erlang:throw({500, [{errcode, 500},{errmsg, "send result error"}, {result, lists:flatten(io_lib:write(E))}, {url, Url}]})
					catch
						_: Reason ->
							erlang:throw({500, [{errcode, 500},{errmsg, "send error"}, {result, lists:flatten(io_lib:write(Reason))}, {url, Url}]})
					end;
				true ->
					erlang:throw({500, [{errcode, 500},{errmsg, "ssl start error"}, {result, lists:flatten(io_lib:write(SR))}, {url, Url}]})
			end;
		true ->
			erlang:throw({500, [{errcode, 500},{errmsg, "inets start error"}, {result, lists:flatten(io_lib:write(Result))}, {url, Url}]})
	end.

% 发送请求获得交易流水号
content_type(json) ->
	"application/json;charset=utf-8";
content_type(xml) ->
	"text/xml";
content_type(multipart) ->
	"multipart/form-data";
content_type(urlencoded) ->
	"application/x-www-form-urlencoded";
content_type(_) ->
	"application/x-www-form-urlencoded".

% 发送请求
http_post(Url, Data) ->
	http_post(Url, Data, urlencoded).

% 发送请求
http_post(Url, Data, ContentType) ->
	% 启动网络环境
	Result = inets:start(),
	if
		(Result =:= ok) orelse (Result =:= {error, {already_started, inets}}) ->
			try httpc:request(post, {Url, [{"connection", "keep-alive"}], content_type(ContentType), Data},
			[{timeout, 10000}, {connect_timeout, 5000}], [{body_format, binary}]) of
				{ok, {{_, RC, _}, _, Body}} when RC >= 200, RC < 400 ->
					unicode:characters_to_list(Body, utf8);
				E ->
					erlang:throw({500, "send result error", [E]})
			catch
				_: Reason ->
					erlang:throw({500, "send error", [Reason]})
			end;
		true ->
			erlang:throw({500, "inets start error", [Result]})
	end.

% 发送请求
https_post(Url, Data) ->
	https_post(Url, Data, urlencoded).

% 发送请求
https_post(Url, Data, ContentType) ->
	% 启动网络环境
	Result = inets:start(),
	if
		(Result =:= ok) orelse (Result =:= {error, {already_started, inets}}) ->
			SR = ssl:start(),
			if
				(SR =:= ok) orelse (SR =:= {error, {already_started, ssl}}) ->
					try httpc:request(post, {Url, [{"connection", "keep-alive"}], content_type(ContentType), Data},
					[{timeout, 10000}, {connect_timeout, 5000}], [{body_format, binary}]) of
						{ok, {{_, RC, _}, _, Body}} when RC >= 200, RC < 400 ->
							unicode:characters_to_list(Body, utf8);
						E -> erlang:throw({500, "send result error", [E]})
					catch
						_: Reason ->
							erlang:throw({500, "send error", [Reason]})
					end;
				true ->
					erlang:throw({500, "ssl start error", [SR]})
			end;
		true ->
			erlang:throw({500, "inets start error", [Result]})
	end.


% 替换文本的内容
replace_text([N | T], Tuple) when is_integer(N), N >= $a ->
	[element(10 + N - $a, Tuple) | replace_text(T, Tuple)];
replace_text([N | T], Tuple) when is_integer(N), N >= $0 ->
	[element(N - $0, Tuple) | replace_text(T, Tuple)];
replace_text([H | T], Tuple) when is_tuple(H) ->
	[list_to_tuple(replace_text(tuple_to_list(H), Tuple)) | replace_text(T, Tuple)];
replace_text([H | T], Tuple) when is_list(H) ->
	[H | replace_text(T, Tuple)];
replace_text([], _) ->
	[].

% 读取xml
read_xml(Str) ->
	{XmlElt, _} = xmerl_scan:string(Str),
	{_, _, _, _, _, _, _, _, EL, _, _, _} = XmlElt,
	[{atom_to_list(EName), EValue} || {xmlElement, EName, _, _, _, _, _, _, [{_, _, _, _, EValue, _} | _], _, _, _} <- EL, EValue =/= ""].

% 随机32位字符串
random_str(Seed, L, N) when N < 32 ->
	Seed1 = z_lib:random(Seed),
	S = erlang:integer_to_list(Seed1, 36),
	random_str(Seed1, S ++ L, length(S) + N);
random_str(_, L, _) ->
	lists:sublist(L, 32).

