%%@doc http 显示模块
%%```
%%使用模板或json进行显示
%%'''
%%@end


-module(zm_http_show).

-description("http show").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([get_global/1, show/5]).

%%%=======================DEFINE=======================
-define(SERVER_INTERNAL_ERROR, 500).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 获得全局的参数
%%```
%%  List = [{"global", [{"date", date()}, {"time", time()},
%%		{"host", z_lib:get_value(Info, host, "")},
%%		{"file", z_lib:get_value(Info, path, "")}]}]
%%'''
%% @spec get_global(Info) -> List
%%@end
%% -----------------------------------------------------------------
get_global(Info) ->
	[{"global", [{"date", date()}, {"time", time()},
		{"host", z_lib:get_value(Info, host, "")},
		{"file", z_lib:get_value(Info, path, "")}]}].

%% -----------------------------------------------------------------
%%@doc 将客户端需要显示的数据，进行模板混合，或者json转换后返回
%% @spec show(Args, any(), Info, Headers, Body) -> return()
%% where
%%      return() = {ok, Info, Headers, Body}
%%@end
%% -----------------------------------------------------------------
show(Args, _Con, Info, Headers, Body) ->
	Headers1 = case z_lib:get_value(Args, mime_type, "") of
		"" ->
			Headers;
		Type ->
			zm_http:set_content_type(Headers, Type, "utf-8")
	end,
	case z_lib:get_value(Body, template_file, "") of
		"" ->
			case z_lib:get_value(Args, template_file, "") of
				"" ->
					throw({?SERVER_INTERNAL_ERROR, "undefined template file"});
				F ->
					merge(F, Args, Info, Headers1, Body)
			end;
		F ->
			merge(F, Args, Info, Headers1, Body)
	end.

%%%===================LOCAL FUNCTIONS==================
%json合并数据
merge(json, Args, Info, Headers, Body) ->
	{ok, Info, Headers, zm_json:encode(get_data(Args, Body))};
merge({json, Prefix, Suffix}, Args, Info, Headers, Body) ->
	{ok, Info, Headers, [Prefix, zm_json:encode(get_data(Args, Body)), Suffix]};
%根据模板合并数据
merge(File, Args, Info, Headers, Body) ->
	case zm_res_loader:read(File) of
		{ok, {_, Template}} ->
			{ok, Info, Headers, zm_template:decode(
				Template, get_data(Args, Info, Body))};
		{error, Reason} ->
			throw({?SERVER_INTERNAL_ERROR, zm_resource:format_error(Reason)})
	end.

%获得数据
get_data(Args, Body) ->
	case z_lib:get_value(Args, attr, []) of
		V when is_list(V) ->
			set_attr(V, Body);
		_ ->
			Body
	end.

%获得数据
get_data(Args, Info, Body) ->
	case z_lib:get_value(Args, attr, []) of
		V when is_list(V) ->
			set_attr(get_global(Info), set_attr(V, Body));
		_ ->
			set_attr(get_global(Info), Body)
	end.

%设置消息属性
set_attr([{K, _V} = KV | T], Msg) when is_list(K) ->
	set_attr(T, [KV | Msg]);
set_attr([{K, V} | T], Msg) when is_atom(K) ->
	set_attr(T, [{atom_to_list(K), V} | Msg]);
set_attr([_ | T], Msg) ->
	set_attr(T, Msg);
set_attr([], Msg) ->
	Msg.
