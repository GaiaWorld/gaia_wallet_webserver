%%@doc代码库函数
%%@end


-module(zm_code).

-description("code").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([name/3, purge/3]).
-export([module/5, scan_parse_term/1, scan_parse_eval/2]).

%%%===============EXPORTED FUNCTIONS===============
%% -----------------------------------------------------------------
%%@doc  仅作为加载时的空函数使用
%% @spec  name(M, Info::any(), Crc32::any()) -> return()
%% where
%%  return() =  File
%%@end
%% -----------------------------------------------------------------
name(M, _Info, _Crc32) ->
	M.

%% -----------------------------------------------------------------
%%@doc 清除旧模块代码，再次调用时则重新加载模块代码
%% @spec purge(M, Info::any(), Crc32::any()) -> return()
%% where
%%  return() = true | false
%%@end
%% -----------------------------------------------------------------
purge(M, _Info, _Crc32) ->
	case code:soft_purge(M) of
		true -> code:delete(M);
		_ -> false
	end.

%% -----------------------------------------------------------------
%%@doc 编译加载指定的模块，FunStringList里面的每个字符串必须以“.”结尾。
%% 如果Vsn为0，则Vsn为checksum。
%% @spec module(Module::atom(), Vsn:: 0 | integer(), Exports::list(), Attr,FunStringList::string()) -> return()
%% where
%%  return() = ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
module(Module, Vsn, [_ | _] = Exports, Attr, FunStringList) ->
	case module_parse(FunStringList, []) of
		L when is_list(L) ->
			module_form(Module, [{attribute, 1, module, Module},
				{attribute, 2, export, Exports} | module_attr(Vsn, Attr, 3, L)]);
		E ->
			E
	end;
module(Module, Vsn, _, Attr, _) ->
	module_form(Module, [{attribute, 1, module, Module} | module_attr(Vsn, Attr, 2, [])]).

% 加载form
module_form(Module, Forms) ->
	case compile:forms(Forms) of
		{ok, _, Bin} ->
			case code:load_binary(Module, atom_to_list(Module) ++ ".compile", Bin) of
				{module, _} -> ok;
				E -> E
			end;
		E ->
			E
	end.

% 分析form
module_parse([H | T], L) ->
	case erl_scan:string(H) of
		{ok, Tokens, _EndLine} ->
			case erl_parse:parse_form(Tokens) of
				{ok, Form} ->
					module_parse(T, [Form | L]);
				E ->
					E
			end;
		E ->
			E
	end;
module_parse(_, L) ->
	L.

% 将attr加入到form
module_attr(Vsn, [{K, V} | T], N, L) ->
	module_attr(Vsn, T, N + 1, [{attribute, N, K, V} | L]);
module_attr(0, _, _, L) ->
	L;
module_attr(Vsn, _, N, L) ->
	[{attribute, N, vsn, Vsn} | L].

%% -----------------------------------------------------------------
%%@doc 将字符串转换成erlang的term，字符串必须以“.”结尾
%% @spec scan_parse_term(Text::string()) -> return()
%% where
%% return() = {ok, Term} | {error, ErrorInfo} | {error, Reason, EndLine}
%%@end
%% -----------------------------------------------------------------
-spec scan_parse_term(string()) ->
	{ok, any()} | {error, any()} | {error, any(), integer()}.
%% -----------------------------------------------------------------
scan_parse_term(Text) ->
	case erl_scan:string(Text) of
		{ok, Tokens, _EndLine} ->
			erl_parse:parse_term(Tokens);
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 动态执行Erlang表达式，字符串必须以“.”结尾
%% @spec scan_parse_eval(Text::string(),Bindings) -> return()
%% where
%% return() = {value, Value, NewBindings} | {error, ErrorInfo} | {error, Reason, EndLine}
%%@end
%% -----------------------------------------------------------------
-spec scan_parse_eval(string(), [{term(), term()}]) ->
	{value, any(), [{term(), term()}]} | {error, any()} | {error, any(), integer()}.
%% -----------------------------------------------------------------
scan_parse_eval(Text, Bindings) ->
	case erl_scan:string(Text) of
		{ok, Tokens, _EndLine} ->
			case erl_parse:parse_exprs(Tokens) of
				{ok, Expr} ->
					erl_eval:exprs(Expr, Bindings);
				E ->
					E
			end;
		E ->
			E
	end.
