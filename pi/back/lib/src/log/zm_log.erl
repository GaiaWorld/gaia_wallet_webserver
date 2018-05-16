%%%@doc 日志模块
%%```
%%% 根据日志配置来确定是否记录日志。
%%% 日志配置表：{{debug | info | warn, 源及类型, 模块名, 参数}, Filter}
%%%
%%% 日志发送时，在日志层中随机选一个节点来发送
%%'''
%%@end


-module(zm_log).

-description("log").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([list/0, get/1, set/2, unset/2, debug/5, info/5, warn/5, log/6, filter/2, take_log/2]).

%%%=======================INLINE=======================
-compile({inline, [log/6, filter/2, match/2, match_list/2]}).
-compile({inline_size, 32}).

%%%=======================DEFINE=======================
-define(LOG, log).
-define(DEBUG, 'zm_logger:debug').
-define(INFO, 'zm_logger:info').
-define(WARN, 'zm_logger:warn').

-define(MOD, '$mod').
-define(CODE, '$code').
-define(SRC, '$src_type').
-define(TYPE, '$err_type').

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 列出当前所有的日志配置
%% @spec list() -> return()
%% where
%% level() = debug | info | warn
%% return() = [{LogType::{Level::level(), Mod::atom(), Code::any(), Src::any(), Type::any()}, FilteData, Monitor}]
%%@end
%% -----------------------------------------------------------------
list() ->
	zm_config:get(?MODULE).

%% -----------------------------------------------------------------
%%@doc 获得指定的日志配置
%% @spec get(LogType::{Level::level(), Mod::atom(), Code::any(), Src::any(), Type::any()}) -> return()
%% where
%% level() = debug | info | warn
%% return() =  none | {LogType::{Level::level(), Mod::atom(), Code::any(), Src::any(), Type::any()}, FilteData, Monitor}
%%@end
%% -----------------------------------------------------------------
get(LogType) ->
	zm_config:get(?MODULE, LogType).

%% -----------------------------------------------------------------
%%@doc
%% @spec set(LogType::{Level::level(), Mod::atom(), Code::any(), Src::any(), Type::any()},Filter::filter()) -> return()
%% where
%% filter() = atom() | [tuple()]
%% level() = debug | info | warn
%% return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set({Level, _Mod, _Code, _Src, _Type} = LogType, Filter) when is_atom(Level) ->
	zm_config:set(?MODULE, {LogType, Filter}).

%% -----------------------------------------------------------------
%%@doc unset log
%% @spec unset(LogType::{Level::level(), Mod::atom(), Code::any(), Src::any(), Type::any()},Filter::filter()) -> return()
%% where
%% filter() = atom() | [tuple()]
%% level() = debug | info | warn
%% return() =  ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(LogType, Filter) ->
	zm_config:unset(?MODULE, {LogType, Filter}).

%% -----------------------------------------------------------------
%%@doc debug log
%% @spec debug(Mod::atom(), Code::any(), Src::any(), Type::any(), Data::[{term(),term()}]) -> return()
%% where
%% return() = ok | false
%%@end
%% -----------------------------------------------------------------
debug(Mod, Code, Src, Type, Data) when is_list(Data) ->
	case zm_config:get(?MODULE, {debug, Mod, Code, Src, Type}) of
		{_, Filter} ->
			case filter([{?MOD, Mod}, {?CODE, Code}, {?SRC, Src}, {?TYPE, Type} | Data], Filter) of
				true ->
					false;
				_ ->
					log(?DEBUG, Mod, Code, Src, Type, Data)
			end;
		_ ->
			false
	end.

%% -----------------------------------------------------------------
%%@doc info log
%% @spec info(Mod::atom(), Code::any(), Src::any(), Type::any(), Data::[{term(),term()}]) -> return()
%% where
%% return() = ok | false
%%@end
%% -----------------------------------------------------------------
info(Mod, Code, Src, Type, Data) when is_list(Data) ->
	case zm_config:get(?MODULE, {info, Mod, Code, Src, Type}) of
		{_, Filter} ->
			case filter([{?MOD, Mod}, {?CODE, Code}, {?SRC, Src}, {?TYPE, Type} | Data], Filter) of
				true ->
					false;
				_ ->
					log(?INFO, Mod, Code, Src, Type, Data)
			end;
		_ ->
			log(?INFO, Mod, Code, Src, Type, Data)
	end.

%% -----------------------------------------------------------------
%%@doc warn log
%% @spec warn(Mod::atom(), Code::any(), Src::any(), Type::any(), Data::[{term(),term()}]) -> return()
%% where
%% return() =  ok | false
%%@end
%% -----------------------------------------------------------------
warn(Mod, Code, Src, Type, Data) when is_list(Data) ->
	case zm_config:get(?MODULE, {warn, Mod, Code, Src, Type}) of
		{_, Filter} ->
			case filter([{?MOD, Mod}, {?CODE, Code}, {?SRC, Src}, {?TYPE, Type} | Data], Filter) of
				true ->
					false;
				_ ->
					log(?WARN, Mod, Code, Src, Type, Data)
			end;
		_ ->
			log(?WARN, Mod, Code, Src, Type, Data)
	end.

%% -----------------------------------------------------------------
%%@doc 发送到随机的日志节点上
%% @spec log(LogName::logname(), Mod::atom(), Code::any(), Src::any(), Type::any(), Data::kvs()) -> return()
%% where
%% kv() = {term(),term()}
%% kvs() = [kv()]
%% logname() = debug | info | warn
%% return() = ok | {error, Reason}
%%@end
%% -----------------------------------------------------------------
log(LogName, Mod, Code, Src, Type, Data) ->
	Msg = {z_lib:now_millisecond(), node(), self(), Mod, Code, Src, Type, Data},
	case zm_node:active_nodes(?LOG, 2) of
		L when is_list(L) ->
			[z_lib:send({LogName, N}, Msg) || N <- L];
		{incomplete, L} = E ->
			[z_lib:send({LogName, N}, Msg) || N <- L],
			error_logger:warning_report({?MODULE, log, E, Msg});
		E ->
			error_logger:warning_report({?MODULE, log, E, Msg})
	end.

%% -----------------------------------------------------------------
%%@doc 过滤条件
%% @spec filter(Data,Filter::filter()) -> return()
%% where
%% filter() = atom() | [tuple()]
%% return() =  ok | false
%%@end
%% -----------------------------------------------------------------
filter(Data, Filter) when is_list(Filter) ->
	match_list(Data, Filter);
filter(Data, Filter) ->
	match(Data, Filter).

%% -----------------------------------------------------------------
%%@doc 获取指定日志
%% @spec take_log(LogType, Indexes, Loc, Len) -> return()
%% where
%% return() =  {ok, L} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
take_log(LogType, Indexes) ->
	try zm_logger:get(LogType) of
		{M, {Dir, File, _Interval, _Limit, _MFA, _NextTime, _FileName, _FileId, _IoDevice, _Loc}} ->
			case M:log_query(Indexes, Dir, File) of
				{ok, BinL} ->
					{ok, [{FileId, binary_to_term(Bin)} || {FileId, Bin} <- BinL]};
				{ignore, non_valid_node} = Ignore ->
					{error, Ignore}
			end
	catch
		_:Reason ->
			{error, Reason, erlang:get_stacktrace()}
	end.

%%%===================LOCAL FUNCTIONS==================
%匹配过滤条件
match(Data, {K, V}) when is_atom(K) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			true;
		_ ->
			false
	end;
match(Data, {K, 'or', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			lists:member(V, A);
		_ ->
			false
	end;
match(Data, {K, '>', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V > A;
		_ ->
			false
	end;
match(Data, {K, '>=', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V >= A;
		_ ->
			false
	end;
match(Data, {K, '<', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V < A;
		_ ->
			false
	end;
match(Data, {K, '=<', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V =< A;
		_ ->
			false
	end;
match(Data, {K, '!', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V =/= A;
		_ ->
			false
	end;
match(Data, {K, '-', A1, A2}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V >= A1 andalso V =< A2;
		_ ->
			false
	end;
match(Data, {K, '!', A1, A2}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} ->
			V < A1 orelse V > A2;
		_ ->
			false
	end;
match(Data, {K, 'in', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} when is_list(V) ->
			lists:member(A, V);
		_ ->
			false
	end;
match(Data, {K, 'out', A}) ->
	case lists:keyfind(K, 1, Data) of
		{_, V} when is_list(V) ->
			lists:member(A, V) =/= true;
		_ ->
			false
	end;
match(Data, K) ->
	lists:keyfind(K, 1, Data) =/= false.

%匹配查询条件列表
match_list(Data, [H | T]) ->
	case match(Data, H) of
		true ->
			match_list(Data, T);
		_ ->
			false
	end;
match_list(_Data, []) ->
	true.
