%% Author: Administrator
%% Created: 2012-11-1
%% Description: 检索器客户端
-module(retrievaler_client).

%%
%% Include files
%%
-include("retrievaler_client.hrl").

%%
%% Exported Functions
%%
-export([retrieval/6, access/2, proxy_retrieval/5]).

%%
%% API Functions
%%

%%
%% Function: retrieval/6
%% Description: 检索，获取满足时间范围和布尔表达式的索引
%% Arguments: 
%%	LogName
%%	TimeRange
%%	ExpreText
%%	Limit
%%	QueryTimeout
%%	Timeout
%% Returns: {ok, ResultSet} | {error, Error}
%% 
retrieval(LogName, TimeRange, ExpreText, Limit, QueryTimeout, Timeout) 
  when is_list(ExpreText), is_integer(QueryTimeout), 
	   is_integer(Timeout), Timeout > 0, Timeout > QueryTimeout ->
	LogNodes=log_nodes(),
	case z_lib:map_reduce({?MODULE, access, [LogName, TimeRange, ExpreText, Limit, QueryTimeout]}, LogNodes, Timeout) of
		{ok, R} ->
			case filter_sort(R, [], []) of
				{[], Indexs} ->
					{ok, Indexs};
				E ->
					{error, E}
			end;
		Reason ->
			{error, Reason}
	end.

%%
%% Function: access/2
%% Description: 异步访问检索请求
%% Arguments: 
%%	Args
%%		LogName
%%		TimeRange
%%		ExpreText
%%		Limit
%%		QueryTimeout
%%	LogNode日志节点
%% Returns: {ok, ResultSet} | {error,Error}
%% 
access([LogName, TimeRange, BoolExpre, Limit, QueryTimeout], LogNode) ->
	Self=self(),
	Name=name(Self),
	erlang:register(Name, Self),
	From={Name, node()},
	rpc:cast(LogNode, ?MODULE, ?PROXY_RETRIEVAL, [From, TimeRange, BoolExpre, Limit, QueryTimeout]),
	access_receive(LogNode, QueryTimeout).

%%
%%代理日志检索
%%
proxy_retrieval(From, TimeRange, BoolExpre, Limit, Timeout) ->
	try retrievaler:retrieval(TimeRange, BoolExpre, Limit, Timeout) of
		{ok, []} ->
			From ! {ignore, index_not_exist};
		{ok, Indexs} ->
			From ! {ok, Indexs};
		{error, _} = E ->
			From ! E;
		{error, _, _} = E ->
			From ! E 
	catch
		_:Reason ->
			From ! {error, Reason, erlang:get_stacktrace()}
	end.

%%
%% Local Functions
%%

log_nodes() -> 
	case zm_node:layer(?SLAVE) of
		{?SLAVE, _, _, Slaves, _} ->
			case rpc:call(lists:nth(z_lib:random(1, length(Slaves)), Slaves), zm_node, layer, [?LOG_LAYER], 5000) of
				{badrpc, _} ->
					[];
				{?LOG_LAYER, _, _, LogNodes, _} ->
					LogNodes;
				none ->
					[]
			end;
		none ->
			[]
	end.


name(Pid) ->
	list_to_atom(lists:concat([?MODULE, "_", pid_to_list(Pid)])).

access_receive(LogNode, QueryTimeout) ->
	receive
		{ok, Indexs} ->
			{LogNode, Indexs};
		{ignore, _} ->
			[];
		{error, Reason} ->
			{error, {LogNode, Reason}};
		{error, Reason, StackTrace} ->
			{error, {LogNode, Reason, StackTrace}}
	after QueryTimeout ->
		{error, {LogNode, {"log_query_error", [{"reason", timeout}]}}}
	end.

filter_sort([[]|T], EL, L) ->
	filter_sort(T, EL, L);
filter_sort([{error, _} = E|T], EL, L) ->
	filter_sort(T, [E|EL], L);
filter_sort([{LogNode, Indexs}|T], EL, []) ->
	filter_sort(T, EL, [{LogNode, FileId, Point} || {FileId, Point} <- Indexs]);
filter_sort([{LogNode, Indexs}|T], EL, L) ->
	filter_sort(T, EL, [[{LogNode, FileId, Point} || {FileId, Point} <- Indexs], L]);
filter_sort([], EL, L) ->
	{EL, lists:keysort(2, lists:flatten(L))}.


