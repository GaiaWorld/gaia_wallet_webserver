%%@doc 数据库表缓存模块。
%%```
%%% 监听指定表的插入、修改和删除，然后发送到指定层的所有活动节点上的zm_cacher。
%%% 配置上，应设置zm_event上listen函数，和zm_cacher上load函数。
%%% 通过组合配置和参数，可以用不同的缓存方式。
%%% {[calc], zm_event, [{Table, {zm_storage, insert}}, {zm_db_cache, listen, {[calc], {zm_pid_dist, table}, 2, cache}}, 3000]}.
%%% {[calc], zm_event, [{Table, {zm_storage, update}}, {zm_db_cache, listen, {[calc], {zm_pid_dist, table}, 2, cache}}, 3000]}.
%%% {[calc], zm_event, [{Table, {zm_storage, delete}}, {zm_db_cache, listen, {[calc], {zm_pid_dist, table}, 2, delete}}, 3000]}.
%%'''
%%@end

-module(zm_db_cache).

-description("db cache").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([listen/4, load/3]).

%%%=======================DEFINE=======================
-define(NIL, '$nil').

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  listen，如果CacheType为table, Loc应为0, CacheType为{zm_pid_dist, table}, Loc应为2,
%% @spec  listen({Layer, CacheType, Loc, cache}, Table, Event, {Key, Time}) -> return()
%% where
%%  return() =  any()
%%@end
%% -----------------------------------------------------------------
listen({Layer, CacheType, Loc, cache}, Table, {_, delete}, {Key, _Time}) ->
	write_send(Layer, z_lib:set_column(CacheType, Loc, Table), Key, {?NIL, 0, 0});
listen({Layer, CacheType, Loc, delete}, Table, {_, delete}, {Key, _Time}) ->
	delete_send(Layer, z_lib:set_column(CacheType, Loc, Table), Key);
listen({Layer, CacheType, Loc, cache}, Table, _, {Key, Value, Vsn, Time}) ->
	write_send(Layer, z_lib:set_column(CacheType, Loc, Table), Key, {Value, Vsn, Time});
listen({Layer, CacheType, Loc, delete}, Table, _, {Key, _Value, _Vsn, _Time}) ->
	delete_send(Layer, z_lib:set_column(CacheType, Loc, Table), Key).

%% -----------------------------------------------------------------
%%@doc  load，如果需要保留无数据的key，则EmptyVsn为0，否则为1
%% @spec  load({Loc, Column, Timeout, EmptyVsn}, Type, Key) -> return()
%% where
%%  return() =  {ok, V} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
load({Loc, Timeout, EmptyVsn}, Type, Key) ->
	case zm_db_client:read(z_lib:get_column(Type, Loc), Key, Timeout) of
		{ok, _Value, EmptyVsn, _Time} ->
			{error, empty_value};
		{ok, Value, Vsn, Time} ->
			{ok, {Value, Vsn, Time}};
		E ->
			E
	end.

%%%===================LOCAL FUNCTIONS==================
% 发送删除消息
delete_send(Layer, Type, Key) ->
	Msg = {clear, Type, Key},
	[z_lib:send({zm_cacher, N}, Msg) || N <- zm_node:active_nodes(Layer)].

% 发送写入消息
write_send(Layer, Type, Key, Value) ->
	Msg = {cache, Type, Key, Value},
	[z_lib:send({zm_cacher, N}, Msg) || N <- zm_node:active_nodes(Layer)].
