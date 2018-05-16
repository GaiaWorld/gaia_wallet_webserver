%%% 根据配置、属性和参数读写数据库


-module(zm_rw_db_port).

-description("read write db").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([read/5, write/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 根据配置读取数据库
%% @spec service(Args::tuple(), Con::tuple(), Attr::sb_trees(), Info::[{Key::atom(), Value::any()}], Msg::[{Key::atom(), Value::any()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
read({Table, IDAttrKey, true}, _Con, Attr, Info, _Msg) when is_atom(Table) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, list_to_tuple(zm_app_db:read(Table, ID))}]};
read({Table, IDAttrKey, _Tuple}, _Con, Attr, Info, _Msg) when is_atom(Table) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, zm_app_db:read(Table, ID)}]};
read({TableList, IDAttrKey, true}, _Con, Attr, Info, _Msg) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, list_to_tuple(zm_app_db:reads([{T, ID} || T <- TableList]))}]};
read({TableList, IDAttrKey, _Tuple}, _Con, Attr, Info, _Msg) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, zm_app_db:reads([{T, ID} || T <- TableList])}]};

read({Table, IDAttrKey, KeyList, true}, _Con, Attr, Info, _Msg) when is_atom(Table) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, list_to_tuple(zm_app_db:reads([{Table, {ID, K}} || K <- KeyList]))}]};
read({Table, IDAttrKey, KeyList, _Tuple}, _Con, Attr, Info, _Msg) when is_atom(Table) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, zm_app_db:reads([{Table, {ID, K}} || K <- KeyList])}]}.

%% -----------------------------------------------------------------
%%@doc 写单数据库, 协议参数: {"data", any()}
%% @spec service(Args::list(), Con::tuple(), Attr::sb_trees(), Info::[{Key::atom(), Value::any()}], Msg::[{Key::atom(), Value::any()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
write({Table, IDAttrKey, Limit, Src, EventType}, Con, Attr, Info, Msg) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	Data = z_lib:get_value(Msg, "data", none),
	[erlang:throw({600, "data size overflow", []}) || byte_size(term_to_binary(Data)) > Limit],
	zm_app_db:write(Table, ID, Data, ?MODULE),
	zm_event:notify(Src, EventType, {Con, Attr, Info, ID, Data}),
	{ok, [], Info, [{ok, ok}]};
write({Table, IDAttrKey, KeyList, Limit, Src, EventType}, Con, Attr, Info, Msg) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	Key = z_lib:get_value(Msg, "key", ""),
	[erlang:throw({600, "invalid key", []}) || lists:member(Key, KeyList) =/= true],
	Data = z_lib:get_value(Msg, "data", none),
	[erlang:throw({600, "data size overflow", []}) || byte_size(term_to_binary(Data)) > Limit],
	zm_app_db:write(Table, {ID, Key}, Data, ?MODULE),
	zm_event:notify(Src, EventType, {Con, Attr, Info, ID, Data}),
	{ok, [], Info, [{ok, ok}]}.
