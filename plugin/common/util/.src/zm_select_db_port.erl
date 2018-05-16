%%% 根据配置、属性和参数选取数据库的键值
%%% 通讯参数"table"，选取指定的表，"start", "end"，将属性中的键对应的值和Start End组成选择范围


-module(zm_select_db_port).

-description("read db").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).
-vsn(1).

%%%=======================EXPORT=======================
-export([service/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 登录, 协议参数: {"tables", string}
%% @spec service(Args::list(), Con::tuple(), Attr::sb_trees(), Info::[{Key::atom(), Value::any()}], Msg::[{Key::atom(), Value::any()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
service({TableMap, IDAttrKey}, _Con, Attr, Info, Msg) ->
	[S, Start, End, ResultType] = z_lib:get_values(
		Msg, [{"table", ""}, {"start", -1}, {"end", <<>>}, {"result_type", ""}]),
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr),
	case sb_trees:get(S, TableMap, 0) of
		0 ->
			erlang:throw({401, "forbidden table", [S]});
		Table ->
			% 从数据库里选取
			case zm_app_db:select(Table, {ID, Start}, {ID, End}, none, attr, ResultType) of
				{ok, R} ->
					{ok, [], Info, [{ok, R}]};
				{limit, R} ->
					{ok, [], Info, [{ok, R}, {limit, 1}]}
			end
	end.
