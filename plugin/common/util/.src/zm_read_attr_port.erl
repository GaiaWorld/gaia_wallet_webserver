%%% 根据配置、属性和参数选取会话属性的键值
%%% 通讯参数"table"，选取指定的表，"start", "end"，将属性中的键对应的值和Start End组成选择范围


-module(zm_read_attr_port).

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
service({TableMap, AttrKey}, _Con, Attr, Info, Msg) ->
	[S, Start, End, ResultType] = z_lib:get_values(
		Msg, [{"table", ""}, {"start", ""}, {"end", ""}, {"result_type", ""}]),
	{ID, _, _} = sb_trees:get(AttrKey, Attr),
	case sb_trees:lookup(S, TableMap) of
		{_, Table} ->
			% 从数据库里选取
			case zm_app_db:select(Table,
				{closed, {ID, Start}, closed, {ID, End}}, none, attr, ResultType, false) of
				{ok, R} ->
					{ok, [], Info, [{ok, R}]};
				{limit, R} ->
					{ok, [], Info, [{ok, R}, {limit, 1}]};
				{error, Reason} ->
					erlang:throw({501, z_lib:string(Reason), []});
				{error, Reason, StackTrace} ->
					erlang:throw({501, z_lib:string(Reason), [z_lib:string(StackTrace)]})
			end;
		_ ->
			erlang:throw({401, "forbidden table", [S]})
	end.

%%%===================LOCAL FUNCTIONS==================
% 根据是否有'开头来判断是否原子，如果不是原子，则尝试将字符串变整数，如果不是整数则返回原字符串
parse_str([$' | S]) ->
	S;
parse_str(S) ->
	try
		list_to_integer(S)
	catch
		_:_ -> S
	end.
