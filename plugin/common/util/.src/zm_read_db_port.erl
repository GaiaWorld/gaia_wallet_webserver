%%% 根据配置、属性和参数读取数据库
%%% 通讯参数"tables"，一般用'|'分隔表，如果有'?'表示需要将该参数（会尽量将该参数转为整数）和ID合成键来获取数据，如果有':'表示取数据的第几列。可读取的表必须在配置上配置
%%% 例如： "app/role@name|app/role@money|app/role@prop:2|app/task?main:2|app/hero?1:2"


-module(zm_read_db_port).

-description("read db").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([service/5]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 根据配置、属性和参数读取数据库, 协议参数: {"tables", string}
%% @spec service(Args::list(), Con::tuple(), Attr::sb_trees(), Info::[{Key::atom(), Value::any()}], Msg::[{Key::atom(), Value::any()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
%% -----------------------------------------------------------------
%%@doc 根据配置读取数据库
%% @spec service(Args::list(), Con::tuple(), Attr::sb_trees(), Info::[{Key::atom(), Value::any()}], Msg::[{Key::atom(), Value::any()}]) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
service({Table, IDAttrKey, true, Timeout}, _Con, Attr, Info, _Msg) when is_atom(Table) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, list_to_tuple(zm_app_db:read(Table, ID, Timeout))}]};
service({Table, IDAttrKey, _Tuple, Timeout}, _Con, Attr, Info, _Msg) when is_atom(Table) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, zm_app_db:read(Table, ID, Timeout)}]};
service({TableList, IDAttrKey, true, Timeout}, _Con, Attr, Info, _Msg) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, list_to_tuple(zm_app_db:reads([{T, ID} || T <- TableList], Timeout))}]};
service({TableList, IDAttrKey, _Tuple, Timeout}, _Con, Attr, Info, _Msg) ->
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	{ok, [], Info, [{ok, zm_app_db:reads([{T, ID} || T <- TableList], Timeout)}]};
% TODO 准备废弃
service({TableMap, TableSeparator, KeySeparator, ColumnSeparator, IDAttrKey, Timeout}, _Con, Attr, Info, Msg) ->
	Tables = z_lib:split(z_lib:get_value(Msg, "tables", ""), TableSeparator),
	{ID, _, _} = sb_trees:get(IDAttrKey, Attr, none),
	case parse(Tables, TableMap,
		KeySeparator, ColumnSeparator, ID, [], [], false) of
		{TableKeys, Columns} ->
			% 从数据库里取Uid对应的Rid
			L = zm_app_db:reads(TableKeys, Timeout),
			{ok, [], Info, [{ok, get_column(L, Columns)}]};
		E ->
		erlang:throw(E)
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

% 将字符串强制转成整数，如果不是整数则返回0
force_int(S) ->
	try
		list_to_integer(S)
	catch
		_:_ -> 0
	end.

% 分析表，返回表、键和列
parse(Table, KeySeparator, ColumnSeparator, ID) ->
	case z_lib:split_first(Table, KeySeparator) of
		{Table1, Key} ->
			case z_lib:split_first(Key, ColumnSeparator) of
				{Key1, Column} ->
					{Table1, {ID, parse_str(Key1)}, force_int(Column)};
				_ ->
					{Table1, {ID, parse_str(Key)}, 0}
			end;
		_ ->
			case z_lib:split_first(Table, ColumnSeparator) of
				{Table1, Column} ->
					{Table1, ID, force_int(Column)};
				_ ->
					{Table, ID, 0}
			end
	end.

% 分析表，返回表键和列
parse([H | T], TableMap, KeySeparator, ColumnSeparator, ID, TableKeys, Columns, Column) ->
	{S, Key, Col} = parse(H, KeySeparator, ColumnSeparator, ID),
	case sb_trees:get(S, TableMap, 0) of
		0 ->
			{401, "forbidden table", [S]};
		Table when Col =:= 0 ->
			parse(T, TableMap, KeySeparator, ColumnSeparator, ID,
				[{Table, Key} | TableKeys], [0 | Columns], Column);
		Table ->
			parse(T, TableMap, KeySeparator, ColumnSeparator, ID,
				[{Table, Key} | TableKeys], [Col | Columns], true)
	end;
parse(_, _, _, _, _, TableKeys, Columns, true) ->
	{TableKeys, Columns};
parse(_, _, _, _, _, TableKeys, _, _) ->
	{TableKeys, 0}.

% 获取值的列
get_column(L, 0) ->
	lists:reverse(L);
get_column(L, Columns) ->
	get_column1(L, Columns, []).

get_column1([V | T1], [0 | T2], L) ->
	get_column1(T1, T2, [V | L]);
get_column1([V | T1], [Column | T2], L) when is_tuple(V) ->
	get_column1(T1, T2, [element(Column, V) | L]);
get_column1([V | T1], [_ | T2], L) ->
	get_column1(T1, T2, [V | L]);
get_column1(_, _, L) ->
	L.
