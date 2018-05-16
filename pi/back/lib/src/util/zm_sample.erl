%%@doc 样本的编解码模块
%%```
%%% 样本格式： {sid,{{Key, Value}|{Key, 变量标记, 默认值}}}
%%% 变量标记  = '$var'|'$svar'|'$slist'|'$var_i'|'$var_f'|'$var_s'|'$var_t'|'$var_l'
%%% 例如：{5, {{type,cailiao}, {count,1}, {name,"青铜"}, {max_count,2}, {limit, '$var', 1}, {info1, '$var', 1}, {info2, '$svar' none}, {attr,[100, 200, 300]}}}
%%% '$var'表示将该字段完全存储
%%% '$svar'嵌套样本 {1,{ {K,'$svar',{2,[{K1,V1}..]}'}  }}
%%% '$slist'嵌套样本列表{1,{ {K,'$slist',[{2,[{K1,V1}..]}'}]  }}
%%%	'$var_i'表示该字段为整数，'$var_f'表示为浮点数，'$var_s'表示为字符串，
%%%	'$var_t'表示为元组，'$var_l'表示为列表
%%% 
%%'''
%%@end


-module(zm_sample).

-description("sample").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([get/2, sid/1, to_kvlist/2]).
-export([set/2, set/3, unset/2, unset/3]).
-export([get_value/3, get_value/4, get_sample_value/3, get_values/3, get_sample_values/3]).
-export([set_value/4, set_values/3]).


%%%=======================DEFINE=======================
-define(SID, sid).%%sid
-define(SEARCH_KEY, search).%%压缩样本的后缀
-define(SUFFIX_SAMPLE, s).%%压缩数据
-define(SUFFIX_CONFIG, c).%%压缩数据
-define(SUFFIX_DEPEND, dep).%%样本嵌套依赖关系
-define(SUFFIX_INFLUENCE, inf).%%样本嵌套影响列表
-define(MAX_DEPTH, 16).%%嵌套最大深度
-define(VAR_TAG, '$var_tag').%%变量统一标记
-define(VAR, '$var').%%变量
-define(SVAR, '$svar').%%样本
-define(SLIST, '$slist').%%样本列表
-define(VAR_I, '$var_i').%%整数
-define(VAR_F, '$var_f').%%浮点数
-define(VAR_S, '$var_s').%%字符串
-define(VAR_T, '$var_t').%%元组
-define(VAR_L, '$var_l').%%列表


%% -----------------------------------------------------------------
%%@doc  创建新样本数据
%%```
%%  详细的描述
%%'''
%% @spec get(Tab::atom(),Sid::integer()) -> return()
%% where
%%	return() = tuple() | none
%%@end
%% ----------------------------------------------------------------
get(Tab, Sid) ->
	%%延迟解析嵌套
	case zm_config:get(Tab, {Sid, ?SUFFIX_SAMPLE}) of
		{_, 0} ->
			update(Tab, Sid);
		{_, Data} ->
			{Sid, Data};
		_ ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 获取sid
%%```
%%  详细的描述
%%'''
%% @spec sid({}) -> return()
%% where
%%	return() = integer() | 0
%%@end
%% ----------------------------------------------------------------
sid({Sid, _}) ->
	Sid;
sid(_) ->
	0.

%% -----------------------------------------------------------------
%%@doc 转换为kv列表
%% @spec to_kvlist(Tab, Sample) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
to_kvlist(Tab, Sample) ->
	decode(Sample, Tab).


%% -----------------------------------------------------------------
%%@doc  根据表名/样本sid/Key列表 获取一组值，如果key对应一个变量则会取默认值
%%```
%%  详细描述同get_value/3
%%'''
%% @spec get_sample_values(Tab, Sid, KeyList) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
get_sample_values(Tab, Sid, KeyList) ->
	[get_sample_value(Tab, Sid, Key) || Key <- KeyList].

%% -----------------------------------------------------------------
%%@doc  根据表名/样本sid/键 获取对应的值
%%```
%%  详细描述同get_value/3
%%'''
%% @spec get_sample_value(Tab, Sid, Key) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
get_sample_value(Tab, Sid, Key) ->
	Sample = get(Tab, Sid),
	get_value(Tab, Sample, Key).

%% -----------------------------------------------------------------
%%@doc 获取一组值
%%```
%%  Sample = 压缩数据
%%  KeyList = list()
%%'''
%% @spec get_values(Tab, Sample, KeyList) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
get_values(Tab, Sample, KeyList) ->
	[get_value(Tab, Sample, Key, none) || Key <- KeyList].

%% -----------------------------------------------------------------
%%@doc  根据表名/压缩数据/键 获取对应的值
%%```
%%  Sample = 压缩数据
%%  Key = atom() | [{index,Key} | atom()]
%%'''
%% @spec get_value(Tab, Sample, Key, Default) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
get_value(Tab, {Sid, Attrs} = _Sample, Key, Default) when is_atom(Key) ->
	SearchTree = get_attr_tree(Tab, Sid),
	AttrSize = size(Attrs),
	case sb_trees:get(Key, SearchTree, Default) of
		{?VAR_TAG, _Var, Index} when (AttrSize >= Index) ->
			element(Index, Attrs);
		{?VAR_TAG, _Var, Index} when (AttrSize < Index) ->
			NewData = get(Tab, Sid),
			{_, NewAttrs} = NewData,
			case size(NewAttrs) >= Index of
				true ->
					element(Index, NewAttrs);
				_ ->
					Default
			end;
		Value ->
			Value
	end;

%%嵌套查找
get_value(Tab, Sample, [Key | T], Default) when is_atom(Key) ->
	NewData = get_value(Tab, Sample, Key, Default),
	get_value(Tab, NewData, T, Default);

%%嵌套列表查找
get_value(Tab, DataList, [{Index, Key} | T], Default) when is_list(DataList), is_integer(Index) ->
	Data = case Index =< length(DataList) of
			   true ->
				   lists:nth(Index, DataList);
			   _ ->
				   none
		   end,
	NewData = get_value(Tab, Data, Key, Default),
	get_value(Tab, NewData, T, Default);
get_value(_Tab, Data, [], _Default) ->
	Data;
get_value(_Tab, _Data, _Key, Default) ->
	Default.

get_value(Tab, Sample, Key) ->
	get_value(Tab, Sample, Key, none).


%% -----------------------------------------------------------------
%%@doc  根据表名/压缩数据/键值列表  修改一组值
%%```
%%  Sample = 压缩数据
%% 	KvList = [{Key, Value}]
%%'''
%% @spec set_values(Tab, Sample, KvList) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
set_values(Tab, Sample, KvList) ->
	lists:foldl(
		fun({Key, Value}, NewData) ->
			set_value(Tab, NewData, Key, Value)
		end,
		Sample, KvList).

%% -----------------------------------------------------------------
%%@doc  根据表名/压缩数据/键/值 设置对应的值
%%```
%%  Sample = 压缩数据
%%  Key = atom() | [{index,Key} | atom()] 
%%  Value = any()
%%'''
%% @spec set_value(Tab, Sample, Key, Value) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
set_value(Tab, {Sid, Attrs} = Sample, Key, Value) when is_atom(Key) ->
	SearchTree = get_attr_tree(Tab, Sid),
	AttrSize = size(Attrs),
	case sb_trees:get(Key, SearchTree, none) of
		{?VAR_TAG, _Var, Index} when AttrSize >= Index ->
			{Sid, setelement(Index, Attrs, Value)};
		{?VAR_TAG, _Var, Index} when AttrSize < Index ->
			%%旧数据新模板
			{Sid, DefaultAttrs} = get(Tab, Sid),
			NewAttrs = list_to_tuple(tuple_to_list(Attrs) ++ lists:nthtail(size(Attrs), tuple_to_list(DefaultAttrs))),
			{Sid, setelement(Index, NewAttrs, Value)};
		none ->
			throw({"sample set const value error", {Sid, Key, Value, none}});
		_ ->
			Sample
	end;
%%嵌套修改
set_value(Tab, Sample, [Key | T], Value) when is_atom(Key) ->
	NewValue = set_value(Tab, get_value(Tab, Sample, Key), T, Value),
	set_value(Tab, Sample, Key, NewValue);

%%嵌套列表修改
set_value(Tab, DataList, [{Index, Key} | T], Value) when is_list(DataList), is_integer(Index) ->
	case Index =< length(DataList) of
		true ->
			{HeadList, [Data | TailList]} = lists:split(Index - 1, DataList),
			NewData = set_value(Tab, Data, [Key | T], Value),
			HeadList ++ [NewData | TailList];
		_ ->
			none
	end;
set_value(_Tab, _Sample, _Key, Value) ->
	Value.


%% -----------------------------------------------------------------
%%@doc 延迟更新，压缩样本数据
%% @spec update(Tab, Sid) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
update(Tab, Sid) ->
	{_, AttrL} = zm_config:get(Tab, {Sid, ?SUFFIX_CONFIG}),
	{_, Data} = Sample = encode({Sid, AttrL}, Tab),
	zm_config:set(Tab, {{Sid, ?SUFFIX_SAMPLE}, Data}),
	Sample.

%% -----------------------------------------------------------------
%%@doc 添加样本
%%```
%%  
%%'''
%% @spec set(Tab::atom(),Config::tuple()) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
set(Tab, Config) ->
	{Sid, AttrLL} = Config,
	%%属性查找模板，用sb_tree存储kv列表
	AttrTree = attrs_tree(AttrLL),
	%%压缩数据
	AttrSort = attrs_sort(AttrLL),
	%% Data = encode(AttrSort),
	build_relation(Tab, Sid, AttrSort),
	zm_config:set(Tab, {Sid, AttrTree}),
	zm_config:set(Tab, {{Sid, ?SUFFIX_SAMPLE}, 0}),
	zm_config:set(Tab, {{Sid, ?SUFFIX_CONFIG}, AttrSort}).
%%添加多个sample配置
set(Tab, ConfigList, Type) ->
	SidL = [Sid || {Sid, _} = Config <- ConfigList, set(Tab, Config) =/= false],
	zm_config:set(Tab, {Type, SidL}).

%%删除样本
unset(Tab, Sid) when is_integer(Sid) ->
	zm_config:delete(Tab, Sid),
	zm_config:delete(Tab, {Sid, ?SUFFIX_SAMPLE}),
	zm_config:delete(Tab, {Sid, ?SUFFIX_CONFIG}),
	remove_relation(Tab, Sid);
unset(Tab, Kv) when is_tuple(Kv) ->
	{Sid, _} = Kv,
	unset(Tab, Sid).
unset(Tab, List, _Opt) when is_list(List) ->
	[unset(Tab, Kv) || Kv <- List].

%%创建关系
build_relation(Tab, Sid, AttrSort) ->
	Dependents = dependents(AttrSort, []),
	add_dependents(Tab, Sid, Dependents),
	[add_influence(Tab, DependSid, Sid) || DependSid <- Dependents],
	InfluenceL = get_influence(Tab, Sid),
	reset(Tab, InfluenceL, 0).
%% -----------------------------------------------------------------
%%@doc 重置压缩数据为未初始化阶段
%% @spec reset(Tab::atom(),list(),Depth::integer()) -> return()
%% where
%%	return() = ok | throw()
%%@end
%% ----------------------------------------------------------------
reset(Tab, [Sid | L], Depth) when (Depth < ?MAX_DEPTH) ->
	zm_config:set(Tab, {{Sid, ?SUFFIX_SAMPLE}, 0}),
	RelationL = get_influence(Tab, Sid),
	reset(Tab, L, Depth),
	reset(Tab, RelationL, Depth + 1);
reset(_Tab, [], _Depth) ->
	ok;
reset(_Tab, L, _Depth) ->
	erlang:error({"sample nested too deeply", L}).
%%添加依赖关系
add_dependents(Tab, Sid, AddL) when is_list(AddL) ->
	case zm_config:get(Tab, {Sid, ?SUFFIX_DEPEND}) of
		{_, DependL} ->
			zm_config:set(Tab, {{Sid, ?SUFFIX_DEPEND}, DependL ++ AddL});
		none ->
			zm_config:set(Tab, {{Sid, ?SUFFIX_DEPEND}, AddL})
	end.

%%添加会影响到的sid
add_influence(Tab, Sid, AddSid) ->
	case zm_config:get(Tab, {Sid, ?SUFFIX_INFLUENCE}) of
		{_, InfluenceL} ->
			zm_config:set(Tab, {{Sid, ?SUFFIX_INFLUENCE}, [AddSid | InfluenceL]});
		none ->
			zm_config:set(Tab, {{Sid, ?SUFFIX_INFLUENCE}, [AddSid]})
	end.
remove_influence(Tab, Sid, RemoveSid) ->
	Influences = get_dependents(Tab, Sid),
	L = [InfSid || InfSid <- Influences, InfSid =/= RemoveSid],
	zm_config:set(Tab, {{Sid, ?SUFFIX_INFLUENCE}, L}).
get_dependents(Tab, Sid) ->
	case zm_config:get(Tab, {Sid, ?SUFFIX_DEPEND}) of
		{_, DependL} ->
			DependL;
		none ->
			[]
	end.
get_influence(Tab, Sid) ->
	case zm_config:get(Tab, {Sid, ?SUFFIX_INFLUENCE}) of
		{_, InfluenceL} ->
			InfluenceL;
		none ->
			[]
	end.
%%移除关联关系
remove_relation(Tab, Sid) ->
	Dependents = get_dependents(Tab, Sid),
	[remove_influence(Tab, DependentSid, Sid) || DependentSid <- Dependents],
	zm_config:delete(Tab, {Sid, ?SUFFIX_DEPEND}).

%%嵌套依赖的样本sid列表
dependents([{_Key, ?SVAR, {Sid, _}} | AttrL], L) ->
	dependents(AttrL, [Sid | L]);
dependents([{_Key, ?SLIST, Value} | AttrL], L) when is_list(Value) ->
	dependents(AttrL, [Sid || {Sid, _} <- Value] ++ L);
dependents([_Attr | AttrL], L) ->
	dependents(AttrL, L);
dependents([], L) ->
	L.

%% -----------------------------------------------------------------
%%@doc  获取样本,样本属性列表为sb_tree
%%```
%%
%%'''
%% @spec get_attr_tree(Tab, Sid) -> return()
%% where
%%	return() = sb_tree()
%%@end
%% ----------------------------------------------------------------
get_attr_tree(Tab, Sid) ->
	{_, AttrTree} = zm_config:get(Tab, Sid),
	AttrTree.

%%原始模板
attrs_sort(AttrLL) ->
	AttrList = lists:foldl(fun(AttrL, AccIn) ->
		AccIn ++ lists:keysort(1, AttrL)
						   end, [], AttrLL),
	AttrList.

%% -----------------------------------------------------------------
%%@doc 用于属性查找的模板
%% @spec attrs_tree(AttrLL) -> return()
%% where
%%	return() = any()
%%@end
%% ----------------------------------------------------------------
attrs_tree(AttrLL) ->
	AttrList = lists:foldl(fun(AttrL, AccIn) ->
		AccIn ++ lists:keysort(1, AttrL)
						   end, [], AttrLL),

	{NewAttrList, _VarCount} = lists:mapfoldl(
		fun(Attr, VarIndex) ->
			%%检查所有的key，必须为integer,atom,tuple
			Key = element(1, Attr),
			if
				is_integer(Key) orelse is_atom(Key) orelse is_tuple(Key) ->
					ok;
				true ->
					throw("invalid key")
			end,
			case Attr of
				{_K, _Value} -> {Attr, VarIndex};
				{K, Var, _Value} -> {{K, {?VAR_TAG, Var, VarIndex}}, VarIndex + 1}
			end
		end,
		1, AttrList),
	sb_trees:from_orddict(lists:keysort(1, NewAttrList)).

%% -----------------------------------------------------------------
%%@doc 压缩样本数据
%% @spec encode({Sid, AttrList}, Tab) -> return()
%% where
%%	return() ={Sid,tuple()}
%%@end
%% ----------------------------------------------------------------
encode({Sid, AttrList}, Tab) when is_integer(Sid), is_list(AttrList) ->
	DataL = [encode_el(Var, Value, Tab) || {_Key, Var, Value} <- AttrList],
	{Sid, list_to_tuple(DataL)};
encode(Sample, Tab) ->
	erlang:error({"must be sample", Sample, Tab}).

encode_el(?SVAR, Value, Tab) ->
	case Value of
		{Sid, AttrL} ->
			Sample = get(Tab, Sid),
			set_values(Tab, Sample, AttrL);
		_ ->
			0
	end;
encode_el(?SLIST, Value, Tab) when is_list(Value) ->
	[case Sample of
		 {_Sid, Attrs} when is_list(Attrs) ->
			 encode_el(?SVAR, Sample, Tab);
		 Value ->
			 Value
	 end
	 || Sample <- Value];
encode_el(?VAR_I, Value, _Tab) when is_integer(Value) ->
	Value;
encode_el(?VAR_F, Value, _Tab) when is_float(Value) ->
	Value;
encode_el(?VAR_S, Value, _Tab) when is_list(Value) ->
	Value;
encode_el(?VAR_T, Value, _Tab) when is_tuple(Value) ->
	Value;
encode_el(?VAR_L, Value, _Tab) when is_list(Value) ->
	Value;
encode_el(?VAR, Value, _Tab) ->
	Value;
encode_el(Var, Value, _Tab) ->
	erlang:error({"invalid data type", Var, Value}).

%%解压缩
decode({Sid, Data}, Tab) ->
	SearchTree = get_attr_tree(Tab, Sid),
	AttrL = sb_trees:to_list(SearchTree),
	AttrSize = size(Data),
	KvL = lists:map(
		fun({Key, {?VAR_TAG, Var, Index}}) when (AttrSize >= Index) ->
			{Key, decode_el(Var, element(Index, Data), Tab)};
			({Key, {?VAR_TAG, Var, Index}}) when (AttrSize < Index) ->
				NewSample = get(Tab, Sid),
				{_, NewData} = NewSample,
				{Key, decode_el(Var, element(Index, NewData), Tab)};
			(Attr) ->
				Attr
		end, AttrL),
	[{?SID, Sid} | KvL];
decode(Value, _Tab) ->
	Value.
decode_el(?SVAR, Value, Tab) ->
	case Value of
		{_Sid, Data} ->
			decode(Data, Tab);
		_ ->
			Value
	end;
decode_el(?SLIST, Value, Tab) when is_list(Value) ->
	[decode(Sample, Tab) || Sample <- Value];
decode_el(_Var, Value, _Tab) ->
	Value.