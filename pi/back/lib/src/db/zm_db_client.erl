%%@doc 数据库客户端
%%@end

-module(zm_db_client).

-description("data base client").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([count/1, clear/1, lock/5, locks/4]).
-export([read/3, read/5, reads/4, reads/2]).
-export([write/6, writes/3, delete/4, deletes/2, transaction/4, unsafe_transaction/4, super_transaction/2]).
-export([select/6, iterate/3, iterate_next/1]).
-export([select_index/6]).
-export([restore/1]).
-export([get_lock/2]).

%%%=======================DEFINE=======================
-define(LOCK_INFINITY_TIME, 30000).
-define(LOCK_SLEEP_TIME, 30).
-define(RETRY_SLEEP_TIME, 2).
-define(RETRY_COUNT, 2).
-define(HANDLE_TIMEOUT, 5000).

-define(TRANSACTION_READ_TIMEOUT, 3000).
-define(NIL, '$nil').
-define(IGNORE, '$ignore').

-define(ITERATE_KEY_COUNT, 31).
-define(ITERATE_TIMEOUT, 5000).

%%%===============EXPORTED FUNCTIONS===============
%%-----------------------------------------------------------------
%%@doc  估算表的大小，如果为estimate，表示表节点没有全部可用，估算出的值误差更大。
%% @spec  count(Table::atom()) -> return()
%% where
%%  return() =  none_table | undefined | {ok, integer} | {estimate, integer}
%%@end
%%-----------------------------------------------------------------
count(Table) ->
	R = count_(Table),
	log(debug, db_count, [{table, Table}, {result, R}]),
	R.

%%-----------------------------------------------------------------
%%@doc  清理表
%% @spec  clear(Table::atom()) -> return()
%% where
%%  return() =  none_table | undefined | {ok, integer} | {estimate, integer}
%%@end
%%-----------------------------------------------------------------
clear(Table) ->
	R = clear_(Table),
	log(debug, db_clear, [{table, Table}, {result, R}]),
	R.

%%-----------------------------------------------------------------
%%@doc lock
%%```
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		table_lock | table_repair | none_table | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec lock(Table, Key, Lock, LockTime, Timeout) -> return()
%% where
%%	return() = ok | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
lock(Table, Key, Lock, LockTime, Timeout) ->
	try
		lock_(Table, Key, zm_service:lock_format(Lock),
			LockTime, Timeout, zm_time:now_millisecond()),
		log(debug, lock_ok,
			[{table, Table}, {key, Key}, {lock, Lock},
			{locktime, LockTime}, {timeout, Timeout}]),
		ok
	catch
		Error:Reason ->
			log(warn, {lock_error, Error},
				[{table, Table}, {key, Key}, {lock, Lock},
				{locktime, LockTime}, {timeout, Timeout},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc locks
%%```
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		{table_lock | table_repair | none_table, Info} | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec locks(TableKeys::[{Table, Key}], Lock, LockTime, Timeout) -> return()
%% where
%%  return() = ok | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
locks([_ | _] = TableKeys, Lock, LockTime, Timeout) ->
	try
		locks_(TableKeys, zm_service:lock_format(Lock),
			LockTime, Timeout, zm_time:now_millisecond()),
		log(debug, locks_ok,
			[{table_keys, TableKeys}, {lock, Lock},
			{locktime, LockTime}, {timeout, Timeout}]),
		ok
	catch
		Error:Reason ->
			log(warn, {locks_error, Error}, [{table_keys, TableKeys},
				{lock, Lock}, {locktime, LockTime}, {timeout, Timeout},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc read
%%```
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		table_lock | table_repair | none_table | any()
%%'''
%% @spec read(Table::atom(), Key, Timeout::integer()) -> return()
%% where
%%  return() = {ok, Value, Vsn, Time} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
read(Table, Key, Timeout) ->
	try
		R = read_(Table, Key, none, -1, Timeout, zm_time:now_millisecond()),
		log(debug, read_ok,
			[{table, Table}, {key, Key}, {result, R}]),
		R
	catch
		Error:Reason ->
			log(warn, {read_error, Error}, [{table, Table}, {key, Key},
				{timeout, Timeout}, {error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc  read
%%```
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		table_lock | table_repair | none_table | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec  read(Table, Key, Lock, LockTime, Timeout) -> return()
%% where
%%  return() = {ok, Value, Vsn, Time} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
read(Table, Key, Lock, LockTime, Timeout) ->
	try
		R = read_(Table, Key, zm_service:lock_format(Lock), LockTime, Timeout, zm_time:now_millisecond()),
		log(debug, read_lock_ok,
			[{table, Table}, {key, Key},
			{lock, Lock}, {locktime, LockTime}, {result, R}]),
		R
	catch
		Error:Reason ->
			log(warn, {read_lock_error, Error},
				[{table, Table}, {key, Key}, {lock, Lock}, {locktime, LockTime}, {timeout, Timeout},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc  reads
%%```
%%  TableKeys = [{Table, Key}]
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		{table_lock | table_repair | none_table, Info} | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec  reads(TableKeys, Timeout) -> return()
%% where
%%  return() = {ok, [{Value, Vsn, Time}]} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
reads([_ | _] = TableKeys, Timeout) ->
	try
		{ok, R, _Order, _MiddleR} = reads_(TableKeys,
			none, -1, Timeout, zm_time:now_millisecond()),
		log(debug, reads_ok, [{table_keys, TableKeys}, {result, R}]),
		{ok, R}
	catch
		Error:Reason ->
			log(warn, {reads_error, Error},
				[{table_keys, TableKeys}, {timeout, Timeout},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end;
reads(_, _) ->
	{error, empty_data}.

%%-----------------------------------------------------------------
%%@doc  reads
%%```
%%  TableKeys = [{Table, Key}]
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		{table_lock | table_repair | none_table, Info} | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec  reads(TableKeys, Lock, LockTime, Timeout) -> return()
%% where
%%  return() = {ok, [{Value, Vsn, Time}]} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
reads([_ | _] = TableKeys, Lock, LockTime, Timeout) ->
	try
		{ok, R, _Order, _MiddleR} = reads_(TableKeys,
			zm_service:lock_format(Lock), LockTime, Timeout, zm_time:now_millisecond()),
		log(debug, reads_ok, [{table_keys, TableKeys},
			{lock, Lock}, {locktime, LockTime}, {result, R}]),
		{ok, R}
	catch
		Error:Reason ->
			log(warn, {reads_lock_error, Error},
				[{table_keys, TableKeys}, {lock, Lock},
				{locktime, LockTime}, {timeout, Timeout},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end;
reads(_, _, _, _) ->
	{error, empty_data}.

%%-----------------------------------------------------------------
%%@doc  version为0表示总是写入。version为1表示插入。新数据的版本总是从2开始。
%%```
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		{table_lock | table_repair | none_table, Info} | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec  write(Table, Key, Value, Vsn, Lock, LockTime) -> return()
%% where
%%  return() = ok | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
write(Table, Key, Value, Vsn, Lock, LockTime) ->
	try
		write_(Table, Key, Value, Vsn, zm_service:lock_format(Lock), LockTime),
		log(debug, write_ok,
			[{table, Table}, {key, Key}, {value, Value},
			{vsn, Vsn}, {lock, Lock}, {locktime, LockTime}]),
		ok
	catch
		Error:Reason ->
			log(warn, {write_error, Error},
				[{table, Table}, {key, Key}, {value, Value}, {vsn, Vsn}, {lock, Lock}, {locktime, LockTime},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc version为0表示总是写入。version为1表示插入。新数据的版本总是从2开始。
%%```
%% TableKeyValueVsns = [{Table, Key, Value, Vsn}]
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		{table_lock | table_repair | none_table, Info} | any()
%%	Locked = {Pid, Module, Key}
%%'''
%% @spec writes(TableKeyValueVsns, Lock, LockTime) -> return()
%% where
%%	return() = ok | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
writes([_ | _] = TableKeyValueVsns, Lock, LockTime) ->
	try
		writes_(TableKeyValueVsns, zm_service:lock_format(Lock), LockTime),
		log(debug, writes_ok,
			[{table_key_value_vsns, TableKeyValueVsns},
			{lock, Lock}, {locktime, LockTime}]),
		ok
	catch
		Error:Reason ->
			log(warn, {writes_error, Error},
				[{table_key_value_vsns, TableKeyValueVsns},
				{lock, Lock}, {locktime, LockTime},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end;
writes(_, _, _) ->
	{error, empty_data}.

%%-----------------------------------------------------------------
%%@doc  delete
%%```
%%	Reason = {timeout, {Node, Table, Key}} |
%%		{lock_error, Node, Table, Key, Locked, Expire, Multi, Pid} |
%%		table_lock | table_repair |
%%		{none_table, Table}
%%'''
%% @spec  delete(Table, Key, Vsn, Lock) -> return()
%% where
%%  return() = ok | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
delete(Table, Key, Vsn, Lock) ->
	try
		delete_(Table, Key, Vsn, zm_service:lock_format(Lock)),
		log(debug, delete_ok,
			[{table, Table}, {key, Key}, {vsn, Vsn}, {lock, Lock}]),
		ok
	catch
		Error:Reason ->
			log(warn, {delete_error, Error},
				[{table, Table}, {key, Key}, {vsn, Vsn}, {lock, Lock},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc deletes
%%```
%%  TableKeys = [{Table, Key, Vsn}]
%% Returns:
%%	ok | {Error, Reason}
%%	Reason = {timeout, {Node, Table, Key} | [{Node, Table, Key}]} |
%%		{lock_error, Node, Table, Key, Locked, Expire, Multi, Pid}
%%'''
%% @spec deletes(TableKeyVsns, Lock) -> return()
%%@end
%%-----------------------------------------------------------------
deletes([_ | _] = TableKeyVsns, Lock) ->
	try
		deletes_(TableKeyVsns, zm_service:lock_format(Lock)),
		log(debug, deletes_ok,
			[{table_key_vsns, TableKeyVsns}, {lock, Lock}]),
		ok
	catch
		Error:Reason ->
			log(warn, {deletes_error, Error},
				[{table_key_vsns, TableKeyVsns}, {lock, Lock},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end;
deletes(_, _) ->
	{error, empty_data}.

%%-----------------------------------------------------------------
%%@doc 事务处理
%%```
%% TableKey = [{Table, Key}]
%%	Fun Arguments:[Args, [Value]]
%%	Fun Returns:
%%		{break, R} | %跳出返回结果
%%		{ok, R, ModifyList} | %修改值列表，并返回结果
%%		{ok, R, ModifyList, InsertList} | %修改值列表和插入列表，并返回结果
%%		{Error, Reason} %错误及原因
%%	ModifyList中，值为'$nil'为删除，为'$ignore'为忽略不修改
%%'''
%% @spec transaction(TableKeys, Fun, Args, Timeout) -> return()
%% where
%%  return() = {ok | break, R} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
transaction([_ | _] = TableKeys, Fun, Args, Timeout) ->
	try
		R = transaction_(TableKeys, Fun, Args, Timeout),
		log(debug, transaction_ok,
			[{table_keys, TableKeys},
			{func, Fun}, {args, Args}, {result, R}]),
		R
	catch
		throw:Reason ->
			{throw, Reason};
		_:Reason ->
			error_logger:error_report({{transaction_error, error},
				[{table_keys, TableKeys},
				{func, Fun}, {args, Args}, {timeout, Timeout},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]}),
			{error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc 非安全的事务处理
%%```
%% TableKey = [{Table, Key}]
%%	Fun Arguments:[Args, [Value]]
%%	Fun Returns:
%%		{break, R} | %跳出返回结果
%%		{ok, R, ModifyList} | %修改值列表，并返回结果
%%		{ok, R, ModifyList, InsertList} | %修改值列表和插入列表，并返回结果
%%		{Error, Reason} %错误及原因
%%	ModifyList中，值为'$nil'为删除，为'$ignore'为忽略不修改
%%'''
%% @spec unsafe_transaction(TableKeys, Fun, Args, Timeout) -> return()
%% where
%%  return() = {ok | break, R}
%%@end
%%-----------------------------------------------------------------
unsafe_transaction([_ | _] = TableKeys, Fun, Args, Timeout) ->
	R = transaction_(TableKeys, Fun, Args, Timeout),
	log(debug, transaction_ok,
		[{table_keys, TableKeys},
		{func, Fun}, {args, Args}, {result, R}]),
	R.

%%-----------------------------------------------------------------
%%@doc 超级事务处理
%%```
%% TableKeysMFAList = [{[{Table, Key}], M, F, A}]
%%	F Arguments:[A, [Value]]
%%	MFA Returns:
%%		{break, R} | %跳出返回结果
%%		{ok, R, ModifyList} | %修改值列表，并返回结果
%%		{ok, R, ModifyList, InsertList} | %修改值列表和插入列表，并返回结果
%%		{Error, Reason} %错误及原因
%%	ModifyList中，值为'$nil'为删除，为'$ignore'为忽略不修改
%%'''
%% @spec super_transaction(TableKeysMFAList, Timeout) -> return()
%% where
%%  return() = {ok, RL} | {break, R} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
super_transaction([_ | _] = TableKeysMFAList, Timeout) ->
	super_transaction_(TableKeysMFAList, Timeout, [], [], 1).

%%-----------------------------------------------------------------
%%@doc 选择表的指定范围的键或者值的数量或列表
%%```
%%  KeyRange = ascending | descending | {ascending, open | closed, Start} | {descending, open | closed, Start} | {open | closed, Start, open | closed, End}
%%	Filter = FilterFun | none
%%	FilterType = attr | value | all
%%	ResultType = count | key | attr | value | all
%%	FilterFun = {M, Fun, A} | {Fun, A}
%%	Fun Arguments:[A, {Key, Vsn, Time} | {Key, Value} | {Key, Value, Vsn, Time}]
%%	Fun Returns:
%%		true | %选中
%%		false | %未选中
%%		{true, A1} | %选中
%%		{false, A1} | %未选中
%%		break_false | %未选中并跳出
%%		break_true %选中并跳出
%%'''
%% @spec select(Table, KeyRange, Filter, FilterType, ResultType, Force) -> return()
%% where
%%	return() =  {ok, R} | {limit, R} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
select(Table, KeyRange, Filter, FilterType, ResultType, Force) ->
	try
		R = select_(select, Table, KeyRange, Filter, FilterType, ResultType, Force),
		log(debug, select_ok, [{table, Table}, {range, KeyRange}, {filter, Filter},
			{filter_type, FilterType}, {result_type, ResultType}, {force, Force}, {result, R}]),
		R
	catch
		Error:Reason ->
			log(warn, {select_error, Error}, [{table, Table}, {range, KeyRange}, {filter, Filter},
				{filter_type, FilterType}, {result_type, ResultType}, {force, Force},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc  迭代表的指定范围的键
%%```
%% KeyRange = ascending | descending | {ascending, open | closed, Start} | {descending, open | closed, Start} | {open | closed, Start, open | closed, End}
%%'''
%% @spec  iterate(Table, KeyRange, Force) -> return()
%% where
%%  return() =  {ok, Iterator} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
iterate(Table, KeyRange, Force) ->
	try
		{ok, {_, _, Nodes, Keys}} = R = iterate_(Table, KeyRange, Force),
		log(debug, iterate,
			[{table, Table}, {range, KeyRange}, {force, Force},
			{nodes, sb_trees:to_list(Nodes)}, {key_len, length(Keys)}]),
		R
	catch
		Error:Reason ->
			log(warn, {iterate_error, Error}, [{table, Table}, {range, KeyRange}, {force, Force},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc  迭代
%% @spec  iterate_next(Iterate) -> return()
%% where
%%  return() =  over | {ok, Key, NewIterator} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
iterate_next({Table, Range, Nodes, [H | T]}) ->
	try
		{ok, Key, _I} = R = iterate_next_(Table, Range, Nodes, H, T),
		log(debug, iterate_next,
			[{iterator, {Table, Range, H}}, {result, Key}]),
		R
	catch
		Error:Reason ->
			log(warn, {iterate_next_error, Error}, [{iterator,
				{Table, Range, sb_trees:to_list(Nodes), H, length(T)}},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end;
iterate_next({_Table, _Range, _Nodes, []}) ->
	over.

%%-----------------------------------------------------------------
%%@doc 选择表的指定范围的键或者值的数量或列表
%%```
%%  ValueRange = ascending | descending | {ascending, open | closed, Start} | {descending, open | closed, Start} | {open | closed, Start, open | closed, End}
%%	Filter = FilterFun | none
%%	FilterType = all
%%	ResultType = count | key | all
%%	FilterFun = {M, Fun, A} | {Fun, A}
%%	Fun Arguments:[A, {Value, Key} | {Value, Key, Vsn, Time}]
%%	Fun Returns:
%%		true | %选中
%%		false | %未选中
%%		{true, A1} | %选中
%%		{false, A1} | %未选中
%%		break_false | %未选中并跳出
%%		break_true %选中并跳出
%%'''
%% @spec select_index(Table, ValueKeyRange, Filter, FilterType, ResultType, Force) -> return()
%% where
%%	return() =  {ok, R} | {limit, R} | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
select_index(Table, ValueKeyRange, Filter, FilterType, ResultType, Force) ->
	try
		R = select_(select_index, Table, ValueKeyRange, Filter, FilterType, ResultType, Force),
		log(debug, select_index_ok, [{table, Table}, {range, ValueKeyRange}, {filter, Filter},
			{filter_type, FilterType}, {result_type, ResultType}, {force, Force}, {result, R}]),
		R
	catch
		Error:Reason ->
			log(warn, {select_index_error, Error}, [{table, Table}, {range, ValueKeyRange}, {filter, Filter},
				{filter_type, FilterType}, {result_type, ResultType}, {force, Force},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc 按照指定的状态恢复数据
%%```
%% TableKeyValueVsnTimes = [{Table, Key, Value, Vsn, Time}]
%%	Reason = {timeout, Info} |
%%		{lock_error, Locked, LockTime, Multi, Pid, Info} |
%%		{table_lock | table_repair | none_table, Info} | any()
%%'''
%% @spec restore(TableKeyValueVsnTimes) -> return()
%% where
%%	return() =  ok | {Error, Reason}
%%@end
%%-----------------------------------------------------------------
restore([]) ->
	{error, empty_data};
restore(TableKeyValueVsnTimes) ->
	try
		restore_(TableKeyValueVsnTimes),
		log(debug, writes_ok,
			[{table_key_value_vsn_times, TableKeyValueVsnTimes}]),
		ok
	catch
		Error:Reason ->
			log(warn, {writes_error, Error},
				[{table_key_value_vsn_times, TableKeyValueVsnTimes},
				{error, Reason}, {stacktrace, erlang:get_stacktrace()}]),
			{Error, Reason}
	end.

%%-----------------------------------------------------------------
%%@doc 获取指定的锁
%%```
%% Arg = any
%%'''
%% @spec get_lock(arg(), default()) -> return()
%% where
%%	arg() = any
%%	default() = any
%%	return() =  any | Default
%%@end
%%-----------------------------------------------------------------
get_lock(Fun, _) when is_function(Fun, 2) ->
	{erlang:fun_info(Fun, module), erlang:fun_info(Fun, name)};
get_lock(_, Default) ->
	Default.

%%%===================LOCAL FUNCTIONS==================
% 日志记录
log(Level, Code, Data) ->
	case get('$log') of
		{Src, Type} ->
			zm_log:Level(?MODULE, Code, Src, Type, Data);
		_ ->
			zm_log:Level(?MODULE, Code, ?MODULE, none, Data)
	end.

% 发送Cast数据
cast_send(Dest, Msg) ->
	z_lib:send(Dest, {'$gen_cast', Msg}).

% 发送Call数据
call_send(Dest, From, Msg) ->
	z_lib:send(Dest, {'$gen_call', From, Msg}).

% 获得表数量的远程调用
count_(Table) ->
	case zm_db:get(Table) of
		{_, Duplication, _} ->
			{_, NL, NT, _, _} = zm_db:nodes(),
			Size = tuple_size(NT),
			D = if
				Size > Duplication ->
					Duplication + 1;
				true ->
					Size
			end,
			count_call(Table, Size, D, zm_db:active_nodes(
				NL, zm_service:get(Table)));
		_ ->
			none_table
	end.

% 获得表数量的远程调用
count_call(_Table, _Amount, _Duplication, []) ->
	undefined;
count_call(Table, Amount, Duplication, NodeList) ->
	{Replies, _BadNodes} = gen_server:multi_call(
		NodeList, Table, count, ?HANDLE_TIMEOUT),
	F = fun
		(A, {_N, undefined}) -> A;
		({NodeCount, Count}, {_N, C}) -> {NodeCount + 1, Count + C}
	end,
	case z_lib:foreach(F, {0, 0}, Replies) of
		{0, _} ->
			undefined;
		{Amount, Count} ->
			{ok, Count div Duplication};
		{NodeCount, Count} ->
			{estimate, Amount * Count div (Duplication * NodeCount)}
	end.


% 清理表的远程调用
clear_(Table) ->
	case zm_db:get(Table) of
		{_, Duplication, _} ->
			{_, NL, NT, _, _} = zm_db:nodes(),
			Size = tuple_size(NT),
			D = if
				Size > Duplication ->
					Duplication + 1;
				true ->
					Size
			end,
			clear_call(Table, Size, D, zm_db:active_nodes(
				NL, zm_service:get(Table)));
		_ ->
			none_table
	end.

% 清理表的远程调用
clear_call(_Table, _Amount, _Duplication, []) ->
	undefined;
clear_call(Table, Amount, Duplication, NodeList) ->
	{Replies, _BadNodes} = gen_server:multi_call(
		NodeList, Table, clear, ?HANDLE_TIMEOUT),
	F = fun
		(A, {_N, undefined}) -> A;
		({NodeCount, Count}, {_N, C}) -> {NodeCount + 1, Count + C}
	end,
	case z_lib:foreach(F, {0, 0}, Replies) of
		{0, _} ->
			undefined;
		{Amount, Count} ->
			{ok, Count div Duplication};
		{NodeCount, Count} ->
			{estimate, Amount * Count div (Duplication * NodeCount)}
	end.

% 获得指定表键所在的节点
active_dht_node(Table, Key) ->
	case zm_db:active_dht_node(Table, Key) of
		[Node | _] ->
			Node;
		[] ->
			erlang:error({none_table, Table})
	end.

% 锁单个键
lock_(Table, Key, Lock, LockTime, Timeout, Now) when Timeout > 1 ->
	Node = active_dht_node(Table, Key),
	Ref = make_ref(),
	call_send({Table, Node}, {self(), Ref}, {lock, Key, Lock, LockTime, 0}),
	receive
		{Ref, ok} ->
			ok;
		{Ref, {lock_error, Locked, Expire, Multi, Pid}} ->
			if
				Expire >= ?LOCK_INFINITY_TIME ->
					erlang:error({lock_error, Locked, Expire, Multi, Pid, Node});
				Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
					timer:sleep(?LOCK_SLEEP_TIME),
					Now1 = zm_time:now_millisecond(),
					lock_(Table, Key, Lock, LockTime,
						Timeout + Now - Now1, Now1);
				true ->
					erlang:error({lock_error, Locked, Expire, Multi, Pid, Node})
			end;
		{Ref, {error, R}} ->
			erlang:error({R, Node})
		after Timeout ->
			erlang:error({timeout, Node})
	end;
lock_(_Table, _Key, _Lock, _LockTime, _Timeout, _Now) ->
	erlang:error({timeout, none}).

% 锁多个键
locks_(TableKeys, Lock, LockTime, Timeout, Now) when Timeout > 1 ->
	L = lock_loc(TableKeys, 1, []),
	Ref = make_ref(),
	lock_receive(TableKeys, Lock, LockTime, Timeout, Now,
		Ref, lock_send(Lock, LockTime, Ref, L, []), ?RETRY_COUNT);
locks_(_TableKeys, _Lock, _LockTime, _Timeout, _Now) ->
	erlang:error({timeout, none}).

% 计算锁表键的位置
lock_loc([{Table, Key} = TableKey | T], N, L) ->
	lock_loc(T, N + 1, [{N, active_dht_node(Table, Key), TableKey} | L]);
lock_loc([], _N, L) ->
	L;
lock_loc([H | _], _N, _L) ->
	erlang:error({badarg, H}).

% 锁发送，返回发送元组
lock_send(Lock, LockTime, Ref, [{Loc, Node, {Table, Key}} = H | T], L) ->
	call_send({Table, Node}, {self(), {Ref, Loc}}, {lock, Key, Lock, LockTime, 1}),
	lock_send(Lock, LockTime, Ref, T, [H | L]);
lock_send(_Lock, _LockTime, _Ref, [], L) ->
	list_to_tuple(L).

% 等待接收返回的值
lock_receive(TableKeys, Lock, LockTime, Timeout,
	Now, Ref, Result, RetryCount) when Timeout > 0 ->
	receive
		{{Ref, Loc}, ok} ->
			case merge_result(Result, Loc, ok) of
				{ok, _} ->
					ok;
				{wait, R} ->
					Now1 = zm_time:now_millisecond(),
					lock_receive(TableKeys, Lock, LockTime,
						Timeout + Now - Now1, Now1, Ref, R, RetryCount);
				{lock, R} ->
					retry_lock(TableKeys, Lock, LockTime,
						Timeout, Now, Ref, R, RetryCount)
			end;
		{{Ref, Loc}, {lock_error, Locked, Expire, Multi, Pid}} ->
			R = set_result(Result, Loc, lock),
			case check_lock(Lock, Timeout,
				R, Locked, Expire, Multi, Pid) of
				sleep ->
					unlock(Result, Lock, LockTime),
					timer:sleep(?LOCK_SLEEP_TIME),
					Now1 = zm_time:now_millisecond(),
					locks_(TableKeys, Lock, LockTime,
						Timeout + Now - Now1, Now1);
				wait ->
					Now1 = zm_time:now_millisecond(),
					lock_receive(TableKeys, Lock, LockTime,
						Timeout + Now - Now1, Now1, Ref, R, RetryCount);
				lock ->
					retry_lock(TableKeys, Lock, LockTime,
						Timeout, Now, Ref, R, RetryCount);
				lock_error ->
					unlock(Result, Lock, LockTime),
					erlang:error({lock_error,
						Locked, Expire, Multi, Pid, Result})
			end;
		{{Ref, _Loc}, E} ->
			unlock(Result, Lock, LockTime),
			erlang:error({E, Result})
		after Timeout ->
			unlock(Result, Lock, LockTime),
			erlang:error({timeout, Result})
	end;
lock_receive(_TableKeys, Lock, LockTime,
	_Timeout, _Now, _Ref, Result, _RetryCount) ->
	unlock(Result, Lock, LockTime),
	erlang:error({timeout, Result}).

% 设置结果集中指定位置的结果的值
set_result(Result, Loc, V) ->
	E = element(Loc, Result),
	setelement(Loc, Result, setelement(1, E, V)).

% 合并成功的结果
merge_result(Result, Loc, V) ->
	R = set_result(Result, Loc, V),
	% 检查结果集左侧
	case check_result(R, 1, Loc - 1, ok) of
		wait ->
			{wait, R};
		Type ->
			% 检查结果集右侧
			{check_result(R, Loc + 1, tuple_size(R), Type), R}
	end.

% 检查结果集
check_result(R, N1, N2, Type) when N1 =< N2 ->
	case element(1, element(N1, R)) of
		N when is_integer(N) ->
			wait;
		lock ->
			check_result(R, N1 + 1, N2, lock);
		_ ->
			check_result(R, N1 + 1, N2, Type)
	end;
check_result(_R, _N1, _N2, Type) ->
	Type.

% 当一个数据被锁时，根据锁的时间，锁的类型来决定处理方式
check_lock(_Lock, _Timeout, _Result, _Locked, Expire, _Multi, _Pid)
	when Expire >= ?LOCK_INFINITY_TIME ->
	% 长锁立即返回锁错误
	lock_error;
check_lock(_Lock, Timeout, _Result, _Locked, _Expire, 0, _Pid) ->
	% 单锁立即休眠，或返回锁错误
	if
		Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
			sleep;
		true ->
			lock_error
	end;
check_lock(Lock, Timeout, Result, Locked, _Expire, _Multi, Pid) ->
	% 比较锁的等级，如果自己高，则继续，否则休眠，或返回锁错误
	Self = self(),
	Hash1 = erlang:phash2({Lock, Self}),
	Hash2 = erlang:phash2({Locked, Pid}),
	if
		Hash1 > Hash2 ->
			% 根据是否为最后一个数据，返回等待或需要再次尝试
			check_result(Result, 1, tuple_size(Result), lock);
		Hash1 =:= Hash2 andalso Self > Pid ->
			check_result(Result, 1, tuple_size(Result), lock);
		true ->
			if
				Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
					sleep;
				true ->
					lock_error
			end
	end.

% 退出前将还未读到和已成功锁住的数据解锁
unlock(Result, Lock, LockTime) when LockTime > 0 ->
	unlock1(Result, unlock, Lock, tuple_size(Result));
unlock(_Result, _Lock, _LockTime) ->
	ok.

% 退出前将还未读到和已成功锁住的数据解锁
lock0(Result, Lock) ->
	unlock1(Result, lock0, Lock, tuple_size(Result)).

% 退出前将还未读到和已成功锁住的数据解锁
unlock1(Result, Type, Lock, N) when N > 0 ->
	E = element(N, Result),
	case element(1, E) of
		lock ->
			unlock1(Result, Type, Lock, N - 1);
		_ ->
			Node = element(2, E),
			{Table, Key} = element(3, E),
			cast_send({Table, Node}, {Type, Key, Lock}),
			unlock1(Result, Type, Lock, N - 1)
	end;
unlock1(_Result, _Type, _Lock, _N) ->
	ok.

% 再次尝试读取被锁定的数据，如果尝试次数到0，则休眠后重新locks
retry_lock(TableKeys, Lock, LockTime,
	Timeout, Now, Ref, Result, RetryCount) when RetryCount > 0 ->
	if
		Timeout >= ?LOCK_SLEEP_TIME + ?RETRY_SLEEP_TIME ->
			timer:sleep(?RETRY_SLEEP_TIME),
			Now1 = zm_time:now_millisecond(),
			lock_receive(TableKeys, Lock, LockTime,
				Timeout + Now - Now1, Now1, Ref,
				retry_lock_send(Lock, LockTime, Ref,
				Result, 1, tuple_size(Result)), RetryCount - 1);
		true ->
			unlock(Result, Lock, LockTime),
			erlang:error({timeout, Result})
	end;
retry_lock(TableKeys, Lock, LockTime, Timeout, Now, _Ref, Result, _RetryCount) ->
	unlock(Result, Lock, LockTime),
	if
		Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
			timer:sleep(?LOCK_SLEEP_TIME),
			Now1 = zm_time:now_millisecond(),
			locks_(TableKeys, Lock, LockTime,
				Timeout + Now - Now1, Now1);
		true ->
			erlang:error({timeout, Result})
	end.

% 重试锁发送，将结果集对应的结果置成等待状态
retry_lock_send(Lock, LockTime, Ref, Result, N1, N2) when N1 =< N2 ->
	case element(N1, Result) of
		{lock, Node, {Table, Key} = TableKey} ->
			call_send({Table, Node},
				{self(), {Ref, N1}}, {lock, Key, Lock, LockTime, 1}),
			retry_lock_send(Lock, LockTime, Ref, setelement(
				N1, Result, {N1, Node, TableKey}), N1 + 1, N2);
		_ ->
			retry_lock_send(Lock, LockTime, Ref, Result, N1 + 1, N2)
	end;
retry_lock_send(_Lock, _LockTime, _Ref, Result, _N1, _N2) ->
	Result.


% 读单个键
read_(Table, Key, Lock, LockTime, Timeout, Now) when Timeout > 1 ->
	Node = active_dht_node(Table, Key),
	Ref = make_ref(),
	call_send({Table, Node}, {self(), Ref}, {read, Key, Lock, LockTime, 0}),
	receive
		{Ref, {ok, _Value, _Vsn, _Time} = R} ->
			R;
		{Ref, {lock_error, Locked, Expire, Multi, Pid}} ->
			if
				Expire >= ?LOCK_INFINITY_TIME ->
					erlang:error({lock_error, Locked, Expire, Multi, Pid, Node});
				Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
					timer:sleep(?LOCK_SLEEP_TIME),
					Now1 = zm_time:now_millisecond(),
					read_(Table, Key, Lock, LockTime,
						Timeout + Now - Now1, Now1);
				true ->
					erlang:error({lock_error, Locked, Expire, Multi, Pid, Node})
			end;
		{Ref, {error, R}} ->
			erlang:error({R, Node})
		after Timeout ->
			erlang:error({timeout, Node})
	end;
read_(_Table, _Key, _Lock, _LockTime, _Timeout, _Now) ->
	erlang:error({timeout, none}).

% 读多个键
reads_(TableKeys, Lock, LockTime, Timeout, Now) ->
	{Order, L} = read_loc(TableKeys, 1, []),
	Ref = make_ref(),
	case read_receive(TableKeys, Lock, LockTime, Timeout, Now,
		Ref, read_send(Lock, LockTime, Ref, L, []), ?RETRY_COUNT) of
		{ok, _, _, _} = R ->
			R;
		R ->
			{ok, order_result(list_to_tuple(Order),
				R, tuple_size(R)), Order, R}
	end.

% 计算读表键的位置
read_loc([{Table, Key} | T], N, L) ->
	read_loc(T, N + 1, [{N, {Table, Key}} | L]);
read_loc([], _N, L) ->
	[{Loc, TableKey} | T] = lists:keysort(2, L),
	{lists:reverse(L), read_merge(T, TableKey, [Loc], 1, [])};
read_loc([H | _], _N, _L) ->
	erlang:error({badarg, H}).

% 合并相同的表、键，并根据表、键计算所在的节点
read_merge([{Loc, TableKey} | T], TableKey, Index, N, L) ->
	read_merge(T, TableKey, [Loc | Index], N, L);
read_merge([{Loc, TableKey1} | T], TableKey2, Index, N, L) ->
	read_merge(T, TableKey1, [Loc], N + 1,
		read_merge(N, TableKey2, Index, L));
read_merge([],TableKey, Index, N, L) ->
	read_merge(N, TableKey, Index, L).

% 根据列是否为单独的请求，合并到列表中
read_merge(N, {Table, Key} = TableKey, Index, L) ->
	[{N, active_dht_node(Table, Key), TableKey, Index} | L].

% 读发送，返回发送元组
read_send(Lock, LockTime, Ref,
	[{Loc, Node, {Table, Key}, _Index} = H | T], L) ->
	call_send({Table, Node}, {self(), {Ref, Loc}},
		{read, Key, Lock, LockTime, 1}),
	read_send(Lock, LockTime, Ref, T, [H | L]);
read_send(_Lock, _LockTime, _Ref, [], L) ->
	list_to_tuple(L).

% 等待接收返回的值
read_receive(TableKeys, Lock, LockTime, Timeout,
	Now, Ref, Result, RetryCount) when Timeout > 0 ->
	receive
		{{Ref, Loc}, {ok, _Value, _Vsn, _Time} = Return} ->
			case merge_result(Result, Loc, Return) of
				{ok, R} ->
					R;
				{wait, R} ->
					Now1 = zm_time:now_millisecond(),
					read_receive(TableKeys, Lock, LockTime,
						Timeout + Now - Now1, Now1, Ref, R, RetryCount);
				{lock, R} ->
					retry_read(TableKeys, Lock, LockTime,
						Timeout, Now, Ref, R, RetryCount)
			end;
		{{Ref, Loc}, {lock_error, Locked, Expire, Multi, Pid}} ->
			R = set_result(Result, Loc, lock),
			case check_lock(Lock, Timeout,
				R, Locked, Expire, Multi, Pid) of
				sleep ->
					unlock(Result, Lock, LockTime),
					timer:sleep(?LOCK_SLEEP_TIME),
					case zm_time:now_millisecond() of
						Now1 when Timeout + Now > Now1 ->
							reads_(TableKeys, Lock, LockTime,
								Timeout + Now - Now1, Now1);
						_ ->
							erlang:error({lock_error,
								Locked, Expire, Multi, Pid, Result})
					end;
				wait ->
					Now1 = zm_time:now_millisecond(),
					read_receive(TableKeys, Lock, LockTime,
						Timeout + Now - Now1, Now1, Ref, R, RetryCount);
				lock ->
					retry_read(TableKeys, Lock, LockTime,
						Timeout, Now, Ref, R, RetryCount);
				lock_error ->
					unlock(Result, Lock, LockTime),
					erlang:error({lock_error,
						Locked, Expire, Multi, Pid, Result})
			end;
		{{Ref, _Loc}, E} ->
			unlock(Result, Lock, LockTime),
			erlang:error({E, Result})
		after Timeout ->
			unlock(Result, Lock, LockTime),
			erlang:error({timeout, Result})
	end;
read_receive(_TableKeys, Lock, LockTime,
	_Timeout, _Now, _Ref, Result, _RetryCount) ->
	unlock(Result, Lock, LockTime),
	erlang:error({timeout, Result}).

% 再次尝试读取被锁定的数据，如果尝试次数到0，则休眠后重新reads
retry_read(TableKeys, Lock, LockTime,
	Timeout, Now, Ref, Result, RetryCount) when RetryCount > 0 ->
	if
		Timeout >= ?LOCK_SLEEP_TIME + ?RETRY_SLEEP_TIME ->
			timer:sleep(?RETRY_SLEEP_TIME),
			Now1 = zm_time:now_millisecond(),
			read_receive(TableKeys, Lock, LockTime,
				Timeout + Now - Now1, Now1, Ref, retry_read_send(
				Lock, LockTime, Ref, Result, 1,
				tuple_size(Result)), RetryCount - 1);
		true ->
			unlock(Result, Lock, LockTime),
			erlang:error({timeout, Result})
	end;
retry_read(TableKeys, Lock, LockTime,
	Timeout, Now, _Ref, Result, _RetryCount) ->
	unlock(Result, Lock, LockTime),
	if
		Timeout >= ?LOCK_SLEEP_TIME + ?LOCK_SLEEP_TIME ->
			timer:sleep(?LOCK_SLEEP_TIME),
			case zm_time:now_millisecond() of
				Now1 when Timeout + Now > Now1 ->
					reads_(TableKeys, Lock, LockTime,
						Timeout + Now - Now1, Now1);
				_ ->
					erlang:error({timeout, Result})
			end;
		true ->
			erlang:error({timeout, Result})
	end.

% 重试读发送，将结果集对应的结果置成等待状态
retry_read_send(Lock, LockTime, Ref, Result, N1, N2) when N1 =< N2 ->
	case element(N1, Result) of
		{lock, Node, {Table, Key} = TableKey, Index} ->
			call_send({Table, Node}, {self(), {Ref, N1}},
				{read, Key, Lock, LockTime, 1}),
			retry_read_send(Lock, LockTime, Ref, setelement(
				N1, Result, {N1, Node, TableKey, Index}), N1 + 1, N2);
		_ ->
			retry_read_send(Lock, LockTime, Ref, Result, N1 + 1, N2)
	end;
retry_read_send(_Lock, _LockTime, _Ref, Result, _N1, _N2) ->
	Result.

% 合并同一个节点和表上的键
order_result(Order, Result, N) when N > 0 ->
	{{ok, V, Vsn, Time}, _Node, _, Index} = element(N, Result),
	case Index of
		[I] ->
			order_result(set_order(Order,
				I, V, Vsn, Time), Result, N - 1);
		_ ->
			order_result(set_order_list(Order,
				Index, V, Vsn, Time), Result, N - 1)
	end;
order_result(Order, _Result, _N) ->
	tuple_to_list(Order).

% 设置指定参数次序位置的值
set_order(Order, Loc, V, Vsn, Time) ->
	setelement(Loc, Order, {V, Vsn, Time}).

% 设置指定参数列表次序位置的值
set_order_list(Order, [Loc | T1], [V | T2], Vsn, Time) ->
	set_order_list(setelement(
		Loc, Order, {V, Vsn, Time}), T1, T2, Vsn, Time);
set_order_list(Order, [Loc | T1], ?NIL, Vsn, Time) ->
	set_order_list(setelement(
		Loc, Order, {?NIL, Vsn, Time}), T1, ?NIL, Vsn, Time);
set_order_list(Order, [], _, _Vsn, _Time) ->
	Order.


% 写数据
write_(Table, Key, Value, Vsn, Lock, LockTime) ->
	Node = active_dht_node(Table, Key),
	Ref = make_ref(),
	call_send({Table, Node}, {self(), Ref},
		{write, Key, Value, Vsn, Lock, LockTime, 0}),
	receive
		{Ref, ok} ->
			ok;
		{Ref, R} ->
			erlang:error({R, Node})
		after ?HANDLE_TIMEOUT ->
			erlang:error({timeout, Node})
	end.

% 写数据
writes_(TableKeyValueVsns, Lock, LockTime) ->
	L = write_loc(TableKeyValueVsns, 1, []),
	Ref = make_ref(),
	action_receive(Lock, LockTime, Ref,
		action_send(Lock, LockTime, Ref, L, [])).

% 计算写表键的位置
write_loc([{{_, _} = TableKey, Value, Vsn} | T], N, L) ->
	write_loc(T, N, [{TableKey, Value, Vsn} | L]);
write_loc([{{_, _} = TableKey, _, Value, Vsn} | T], N, L) ->
	write_loc(T, N, [{TableKey, Value, Vsn} | L]);
write_loc([{Table, Key, Value, Vsn} | T], N, L) ->
	write_loc(T, N, [{{Table, Key}, Value, Vsn} | L]);
write_loc([], N, L) ->
	[{TableKey, Value, Vsn} | T] = L,
	write_merge(T, TableKey, [Value], Vsn, N, []);
write_loc([H | _], _N, _L) ->
	erlang:error({badarg, H}).

% 合并相同的表、键、值和版本，并根据表、键计算所在的节点
write_merge([{TableKey, Val, Vsn} | T], TableKey, Value, Vsn, N, L) ->
	write_merge(T, TableKey, [Val | Value], Vsn, N, L);
write_merge([{TableKey, Val, 0} | T], TableKey, Value, Vsn, N, L) ->
	write_merge(T, TableKey, [Val | Value], Vsn, N, L);
write_merge([{TableKey, Val, Vsn} | T], TableKey, Value, 0, N, L) ->
	write_merge(T, TableKey, [Val | Value], Vsn, N, L);
write_merge([{TableKey, _Val, _Vsn1} | _],
	TableKey, _Value, _Vsn2, _N, _L) ->
	erlang:error({invalid_vsn, TableKey});
write_merge([{TableKey1, Val, Vsn1} | T], TableKey2, Value, Vsn, N, L) ->
	write_merge(T, TableKey1, [Val], Vsn1, N + 1,
		write_merge(N, TableKey2, Value, Vsn, L));
write_merge([], TableKey, Value, Vsn, N, L) ->
	write_merge(N, TableKey, Value, Vsn, L).

% 根据列和值是否为单独的请求，合并到列表中
write_merge(N, {Table, Key} = TableKey, [Value], Vsn, L) ->
	[{N, active_dht_node(Table, Key), TableKey, Value, Vsn} | L];
write_merge(N, {Table, Key} = TableKey, Value, Vsn, L) ->
	[{N, active_dht_node(Table, Key), TableKey, Value, Vsn} | L].

% 操作发送，返回发送元组
action_send(Lock, LockTime, Ref,
	[{Loc, Node, {Table, Key}, Value, Vsn} = H | T], L) ->
	call_send({Table, Node}, {self(), {Ref, Loc}},
		{write, Key, Value, Vsn, Lock, LockTime, 1}),
	action_send(Lock, LockTime, Ref, T, [H | L]);
action_send(Lock, LockTime, Ref, [{Loc, Node, {Table, Key}, Vsn} = H | T], L) ->
	call_send({Table, Node}, {self(), {Ref, Loc}},
		{delete, Key, Vsn, Lock, LockTime, 1}),
	action_send(Lock, LockTime, Ref, T, [H | L]);
action_send(_Lock, _LockTime, _Ref, [], L) ->
	list_to_tuple(L).

% 等待接收返回的值
action_receive(Lock, LockTime, Ref, Result) ->
	receive
		{{Ref, Loc}, ok} ->
			case merge_result(Result, Loc, ok) of
				{ok, _} ->
					ok;
				{wait, R} ->
					action_receive(Lock, LockTime, Ref, R)
			end;
		{{Ref, Loc}, {lock_error, Locked, Expire, Multi, Pid}} ->
			R = set_result(Result, Loc, lock),
			unlock(R, Lock, LockTime),
			erlang:error({lock_error, Locked, Expire, Multi, Pid, Result});
		{{Ref, _Loc}, E} ->
			io:format("!!!!!!!!!!!!!E:~p~n", [E]),
			unlock(Result, Lock, LockTime),
			erlang:error({E, Result})
		after ?HANDLE_TIMEOUT ->
			unlock(Result, Lock, LockTime),
			erlang:error({timeout, Result})
	end.


% 删除
delete_(Table, Key, Vsn, Lock) ->
	Node = active_dht_node(Table, Key),
	Ref = make_ref(),
	call_send({Table, Node}, {self(), Ref}, {delete, Key, Vsn, Lock, 0, 0}),
	receive
		{Ref, ok} ->
			ok;
		{Ref, R} ->
			erlang:error({R, Node})
		after ?HANDLE_TIMEOUT ->
			erlang:error({timeout, Node})
	end.

% 删除
deletes_(TableKeyVsns, Lock) ->
	L = delete_loc(TableKeyVsns, 1, []),
	Ref = make_ref(),
	action_receive(Lock, 0, Ref, action_send(Lock, 0, Ref, L, [])).

% 计算删除表键的位置
delete_loc([{Table, Key, Vsn} | T], N, L) ->
	delete_loc(T, N + 1, [{N, active_dht_node(Table, Key), {Table, Key}, Vsn} | L]);
delete_loc([{{Table, Key} = TableKey, Vsn} | T], N, L) ->
	delete_loc(T, N + 1, [{N, active_dht_node(Table, Key), TableKey, Vsn} | L]);
delete_loc([], _N, L) ->
	L;
delete_loc([H | _], _N, _L) ->
	erlang:error({badarg, H}).

check_repeat(L) ->
	check_repeat(lists:sort(L), []).

check_repeat([TableKey|_], TableKey) ->
	{table_key_repeat, TableKey};
check_repeat([TableKey|T], _) ->
	check_repeat(T, TableKey);
check_repeat([], _) ->
	ok.
	
% 事务
transaction_(TableKeys, Fun, Args, Timeout) ->
	case check_repeat(TableKeys) of
		ok ->
			Lock = zm_service:lock_format(Fun),
			{ok, R, Order, MiddleR} = reads_(TableKeys, Lock,
				Timeout + ?TRANSACTION_READ_TIMEOUT,
				?TRANSACTION_READ_TIMEOUT, zm_time:now_millisecond()),
			RL = [V || {V, _Vsn, _Time} <- R],
			try Fun(Args, RL) of
				{break, _Result} = BR ->
					lock0(MiddleR, Lock),
					BR;
				{ok, Result, ModifyList} when length(Order) =:= length(ModifyList) ->
					transaction_action(Lock, Order, MiddleR, R, ModifyList, []),
					{ok, Result};
				{ok, Result, ModifyList, InsertList} when length(Order) =:= length(ModifyList) ->
					transaction_action(
						Lock, Order, MiddleR, R, ModifyList, InsertList),
					{ok, Result};
				FunR ->
					lock0(MiddleR, Lock),
					erlang:error({invalid_return, RL, FunR})
			catch
				Error:Reason ->
					lock0(MiddleR, Lock),
					erlang:raise(Error, {fun_error, Reason, RL}, erlang:get_stacktrace())
			end;
		Reason ->
			erlang:error(Reason)
	end.

% 事务action
transaction_action(Lock, Order, MiddleR, R, ModifyList, InsertList) ->
	try
		case transaction_result(Order, R, ModifyList, [], [], sb_trees:empty(), InsertList) of
			{_Ignore, [], []} ->
				lock0(MiddleR, Lock);
			{Ignore, Write, Delete} ->
				Ref = make_ref(),
				action_receive(Lock, 0, Ref, action_send(
					Lock, 0, Ref, write_delete(Write, Delete), [])),
				[cast_send({Table, N}, {lock0, Key, Lock})
					|| {Table, Key} <- Ignore,
					begin
						N = case zm_db:active_dht_node(Table, Key) of
							[Node | _] ->
								Node;
							_ ->
								false
						end,
						N =/= false
					end]
		end
	catch
		Error:Reason ->
			lock0(MiddleR, Lock),
			erlang:raise(Error, {write_error, Reason, ModifyList, InsertList}, erlang:get_stacktrace())
	end.

% 将事务结果分拣成插入更新、删除和忽略列表
transaction_result([{_Loc, TableKey} | T1],
	[{_Value, _Vsn, _Time} | T2], [?IGNORE | T3], Ignore, Update, Delete, Insert) ->
	transaction_result(T1, T2, T3, [TableKey | Ignore], Update, Delete, Insert);
transaction_result([{_Loc, TableKey} | T1],
	[{Value, _Vsn, _Time} | T2], [Value | T3], Ignore, Update, Delete, Insert) ->
	transaction_result(T1, T2, T3, [TableKey | Ignore], Update, Delete, Insert);
transaction_result([{_Loc, TableKey} | T1],
	[{_Value, Vsn, _Time} | T2], [?NIL | T3], Ignore, Update, Delete, Insert) ->
	transaction_result(T1, T2, T3, Ignore, Update,
		sb_trees:enter(TableKey, Vsn, Delete), Insert);
transaction_result([{_Loc, TableKey} | T1],
	[{_Value, Vsn, _Time} | T2], [V | T3], Ignore, Update, Delete, Insert) ->
	transaction_result(T1, T2, T3, Ignore,
		[{TableKey, V, Vsn} | Update], Delete, Insert);
transaction_result(_, _, [], Ignore, Update, Delete, Insert) ->
	{Ignore, lists:reverse([{{Table, Key}, 0, V, 1} || X <- Insert, is_tuple({Table, Key, V} = check_insert_format(X))], Update), sb_trees:to_list(Delete)}.

%%检查插入列表的格式
check_insert_format({_, _, _} = T) ->
	T;
check_insert_format(_) ->
	erlang:error(invalid_insert_format).

% 将写入和删除列表计算其位置并合并
write_delete([], Delete) ->
	delete_loc(Delete, 1, []);
write_delete(Write, Delete) ->
	[{N, _, _, _, _} | _] = L = write_loc(Write, 1, []),
	delete_loc(Delete, N + 1, L).


% 超级事务
super_transaction_([{TableKeys, MFA} | T], Timeout, TableKeyList, MFAList, N) ->
	super_transaction_1(TableKeys, MFA, T, Timeout, TableKeyList, MFAList, N, []);
super_transaction_(_, Timeout, [_ | _] = TableKeyList, MFAList, N) ->
	% 依次调用MFAList
	F = fun({MFALocList, Len}, Values) ->
		super_transaction_2(MFALocList, Len, Values, [], [])
	end,
	zm_db_client:transaction(TableKeyList, F, {MFAList, N}, Timeout);
super_transaction_(_, _, _, _, _) ->
	{ok, []}.

% 将表键记录到TableKeyList，位置记录在MFAList上
super_transaction_1([{Table, _} = H | T], MFA, Next, Timeout, TableKeyList, MFAList, N, CL) when is_atom(Table) ->
	case z_lib:list_index(H, TableKeyList) of
		0 ->
			super_transaction_1(T, MFA, Next, Timeout, [H | TableKeyList], MFAList, N + 1, [N | CL]);
		C ->
			super_transaction_1(T, MFA, Next, Timeout, TableKeyList, MFAList, N, [N - C | CL])
	end;
super_transaction_1(_, MFA, Next, Timeout, TableKeyList, MFAList, N, [_ | _] = CL) ->
	super_transaction_(Next, Timeout, TableKeyList, [{MFA, CL} | MFAList], N);
super_transaction_1(_, _, Next, Timeout, TableKeyList, MFAList, N, _) ->
	super_transaction_(Next, Timeout, TableKeyList, MFAList, N).

% 依次调用MFAList
super_transaction_2([{{M, F, A}, CL} | T], N, Values, RL, InsertL) ->
	{RCL, VL} = get_values(CL, N, Values, [], []),
	case M:F(A, VL) of
		{break, R} = R ->
			R;
		{ok, R, ModifyList} ->
			super_transaction_2(T, N, set_values(RCL, ModifyList, N, Values), [R | RL], InsertL);
		{ok, R, ModifyList, InsertList} ->
			super_transaction_2(T, N, set_values(RCL, ModifyList, N, Values), [R | RL], InsertList ++ InsertL);
		E ->
			E
	end;
super_transaction_2(_, _N, Values, RL, InsertL) ->
	{ok, RL, Values, InsertL}.

% 获得对应顺序的值
get_values([C | T], N, Values, CL, VL) ->
	get_values(T, N, Values, [C | CL], [lists:nth(N - C, Values) | VL]);
get_values(_, _N, _Values, CL, VL) ->
	{CL, VL}.

% 设置对应顺序的值
set_values([_C | CT], ['$ignore' | VT], N, Values) ->
	set_values(CT, VT, N, Values);
set_values([C | CT], [V | VT], N, Values) ->
	set_values(CT, VT, N, list_replace(N - C, Values, V, []));
set_values(_, _, _N, Values) ->
	Values.

% 设置对应位置的值
list_replace(N, [H | T], New, L) when N > 1 ->
	list_replace(N - 1, T, New, [H | L]);
list_replace(1, [_ | T], New, L) ->
	lists:reverse(L, [New | T]).


% 选择方法
select_(Cmd, Table, KeyRange, Filter, FilterType, ResultType, Force) ->
	case zm_db:check_complete(Table) of
		{ok, NL, true} ->
			select_map_reduce(Cmd, Table, KeyRange, Filter, FilterType, ResultType, NL);
		{ok, NL, false} when Force =:= true ->
			select_map_reduce(Cmd, Table, KeyRange, Filter, FilterType, ResultType, NL);
		{ok, NL, false} ->
			erlang:error({incomplete_table, NL});
		E ->
			erlang:error(E)
	end.

% 选择方法，用map_reduce发送到各节点上进行处理
select_map_reduce(Cmd, Table, KeyRange, Filter, FilterType, ResultType, NL) ->
	case z_lib:map_reduce(
		fun(Node) ->
			gen_server:call({Table, Node}, {Cmd, KeyRange, Filter, FilterType, ResultType, NL, 0}, infinity)
		end
		, NL, 16#7fffffff) of
		{ok, RL} when ResultType =:= count ->
			select_count(RL, 0, ok);
		{ok, RL} ->
			select_list(sort_type(KeyRange), RL, [], ok);
		timeout ->
			erlang:error(timeout);
		E ->
			erlang:error(E)
	end.

% 选择结果是数量
select_count([{ok, R} | T], N, RType) ->
	select_count(T, R + N, RType);
select_count([{break, R} | T], N, RType) ->
	select_count(T, R + N, RType);
select_count([{limit, R} | T], N, _RType) ->
	select_count(T, R + N, limit);
select_count([{error, Reason} | _T], _N, _RType) ->
	{error, Reason};
select_count([], N, RType) ->
	{RType, N}.

% 选择结果是列表
select_list(SortType, [{ok, R} | T], L, RType) ->
	select_list(SortType, T, z_lib:SortType(R, lists:reverse(L)), RType);
select_list(SortType, [{break, R} | T], L, RType) ->
	select_list(SortType, T, z_lib:SortType(R, lists:reverse(L)), RType);
select_list(SortType, [{limit, R} | T], L, _RType) ->
	select_list(SortType, T, z_lib:SortType(R, lists:reverse(L)), limit);
select_list(_SortType, [{error, Reason} | _T], _L, _RType) ->
	{error, Reason};
select_list(_SortType, [], L, RType) ->
	{RType, L}.

%根据范围，获得排序的方法
sort_type(ascending) ->
	merge_order_desc;
sort_type(descending) ->
	merge_order;
sort_type({ascending, _, _}) ->
	merge_order_desc;
sort_type({descending, _, _}) ->
	merge_order;
sort_type({_, Start, _, End}) when Start < End ->
	merge_order_desc;
sort_type({_, _Start, _, _End}) ->
	merge_order.

% 迭代方法
iterate_(Table, KeyRange, Force) ->
	case zm_db:check_complete(Table) of
		{ok, NL, true} ->
			iterate_map_reduce(Table, KeyRange, NL);
		{ok, NL, false} when Force =:= true ->
			iterate_map_reduce(Table, KeyRange, NL);
		{ok, NL, false} ->
			erlang:error({incomplete_table, NL});
		E ->
			erlang:error(E)
	end.

% 迭代方法，用map_reduce到各节点上取10个键
iterate_map_reduce(Table, KeyRange, NL) ->
	case z_lib:map_reduce(
		fun(Node) ->
			{Node, gen_server:call({Table, Node},
				{iterate, KeyRange, none, attr, attr, NL,
				?ITERATE_KEY_COUNT}, ?ITERATE_TIMEOUT)}
			end
		, NL, ?ITERATE_TIMEOUT) of
		{ok, RL} ->
			{Nodes, Keys} = iterate_list(
				sort_type(KeyRange), RL, sb_trees:empty(), []),
			{ok, {Table, KeyRange, Nodes, Keys}};
		timeout ->
			erlang:error(timeout);
		E ->
			erlang:error(E)
	end.

% 迭代结果列表
iterate_list(SortType, [{Node, {ok, R}} | T], Nodes, L) ->
	iterate_list(SortType, T, sb_trees:insert(Node, {ok, length(R)}, Nodes),
		z_lib:SortType([{K, Node} || K <- R], lists:reverse(L)));
iterate_list(SortType, [{Node, {limit, R}} | T], Nodes, L) ->
	iterate_list(SortType, T, sb_trees:insert(Node, {limit, length(R)}, Nodes),
		z_lib:SortType([{K, Node} || K <- R], lists:reverse(L)));
iterate_list(_SortType, [{error, Reason} | _T], _Nodes, _L) ->
	erlang:error(Reason);
iterate_list(_SortType, [], Nodes, L) ->
	{Nodes, L}.

% 迭代继续方法
iterate_next_(Table, KeyRange, Nodes, {{Key, _Vsn, _Time} = K, Node}, T) ->
	case sb_trees:get(Node, Nodes, none) of
		{limit, C} when C > 1 ->
			{ok, K, {Table, KeyRange,
				sb_trees:enter(Node, {limit, C - 1}, Nodes), T}};
		{limit, _} ->
			case zm_db:check_complete(Table) of
				{ok, NL, _} ->
					case sb_trees:keys(Nodes) of
						NL ->
							iterate_node(K, Node, Table, iterate_range_modify(KeyRange, Key), Nodes, T);
						Old ->
							erlang:error({data_Layout_change, Old, NL})
					end;
				E ->
					erlang:error(E)
			end;
		{ok, C} ->
			{ok, K, {Table, KeyRange,
				sb_trees:enter(Node, {ok, C - 1}, Nodes), T}}
	end.

%迭代范围修改
iterate_range_modify(ascending, Key) ->
	{ascending, open, Key};
iterate_range_modify(descending, Key) ->
	{descending, open, Key};
iterate_range_modify({ascending, _, _}, Key) ->
	{ascending, open, Key};
iterate_range_modify({descending, _, _}, Key) ->
	{descending, open, Key};
iterate_range_modify({_, _Start, Open, End}, Key) ->
	{open, Key, Open, End}.

% 迭代继续，和节点进行通讯获得下一批键
iterate_node(Key, Node, Table, KeyRange, Nodes, L) ->
	case gen_server:call({Table, Node},
		{iterate_next, KeyRange, none, attr, attr, sb_trees:keys(Nodes),
		?ITERATE_KEY_COUNT}, ?ITERATE_TIMEOUT) of
		{ok, R} ->
			SortType = sort_type(KeyRange),
			{ok, Key, {Table, KeyRange,
				sb_trees:enter(Node, {ok, length(R)}, Nodes),
				z_lib:SortType([{K, Node} || K <- R], lists:reverse(L))}};
		{limit, R} ->
			SortType = sort_type(KeyRange),
			{ok, Key, {Table, KeyRange,
				sb_trees:enter(Node, {limit, length(R)}, Nodes),
				z_lib:SortType([{K, Node} || K <- R], lists:reverse(L))}};
		E ->
			erlang:error(E)
	end.

% 恢复数据
restore_(TableKeyValueVsnTimes) ->
	L = restore_loc(TableKeyValueVsnTimes, 1, []),
	Ref = make_ref(),
	action_receive(Ref, action_send(Ref, L, [])).

% 计算写表键的位置
restore_loc([{{_, _} = TableKey, Value, Vsn, Time} | T], N, L) ->
	restore_loc(T, N, [{TableKey, Value, Vsn, Time} | L]);
restore_loc([{{_, _} = TableKey, _, Value, Vsn, Time} | T], N, L) ->
	restore_loc(T, N, [{TableKey, Value, Vsn, Time} | L]);
restore_loc([{Table, Key, Value, Vsn, Time} | T], N, L) ->
	restore_loc(T, N, [{{Table, Key}, Value, Vsn, Time} | L]);
restore_loc([], N, L) ->
	[{TableKey, Value, Vsn, Time} | T] = L,
	restore_merge(T, TableKey, [Value], Vsn, Time, N, []);
restore_loc([H | _], _N, _L) ->
	erlang:error({badarg, H}).

% 合并相同的表、键、值，版本和时间，并根据表、键计算所在的节点
restore_merge([{TableKey, Val, Vsn, Time} | T], TableKey, Value, Vsn, _Time, N, L) ->
	restore_merge(T, TableKey, [Val | Value], Vsn, Time, N, L);
restore_merge([{TableKey, Val, 0, _Time} | T], TableKey, Value, Vsn, Time, N, L) ->
	restore_merge(T, TableKey, [Val | Value], Vsn, Time, N, L);
restore_merge([{TableKey, Val, Vsn, Time} | T], TableKey, Value, 0, _Time, N, L) ->
	restore_merge(T, TableKey, [Val | Value], Vsn, Time, N, L);
restore_merge([{TableKey, _Val, _Vsn1, _Time1} | _],
	TableKey, _Value, _Vsn2, _Time2, _N, _L) ->
	erlang:error({invalid_vsn, TableKey});
restore_merge([{TableKey1, Val, Vsn1, Time1} | T], TableKey2, Value, Vsn2, Time2, N, L) ->
	restore_merge(T, TableKey1, [Val], Vsn1, Time1, N + 1,
		restore_merge(N, TableKey2, Value, Vsn2, Time2, L));
restore_merge([], TableKey, Value, Vsn, Time, N, L) ->
	restore_merge(N, TableKey, Value, Vsn, Time, L).

% 根据列和值是否为单独的请求，合并到列表中
restore_merge(N, {Table, Key} = TableKey, [Value], Vsn, Time, L) ->
	[{N, active_dht_node(Table, Key), TableKey, Value, Vsn, Time} | L];
restore_merge(N, {Table, Key} = TableKey, Value, Vsn, Time, L) ->
	[{N, active_dht_node(Table, Key), TableKey, Value, Vsn, Time} | L].

% 操作发送，返回发送元组
action_send(Ref, [{Loc, Node, {Table, Key}, Value, Vsn, Time} = H | T], L) ->
	call_send({Table, Node}, {self(), {Ref, Loc}},
		{restore, Key, Value, Vsn, Time}),
	action_send(Ref, T, [H | L]);
action_send(_Ref, [], L) ->
	list_to_tuple(L).

% 等待接收返回的值
action_receive(Ref, Result) ->
	receive
		{{Ref, Loc}, ok} ->
			case merge_result(Result, Loc, ok) of
				{ok, _} ->
					ok;
				{wait, R} ->
					action_receive(Ref, R)
			end;
		{{Ref, _Loc}, E} ->
			io:format("!!!!!!!!!!!!!E:~p~n", [E]),
			erlang:error({E, Result})
		after ?HANDLE_TIMEOUT ->
			erlang:error({timeout, Result})
	end.
