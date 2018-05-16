%%% 封装数据库操作，主要给port用，抛出标准错误


-module(zm_app_db).

-description("app read write select transaction db").
-copyright({seasky, 'www.seasky.cn'}).
-author({zmyth, leo, 'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([read/2, read/3, read/5, reads/1, reads/2, write/4, writes/2, delete/3, delete/4, deletes/2, select/5, transaction/4, super_transaction/2]).

%%%=======================DEFINE=======================
-define(TIMEOUT, 3000).

-define(DB_ERROR, 550).
-define(FUN_ERROR, 555).
-define(APP_ERROR, 600).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 读单数据库
%% @spec read(Table::atom(), Key::any()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
read(Table, Key) ->
	case zm_db_client:read(Table, Key, ?TIMEOUT) of
		{ok, Value, _, _} ->
			Value;
		{_, E} ->
			erlang:throw({?DB_ERROR, {read, Table, Key, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 读单数据库
%% @spec read(Table::atom(), Key::any(), Timeout::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
read(Table, Key, Timeout) ->
	case zm_db_client:read(Table, Key, Timeout) of
		{ok, Value, _, _} ->
			Value;
		{_, E} ->
			erlang:throw({?DB_ERROR, {read, Table, Key, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 读单数据库
%% @spec read(Table::atom(), Key::any(), Timeout::integer()) -> return()
%% where
%%  return() = any()
%%@end
%% -----------------------------------------------------------------
read(Table, Key, Lock, LockTime, Timeout) ->
	case zm_db_client:read(Table, Key, Lock, LockTime, Timeout) of
		{ok, Value, _, _} ->
			Value;
		{_, E} ->
			erlang:throw({?DB_ERROR, {read, Table, Key, Lock, LockTime, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 读多数据库
%% @spec reads(TableKeys::list()) -> return()
%% where
%%  return() = list()
%%@end
%% -----------------------------------------------------------------
reads(TableKeys) ->
	case zm_db_client:reads(TableKeys, ?TIMEOUT) of
		{ok, L} ->
			[V || {V, _, _} <- L];
		{_, E} ->
			erlang:throw({?DB_ERROR, {reads, TableKeys, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 读多数据库
%% @spec reads(TableKeys::list(), Timeout::integer()) -> return()
%% where
%%  return() = list()
%%@end
%% -----------------------------------------------------------------
reads(TableKeys, Timeout) ->
	case zm_db_client:reads(TableKeys, Timeout) of
		{ok, L} ->
			[V || {V, _, _} <- L];
		{_, E} ->
			erlang:throw({?DB_ERROR, {reads, TableKeys, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 写单数据库
%% @spec write(Table::atom(), Key::any(), Value::any(), Lock::any()) -> return()
%% where
%%  return() = ok
%%@end
%% -----------------------------------------------------------------
write(Table, Key, Value, Lock) ->
	case zm_db_client:write(Table, Key, Value, 0, Lock, 0) of
		ok ->
			ok;
		{_, E} ->
			erlang:throw({?DB_ERROR, {write, Table, Key, Value, Lock, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 写多数据库
%% @spec writes(TableKeyValueVsns = [{Table, Key, Value, Vsn}], Lock::any()) -> return()
%% where
%%  return() = ok
%%@end
%% -----------------------------------------------------------------
writes(TableKeyValueVsns, Lock) ->
	case zm_db_client:writes(TableKeyValueVsns, Lock, 0) of
		ok ->
			ok;
		{_, E} ->
			erlang:throw({?DB_ERROR, {writes, TableKeyValueVsns, Lock, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 删除单数据库
%% @spec delete(Table::atom(), Key::any(), Lock::any()) -> return()
%% where
%%  return() = ok
%%@end
%% -----------------------------------------------------------------
delete(Table, Key, Lock) ->
	case zm_db_client:delete(Table, Key, 0, Lock) of
		ok ->
			ok;
		{_, E} ->
			erlang:throw({?DB_ERROR, {delete, Table, Key, Lock, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 删除单数据库
%% @spec delete(Table::atom(), Key::any(), Lock::any()) -> return()
%% where
%%  return() = ok
%%@end
%% -----------------------------------------------------------------
delete(Table, Key, Vsn, Lock) ->
	case zm_db_client:delete(Table, Key, Vsn, Lock) of
		ok ->
			ok;
		{_, E} ->
			erlang:throw({?DB_ERROR, {delete, Table, Key, Lock, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 删除多数据库
%% @spec deletes(TableKeyVsns = [{Table, Key, Vsn}], Lock::any()) -> return()
%% where
%%  return() = ok
%%@end
%% -----------------------------------------------------------------
deletes(TableKeyVsns, Lock) ->
	case zm_db_client:deletes(TableKeyVsns, Lock) of
		ok ->
			ok;
		{_, E} ->
			erlang:throw({?DB_ERROR, {deletes, TableKeyVsns, Lock, E}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 选择数据库
%% @spec write(Table::atom(), KeyRange::any(), ResultType::count | key | attr | value | all) -> return()
%% where
%%  return() = ok
%%@end
%% -----------------------------------------------------------------
select(Table, KeyRange, Filter, FilterType, ResultType) ->
	case zm_db_client:select(Table,
		KeyRange, Filter, FilterType, ResultType, true) of
		{ok, _} = R ->
			R;
		{limit, _} = R ->
			R;
		{error, Reason} ->
			erlang:throw({?DB_ERROR, {select, Reason}, []})
	end.

%% -----------------------------------------------------------------
%%@doc 事务操作数据库
%% @spec transaction(TableKeys::list(), Fun::function(), End::any(), Timeout::integer()) -> return()
%% where
%%  return() = {ok, _R} | {break, _R}
%%@end
%% -----------------------------------------------------------------
transaction(TableKeys, Fun, Args, Timeout) ->
	case zm_db_client:transaction(TableKeys, Fun, Args, Timeout) of
		{ok, _R} = R ->
			R;
		{break, _R} = R ->
			R;
		{throw, {fun_error, {Why, Vars}, _}} ->
			erlang:throw({?APP_ERROR, Why, Vars});
		{throw, {fun_error, {Type, Why, Vars}, _}} ->
			erlang:throw({Type, Why, Vars});
		{_, {fun_error, Reason, _}} ->
			erlang:throw({?FUN_ERROR, Reason, []});
		{error, {invalid_return, _, FunR}} ->
			erlang:throw({?FUN_ERROR, {invalid_return, FunR}, []});
		{_, R} ->
			erlang:throw({?DB_ERROR, R, []})
	end.

%% -----------------------------------------------------------------
%%@doc 事务操作数据库
%% @spec super_transaction(TableKeysMFAList::list(), Timeout::integer()) -> return()
%% where
%%  return() = {ok, _R} | {break, _R}
%%@end
%% -----------------------------------------------------------------
super_transaction(TableKeysMFAList, Timeout) ->
	case zm_db_client:super_transaction(TableKeysMFAList, Timeout) of
		{ok, _R} = R ->
			R;
		{break, _R} = R ->
			R;
		{throw, {fun_error, {0, Why, Vars}, _}} ->
			erlang:throw({?APP_ERROR, Why, Vars});
		{throw, {fun_error, {Type, Why, Vars}, _}} ->
			erlang:throw({Type, Why, Vars});
		{_, {fun_error, Reason, _}} ->
			erlang:throw({?FUN_ERROR, Reason, []});
		{error, {invalid_return, _, FunR}} ->
			erlang:throw({?FUN_ERROR, {invalid_return, FunR}, []});
		{_, R} ->
			erlang:throw({?DB_ERROR, R, []})
	end.
