%%@doc 数据库的工具模块
%%@end


-module(zm_db_util).

-description("db util").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([table_opts/1, default_value/0]).

%%%=======================INLINE=======================
-compile({inline, [format_type/4, table_opts_format_list/4, check_name/1, check_type/2]}).
-compile({inline_size, 32}).

%%%=======================DEFINE=======================
-define(CACHE_SIZE, 64*1024).
-define(CACHE_TIME, 1800).
-define(INTERVAL, {any, 3}).
-define(SNAPSHOT, {{dw, 3}, 3}).

-define(NIL, '$nil').

-define(FORBIDDEN, ["ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GOTO", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LABEL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "RAID0", "RANGE", "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN", "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "X509", "XOR", "YEAR_MONTH", "ZEROFILL", "ACTION", "BIT", "DATE", "ENUM", "NO", "TEXT", "TIME", "TIMESTAMP"]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc  获得标准的表配置
%% @spec  table_opts(Opts) -> return()
%% where
%%  return() =  Opts
%%@end
%% -----------------------------------------------------------------
table_opts(Opts) ->
	case z_lib:get_values(Opts, [{type, none}, {format, none}, {index, false}]) of
		[_, none, _] ->
			erlang:error({undef_format, Opts});
		[memory, Format, Index] ->
			Format1 = table_opts_format(Opts, Format, memory),
			[{type, memory}, {format, Format1}, {index, Index}];
		[file, Format, Index] ->
			Format1 = table_opts_format(Opts, Format, file),
			[CacheSize, CacheTime, Interval, Snapshot, Length] =
				z_lib:get_values(Opts, [{cache_size, ?CACHE_SIZE},
				{cache_time, ?CACHE_TIME}, {interval, ?INTERVAL},
				{snapshot, ?SNAPSHOT}, {length, 0}]),
			[{type, file}, {cache_size, CacheSize}, {cache_time, CacheTime},
				{interval, Interval}, {snapshot, Snapshot},
				{length, Length}, {format, Format1}, {index, Index}];
		_ ->
			erlang:error({invalid_table_opts, Opts})
	end.

% 检查表配置中的数据格式是否正确
table_opts_format(Opts, {KeyFormat, ValueFormat}, MF) ->
	{table_opts_format(Opts, key, KeyFormat, MF),
		table_opts_format(Opts, value, ValueFormat, MF)};
table_opts_format(Opts, Format, _MF) ->
	erlang:error({invalid_table_opts_format, Opts, Format}).

table_opts_format(Opts, KV, [_H | _] = L, MF) ->
	table_opts_format_list(Opts, KV, L, MF),
	list_to_tuple(L);
table_opts_format(Opts, KV, Format, MF) ->
	case element(1, Format) of
		E when is_atom(E) ->
			format_type(Opts, KV, Format, MF),
			Format;
		_ ->
			table_opts_format_list(Opts, KV, tuple_to_list(Format), MF),
			Format
	end.

table_opts_format_list(Opts, KV, [H | T], MF) ->
	format_type(Opts, KV, H, MF),
	table_opts_format_list(Opts, KV, T, MF);
table_opts_format_list(_Opts, _KV, [], _MF) ->
	ok.

% 检查数据格式是否正确，名称检查，类型检查，长度检查
format_type(Opts, KV, {_Name, _Type, Size} = Format, _MF) when Size < 0 ->
	erlang:error({invalid_table_opts_format, Opts, KV, Format});
format_type(Opts, KV, {_Name, _Type, Size, _} = Format, _MF) when Size < 0 ->
	erlang:error({invalid_table_opts_format, Opts, KV, Format});
format_type(Opts, KV, {Name, Type, Size} = Format, MF) when is_atom(Name), Size < 16#7fffffff ->
	[erlang:error({invalid_table_opts_format, Opts, KV, Format}) || check_name(Name), check_type(Type, MF)];
format_type(Opts, KV, {Name, Type, Size, _} = Format, MF) when is_atom(Name), Size < 16#7fffffff ->
	[erlang:error({invalid_table_opts_format, Opts, KV, Format}) || check_name(Name), check_type(Type, MF)];
format_type(Opts, value, {_Name, _Type, Size, index} = Format, _MF) when Size < 0 ->
	erlang:error({invalid_table_opts_format, Opts, value, Format});
format_type(Opts, value, {_Name, _Type, Size, _, index} = Format, _MF) when Size < 0 ->
	erlang:error({invalid_table_opts_format, Opts, value, Format});
format_type(Opts, value, {Name, Type, Size, index} = Format, MF) when is_atom(Name), Size < 16#7fffffff ->
	[erlang:error({invalid_table_opts_format, Opts, value, Format}) || check_name(Name), check_type(Type, MF)];
format_type(Opts, value, {Name, Type, Size, _, index} = Format, MF) when is_atom(Name), Size < 16#7fffffff ->
	[erlang:error({invalid_table_opts_format, Opts, value, Format}) || check_name(Name), check_type(Type, MF)];
format_type(Opts, key, {_Name, _Type, Size, keyless} = Format, _MF) when Size < 0 ->
	erlang:error({invalid_table_opts_format, Opts, key, Format});
format_type(Opts, key, {_Name, _Type, Size, _, keyless} = Format, _MF) when Size < 0 ->
	erlang:error({invalid_table_opts_format, Opts, key, Format});
format_type(Opts, key, {Name, Type, Size, keyless} = Format, MF) when is_atom(Name), Size < 16#7fffffff ->
	[erlang:error({invalid_table_opts_format, Opts, key, Format}) || check_name(Name), check_type(Type, MF)];
format_type(Opts, key, {Name, Type, Size, _, keyless} = Format, MF) when is_atom(Name), Size < 16#7fffffff ->
	[erlang:error({invalid_table_opts_format, Opts, key, Format}) || check_name(Name), check_type(Type, MF)];
format_type(Opts, KV, Format, _MF) ->
	erlang:error({invalid_table_opts_format, Opts, KV, Format}).

% 名称检查
check_name(Name) ->
	case atom_to_list(Name) of
		[$$ | _] ->
			false;
		S ->
			lists:member(string:to_upper(S), ?FORBIDDEN)
	end.

% 类型检查
check_type(any, _) ->
	true;
check_type(string, _) ->
	true;
check_type(integer, _) ->
	true;
check_type(binary, _) ->
	true;
check_type(atom, _) ->
	true;
check_type(tuple, _) ->
	true;
check_type(list, _) ->
	true;
check_type(float, _) ->
	true;
check_type(pid, memery) ->
	true;
check_type(port, memery) ->
	true;
check_type(ref, memery) ->
	true;
check_type(_Type, _) ->
	false.

%% -----------------------------------------------------------------
%%@doc  获得表的默认值
%% @spec  default_value() -> return()
%% where
%%  return() =  term()
%%@end
%% -----------------------------------------------------------------
default_value() ->
	?NIL.

