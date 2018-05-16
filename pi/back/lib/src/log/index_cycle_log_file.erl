%% Author: Administrator
%% Created: 2012-10-31
%% Description: 有索引的循环日志文件存储
%%	将日志按指定的时间（分钟为间隔，从0:0分开始）分文件，如果文件数超过限制，则删除最旧的文件
%%可以在初始配置中使用{format, {M, F, A}}自定义的格式函数
-module(index_cycle_log_file).

%%
%% Include files
%%
-define(INTERVAL, 720).
-define(LIMIT, 60).

-define(DATE_MINUTES, 1440).
-define(DATE_SECONDS, 86400).

-define(Q, 34).
-define(SQ, 39).

%%
%% Exported Functions
%%
-export([init/1, log/2, log_query/3, close/1]).

%%
%% API Functions
%%

%% -----------------------------------------------------------------
%% Func: init/1
%% Description: 初始化模块
%% Returns: Args
%% -----------------------------------------------------------------
init(Opts) ->
	case z_lib:get_values(Opts, [{file, none}, {interval, ?INTERVAL},
		{limit, ?LIMIT}, {format, none}]) of
		[File, Interval, Limit, Format] when is_list(File) ->
			F = filename:absname(File),
			Dir = filename:dirname(F),
			case z_lib:make_dir(Dir) of
				ok ->
					BaseName = filename:basename(F),
					{Time, FileName, FileId, IoDevice, Loc} =
						open_file(Dir, BaseName, Interval, erlang:localtime()),
					start_indexer(Dir, {Dir, BaseName, Interval, Limit, Format, Time, FileName, FileId, IoDevice, Loc});
				_ ->
					erlang:error({"make_dir fail", Dir, Opts})
			end;
		_ ->
			erlang:error({"invalid Opts", Opts})
	end.

%% -----------------------------------------------------------------
%% Func: log/2
%% Description: 记录日志
%% Returns: Arg
%% -----------------------------------------------------------------
log(Arg, [Msg | T]) ->
	log(do_log(Arg, Msg), T);
log(Arg, []) ->
	Arg.

%% -----------------------------------------------------------------
%% Func: log_query/3
%% Description: 根据反向索引获取日志文档
%% Returns: Arg
%% -----------------------------------------------------------------
log_query(Indexs, Dir, File) ->
	log_query(Indexs, node(), Dir, File, [], []).

%% -----------------------------------------------------------------
%% Func: close/1
%% Description: 关闭日志
%% Returns: Arg
%% -----------------------------------------------------------------
close({Dir, File, Interval, Limit, MFA, _NextTime, _Name, IoDevice}) ->
	file:close(IoDevice),
	{Dir, File, Interval, Limit, MFA};
close(Arg) ->
	Arg.

%%
%% Local Functions
%%

% 打开指定目录下的文件，如果没有则创建新文件
open_file(Dir, File, Interval, Time) ->
	{ok, Files} = file:list_dir(Dir),
	{F, RealTime} = case [N || N <- Files, string:str(N, File) =/= 0] of
		[] ->
			{get_file_name(Dir, File, Time), Time};
		Names ->
			Name = lists:max(Names),
			case next_interval(get_file_time(Name), Interval) > Time of
				true ->
					{filename:join(Dir, Name), file_to_time(Name)};
				false ->
					{get_file_name(Dir, File, Time), Time}
			end
	end,
	case file:open(F, [read, write, binary, raw]) of
		{ok, IoDevice} ->
			case file:position(IoDevice, eof) of
				{ok, Loc} ->
					{next_interval(Time, Interval), F, time_to_fileid(RealTime), IoDevice, Loc};
				{error, Reason} ->
					erlang:error({"load file fail", Reason, {file, F}})
			end;
		{error, Reason} ->
			erlang:error({"open file fail", Reason, {file, F}})
	end.

% 打开指定目录下的文件，如果没有则抛异常
open_file(FileName) ->
	case file:open(FileName, [read, binary, raw]) of
		{ok, IoDevice} ->
			IoDevice;
		{error, Reason} ->
			erlang:error({"open file fail", Reason, {file, FileName}})
	end.

% 根据时间获得文件名
get_file_name(Dir, File, {{Y, M, D}, {H, Mi, _}}) ->
	filename:join([Dir, File ++
		[$. | integer_to_list(Y)] ++
		[$- | z_lib:integer_to_list(M, 2)] ++
		[$- | z_lib:integer_to_list(D, 2)] ++
		[$_ | z_lib:integer_to_list(H, 2)] ++
		[$- | z_lib:integer_to_list(Mi, 2)]]).

% 根据文件名获得指定文件的时间
get_file_time(File) ->
	case filename:extension(File) of
		[$. | Ext] ->
			case z_lib:split(Ext, $_) of
				[Ext1, Ext2] ->
					case z_lib:split(Ext1, $-) of
						[Y, M, D] ->
							case z_lib:split(Ext2, $-) of
								[H, Mi] ->
									{{list_to_integer(Y),
									list_to_integer(M),
									list_to_integer(D)},
									{list_to_integer(H), list_to_integer(Mi), 0}};
								_ ->
									{{0, 0, 0}, {0, 0, 0}}
							end;
						_ ->
							{{0, 0, 0}, {0, 0, 0}}
					end;
				_ ->
					{{0, 0, 0}, {0, 0, 0}}
			end;
		_ ->
			{{0, 0, 0}, {0, 0, 0}}
	end.

%%列表时间转换为标准时间
list_to_time([[Y, M, D], [H, Mi]]) ->
	{{Y, M, D}, {H, Mi, 0}}.

%%通过已有文件名获取时间
file_to_time(File) ->
	case string:tokens(File, ".") of
		[] ->
			erlang:error({file_to_time_failed, File});
		L ->
			case string:tokens(lists:last(L), "_") of
				[_, _] = L1 ->
					list_to_time([begin 
						 [list_to_integer(Y) || Y <- string:tokens(X, "-")]
					 end || X <- L1]);
				[] ->
					erlang:error({file_to_time_failed, File})
			end
	end.

% 根据时间获得指定文件的文件id(时间的UTC)
time_to_fileid({{Y, M, D}, {H, Mi, _}}) ->
	z_lib:localtime_to_second({{Y, M, D}, {H, Mi, 0}}).

% 根据指定文件的文件id获得文件名
fileid_to_filename(Dir, File, FileId) ->
	get_file_name(Dir, File, z_lib:second_to_localtime(FileId)).

% 获得下一个间隔的时间，超过00:00，则为00:00
next_interval({YMD, {H, M, _}} = DateTime, Interval) ->
	N = ((H * 60 + M) div Interval + 1) * Interval,
	case N < ?DATE_MINUTES of
		true ->
			{YMD, {N div 60, N rem 60, 0}};
		false ->
			S = calendar:datetime_to_gregorian_seconds(DateTime),
			{YMD1, _} = calendar:gregorian_seconds_to_datetime(
				S + ?DATE_SECONDS),
			{YMD1, {0, 0, 0}}
	end.

% 创建新文件
do_log({Dir, File, Interval, Limit, MFA, NextTime, FileName, FileId, IoDevice, Loc},
	{MS, _, _, _, _, _, _, _} = LogData) ->
	Time = z_lib:second_to_localtime(MS div 1000),
	{Size, Data, Doc}=format(MFA, Time, LogData),
	case Time < NextTime of
		true ->
			case file:pwrite(IoDevice, Loc, Data) of
				ok ->
					NewLoc=if
							   Loc =:= 0 ->
								   Loc + Size + 1;
							   true ->
								   Loc + Size
					end,
					indexer:indexing(FileId, {Loc, Size}, Doc),
					{Dir, File, Interval, Limit, MFA, NextTime, FileName, FileId, IoDevice, NewLoc};
				{error, Reason} ->
					erlang:error({"file write fail", Reason, {file, FileName}})
			end;
		false ->
			file:close(IoDevice),
			{NewFileName, NewFileId, NewIoDevice} = create_file(Dir, File, Limit, NextTime),
			case file:pwrite(NewIoDevice, 0, Data) of
				ok ->
					indexer:indexing(NewFileId, {0, Size}, Doc),
					{Dir, File, Interval, Limit, MFA, next_interval(
						NextTime, Interval), NewFileName, NewFileId, NewIoDevice, Size + 1};
				{error, Reason} ->
					erlang:error({"file write fail", Reason, {file, NewFileName}})
			end
	end.

% 格式化信息
format({M, F, A}, Time, Log) ->
	try
		M:F(A, Time, Log)
	catch
		_:_ ->
			format(none, Time, Log)
	end;
format(_MFA, _Time, {MS, NS, _Pid, Src, Type, Mod, Code, Data}) ->
	Doc=[
		 {'$time', MS},
		 {'$src', Src},
		 {'$mod', Mod},
		 {'$code', Code},
		 {'$type', Type}
	] ++ Data,
	Bin=term_to_binary([{'$node', NS}|Doc]),
	{size(Bin), Bin, Doc}.

% 创建指定目录下的文件
create_file(Dir, File, Limit, Time) ->
	delete_file(Dir, File, Limit),
	F = get_file_name(Dir, File, Time),
	FileId=time_to_fileid(Time),
	case file:open(F, [write, binary, raw]) of
		{ok, IoDevice} ->
			{F, FileId, IoDevice};
		{error, Reason} ->
			erlang:error({"create file fail", Reason, {file, F}})
	end.

% 删除指定目录下该文件名打头的数量，如果超出就删除
delete_file(Dir, File, Limit) ->
	{ok, Files} = file:list_dir(Dir),
	L = [N || N <- Files, string:str(N, File) =/= 0],
	case length(L) - Limit of
		C when C > 0 ->
			{C, delete_files(Dir, lists:sort(L), C)};
		_ ->
			{0, L}
	end.

% 删除最小的几个文件
delete_files(Dir, [H | T], N) when N > 0 ->
	catch file:delete(filename:join(Dir, H)),
	delete_files(Dir, T, N - 1);
delete_files(_Dir, L, _N) ->
	L.

%通过索引查询日志, 去重返回
log_query([{FileId, {Loc, Size}}|T], Node, Dir, File, LastFile, L) ->
	F=case LastFile of
		{FileId, IoDevice} ->
			IoDevice;
		{_, IoDevice} ->
			close(IoDevice),
			open_file(fileid_to_filename(Dir, File, FileId));
		[] ->
			open_file(fileid_to_filename(Dir, File, FileId))
	end,
	try file:pread(F, Loc, Size) of
		{ok, Data} ->
			LogDoc=binary_to_term(Data),
			case lists:keyfind('$node', 1, LogDoc) of
				{_, Node} ->
					{_, Time}=lists:keyfind('$time', 1, LogDoc),
					log_query(T, Node, Dir, File, {FileId, F}, [{Time, Data}|L]);
				{_, MainNode} ->
					case exist_main_node(MainNode) of
						true ->
							log_query(T, Node, Dir, File, {FileId, F}, L);
						false ->
							{_, Time}=lists:keyfind('$time', 1, LogDoc), 
							log_query(T, Node, Dir, File, {FileId, F}, [{Time, Data}|L])
					end;
				false ->
					log_query(T, Node, Dir, File, {FileId, F}, L)
			end;
		eof ->
			log_query(T, Node, Dir, File, {FileId, F}, L);
		{error, _} = E ->
			erlang:error(E)
	after
		file:close(F)
	end;
log_query([], _Node, _Dir, _File, LastFile, L) ->
	case LastFile of
		{_, IoDevice} ->
			close(IoDevice);
		[] ->
			next
	end,
	{ok, lists:reverse(L)}.

exist_main_node(Node) ->
	case zm_node:layer(log) of
		{_, List, _, _, _} ->
			lists:member(Node, List);
		none ->
			false
	end.

start_indexer(Dir, Reply) ->
	Args=[{"dir_path", lists:concat([Dir, "/index/"])}, {"is_k_gram", true}],
	case indexer:start_link(Args) of
		{ok, _} ->
			start_retrievaler(Args, Reply);
		{error, {already_started, _}} ->
			start_retrievaler(Args, Reply);
		Reason ->
			erlang:error(Reason)
	end.

start_retrievaler(Args, Reply) ->
	case retrievaler:start_link(Args) of
		{ok, _} ->
			Reply;
		{error, {already_started, _}} ->
			Reply;
		Reason ->
			erlang:error(Reason)
	end.

	