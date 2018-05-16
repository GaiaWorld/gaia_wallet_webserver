%%@doc 循环日志文件存储
%%```
%%% 将日志按指定的时间（分钟为间隔，从0:0分开始）分文件，如果文件数超过限制，则删除最旧的文件
%%可以在初始配置中使用{format, {M, F, A}}自定义的格式函数
%%'''
%%@end


-module(zm_cycle_log_file).

-description("cycle log file").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([init/1, log/2, close/1]).

%%%=======================DEFINE=======================
-define(INTERVAL, 720).
-define(LIMIT, 60).

-define(DATE_MINUTES, 1440).
-define(DATE_SECONDS, 86400).

-define(Q, 34).
-define(SQ, 39).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 初始化模块
%% @spec init(Opts::list()) -> tuple()
%%@end
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
					{Time, FileName, IoDevice} =
						open_file(Dir, BaseName, Interval, erlang:localtime()),
					{Dir, BaseName, Interval, Limit, Format, Time, FileName, IoDevice};
				_ ->
					erlang:error({"make_dir fail", Dir, Opts})
			end;
		_ ->
			erlang:error({"invalid Opts", Opts})
	end.

%% -----------------------------------------------------------------
%%@doc 记录日志
%% @spec log(Arg,Msg::list()) -> tuple()
%%@end
%% -----------------------------------------------------------------
log(Arg, [Msg | T]) ->
	log(do_log(Arg, Msg), T);
log(Arg, []) ->
	Arg.

%% -----------------------------------------------------------------
%%@doc 关闭日志
%% @spec close(Arg::tuple()) -> tuple()
%%@end
%% -----------------------------------------------------------------
close({Dir, File, Interval, Limit, MFA, _NextTime, _Name, IoDevice}) ->
	file:close(IoDevice),
	{Dir, File, Interval, Limit, MFA};
close(Arg) ->
	Arg.

%%%===================LOCAL FUNCTIONS==================
% 打开指定目录下的文件，如果没有则创建新文件
open_file(Dir, File, Interval, Time) ->
	{ok, Files} = file:list_dir(Dir),
	F = case [N || N <- Files, hd(z_lib:split(N, ".")) =:= File] of
		[] ->
			get_file_name(Dir, File, Time);
		Names ->
			Name = lists:max(Names),
			case next_interval(get_file_time(Name), Interval) > Time of
				true ->
					filename:join(Dir, Name);
				false ->
					get_file_name(Dir, File, Time)
			end
	end,
	case file:open(F, [append, raw]) of
		{ok, IoDevice} ->
			{next_interval(Time, Interval), F, IoDevice};
		{error, Reason} ->
			erlang:error({"open_file fail", Reason, {file, F}})
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
do_log({Dir, File, Interval, Limit, MFA, NextTime, Name, IoDevice} = Arg,
	{MS, _, _, _, _, _, _, _} = LogData) ->
	Time = z_lib:second_to_localtime(MS div 1000),
	case Time < NextTime of
		true ->
			case file:write(IoDevice, format(MFA, Time, MS rem 1000, LogData)) of
				ok ->
					Arg;
				{error, Reason} ->
					erlang:error({"file write fail", Reason, {file, Name}})
			end;
		false ->
			Tmp = (z_lib:now_second() - z_lib:localtime_to_second(NextTime)) div 60,
			NewNextTime = if 
				  Tmp > Interval ->
					erlang:localtime();
				true ->
					NextTime
			end,
			file:close(IoDevice),
			{FileName, NewIoDevice} = create_file(Dir, File, Limit, NewNextTime),
			case file:write(NewIoDevice, format(
				MFA, Time, MS rem 1000, LogData)) of
				ok ->
					{Dir, File, Interval, Limit, MFA, next_interval(
						NewNextTime, Interval), FileName, NewIoDevice};
				{error, Reason} ->
					erlang:error({"file write fail", Reason, {file, Name}})
			end
	end.

% 格式化信息
format({M, F, A}, Time, MS, Log) ->
	try
		M:F(A, Time, MS, Log)
	catch
		_:_ ->
			format(none, Time, MS, Log)
	end;
format(IsLineBreak, {{Y, M, D}, {H, Mi, S}}, MS, {_, N, Pid, Mod, Code, Src, Type, Data}) ->
	[${,
		?Q,
		integer_to_list(Y),
		$-, z_lib:integer_to_list(M, 2),
		$-, z_lib:integer_to_list(D, 2),
		$\s, z_lib:integer_to_list(H, 2),
		$:, z_lib:integer_to_list(Mi, 2),
		$:, z_lib:integer_to_list(S, 2),
		$,, z_lib:integer_to_list(MS, 3),
		?Q,
		$,, $\s, ?SQ, atom_to_list(N), ?SQ,
		$,, $\s, ?Q, pid_to_list(Pid), ?Q,
		$,, $\s, z_lib:string(Mod),
		$,, $\s, z_lib:string(Code),
		$,, $\s, z_lib:string(Src),
		$,, $\s, z_lib:string(Type),
		$,, $\s, $[,
		format(IsLineBreak, Data, [$\s]),
		$], $}, $.,
	$\n, $\n].

% 格式化数据
format(IsLineBreak, Data, L) ->
	try
		case IsLineBreak of
			true -> format1(Data, L);
			_ -> format2(Data, L)
		end
	catch
		_:_ ->
			[io_lib:write(Data), $\n, "======DATA_WITHOUT_FORMAT======", $\n]
	end.

% 格式化数据，换行
format1([{K, V} | T], L) ->
	format1(T, [$,, $}, z_lib:string(V), $\s, $,, z_lib:string(K), ${, $\t, $\n | L]);
format1([H | T], L) ->
	format1(T, [$,, z_lib:string(H), $\t, $\n | L]);
format1(_, [_| L]) ->
	lists:reverse(L).

% 格式化数据，不换行
format2([{K, V} | T], L) ->
	format2(T, [$,, $}, z_lib:string(V), $\s, $,, z_lib:string(K), ${ | L]);
format2([H | T], L) ->
	format2(T, [$,, z_lib:string(H) | L]);
format2(_, [_| L]) ->
	lists:reverse(L).

% 创建指定目录下的文件
create_file(Dir, File, Limit, Time) ->
	delete_file(Dir, File, Limit),
	F = get_file_name(Dir, File, Time),
	case file:open(F, [append, raw]) of
		{ok, IoDevice} ->
			{F, IoDevice};
		{error, Reason} ->
			erlang:error({"create_file fail", Reason, {file, F}})
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
