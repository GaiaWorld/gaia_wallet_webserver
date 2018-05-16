%%@doc http 文件模块
%%```
%%% 参数Args，可设置根路径root，隐藏文件的后缀列表hidden_file，目录缺省文件列表default_file，最大缓冲时间max_age，需要压缩的媒体类型zip_mime
%%'''
%%@end



-module(zm_http_file).

-description("http file").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([max_age/3, etag/4, read/5, read_data/3, read_data_list/3]).

%%%=======================DEFINE=======================
-define(DEFAULT_FILE, ["index.htm", "index.html"]).
-define(ZIP_MIME, ["text", "application", "message", "java"]).
-define(SMALL_FILE_SIZE, 512*1024).
-define(BIG_FILE_BLOCK_SIZE, 131072).
-define(FILE_BLOCK_SIZE, 65536).

-define(BAD_REQUEST, 400).
-define(FILE_NOT_FOUND, 404).
-define(METHOD_NOT_ALLOWED, 405).
-define(SERVER_INTERNAL_ERROR, 500).

-define(MAX_AGE, 86400 * 365).

-define(SESSION_COOKIE_ID, "ZMSCID").

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 设置max_age, HTTP 1.0版本设置Expires
%% @spec max_age(Info, MaxAge, RespHeaders) -> return()
%% where
%%      return() = RespHeaders
%%@end
%% -----------------------------------------------------------------
max_age(Info, Args, RespHeaders) ->
	MaxAge = z_lib:get_value(Args, max_age, ?MAX_AGE),
	case z_lib:get_value(Info, version, {1, 0}) of
		{1, 1} ->
			zm_http:set_header(RespHeaders, "Cache-Control",
				"public, max-age=" ++ integer_to_list(MaxAge));
		_ ->
			zm_http:set_header(RespHeaders, "Expires", zm_http:rfc1123_date(
				z_lib:second_to_localtime(zm_time:now_second() + MaxAge)))
	end.

%% -----------------------------------------------------------------
%%@doc 检查if-none-match, Etag，和if-modified-since, Last-Modified
%% @spec etag(ETag, Time, Headers, RespHeaders) -> return()
%% where
%%      return() = RespHeaders
%%@end
%% -----------------------------------------------------------------
etag(ETag, Time, Headers, RespHeaders) ->
	Modified = zm_http:rfc1123_date(Time),
	case z_lib:get_value(Headers, "if-none-match", undefined) of
		ETag ->
			case z_lib:get_value(Headers, "if-modified-since", undefined) of
				Modified ->
					cache;
				undefined ->
					cache;
				_ ->
					zm_http:set_header(zm_http:set_header(
						RespHeaders, "ETag", ETag), "Last-Modified", Modified)
			end;
		"W/" ++ ETag ->
			zm_http:set_header(zm_http:set_header(
				RespHeaders, "ETag", ETag), "Last-Modified", Modified);
		undefined ->
			case z_lib:get_value(Headers, "if-modified-since", undefined) of
				Modified ->
					cache;
				undefined ->
					zm_http:set_header(zm_http:set_header(
						RespHeaders, "ETag", ETag), "Last-Modified", Modified);
				_ ->
					zm_http:set_header(RespHeaders, "Last-Modified", Modified)
			end;
		_ ->
			zm_http:set_header(zm_http:set_header(
				RespHeaders, "ETag", ETag), "Last-Modified", Modified)
	end.

%% -----------------------------------------------------------------
%%@doc 客户端从服务器端读取指定项目路径内资源目录的文件内容
%% @spec read(Args, Con, Info, Headers, Body) -> return()
%% where
%%      return() = {ok, Info, Headers, Body}
%%@end
%% -----------------------------------------------------------------
read(Args, Con, Info, Headers, _Body) ->
	Root = z_lib:get_value(Args, root, ""),
	[throw({?SERVER_INTERNAL_ERROR, "undefined root path"}) || Root =:= ""],
	Hidden = z_lib:get_value(Args, hidden_file, ""),
	Path = z_lib:get_value(Info, path, ""),
	Suffix = filename:extension(Path),
	[throw({?FILE_NOT_FOUND, zm_resource:format_error(enoent)}) ||
		lists:member(Suffix, Hidden)],
	SidKey = z_lib:get_value(Args, sid, ?SESSION_COOKIE_ID),
	CurPath = z_lib:get_value(Info, cur_path, ""),
	File = z_lib:safe_path(lists:nthtail(length(CurPath), Path)),
	read_file(get_file(File, Root, Args), Args, Con, Info, Headers, SidKey).

%% -----------------------------------------------------------------
%%@doc 读取已打开文件的数据
%% @spec read_data(Con,FInfo::tuple(),Location::integer()) -> ok
%%@end
%% -----------------------------------------------------------------
read_data(Con, {_File, Size, _, Bin} = FInfo, Location)
	when Size > Location + ?BIG_FILE_BLOCK_SIZE ->
	<<_:Location/binary, Data:?FILE_BLOCK_SIZE/binary, _/binary>> = Bin,
	zm_http:send(Con, Data, {?MODULE, read_data,
		[Con, FInfo, Location + ?FILE_BLOCK_SIZE]});
read_data(Con, {_File, Size, _, Bin} = FInfo, Location) when Size > Location ->
	<<_:Location/binary, Data/binary>> = Bin,
	zm_http:send(Con, Data, {?MODULE, read_data,
		[Con, FInfo, Location + ?BIG_FILE_BLOCK_SIZE]});
read_data(Con, {_File, _Size, _, _Bin}, _Location) ->
	zm_http:send(Con, [], []).

%% -----------------------------------------------------------------
%%@doc 读取已打开文件的数据
%% @spec read_data_list(Con,FInfo::tuple(),LLList::lllist()) -> ok
%% where
%%      lllist() = [{Location,Length}]
%%@end
%% -----------------------------------------------------------------
read_data_list(Con, {_File, _Size, _, Bin} = FInfo, [{Location, Length} | T])
	when Length < ?BIG_FILE_BLOCK_SIZE ->
	<<_:Location/binary, Data:Length/binary, _/binary>> = Bin,
	zm_http:send(Con, Data, {?MODULE, read_data_list, [Con, FInfo, T]});
read_data_list(Con, {_File, _Size, _, Bin} = FInfo, [{Location, Length} | T]) ->
	<<_:Location/binary, Data:?FILE_BLOCK_SIZE/binary, _/binary>> = Bin,
	zm_http:send(Con, Data, {?MODULE, read_data_list,
		[Con, FInfo, [{Location + ?FILE_BLOCK_SIZE, Length - ?FILE_BLOCK_SIZE} | T]]});
read_data_list(Con, _FInfo, []) ->
	zm_http:send(Con, [], []).

%%%===================LOCAL FUNCTIONS==================
% 获得文件的信息
get_file([_ | _] = File, Root, Args) ->
	case lists:last(File) of
		$/ ->
			get_default_file_info(filename:join(Root, File), Args);
		_ ->
			get_file_info(filename:join(Root, File))
	end;
get_file("", Root, Args) ->
	get_default_file_info(Root, Args);
get_file(_, _Root, _Args) ->
	throw({?BAD_REQUEST, "invalid file"}).

% 获得目录缺省文件的信息
get_default_file_info(Dir, Args) ->
	list_default_file_info(Dir,
		z_lib:get_value(Args, default_file, ?DEFAULT_FILE)).

% 依次获得目录缺省文件的信息
list_default_file_info(Dir, [H | T]) ->
	File = filename:join(Dir, H),
	case zm_res_loader:read(File) of
		{ok, {{Size, regular, MTime, _CTime}, Bin}} ->
			{File, Size, MTime, Bin};
		_ ->
			list_default_file_info(Dir, T)
	end;
list_default_file_info(_Dir, _) ->
	throw({?FILE_NOT_FOUND, zm_resource:format_error(enoent)}).

% 获得文件的信息
get_file_info(File) ->
	case zm_res_loader:read(File) of
		{ok, {{Size, regular, MTime, _CTime}, Bin}} ->
			{File, Size, MTime, Bin};
		{ok, _X} ->
			throw({?METHOD_NOT_ALLOWED, "File not allowed"});
		{error, enoent} ->
			throw({?FILE_NOT_FOUND, zm_resource:format_error(enoent)});
		{error, Reason} ->
			throw({?SERVER_INTERNAL_ERROR, zm_resource:format_error(Reason)})
	end.

% 读取文件
read_file({File, Size, Time, _Bin} = FInfo, Args, Con, Info, Headers, SidKey) ->
	Ext = filename:extension(File),
	RespHeaders = max_age(Info, Args,
		zm_http:resp_headers(Info, Headers, Ext, SidKey)),
	ETag = case zm_res_loader:file_crc32(File) of
		Crc32 when is_integer(Crc32) ->
			"\"" ++ integer_to_list(Size) ++
				[$- | integer_to_list(Crc32) ++ "\""];
		none ->
			"\"" ++ integer_to_list(Size) ++
				[$_ | integer_to_list(z_lib:localtime_to_second(Time)) ++ "\""]
	end,
	case etag(ETag, Time, Headers, RespHeaders) of
		cache ->
			{ok, [{code, 304} | Info], RespHeaders, ""};
		RespHeaders1 ->
			RespHeaders2 = zm_http:set_header(RespHeaders1, "Access-Control-Allow-Origin", "*"),
			case z_lib:get_value(Info, method, 'GET') of
				'HEAD' ->
					{ok, Info, RespHeaders2, ""};
				_ ->
					read_file_body(FInfo, Args, Con, Info, Headers,
						reset_headers(Ext, Args, RespHeaders2))
			end
	end.

% 根据配置和响应参数修改响应头信息，处理是否压缩等问题
% {true | false, RespHeaders}  true 表示可压缩，false不可压缩
reset_headers(Ext, Args, RespHeaders) ->
	case z_lib:get_value(Args, zip_mime, ?ZIP_MIME) of
		[] ->
			{false, zm_http:del_header(RespHeaders, "Content-Encoding")};
		ZipMime ->
			MT = zm_http:mime_extension(Ext),
			case lists:member(MT, ZipMime) of
				false ->
					{Mime, _} = z_lib:split_first(MT, $/),
					case lists:member(Mime, ZipMime) of
						false ->
							{false, zm_http:del_header(RespHeaders, "Content-Encoding")};
						true ->
							{true, RespHeaders}
					end;
				true ->
					{true, RespHeaders}
			end
	end.

% 读取文件
read_file_body({_File, Size, _Time, Bin}, _Args, _Con, Info, Headers, {Type, RespHeaders})
	when Size =< ?SMALL_FILE_SIZE orelse Type =:= true ->
	case zm_http:get_range(Headers) of
		X when X =:= undefined orelse X =:= fail ->
			{ok, Info, RespHeaders, Bin};
		L ->
			case range_size(L, Size, 0, []) of
				{_N, Ranges} ->
					{ok, [{code, 206} | Info], RespHeaders,
						[[binary_part(Bin, Offset, Len)] || {Offset, Len} <- Ranges]};
				fail ->
					{ok, Info, RespHeaders, Bin}
			end
	end;

% 读取文件
read_file_body({_File, Size, _, _Bin} = FInfo, _Args, Con, Info, Headers, {_, RespHeaders}) ->
	case zm_http:get_range(Headers) of
		X when X =:= undefined orelse X =:= fail ->
			{ok, Info, zm_http:set_header(
				zm_http:del_header(RespHeaders,
				"Content-Encoding"), "Content-Length", Size),
				{?MODULE, read_data, [Con, FInfo, 0]}};
		L ->
			case range_size(L, Size, 0, []) of
				{N, Ranges} ->
					{ok, [{code, 206} | Info], zm_http:set_header(
						zm_http:del_header(RespHeaders,
						"Content-Encoding"), "Content-Length", N),
						{?MODULE, read_data_list, [Con, FInfo, Ranges]}};
				fail ->
					{ok, Info, zm_http:set_header(
						zm_http:del_header(RespHeaders,
						"Content-Encoding"), "Content-Length", Size),
						{?MODULE, read_data, [Con, FInfo, 0]}}
			end
	end.

% 根据大小将文件范围进行约束
range_size([Range | T], Size, N, L) ->
	case Range of
		{none, R} when R =< Size, R >= 0 ->
			range_size(T, Size, N + R, [{Size - R, R} | L]);
		{none, _OutOfRange} ->
			range_size(T, Size, N + Size, [{0, Size} | L]);
		{R, none} when R >= 0, R < Size ->
			range_size(T, Size, N + Size - R, [{R, Size - R} | L]);
		{_OutOfRange, none} ->
			fail;
		{Start, End} when 0 =< Start, Start =< End, End < Size ->
			Len = End - Start + 1,
			range_size(T, Size, N + Len, [{Start, Len} | L]);
		{_OutOfRange, _End} ->
			fail
	end;
range_size([], _Size, N, L) ->
	{N, lists:reverse(L)}.
