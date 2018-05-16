%%@doc http目录模块
%%```
%%% 返回目录下所有目录和文件的文件信息及CRC32。
%%'''
%%@end



-module(zm_http_dir).

-description("http dir").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([read/5]).

%%%=======================DEFINE=======================
-define(BAD_REQUEST, 400).
-define(SERVER_INTERNAL_ERROR, 500).

-define(SESSION_COOKIE_ID, "ZMSCID").

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 客户端向服务器端发送http请求，返回客户端指定的项目路径内资源目录的文件信息
%% @spec read(Args, any(), Info, Headers, any()) -> return()
%% where
%%      return() = {ok, Info, Headers, Body}
%%@end
%% -----------------------------------------------------------------
read(Args, _Con, Info, Headers, _Body) ->
	Root = z_lib:get_value(Args, root, ""),
	[throw({?SERVER_INTERNAL_ERROR, "undefined root path"}) || Root =:= ""],
	Path = z_lib:get_value(Info, path, ""),
	CurPath = z_lib:get_value(Info, cur_path, ""),
	Dir = z_lib:safe_path(lists:nthtail(length(CurPath), Path)),
	[throw({?BAD_REQUEST, "invalid dir"}) || Dir =:= false],
	SidKey = z_lib:get_value(Args, sid, ?SESSION_COOKIE_ID),
	case lists:reverse(Dir) of
		[$/ | T] ->
			read_dir(filename:join(Root, lists:reverse(T)), Args, Info, Headers, SidKey);
		_ ->
			read_dir(filename:join(Root, Dir), Args, Info, Headers, SidKey)
	end.

%%%===================LOCAL FUNCTIONS==================
% 读取目录
read_dir(Dir, Args, Info, Headers, SidKey) ->
	FileList = zm_res_loader:list_dir(Dir),
	RespHeaders = zm_http_file:max_age(Info, Args,
		zm_http:resp_headers(Info, Headers, z_lib:get_value(Args, mime_type, ".txt"), SidKey)),
	{Time, L} = max_time(length(Dir) + 1, FileList, {{1970, 1, 1}, {0, 0, 0}}, []),
	ETag = "\"" ++ integer_to_list(erlang:phash2(FileList, 16#ffffffff)) ++ "\"",
	case zm_http_file:etag(ETag, Time, Headers, RespHeaders) of
		cache ->
			{ok, [{code, 304} | Info], RespHeaders, ""};
		RespHeaders1 ->
			case z_lib:get_value(Info, method, 'GET') of
				'HEAD' ->
					{ok, Info, RespHeaders1, ""};
				_ ->
					{ok, Info, RespHeaders1, [{"filelist", L}]}
			end
	end.

% 获得最大的时间
max_time(Len, [{Name, {_, Size, Type, MTime, CTime}, Crc32} | T], Max, L) ->
	Type1 = case Type of
		regular -> 1;
		_ -> 0
	end,
	Max1 = if
		MTime > Max -> MTime;
		true -> Max
	end,
	Obj = sb_trees:insert("name", lists:nthtail(Len, Name),
		sb_trees:insert("type", Type1, sb_trees:insert("size", Size,
		sb_trees:insert("mtime", z_lib:localtime_to_second(MTime),
		sb_trees:insert("ctime", z_lib:localtime_to_second(CTime),
		sb_trees:insert("crc32", Crc32, sb_trees:empty())))))),
	max_time(Len, T, Max1, [Obj | L]);
max_time(_Len, [], Max, L) ->
	{Max, L}.
