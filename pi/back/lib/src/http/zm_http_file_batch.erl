%%@doc http 批量加载文件模块
%%```
%%'''
%%@end



-module(zm_http_file_batch).

-description("http file").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([read/5, decode/1]).

%%%=======================DEFINE=======================
-define(FILE_NOT_FOUND, 404).
-define(METHOD_NOT_ALLOWED, 405).
-define(SERVER_INTERNAL_ERROR, 500).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 客户端从服务器端读取指定项目路径内资源目录的文件内容
%% @spec read(Args, Con, Info, Headers, Body) -> return()
%% where
%%      return() = {ok, Info, Headers, Body}
%%@end
%% -----------------------------------------------------------------
read(Args, _Con, Info, Headers, Body) ->
	Root = z_lib:get_value(Args, root, ""),
	[throw({?SERVER_INTERNAL_ERROR, "undefined root path"}) || Root =:= ""],
	Hash = z_lib:get_value(Body, "h", ""),
	Files = z_lib:get_value(Body, "f", ""),
	Dir = z_lib:get_value(Body, "d", ""),
	Size = erlang:list_to_integer(z_lib:get_value(Body, "s", "")),
	FileList = [filename:join(Root, F) || F <- decode(Files)],
	DirList = decode(Dir),
	DirFileList = decode_dir(DirList, Root, []),
	TotalFile = lists:append(DirFileList, FileList),
	{DownLoadSize, DownLoadHash, DownLoadData} = get_download_file_info(TotalFile, 0, 0, []),
	[begin
		error_logger:error_report([?MODULE, ?FUNCTION_NAME, Size, DownLoadSize, [begin {Size1, _, _} = get_file_info(File), {File, Size1} end || File <- TotalFile]]),
		throw({?SERVER_INTERNAL_ERROR, "invalid size"}) 
	 end || DownLoadSize =/= Size],
	% [throw({?SERVER_INTERNAL_ERROR, "hash err"}) || DownLoadHash =/= Hash],
	RespHeaders1 = zm_http_file:max_age(Info, Args, zm_http:resp_headers(Info, Headers, none)),
	RespHeaders2 = zm_http:set_header(RespHeaders1, "Access-Control-Allow-Origin", "*"),
	{ok, Info, RespHeaders2, DownLoadData}.


%%%===================LOCAL FUNCTIONS==================
%% 获取 整个文件列表的 大小 hash 和具体数据
get_download_file_info([], StoreSize, StoreHash, StoreData) ->
	{StoreSize, StoreHash, lists:reverse(StoreData)};
get_download_file_info([OneFile | H], StoreSize, StoreHash, StoreData) ->
	{Size, Bin, Crc} = get_file_info(OneFile),
	get_download_file_info(H, StoreSize + Size, hash(Crc, StoreHash), [Bin | StoreData]).

hash([], Hash) ->
	Hash;
hash([CrcH | CrcT], Hash) ->
	hash(CrcT, ((Hash bsl 5) - Hash + CrcH)).

% 上行消息结构： 后缀1(文件名1:文件名2)后缀2(文件名1:文件名2):目录1(js(J1:J2)css(c1:c2):)目录2()
% 3个分隔符 (): $作为转义, $转$$ (转$1 )转$2
decode(S) ->
	decode_suffix(S, [], [""], []).

% 解析文件后缀
decode_suffix([$$, $$ | T], L, Stack, S) ->
	decode_suffix(T, L, Stack, [$$ | S]);
decode_suffix([$$, $1 | T], L, Stack, S) ->
	decode_suffix(T, L, Stack, [$( | S]);
decode_suffix([$$, $2 | T], L, Stack, S) ->
	decode_suffix(T, L, Stack, [$) | S]);
decode_suffix([$( | T], L, Stack, []) ->
	decode_file(T, L, Stack, [], []);
decode_suffix([$( | T], L, Stack, S) ->
	decode_file(T, L, Stack, [$. | lists:reverse(S)], []);
decode_suffix([$: | T], L, Stack, _) ->
	decode_path(T, L, Stack, []);
decode_suffix([H | T], L, Stack, S) ->
	decode_suffix(T, L, Stack, [H | S]);
decode_suffix(_, L, _Stack, _S) ->
	lists:reverse(L).

% 解析文件基础名
decode_file([$$, $$ | T], L, Stack, Suffix, S) ->
	decode_file(T, L, Stack, Suffix, [$$ | S]);
decode_file([$$, $1 | T], L, Stack, Suffix, S) ->
	decode_file(T, L, Stack, Suffix, [$( | S]);
decode_file([$$, $2 | T], L, Stack, Suffix, S) ->
	decode_file(T, L, Stack, Suffix, [$) | S]);
decode_file([$: | T], L, [H | _ ] = Stack, Suffix, S) ->
	decode_file(T, [lists:reverse(H, lists:reverse(S, Suffix)) | L], Stack, Suffix, []);
decode_file([$) | T], L, [H | _ ] = Stack, Suffix, S) ->
	decode_suffix(T, [lists:reverse(H, lists:reverse(S, Suffix)) | L], Stack, []);
decode_file([H | T], L, Stack, Suffix, S) ->
	decode_file(T, L, Stack, Suffix, [H | S]).

% 解析目录名
decode_path([$$, $$ | T], L, Stack, S) ->
	decode_path(T, L, Stack, [$$ | S]);
decode_path([$$, $1 | T], L, Stack, S) ->
	decode_path(T, L, Stack, [$( | S]);
decode_path([$$, $2 | T], L, Stack, S) ->
	decode_path(T, L, Stack, [$) | S]);
decode_path([$( | T], L, [H | _] = Stack, S) ->
	decode_suffix(T, L, [[$/ | S ++ H] | Stack], []);
decode_path([$) | T], L, [_ | Stack], _) ->
	decode_path(T, L, Stack, []);
decode_path([H | T], L, Stack, S) ->
	decode_path(T, L, Stack, [H | S]);
decode_path(_, L, _Stack, _S) ->
	lists:reverse(L).

% 获取目录为指定dir的指定后缀的文件，没有后缀表示获取全部文件
decode_dir([Dir | T], Root, L) ->
	FL = case filename:extension(Dir) of
		[] ->
			[Name || {Name, {_, Size, _, _, _}, _} <- zm_res_loader:list_dir(filename:join(Root, Dir ++ "/")), Size > 0 ];
		Suf ->
			Dir1 = lists:sublist(Dir, length(Dir) - length(Suf)),
			[Name || {Name, {_, Size, _, _, _}, _} <- zm_res_loader:list_dir(filename:join(Root, Dir1 ++ "/")), Size > 0, filename:extension(Name) =:= Suf ]
	end,
	decode_dir(T, Root, lists:append(L, lists:sort(FL)));
decode_dir([], _, L) ->
	L.

% 获得文件的信息
get_file_info(File) ->
	case zm_res_loader:read(File) of
		{ok, {{Size, regular, _MTime, _CTime}, Bin}} ->
			Crc = base58:encode_int(zm_res_loader:file_crc32(File)),
			{Size, Bin, Crc};
		{ok, _X} ->
			throw({?METHOD_NOT_ALLOWED, "File not allowed"});
		{error, enoent} ->
			throw({?FILE_NOT_FOUND, zm_resource:format_error(enoent) ++ "file: " ++ File});
		{error, Reason} ->
			throw({?SERVER_INTERNAL_ERROR, zm_resource:format_error(Reason)})
	end.

