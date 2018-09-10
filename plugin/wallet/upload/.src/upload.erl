%% @author luobin
%% @doc @todo Add description to upload.


-module(upload).

-define(DEFAULT_UPLOAD_FILE_KEY, "upload").

-define(RESULT_NAME, "result").
-define(RESULT_OK, 1).

%%数据表
-define(TABLE, table).

-define(GEN_NIL, '$nil').

%%------错误编号----
-define(ERROR_EXIST, -100).




%% ====================================================================
%% API functions
%% ====================================================================
-export([upload/5, upload_wx/5, get/5]).


%%普通模式文件上传
upload({Table}, _Session, _Attr, Info, Msg) ->
%% 	{UID, _, _} = z_lib:get_value(Attr, ?UID, ""),
%% 	{EID, _, _} = z_lib:get_value(Attr, ?EID, ""),
%% 	{_AuthL, _, _} = z_lib:get_value(Attr, ?SYS_AUTH, ""),
	{_FileName, _, Content} = z_lib:get_value(Msg, ?DEFAULT_UPLOAD_FILE_KEY, []),
	%%随机生成SID
	SID = zm_http:new_scid(z_lib:get_value(Info, ip, {0, 0, 0, 0}), z_lib:get_value(Info, port, 0), self()),
	%%获取数据表
	case zm_app_db:read(Table, SID) of
		?GEN_NIL ->
			ok = zm_app_db:write(Table, SID, {size(Content), Content, z_lib:now_millisecond()}, ?MODULE);
		_ ->
			erlang:throw({?ERROR_EXIST, {sid_exist, SID}})
	end,
	{ok, [], Info, [{?RESULT_NAME, 1}, {"sid", SID}]}.

%%微信模式文件上传
upload_wx({Url, Table, _WXCertMap, _WXTokenUrl, _AheadOfTime}, _Session, _Attr, Info, Msg) ->
%% 	{UID, _, _} = z_lib:get_value(Attr, ?UID, ""),
%% 	{EID, _, _} = z_lib:get_value(Attr, ?EID, ""),
%% 	{_AuthL, _, _} = z_lib:get_value(Attr, ?SYS_AUTH, ""),
	Pid = z_lib:get_value(Msg, "pid", []),
	MediaId = z_lib:get_value(Msg, "serverId", []),
	%%从平台获取微信访问地址
	WXDownloadUrl=zm_app_util:http_get(lists:concat([Url, MediaId, "&pid=", Pid])),
	%%从微信服务器获取数据
	{error, "", Bin} = zm_app_util:https_get(WXDownloadUrl),
	case zm_app_db:read(Table, MediaId) of
		?GEN_NIL ->
			zm_log:info(?MODULE, upload_wx, cims, "upload_wx ok", [{serverId, MediaId}, {size, size(Bin)}]),
			ok = zm_app_db:write(Table, MediaId, {size(Bin), Bin, z_lib:now_millisecond()}, ?MODULE);
		_ ->
			zm_log:info(?MODULE, upload_wx, cims, "upload_wx exist", [{serverId, MediaId}, {size, size(Bin)}])
	end,
	{ok, [], Info, [{?RESULT_NAME, 1}, {"sid", MediaId}]}.

%%获取文件
get({Table}, _Con, Info, Headers, Body) ->
%% 	{UID, _, _} = z_lib:get_value(Attr, ?UID, ""),
%% 	{EID, _, _} = z_lib:get_value(Attr, ?EID, ""),
%% 	{_AuthL, _, _} = z_lib:get_value(Attr, ?SYS_AUTH, ""),
	SID = z_lib:get_value(Body, "sid", ""),
	case zm_app_db:read(Table, SID) of
		{_Size, Content, _Time} ->
			io:format("getfile!!!!!!!!!!!BinSize:~p~n", [size(Content)]),
			NewHeaders=zm_http:set_header(zm_http:resp_headers(Info, Headers, ".jpg", none), "Access-Control-Allow-Origin", "*"),
			{ok, Info, NewHeaders, Content};
		_ ->
			erlang:throw({?ERROR_EXIST, {sid_exist, SID}})
	end.
	

%% ====================================================================
%% Internal functions
%% ====================================================================

