%%@doc 资源加载模块
%%```
%%% 将所有的资源文件的文件信息和CRC32都记录在zm_res_loader表中，该表为有序表，
%%% 根据文件的后缀名，提供本地缓冲和资源到数据的转换，
%%% 默认资源加载器的缓冲大小为64兆，30分钟
%%'''
%%@end



-module(zm_res_loader).

-description("resource loader").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([name/3, purge/3]).
-export([file_info/1, file_crc32/1, file_info_crc32/1, list_dir/1, read/1, load/3, template/3, json/3]).

%%%=======================DEFINE=======================
-define(SIZEOUT, 64).
-define(TIMEOUT, 1800).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 作为加载时将资源文件的文件信息和CRC32保存到本地表上
%%```
%%  File 文件名
%%'''
%% @spec name(File::list(), Info, Crc32) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
name(File, Info, Crc32) ->
	zm_config:set(?MODULE, [{File, Info, Crc32}], ordered_set).

%% -----------------------------------------------------------------
%%@doc 取消设置资源文件的文件信息和CRC32，并清理指定资源中的缓冲
%% @spec purge(File::list(), Info, Crc32) -> any()
%%@end
%% -----------------------------------------------------------------
purge(File, Info, Crc32) when is_list(File) ->
	zm_config:unset(?MODULE, {File, Info, Crc32}),
	case filename:extension(File) of
		"" ->
			zm_cacher:clear({?MODULE, ""}, File);
		Suffix ->
			zm_cacher:clear({?MODULE, Suffix}, File),
			zm_cacher:clear({?MODULE, ""}, File)
	end.

%% -----------------------------------------------------------------
%%@doc 获得文件信息
%% @spec file_info(File::list()) -> return()
%% where
%%      return() = Info | none
%%@end
%% -----------------------------------------------------------------
file_info(File) ->
	case zm_config:get(?MODULE, File) of
		{_, Info, _Crc32} ->
			Info;
		none ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 获得文件crc32
%% @spec file_crc32(File::list()) -> return()
%% where
%%      return() = Crc32 | none
%%@end
%% -----------------------------------------------------------------
file_crc32(File) ->
	case zm_config:get(?MODULE, File) of
		{_, _Info, Crc32} ->
			Crc32;
		none ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 获得文件信息和crc32
%% @spec file_info_crc32(File::list()) -> return()
%% where
%%      return() = {Info, Crc32} | none
%%@end
%% -----------------------------------------------------------------
file_info_crc32(File) ->
	case zm_config:get(?MODULE, File) of
		{_, Info, Crc32} ->
			{Info, Crc32};
		none ->
			none
	end.

%% -----------------------------------------------------------------
%%@doc 获得指定目录的所有子目录及文件
%% @spec list_dir(Dir::string()) -> return()
%% where
%%      return() = none | [tuple()]
%%@end
%% -----------------------------------------------------------------
list_dir(Dir) ->
	zm_config:range(?MODULE, Dir ++ "/", Dir ++ [$/, 16#ffffffff]).

%% -----------------------------------------------------------------
%%@doc 获得指定资源中的信息
%% @spec read(File::list()) -> return()
%% where
%%      return() = {ok, {Info, Data}} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read(File) when is_list(File) ->
	case filename:extension(File) of
		"" ->
			load(File);
		Suffix ->
			load(File, Suffix)
	end.

%% -----------------------------------------------------------------
%% Function: load/3
%% Description: 远程资源调用进行加载，并进行处理
%% Returns: Template
%% -----------------------------------------------------------------
load(Handler, _Type, File) ->
	case zm_resource:read(File) of
		{ok, R} ->
			{ok, handle(Handler, File, R)};
		E ->
			E
	end.

%% -----------------------------------------------------------------
%%@doc 加载模板
%% @spec template(any(),any(),Bin) -> return()
%% where
%%      return() = Template
%%@end
%% -----------------------------------------------------------------
template(_Args, _File, Bin) ->
	zm_template:encode(Bin).

%% -----------------------------------------------------------------
%% @doc 加载Json
%% @spec json(any(),any(),Bin) -> return()
%% where
%%      return() = Json
%%@end
%% -----------------------------------------------------------------
json(_Args, _File, Bin) ->
	zm_json:decode(unicode:characters_to_list(Bin, utf8)).

%%%===================LOCAL FUNCTIONS==================
% 从缓冲中加载默认文件类型
load(File) ->
	Type = {?MODULE, ""},
	case zm_cacher:load(Type, File) of
		{ok, _Value} = R ->
			R;
		none ->
			zm_cacher:set(Type, ?SIZEOUT, ?TIMEOUT, true, {?MODULE, load, none}),
			case zm_cacher:load(Type, File) of
				{ok, _Value} = R ->
					R;
				E ->
					E
			end;
		E ->
			E
	end.

% 从缓冲中加载指定的文件类型
load(File, Suffix) ->
	Type = {?MODULE, Suffix},
	case zm_cacher:load(Type, File) of
		{ok, _Value} = R ->
			R;
		none ->
			load(File);
		E ->
			E
	end.

% 将二进制数据转化成可用数据
handle(none, _File, R) ->
	R;
handle({M, F, A} = MFA, File, {Info, Bin} = R) ->
	try
		{Info, M:F(A, File, Bin)}
	catch
		Error:Why ->
			{Info, {Error, {Why, {MFA, File, R}, erlang:get_stacktrace()}}}
	end.
