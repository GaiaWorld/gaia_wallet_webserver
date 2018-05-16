%%@doc 资源（只读文件）表
%%```
%%% 分布式系统中节点根据资源表查找资源节点和映射路径。
%%% 操作时，映射到实际的文件名上，采用rpc方式操作实际文件。
%%% 管理节点的资源表只包含管理节点插件目录下的资源，
%%% 从节点的资源表只包含从节点插件目录下的资源。
%%'''
%%@end


-module(zm_resource).

-description("resource (read only file) table").
-copyright({seasky,'www.seasky.cn'}).
-author({zmyth,leo,'zmythleo@gmail.com'}).


%%%=======================EXPORT=======================
-export([list/0, get/1, set/3, unset/3, delete/1, mapping/1]).
-export([format_error/1, list_dir/1, read_file/1, read_file_info/1, read/1, file/1]).

%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%%@doc 列出所有路径及映射
%% @spec list() -> [tuple()]
%%@end
%% -----------------------------------------------------------------
list() ->
	zm_config:get(?MODULE).

%% -----------------------------------------------------------------
%%@doc 获得指定路径的映射
%% @spec get(Path::list()) -> return()
%% where
%%      return() = tuple() | false
%%@end
%% -----------------------------------------------------------------
get(Path) ->
	zm_config:get(?MODULE, Path).

%% -----------------------------------------------------------------
%%@doc 设置指定路径的节点及映射
%%```
%%  路径及映射结尾都不应有“/”，映射可以是路径，也可以是MFA。
%%  M::atom()
%%  F::atom()
%%'''
%% @spec set(Path::list(), Node::atom(), Args::args()) -> return()
%% where
%%      args() = MapPath | {M,F,A}
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
set(Path, Node, MapPath) when is_list(Path), is_list(MapPath) ->
	zm_config:set(?MODULE, {Path, Node, MapPath});
set(Path, Node, {M, F, A}) when is_list(Path), is_atom(M), is_atom(F) ->
	zm_config:set(?MODULE, {Path, Node, {M, F, A}}).

%% -----------------------------------------------------------------
%%@doc 取消设置指定路径的节点及映射
%% @spec unset(Path::list(), Node::atom(), MapPath::list()) -> return()
%% where
%%      return() = ok | {ok, Old}
%%@end
%% -----------------------------------------------------------------
unset(Path, Node, MapPath) ->
	zm_config:unset(?MODULE, {Path, Node, MapPath}).

%% -----------------------------------------------------------------
%%@doc 删除指定路径及映射
%% @spec delete(Path::list()) -> return()
%% where
%%      return() = none | {ok, Old}
%%@end
%% -----------------------------------------------------------------
delete(Path) ->
	zm_config:delete(?MODULE, Path).

%% -----------------------------------------------------------------
%%@doc 文件名称映射
%% @spec mapping(File::list()) -> return()
%% where
%%      return() = {atom(), list()} | false
%%@end
%% -----------------------------------------------------------------
mapping(File) ->
	file_mapping(z_lib:safe_path(File), "").

%% -----------------------------------------------------------------
%%@doc
%%```
%%Given the error reason returned by any function in this module,
%%returns a descriptive string of the error
%%'''
%% @spec format_error(Reason) -> list()
%%@end
%% -----------------------------------------------------------------
format_error(Reason) when is_list(Reason) ->
	Reason;
format_error(Reason) ->
	file:format_error(Reason).

%% -----------------------------------------------------------------
%%@doc
%% @spec list_dir(Dir::list()) -> return()
%% where
%%      return() = {ok, Filenames} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
list_dir(Dir) ->
	try
		case file_mapping(z_lib:safe_path(Dir), "") of
			false ->
				{error, {mapping_not_found, Dir}};
			{N, F} ->
				rpc(N, file, list_dir, [F])
		end
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc
%% @spec read_file(File::list()) -> return()
%% where
%%      return() = {ok, Binary} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read_file(File) ->
	try
		case file_mapping(z_lib:safe_path(File), "") of
			false ->
				{error, {mapping_not_found, File}};
			{N, F} ->
				rpc(N, file, read_file, [F])
		end
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc
%% @spec read_file_info(File::list()) -> return()
%% where
%%      return() = {ok, FileInfo} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read_file_info(File) ->
	try
		case file_mapping(z_lib:safe_path(File), "") of
			false ->
				{error, {mapping_not_found, File}};
			{N, F} ->
				rpc(N, file, read_file_info, [F])
		end
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc
%% @spec read(File::list()) -> return()
%% where
%%      return() = {ok, {Info, Data}} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
read(File) ->
	try
		case file_mapping(z_lib:safe_path(File), "") of
			false ->
				{error, {mapping_not_found, File}};
			{N, F} ->
				rpc(N, ?MODULE, file, [F])
		end
	catch
		_:Reason ->
			{error, Reason}
	end.

%% -----------------------------------------------------------------
%%@doc
%% @spec file(File::list()) -> return()
%% where
%%      return() = {ok, {Info, Data}} | {error, Reason}
%%@end
%% -----------------------------------------------------------------
file(File) ->
	case z_lib:get_file_info(File) of
		{_File, Size, regular, MTime, CTime} ->
			case file:read_file(File) of
				{ok, Bin} ->
					{ok, {{Size, regular, MTime, CTime}, Bin}};
				E ->
					E
			end;
		{_File, Size, directory, MTime, CTime} ->
			case file:list_dir(File) of
				{ok, L} ->
					{ok, {{Size, directory, MTime, CTime}, L}};
				E ->
					E
			end;
		E ->
			E
	end.

%%%===================LOCAL FUNCTIONS==================
% 文件名称映射
file_mapping(Path, File) ->
	case zm_config:get(?MODULE, Path) of
		{_, N, {M, F, A}} ->
			{N, M:F(A, File)};
		{_, N, MapPath} ->
			{N, MapPath ++ File};
		none ->
			case filename:dirname(Path) of
				"." ->
					false;
				"/" ->
					false;
				Dir ->
					Name = lists:nthtail(length(Dir), Path),
					file_mapping(Dir, Name ++ File)
			end
	end.

% 远程调用
rpc(N, M, F, A) ->
	case rpc:call(N, M, F, A) of
		{badrpc, _Reason} = E ->
			{error, E};
		R ->
			R
	end.
